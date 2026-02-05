"""
Trova tabelle/viste/sinonimi su EPCP3 a partire da un Excel con una sola colonna `table`.
Per ogni voce, cerca su tutti i DB utente del server e scrive un Excel con:
Server, DB, Schema, Table, ObjectType, Error

Se la voce non viene trovata in nessun DB, scrive una riga con Error valorizzato.
Supporta anche input "schema.table" nella colonna `table`.

Limitazioni note: in SQL Server, senza permesso di accesso (`CONNECT`) e/o `VIEW DEFINITION` su un database,
non è possibile leggere i metadati degli oggetti (tabelle/viste/etc.) contenuti in quel database.
Questo script rileva i database ai quali l'utente non ha accesso e li segnala nel risultato,
ma non può verificare l'esistenza degli oggetti al loro interno senza i permessi adeguati.
"""

import os
from typing import Dict, List, Optional, Set, Tuple

try:
    import pyodbc
except Exception:
    pyodbc = None  # type: ignore

try:
    import pandas as pd
except Exception:
    pd = None  # type: ignore

# ---------------- Config ----------------
INPUT_EXCEL_PATH: Optional[str] = None  # es: r"C:\\path\\nomi_tabelle.xlsx"
OUTPUT_EXCEL_PATH: Optional[str] = None  # es: r"C:\\path\\risultati.xlsx"

DEFAULT_SERVER: str = "EPCP3"
TRUSTED_CONNECTION: bool = True
SQL_USERNAME: Optional[str] = None
SQL_PASSWORD: Optional[str] = None

ODBC_DRIVERS: List[str] = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "SQL Server",
    "SQL Server Native Client 11.0",
    "ODBC Driver 13 for SQL Server",
    "ODBC Driver 11 for SQL Server",
]
ODBC_ENCRYPT_OPTS: str = "Encrypt=no;TrustServerCertificate=yes;"
CONNECTION_TEST_TIMEOUT: int = 3
QUERY_TIMEOUT: int = 60

# Cosa includere nella ricerca
INCLUDE_VIEWS: bool = True
INCLUDE_SYNONYMS: bool = True

# Se true, aggiunge l'elenco dei DB non accessibili al messaggio di errore
REPORT_NO_ACCESS_DBS: bool = True


class ServerObjectFinder:
    def __init__(self, input_excel: str, output_excel: str, server: str = DEFAULT_SERVER):
        if pd is None:
            raise RuntimeError("pandas non installato. Installa 'pip install pandas openpyxl'.")
        if pyodbc is None:
            raise RuntimeError("pyodbc non installato. Installa 'pip install pyodbc'.")
        if not input_excel:
            raise ValueError("Percorso Excel di input non valorizzato.")
        self.input_excel = input_excel
        self.output_excel = output_excel or os.path.join(os.getcwd(), "trovati.xlsx")
        self.server = server or DEFAULT_SERVER

    def _read_targets(self) -> List[Tuple[Optional[str], str]]:
        """Ritorna lista di (schema_optional, table_name)."""
        df = pd.read_excel(self.input_excel)
        if df.empty:
            return []
        df.columns = [str(c).strip().lower() for c in df.columns]
        if "table" not in df.columns:
            raise RuntimeError("L'Excel deve contenere la sola colonna 'table'.")
        targets: List[Tuple[Optional[str], str]] = []
        for _, row in df.iterrows():
            raw_table = str(row["table"]).strip() if pd.notna(row["table"]) else ""
            if not raw_table:
                continue
            schema: Optional[str] = None
            name = raw_table
            if "." in raw_table:
                parts = raw_table.split(".", 1)
                schema = parts[0].strip()
                name = parts[1].strip()
            targets.append((schema, name))
        return targets

    def _build_conn_str(self, database: Optional[str]) -> str:
        last_error: Optional[Exception] = None
        for drv in ODBC_DRIVERS:
            try:
                conn_str = f"DRIVER={{{drv}}};SERVER={self.server};"
                if database:
                    conn_str += f"DATABASE={database};"
                if TRUSTED_CONNECTION:
                    conn_str += "Trusted_Connection=yes;"
                else:
                    if not SQL_USERNAME or not SQL_PASSWORD:
                        raise RuntimeError("Imposta SQL_USERNAME e SQL_PASSWORD oppure usa Trusted_Connection.")
                    conn_str += f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
                if drv.lower().strip() != "sql server":
                    conn_str += ODBC_ENCRYPT_OPTS
                # Test
                tconn = pyodbc.connect(conn_str if database else conn_str + "DATABASE=master;", timeout=CONNECTION_TEST_TIMEOUT)
                tconn.close()
                return conn_str
            except Exception as e:
                last_error = e
                continue
        raise RuntimeError(f"Nessun driver ODBC valido trovato. Ultimo errore: {last_error}")

    def _list_user_databases(self) -> List[str]:
        conn = pyodbc.connect(self._build_conn_str(None), timeout=QUERY_TIMEOUT)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT name
                FROM sys.databases
                WHERE name NOT IN ('master','tempdb','model','msdb')
                  AND state = 0
                ORDER BY name;
                """
            )
            return [str(r[0]) for r in cur.fetchall()]
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _db_has_access(self, db: str) -> bool:
        """Ritorna True se l'utente corrente ha accesso al DB (HAS_DBACCESS=1)."""
        conn = pyodbc.connect(self._build_conn_str(None), timeout=QUERY_TIMEOUT)
        try:
            cur = conn.cursor()
            cur.execute("SELECT HAS_DBACCESS(?)", (db,))
            row = cur.fetchone()
            if not row:
                return False
            try:
                return int(row[0]) == 1
            except Exception:
                return False
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _fetch_objects_in_db(self, db: str) -> Set[Tuple[str, str, str]]:
        """Ritorna set (schema, name, object_type_label)."""
        conn = pyodbc.connect(self._build_conn_str(db), timeout=QUERY_TIMEOUT)
        try:
            cur = conn.cursor()
            parts: List[str] = [
                """
                SELECT s.name AS schema_name, t.name AS object_name, 'USER_TABLE' AS object_type
                FROM sys.tables AS t
                JOIN sys.schemas AS s ON s.schema_id = t.schema_id
                """
            ]
            if INCLUDE_VIEWS:
                parts.append(
                    """
                    SELECT s.name AS schema_name, v.name AS object_name, 'VIEW' AS object_type
                    FROM sys.views AS v
                    JOIN sys.schemas AS s ON s.schema_id = v.schema_id
                    """
                )
            if INCLUDE_SYNONYMS:
                parts.append(
                    """
                    SELECT s.name AS schema_name, sy.name AS object_name, 'SYNONYM' AS object_type
                    FROM sys.synonyms AS sy
                    JOIN sys.schemas AS s ON s.schema_id = sy.schema_id
                    """
                )
            sql = "\nUNION ALL\n".join(parts)
            cur.execute(sql)
            return {(str(r[0]), str(r[1]), str(r[2])) for r in cur.fetchall()}
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def run(self) -> str:
        targets = self._read_targets()
        if not targets:
            raise RuntimeError("Nessun nome tabella fornito.")

        databases = self._list_user_databases()
        accessible_dbs: List[str] = []
        no_access_dbs: List[str] = []
        for db in databases:
            if self._db_has_access(db):
                accessible_dbs.append(db)
            else:
                no_access_dbs.append(db)

        results: List[List[str]] = []
        found_flags: List[bool] = [False] * len(targets)

        # Cerca solo sui DB accessibili
        for db in accessible_dbs:
            existing = self._fetch_objects_in_db(db)
            by_table: Dict[str, List[Tuple[str, str, str]]] = {}
            for sch, nm, typ in existing:
                by_table.setdefault(nm.lower(), []).append((sch, nm, typ))

            for idx, (schema_opt, tname) in enumerate(targets):
                candidates = by_table.get(tname.lower(), [])
                matches = [(s, n, t) for (s, n, t) in candidates if (schema_opt is None or s.lower() == schema_opt.lower())]
                for (s, n, t) in matches:
                    results.append([self.server, db, s, n, t, ""])  # nessun errore
                    found_flags[idx] = True

        # Righe non trovate: aggiunge nota sui DB non accessibili
        for idx, (schema_opt, tname) in enumerate(targets):
            if not found_flags[idx]:
                err = "Non trovato su nessun DB utente del server."
                if REPORT_NO_ACCESS_DBS and no_access_dbs:
                    err += f" DB non accessibili: {', '.join(no_access_dbs)}"
                results.append([self.server, "", schema_opt or "", tname, "", err])

        # Write Excel
        out_dir = os.path.dirname(self.output_excel)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        df_out = pd.DataFrame(results, columns=["Server", "DB", "Schema", "Table", "ObjectType", "Error"])
        df_out["Error"] = df_out["Error"].fillna("")

        try:
            from Report.Excel_Writer import write_dataframe_split_across_files
        except Exception:
            write_dataframe_split_across_files = None  # type: ignore

        if write_dataframe_split_across_files is not None:
            write_dataframe_split_across_files(df_out, self.output_excel, sheet_name="Trovati")
        else:
            with pd.ExcelWriter(self.output_excel, engine="openpyxl", mode="w") as w:
                df_out.to_excel(w, index=False, sheet_name="Trovati")
        return self.output_excel


if __name__ == "__main__":
    if not INPUT_EXCEL_PATH:
        raise SystemExit("Imposta INPUT_EXCEL_PATH a inizio file.")
    finder = ServerObjectFinder(INPUT_EXCEL_PATH, OUTPUT_EXCEL_PATH or "")
    print(finder.run())
