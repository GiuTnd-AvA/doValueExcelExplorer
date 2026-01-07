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

# -----------------------------------------------------------------------------
# Configurazione connessione
# -----------------------------------------------------------------------------
DEFAULT_SERVER: str = "EPCP3"
TRUSTED_CONNECTION: bool = True
SQL_USERNAME: Optional[str] = None  # usato se TRUSTED_CONNECTION=False
SQL_PASSWORD: Optional[str] = None  # usato se TRUSTED_CONNECTION=False

ODBC_DRIVERS: List[str] = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "SQL Server",
    # legacy/common names
    "SQL Server Native Client 11.0",
    "ODBC Driver 13 for SQL Server",
    "ODBC Driver 11 for SQL Server",
]

ODBC_ENCRYPT_OPTS: str = "Encrypt=no;TrustServerCertificate=yes;"
CONNECTION_TEST_TIMEOUT: int = 3
QUERY_TIMEOUT: int = 60


class TableExistenceChecker:
    """Legge un Excel con una lista di tabelle e verifica se esistono
    su SQL Server (server=EPCP3 per default). Per ogni tabella trovata,
    scrive in un Excel: Server, DB, Schema, Table.

    Formati input supportati (prima riga come intestazione):
    - Colonne separate: [schema] (opzionale), table (obbligatoria), [db] (opzionale)
    - Oppure una colonna unica 'table' che può contenere anche 'schema.table'

    Se "db" non è fornito, la ricerca avviene su tutti i DB utente del server.
    Se "schema" non è fornito, la ricerca avviene per nome tabella a prescindere dallo schema.
    """

    def __init__(self, input_excel: str, output_excel: str, server: str = DEFAULT_SERVER):
        if pd is None:
            raise RuntimeError("pandas non installato. Installa 'pip install pandas openpyxl'.")
        if pyodbc is None:
            raise RuntimeError("pyodbc non installato. Installa 'pip install pyodbc'.")
        if not input_excel:
            raise ValueError("Percorso Excel di input non valorizzato.")
        self.input_excel = input_excel
        self.output_excel = output_excel or os.path.join(os.getcwd(), "TabelleEsistenti.xlsx")
        self.server = server or DEFAULT_SERVER

    # ------------------------------ Utilità Excel ------------------------------
    def _read_targets(self) -> List[Tuple[Optional[str], Optional[str], str]]:
        """Ritorna lista di triple (db, schema, table).
        - db può essere None => cerca in tutti i DB utente
        - schema può essere None => cerca per nome tabella su tutti gli schemi
        - table è obbligatoria
        """
        df = pd.read_excel(self.input_excel)
        if df.empty:
            return []
        df.columns = [str(c).strip().lower() for c in df.columns]

        # Colonne rilevanti, flessibili
        db_col = "db" if "db" in df.columns else ("database" if "database" in df.columns else None)
        schema_col = "schema" if "schema" in df.columns else None
        table_col = "table" if "table" in df.columns else None

        targets: List[Tuple[Optional[str], Optional[str], str]] = []
        for _, row in df.iterrows():
            db = str(row[db_col]).strip() if db_col and pd.notna(row[db_col]) else None
            schema = str(row[schema_col]).strip() if schema_col and pd.notna(row[schema_col]) else None
            if table_col and pd.notna(row.get(table_col)):
                raw_table = str(row[table_col]).strip()
            else:
                # fallback: cerca prima colonna significativa
                non_na = [str(v).strip() for v in row.values if pd.notna(v)]
                raw_table = non_na[0] if non_na else ""
            if not raw_table:
                continue

            # Permetti formato "schema.table" nella colonna table
            if not schema and "." in raw_table:
                parts = raw_table.split(".", 1)
                schema = parts[0].strip()
                table = parts[1].strip()
            else:
                table = raw_table

            targets.append((db or None, schema or None, table))
        return targets

    # ------------------------------ Utilità SQL -------------------------------
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
                conn_str += ODBC_ENCRYPT_OPTS
                # Test rapido
                tconn = pyodbc.connect(conn_str, timeout=CONNECTION_TEST_TIMEOUT)
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
                  AND state = 0 -- ONLINE
                ORDER BY name;
                """
            )
            return [str(r[0]) for r in cur.fetchall()]
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _fetch_tables_in_db(self, db: str) -> Set[Tuple[str, str]]:
        """Ritorna set di (schema, table) esistenti nel DB."""
        conn = pyodbc.connect(self._build_conn_str(db), timeout=QUERY_TIMEOUT)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT s.name AS schema_name, t.name AS table_name
                FROM sys.tables AS t
                JOIN sys.schemas AS s ON s.schema_id = t.schema_id
                """
            )
            return {(str(r[0]), str(r[1])) for r in cur.fetchall()}
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ------------------------------ Run principale ----------------------------
    def run(self) -> str:
        targets = self._read_targets()
        if not targets:
            print("[CHECK] Nessuna tabella in input.")
            return self.output_excel

        # Prepara lista DB da ispezionare
        # Se nell'input ci sono DB specificati, usa quelli; altrimenti lista completa user DB
        dbs_in_input = sorted({db for (db, _schema, _table) in targets if db})
        if dbs_in_input:
            databases = dbs_in_input
        else:
            databases = self._list_user_databases()

        print(f"[CHECK] DB da verificare: {len(databases)}")

        # Normalizza input per confronto case-insensitive
        # Manteniamo comunque le tuple originali per eventuale debug
        norm_targets: List[Tuple[Optional[str], Optional[str], str]] = []
        for (db, schema, table) in targets:
            norm_targets.append(
                (db.lower() if db else None, schema.lower() if schema else None, table.lower())
            )

        results: List[List[str]] = []

        # Per efficienza, per ogni DB carichiamo tutte le tabelle e poi confrontiamo in memoria
        for db in databases:
            print(f"[CHECK] Scansione tabelle in DB: {db}")
            existing = self._fetch_tables_in_db(db)
            # Costruisci indici
            by_table: Dict[str, List[Tuple[str, str]]] = {}
            for sch, tbl in existing:
                key = tbl.lower()
                by_table.setdefault(key, []).append((sch, tbl))

            # Valuta target che chiedono proprio questo DB (o tutti i DB)
            for (tdb, tschema, ttable) in norm_targets:
                if tdb is not None and tdb != db.lower():
                    continue
                matches: List[Tuple[str, str]] = []
                if tschema:
                    # match preciso schema.table
                    candidates = by_table.get(ttable, [])
                    matches = [(s, t) for (s, t) in candidates if s.lower() == tschema]
                else:
                    # qualsiasi schema con quel table name
                    matches = by_table.get(ttable, [])

                for (sch, tbl) in matches:
                    results.append([self.server, db, sch, tbl])

        # Scrivi risultati
        out_dir = os.path.dirname(self.output_excel)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        df_out = pd.DataFrame(results, columns=["Server", "DB", "Schema", "Table"])
        with pd.ExcelWriter(self.output_excel, engine="openpyxl", mode="w") as writer:
            df_out.to_excel(writer, index=False, sheet_name="Tabelle")
        print(f"[CHECK] Output scritto in: {self.output_excel} (righe: {len(results)})")
        return self.output_excel


if __name__ == "__main__":
    # Esempio d'uso rapido: imposta i percorsi sotto
    INPUT_EXCEL = os.environ.get("TABLES_INPUT_XLSX", "")  # es: C:\path\ListaTabelle.xlsx
    OUTPUT_EXCEL = os.environ.get("TABLES_OUTPUT_XLSX", "")  # es: C:\path\TabelleEsistenti.xlsx
    if not INPUT_EXCEL:
        raise SystemExit("Imposta la variabile TABLES_INPUT_XLSX oppure modifica lo script.")
    checker = TableExistenceChecker(INPUT_EXCEL, OUTPUT_EXCEL)
    out = checker.run()
    print(out)
