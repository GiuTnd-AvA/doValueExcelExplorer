"""
Gap Analysis: dato un Excel con colonne
Server, DB, Schema, Table, ObjectType, DDL
collega a EPCP3, legge metadata e stampa un Excel con:
Server, DB, Schema, Table, ObjectType, DDL, Column, DataType, PrimaryKey (Y/N), ForeignKey (Y/N), ExampleValue

Supporta tabelle e viste. Per altri oggetti (proc/trigger/synonym) riporta righe vuote e un errore.
"""

import os
from typing import Any, Dict, List, Optional, Tuple

try:
    import pyodbc
except Exception:
    pyodbc = None  # type: ignore

try:
    import pandas as pd
except Exception:
    pd = None  # type: ignore

# ---------------- Config ----------------
INPUT_EXCEL_PATH: Optional[str] = None  # es: r"C:\\path\\input.xlsx"
OUTPUT_EXCEL_PATH: Optional[str] = None  # es: r"C:\\path\\gap_output.xlsx"

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


class GapAnalyzer:
    def __init__(self, input_excel: str, output_excel: str, server: str = DEFAULT_SERVER):
        if pd is None:
            raise RuntimeError("pandas non installato. Installa 'pip install pandas openpyxl'.")
        if pyodbc is None:
            raise RuntimeError("pyodbc non installato. Installa 'pip install pyodbc'.")
        if not input_excel:
            raise ValueError("Percorso Excel di input non valorizzato.")
        self.input_excel = input_excel
        self.output_excel = output_excel or os.path.join(os.getcwd(), "gap_output.xlsx")
        self.server = server or DEFAULT_SERVER

    # --------------- Excel ---------------
    def _read_items(self) -> List[Tuple[str, str, str, str, str, str]]:
        """Ritorna lista di tuple (server, db, schema, table, object_type, ddl)."""
        df = pd.read_excel(self.input_excel)
        if df.empty:
            return []
        df.columns = [str(c).strip().lower() for c in df.columns]
        required = ["server", "db", "schema", "table", "object type", "ddl"]
        for r in required:
            if r not in df.columns:
                raise RuntimeError("Input deve avere colonne: Server, DB, Schema, Table, Object Type, DDL")
        items: List[Tuple[str, str, str, str, str, str]] = []
        for _, row in df.iterrows():
            server = str(row["server"]).strip() if pd.notna(row["server"]) else self.server
            db = str(row["db"]).strip() if pd.notna(row["db"]) else "master"
            schema = str(row["schema"]).strip() if pd.notna(row["schema"]) else "dbo"
            table = str(row["table"]).strip() if pd.notna(row["table"]) else ""
            objtype = str(row["object type"]).strip() if pd.notna(row["object type"]) else ""
            ddl = str(row["ddl"]).strip() if pd.notna(row["ddl"]) else ""
            if not table:
                continue
            # Forza server EPCP3
            server = self.server
            items.append((server, db, schema, table, objtype, ddl))
        return items

    # --------------- SQL ---------------
    def _candidate_drivers(self) -> List[str]:
        try:
            installed = [d for d in pyodbc.drivers() if "sql server" in d.lower()]
        except Exception:
            installed = []
        preferred = ODBC_DRIVERS
        ordered = [d for d in preferred if d in installed]
        ordered += [d for d in installed if d not in ordered]
        if not ordered:
            ordered = preferred
        return ordered

    def _build_conn_str(self, database: str) -> str:
        last_error: Optional[Exception] = None
        for drv in self._candidate_drivers():
            try:
                test_conn_str = f"DRIVER={{{drv}}};SERVER={self.server};DATABASE=master;"
                if TRUSTED_CONNECTION:
                    test_conn_str += "Trusted_Connection=yes;"
                else:
                    if not SQL_USERNAME or not SQL_PASSWORD:
                        raise RuntimeError("Credenziali mancanti.")
                    test_conn_str += f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
                if drv.lower().strip() != "sql server":
                    test_conn_str += ODBC_ENCRYPT_OPTS
                conn = pyodbc.connect(test_conn_str, timeout=CONNECTION_TEST_TIMEOUT)
                conn.close()
                # ok, costruisco finale
                final = f"DRIVER={{{drv}}};SERVER={self.server};DATABASE={database};"
                if TRUSTED_CONNECTION:
                    final += "Trusted_Connection=yes;"
                else:
                    final += f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
                if drv.lower().strip() != "sql server":
                    final += ODBC_ENCRYPT_OPTS
                return final
            except Exception as e:
                last_error = e
                continue
        raise RuntimeError(f"Nessun driver ODBC valido trovato. Ultimo errore: {last_error}")

    def _get_obj_id(self, conn, schema: str, name: str) -> Optional[int]:
        cur = conn.cursor()
        cur.execute("SELECT OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))", (schema, name))
        r = cur.fetchone()
        if r and r[0] is not None:
            return int(r[0])
        # Fallback: risolvi via sys.all_objects
        cur.execute(
            """
            SELECT o.object_id
            FROM sys.all_objects AS o
            JOIN sys.schemas AS s ON s.schema_id = o.schema_id
            WHERE s.name = ? AND o.name = ?
            """,
            (schema, name),
        )
        r2 = cur.fetchone()
        return int(r2[0]) if r2 and r2[0] is not None else None

    def _get_obj_type_code(self, conn, schema: str, name: str) -> Tuple[str, str]:
        """Ritorna (type_code, type_desc) da sys.all_objects per includere anche oggetti di sistema."""
        sql = (
            """
            SELECT o.type, o.type_desc
            FROM sys.all_objects AS o
            JOIN sys.schemas AS s ON s.schema_id = o.schema_id
            WHERE s.name = ? AND o.name = ?
            """
        )
        cur = conn.cursor()
        cur.execute(sql, (schema, name))
        r = cur.fetchone()
        if not r:
            return ("", "")
        return (str(r[0]), str(r[1]))

    def _get_columns_info(self, conn, obj_id: int) -> List[Dict[str, Any]]:
        sql = (
            """
            SELECT c.column_id, c.name AS column_name,
                   t.name AS type_name,
                   c.max_length, c.precision, c.scale,
                   c.is_nullable
            FROM sys.columns AS c
            JOIN sys.types   AS t ON c.user_type_id = t.user_type_id
            WHERE c.object_id = ?
            ORDER BY c.column_id
            """
        )
        cur = conn.cursor()
        cur.execute(sql, (obj_id,))
        rows = cur.fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append({
                "column_id": int(r[0]),
                "column": str(r[1]),
                "type_name": str(r[2]).upper(),
                "max_length": int(r[3]) if r[3] is not None else None,
                "precision": int(r[4]) if r[4] is not None else None,
                "scale": int(r[5]) if r[5] is not None else None,
                "is_nullable": bool(r[6]) if r[6] is not None else True,
            })
        return out

    def _get_columns_info_info_schema(self, conn, schema: str, name: str) -> List[Dict[str, Any]]:
        """Fallback: usa INFORMATION_SCHEMA.COLUMNS se object_id non è disponibile.
        Non fornisce column_id reale, lo sintetizza con ROW_NUMBER su ordine di COLUMN_NAME.
        """
        sql = (
            """
            SELECT COLUMN_NAME, DATA_TYPE,
                   CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
                   IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """
        )
        cur = conn.cursor()
        cur.execute(sql, (schema, name))
        rows = cur.fetchall()
        out: List[Dict[str, Any]] = []
        col_id = 1
        for r in rows:
            out.append({
                "column_id": col_id,
                "column": str(r[0]),
                "type_name": str(r[1]).upper(),
                "max_length": int(r[2]) if r[2] is not None else None,
                "precision": int(r[3]) if r[3] is not None else None,
                "scale": int(r[4]) if r[4] is not None else None,
                "is_nullable": True if (str(r[5]).upper() == "YES") else False,
            })
            col_id += 1
        return out

    def _format_datatype(self, col: Dict[str, Any]) -> str:
        t = col["type_name"]
        ml = col["max_length"]
        pr = col["precision"]
        sc = col["scale"]
        if t in ("CHAR","NCHAR","VARCHAR","NVARCHAR","BINARY","VARBINARY"):
            if ml is None:
                return t
            if t in ("NCHAR","NVARCHAR"):
                val = "MAX" if ml == -1 else str(max(1, ml//2))
            else:
                val = "MAX" if ml == -1 else str(max(1, ml))
            return f"{t}({val})"
        if t in ("DECIMAL","NUMERIC"):
            if pr is not None and sc is not None:
                return f"{t}({pr},{sc})"
            return t
        if t in ("DATETIME2","TIME","DATETIMEOFFSET"):
            if sc is not None:
                return f"{t}({sc})"
            return t
        return t

    def _pk_members(self, conn, obj_id: int) -> List[int]:
        sql = (
            """
            SELECT ic.column_id
            FROM sys.key_constraints AS kc
            JOIN sys.indexes AS i ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id
            JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            WHERE kc.type = 'PK' AND kc.parent_object_id = ?
            """
        )
        cur = conn.cursor()
        cur.execute(sql, (obj_id,))
        return [int(r[0]) for r in cur.fetchall()]

    def _fk_members(self, conn, obj_id: int) -> List[int]:
        sql = (
            """
            SELECT parent_column_id AS column_id
            FROM sys.foreign_key_columns
            WHERE parent_object_id = ?
            UNION
            SELECT referenced_column_id AS column_id
            FROM sys.foreign_key_columns
            WHERE referenced_object_id = ?
            """
        )
        cur = conn.cursor()
        cur.execute(sql, (obj_id, obj_id))
        return [int(r[0]) for r in cur.fetchall()]

    def _sample_row(self, conn, schema: str, name: str, obj_type_code: str) -> Optional[Dict[str, Any]]:
        # Per TVF (IF/TF) aggiunge le parentesi senza argomenti; per altri oggetti semplice select
        if obj_type_code in ("IF", "TF"):
            sql = f"SELECT TOP 1 * FROM [{schema}].[{name}]()"
        else:
            sql = f"SELECT TOP 1 * FROM [{schema}].[{name}]"
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            if not row:
                return None
            # map columns to values
            cols = [d[0] for d in cur.description]
            return {cols[i]: row[i] for i in range(len(cols))}
        except Exception:
            return None

    # --------------- Main ---------------
    def run(self) -> str:
        items = self._read_items()
        if not items:
            raise RuntimeError("Nessun elemento in input.")

        results: List[List[Any]] = []
        conns: Dict[str, Any] = {}
        try:
            for (server, db, schema, name, objtype, ddl) in items:
                # connessione per DB
                if db not in conns:
                    conns[db] = pyodbc.connect(self._build_conn_str(db), timeout=QUERY_TIMEOUT)
                conn = conns[db]

                # risolvi tipo (serve per TVF) e object_id
                type_code, _ = self._get_obj_type_code(conn, schema, name)
                obj_id = self._get_obj_id(conn, schema, name)

                if obj_id:
                    columns = self._get_columns_info(conn, obj_id)
                    pk_cols = set(self._pk_members(conn, obj_id))
                    fk_cols = set(self._fk_members(conn, obj_id))
                else:
                    columns = self._get_columns_info_info_schema(conn, schema, name)
                    pk_cols = set()
                    fk_cols = set()

                sample = self._sample_row(conn, schema, name, type_code)

                for col in columns:
                    colname = col["column"]
                    dtype = self._format_datatype(col)
                    isnull = "Y" if col.get("is_nullable", True) else "N"
                    pk = "Y" if col["column_id"] in pk_cols else "N"
                    fk = "Y" if col["column_id"] in fk_cols else "N"
                    example = None
                    if sample is not None:
                        example = sample.get(colname)
                    results.append([server, db, schema, name, objtype, ddl, colname, dtype, isnull, pk, fk, example])
        finally:
            for c in conns.values():
                try:
                    c.close()
                except Exception:
                    pass

        # write excel
        out_dir = os.path.dirname(self.output_excel)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        df = pd.DataFrame(results, columns=[
            "Server", "DB", "Schema", "Table", "ObjectType", "DDL",
            "Column", "DataType", "IsNullable", "PrimaryKey", "ForeignKey", "ExampleValue"
        ])

        try:
            from Report.Excel_Writer import write_dataframe_split_across_files
        except Exception:
            write_dataframe_split_across_files = None  # type: ignore

        if write_dataframe_split_across_files is not None:
            write_dataframe_split_across_files(df, self.output_excel, sheet_name="Gap")
        else:
            with pd.ExcelWriter(self.output_excel, engine="openpyxl", mode="w") as w:
                df.to_excel(w, index=False, sheet_name="Gap")
        return self.output_excel


if __name__ == "__main__":
    if not INPUT_EXCEL_PATH:
        raise SystemExit("Imposta INPUT_EXCEL_PATH a inizio file.")
    analyzer = GapAnalyzer(INPUT_EXCEL_PATH, OUTPUT_EXCEL_PATH or "")
    out = analyzer.run()
    print(out)
