"""Check tables without derived objects in the lineage Excel against SQL Server.

For each row whose `oggetti_totali7.*` fields are empty, the script
checks whether the referenced SQL table actually has dependent views or
procedural objects. Any mismatch (i.e., dependencies found even though
no derived objects are recorded) is reported.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import pyodbc

DEFAULT_INPUT = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse engineering\Lineage completo\Lineage_Report_Tabelle_Oggetti 5 (version 1).xlsx"
)
DEFAULT_OUTPUT = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse engineering\Lineage completo\Lineage_Report_Tabelle_Oggetti 5 (version 1)_tables_with_dependencies.xlsx"
)
DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"
OBJECT_TYPES_OF_INTEREST = {
    "VIEW",
    "SQL_STORED_PROCEDURE",
    "SQL_SCALAR_FUNCTION",
    "SQL_TABLE_VALUED_FUNCTION",
    "SQL_INLINE_TABLE_VALUED_FUNCTION",
    "SQL_TRIGGER",
}
OBJECT_INFO_COLUMNS = [
    "oggetti_totali7.Database",
    "oggetti_totali7.Schema",
    "oggetti_totali7.ObjectName",
    "oggetti_totali7.ObjectType",
    "oggetti_totali7.SQLDefinition",
]


@dataclass
class RowRef:
    excel_row: int
    path_file: str
    file_name: str


class ConnectionPool:
    def __init__(self, driver: str) -> None:
        self.driver = driver
        self._pool: Dict[Tuple[str, str], pyodbc.Connection] = {}

    def get(self, server: str, database: str) -> pyodbc.Connection | None:
        server_clean = server.strip()
        database_clean = database.strip()
        if not server_clean or not database_clean:
            return None
        key = (server_clean.lower(), database_clean.lower())
        if key not in self._pool:
            conn_str = (
                f"DRIVER={{{self.driver}}};SERVER={server_clean};DATABASE={database_clean};"
                "Trusted_Connection=yes;"
            )
            try:
                self._pool[key] = pyodbc.connect(conn_str, timeout=15)
            except pyodbc.Error as exc:
                print(f"[ERR] Connessione fallita {server_clean}/{database_clean}: {exc}")
                return None
        return self._pool[key]

    def close(self) -> None:
        for conn in self._pool.values():
            try:
                conn.close()
            except pyodbc.Error:
                pass
        self._pool.clear()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verifica se le tabelle senza oggetti derivati hanno dipendenze reali")
    parser.add_argument("--excel", default=DEFAULT_INPUT, help="Percorso del file Excel di lineage")
    parser.add_argument("--sheet", default=0, help="Indice o nome del foglio da analizzare")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Percorso del file di output con le incongruenze")
    parser.add_argument("--driver", default=DEFAULT_DRIVER, help="Driver ODBC da usare per pyodbc")
    parser.add_argument(
        "--default-server",
        dest="default_server",
        default="",
        help="Server da usare se la colonna Server è vuota",
    )
    return parser.parse_args()


def normalize(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "nat", "none"}:
        return ""
    return text


def has_object_metadata(row: pd.Series) -> bool:
    return any(normalize(row.get(col)) for col in OBJECT_INFO_COLUMNS)


def fetch_referencing_objects(conn: pyodbc.Connection, schema: str, table: str) -> List[Tuple[str, str, str]]:
    query = (
        "SELECT DISTINCT"
        "    SCHEMA_NAME(o.schema_id) AS object_schema,"
        "    o.name AS object_name,"
        "    o.type_desc"
        " FROM sys.sql_expression_dependencies d"
        " JOIN sys.objects o ON d.referencing_id = o.object_id"
        " WHERE ISNULL(d.referenced_schema_name, 'dbo') = ?"
        "   AND d.referenced_entity_name = ?"
        "   AND o.is_ms_shipped = 0"
        "   AND o.type_desc IN ("
        + ",".join(f"'{t}'" for t in OBJECT_TYPES_OF_INTEREST)
        + ")"
    )
    cursor = conn.cursor()
    cursor.execute(query, schema or "dbo", table)
    rows = [(row.object_schema or "dbo", row.object_name, row.type_desc) for row in cursor.fetchall()]
    cursor.close()
    return rows


def main() -> None:
    args = parse_args()
    excel_path = Path(args.excel)
    if not excel_path.exists():
        raise FileNotFoundError(f"File non trovato: {excel_path}")

    df = pd.read_excel(excel_path, sheet_name=args.sheet)
    df.rename(columns=lambda c: str(c).strip(), inplace=True)

    pending: Dict[Tuple[str, str, str, str], List[RowRef]] = defaultdict(list)
    for idx, row in df.iterrows():
        if has_object_metadata(row):
            continue
        schema = normalize(row.get("Schema")) or "dbo"
        table = normalize(row.get("Table"))
        database = normalize(row.get("Database"))
        server = normalize(row.get("Server")) or args.default_server
        if not table or not database or not server:
            continue
        key = (server, database, schema, table)
        pending[key].append(
            RowRef(
                excel_row=idx + 2,
                path_file=normalize(row.get("Path_File")),
                file_name=normalize(row.get("File_name")),
            )
        )

    print(f"Tabelle senza oggetti registrati: {len(pending)}")
    pool = ConnectionPool(args.driver)
    mismatches: List[Dict[str, str]] = []

    try:
        for (server, database, schema, table), refs in pending.items():
            conn = pool.get(server, database)
            if conn is None:
                continue
            objects = fetch_referencing_objects(conn, schema, table)
            if not objects:
                continue
            object_desc = "; ".join(f"{obj_schema}.{obj_name} ({obj_type})" for obj_schema, obj_name, obj_type in objects)
            row_numbers = "; ".join(str(ref.excel_row) for ref in refs)
            report_paths = "; ".join(sorted({ref.path_file for ref in refs if ref.path_file}))
            report_files = "; ".join(sorted({ref.file_name for ref in refs if ref.file_name}))
            mismatches.append(
                {
                    "Server": server,
                    "Database": database,
                    "Schema": schema,
                    "Table": table,
                    "ExcelRows": row_numbers,
                    "ReportPaths": report_paths,
                    "ReportFiles": report_files,
                    "FoundObjects": object_desc,
                    "FoundCount": str(len(objects)),
                }
            )
    finally:
        pool.close()

    print(f"Trovate incongruenze per {len(mismatches)} tabelle")
    if mismatches:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(mismatches).to_excel(output_path, index=False)
        print(f"Dettaglio salvato in {output_path}")
    else:
        print("Nessuna tabella con dipendenze mancanti")


if __name__ == "__main__":
    main()
