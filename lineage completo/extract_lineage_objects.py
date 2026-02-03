"""Normalize lineage extraction: one row per SQL object feeding each table.

Given an Excel workbook that lists report files and the tables they read,
this script discovers every view/procedure/function/trigger that
references each table and outputs a flattened dataset where every row
represents a single object referencing a single table. This avoids the
previous ";" concatenations and makes the lineage easier to analyse or
load into BI tools.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pyodbc

DEFAULT_INPUT = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse engineering\Lineage completo\input_test_lineage.xlsx"
)
DEFAULT_OUTPUT = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse engineering\Lineage completo\input_test_lineage_objects.xlsx"
)
DEFAULT_DEPENDENCY_OUTPUT = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse engineering\Lineage completo\input_test_lineage_dependencies.xlsx"
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
WRITE_PATTERNS = (
    r"\binsert\s+into\s+{target}\b",
    r"\bupdate\s+{target}\b",
    r"\bdelete\s+from\s+{target}\b",
)
OUTPUT_COLUMNS = [
    "Path_File",
    "File_name",
    "Server",
    "Database",
    "Schema",
    "Table",
    "ObjectSchema",
    "ObjectName",
    "ObjectType",
    "Motivo",
    "ObjectDefinition",
    "DependencyTables",
    "DependencyObjects",
]

DEPENDENCY_COLUMNS = [
    "ObjectSchema",
    "ObjectName",
    "ObjectType",
    "DependencyType",
    "DependencyName",
]


@dataclass(frozen=True)
class LineageObject:
    object_schema: str
    object_name: str
    object_type: str
    definition: str
    dep_tables: Tuple[str, ...]
    dep_objects: Tuple[str, ...]


class ConnectionPool:
    def __init__(self, driver: str) -> None:
        self.driver = driver
        self._pool: Dict[Tuple[str, str], pyodbc.Connection] = {}

    def get(self, server: str, database: str) -> Optional[pyodbc.Connection]:
        server_clean = server.strip()
        database_clean = database.strip()
        if not server_clean or not database_clean:
            return None
        cache_key = (server_clean.lower(), database_clean.lower())
        if cache_key not in self._pool:
            conn_str = (
                f"DRIVER={{{self.driver}}};SERVER={server_clean};DATABASE={database_clean};"
                "Trusted_Connection=yes;"
            )
            try:
                self._pool[cache_key] = pyodbc.connect(conn_str, timeout=15)
            except pyodbc.Error as exc:
                print(f"[ERR] Connessione fallita {server_clean}/{database_clean}: {exc}")
                return None
        return self._pool[cache_key]

    def close(self) -> None:
        for conn in self._pool.values():
            try:
                conn.close()
            except pyodbc.Error:
                pass
        self._pool.clear()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Estrai oggetti derivati in formato normalizzato")
    parser.add_argument("--excel", default=DEFAULT_INPUT, help="Percorso del file Excel di input")
    parser.add_argument("--sheet", default=0, help="Indice o nome del foglio da elaborare")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Percorso del file Excel normalizzato")
    parser.add_argument(
        "--dependency-output",
        dest="dependency_output",
        default=DEFAULT_DEPENDENCY_OUTPUT,
        help="Percorso del file Excel con la lista normalizzata delle dipendenze",
    )
    parser.add_argument("--driver", default=DEFAULT_DRIVER, help="Driver ODBC da usare per pyodbc")
    return parser.parse_args()


def normalize(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "nat", "none"}:
        return ""
    return text


def classify_motivo(sql_definition: Optional[str], target_schema: str, target_table: str) -> str:
    if not sql_definition:
        return "Sconosciuto"
    lowered = sql_definition.lower()
    schema = re.escape((target_schema or "dbo").lower())
    table = re.escape(target_table.lower())
    candidates = {
        f"{schema}\\.{table}",
        f"[{(target_schema or 'dbo').lower()}].[{target_table.lower()}]",
        table,
    }
    for template in WRITE_PATTERNS:
        for candidate in candidates:
            if re.search(template.format(target=candidate), lowered):
                return "Scrittura"
    return "Lettura"


def fetch_object_dependencies(conn: pyodbc.Connection, object_id: int) -> Tuple[List[str], List[str]]:
    query = (
        "SELECT"
        "    ISNULL(d.referenced_schema_name, '') AS ref_schema,"
        "    d.referenced_entity_name AS ref_name,"
        "    obj.type_desc AS ref_type"
        " FROM sys.sql_expression_dependencies d"
        " LEFT JOIN sys.objects obj ON d.referenced_id = obj.object_id"
        " WHERE d.referencing_id = ?"
    )
    cursor = conn.cursor()
    cursor.execute(query, object_id)
    tables: List[str] = []
    objects: List[str] = []
    for ref_schema, ref_name, ref_type in cursor.fetchall():
        if not ref_name:
            continue
        schema_part = ref_schema or "dbo"
        qualified = f"{schema_part}.{ref_name}"
        if ref_type in {"USER_TABLE", "VIEW"}:
            tables.append(qualified)
        else:
            objects.append(qualified)
    cursor.close()
    return sorted(set(tables)), sorted(set(objects))


def fetch_referencing_objects(
    conn: pyodbc.Connection,
    target_schema: str,
    target_table: str,
) -> List[LineageObject]:
    query = (
        "SELECT"
        "    o.object_id,"
        "    SCHEMA_NAME(o.schema_id) AS object_schema,"
        "    o.name AS object_name,"
        "    o.type_desc,"
        "    OBJECT_DEFINITION(o.object_id) AS definition"
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
    cursor.execute(query, target_schema or "dbo", target_table)
    rows = cursor.fetchall()
    results: List[LineageObject] = []
    for object_id, object_schema, object_name, object_type, definition in rows:
        dep_tables, dep_objects = fetch_object_dependencies(conn, object_id)
        results.append(
            LineageObject(
                object_schema=(object_schema or "dbo"),
                object_name=object_name,
                object_type=object_type,
                definition=(definition or ""),
                dep_tables=tuple(dep_tables),
                dep_objects=tuple(dep_objects),
            )
        )
    cursor.close()
    return results


def main() -> None:
    args = parse_args()
    excel_path = Path(args.excel)
    if not excel_path.exists():
        raise FileNotFoundError(f"File non trovato: {excel_path}")

    base_df = pd.read_excel(excel_path, sheet_name=args.sheet)
    base_df.rename(columns=lambda c: str(c).strip(), inplace=True)

    records: List[Dict[str, str]] = []
    dependency_rows: List[Dict[str, str]] = []
    cache: Dict[Tuple[str, str, str, str], List[LineageObject]] = {}
    pool = ConnectionPool(args.driver)

    try:
        for _, row in base_df.iterrows():
            server = normalize(row.get("Server"))
            database = normalize(row.get("Database"))
            schema = normalize(row.get("Schema")) or "dbo"
            table = normalize(row.get("Table"))
            if not (server and database and table):
                continue
            cache_key = (server.lower(), database.lower(), schema.lower(), table.lower())
            if cache_key not in cache:
                conn = pool.get(server, database)
                if conn is None:
                    cache[cache_key] = []
                else:
                    cache[cache_key] = fetch_referencing_objects(conn, schema, table)
            objects = cache[cache_key]
            if not objects:
                continue
            for obj in objects:
                records.append(
                    {
                        "Path_File": normalize(row.get("Path_File")),
                        "File_name": normalize(row.get("File_name")),
                        "Server": server,
                        "Database": database,
                        "Schema": schema,
                        "Table": table,
                        "ObjectSchema": obj.object_schema,
                        "ObjectName": obj.object_name,
                        "ObjectType": obj.object_type,
                        "Motivo": classify_motivo(obj.definition, schema, table),
                        "ObjectDefinition": obj.definition.strip(),
                        "DependencyTables": " ; ".join(obj.dep_tables),
                        "DependencyObjects": " ; ".join(obj.dep_objects),
                    }
                )
                for dep in obj.dep_tables:
                    dependency_rows.append(
                        {
                            "ObjectSchema": obj.object_schema,
                            "ObjectName": obj.object_name,
                            "ObjectType": obj.object_type,
                            "DependencyType": "TABLE",
                            "DependencyName": dep,
                        }
                    )
                for dep in obj.dep_objects:
                    dependency_rows.append(
                        {
                            "ObjectSchema": obj.object_schema,
                            "ObjectName": obj.object_name,
                            "ObjectType": obj.object_type,
                            "DependencyType": "OBJECT",
                            "DependencyName": dep,
                        }
                    )
    finally:
        pool.close()

    if not records:
        print("Nessun oggetto derivato trovato; nessun file creato")
        return

    output_df = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_excel(output_path, index=False)
    print(f"Creato file normalizzato con {len(records)} righe: {output_path}")

    if dependency_rows:
        dep_df = pd.DataFrame(dependency_rows, columns=DEPENDENCY_COLUMNS)
        dep_path = Path(args.dependency_output)
        dep_path.parent.mkdir(parents=True, exist_ok=True)
        dep_df.to_excel(dep_path, index=False)
        print(f"Elenco dipendenze esportato con {len(dependency_rows)} righe: {dep_path}")
    else:
        print("Nessuna dipendenza da esportare")


if __name__ == "__main__":
    main()
