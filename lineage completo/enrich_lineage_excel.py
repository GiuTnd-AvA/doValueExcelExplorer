"""Enriches an Excel catalogue of reporting tables with the SQL objects
that populate them (stored procedures, functions, views, etc.).

The script inspects each row of the input Excel. For rows where the
`oggetti_totali7.*` columns are empty, it queries SQL Server (default
server EPCP3) to discover objects referencing the target table and fills
in:
- oggetti_totali7.Database / Schema / ObjectName
- oggetti_totali7.SQLDefinition
- oggetti_totali7.ObjectType
- oggetti_totali7.Motivo (READ vs WRITE)
- oggetti_totali7.Dipendenze_Tabella + count
- oggetti_totali7.Dipendenze_Oggetto + count

Multiple objects can reference the same table; in that case values are
joined with " ; " (definitions are separated by blank lines) so the
information remains compact while avoiding data loss.
"""

from __future__ import annotations

import argparse
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pyodbc

DEFAULT_INPUT = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse Engingeering\Lineage completo\Lineage_Report_Tabelle_Oggetti 5.xlsx"
)
DEFAULT_OUTPUT = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse Engingeering\Lineage completo\Lineage_Report_Tabelle_Oggetti 5_NEW.xlsx"
)
DEFAULT_SHEET = 0
DEFAULT_SERVER = "EPCP3"
DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"
DEFAULT_EXTRA_DATABASES: Tuple[str, ...] = ()
TARGET_COLUMNS = [
    "oggetti_totali7.Database",
    "oggetti_totali7.Schema",
    "oggetti_totali7.ObjectName",
    "oggetti_totali7.SQLDefinition",
    "oggetti_totali7.ObjectType",
    "oggetti_totali7.Motivo",
    "oggetti_totali7.Dipendenze_Tabella",
    "oggetti_totali7.Count_Dipendenza_Tabella",
    "oggetti_totali7.Dipendenze_Oggetto",
    "oggetti_totali7.Count_Dipendenze_Oggetto",
]
OBJECT_TYPES_OF_INTEREST = {
    "SQL_STORED_PROCEDURE",
    "SQL_SCALAR_FUNCTION",
    "SQL_TABLE_VALUED_FUNCTION",
    "SQL_INLINE_TABLE_VALUED_FUNCTION",
    "VIEW",
    "SQL_TRIGGER",
}
WRITE_PATTERNS = (
    r"\binsert\s+into\s+{target}\b",
    r"\bupdate\s+{target}\b",
    r"\bdelete\s+from\s+{target}\b",
)


@dataclass(frozen=True)
class LineageObject:
    object_schema: str
    object_name: str
    object_type: str
    definition: str
    dep_tables: Tuple[str, ...]
    dep_objects: Tuple[str, ...]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich lineage Excel with SQL metadata")
    parser.add_argument("--excel", default=DEFAULT_INPUT, help="Input Excel path")
    parser.add_argument("--sheet", default=DEFAULT_SHEET, help="Worksheet index or name")
    parser.add_argument("--server", default=DEFAULT_SERVER)
    parser.add_argument("--driver", default=DEFAULT_DRIVER)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--extra-db",
        dest="extra_db",
        action="append",
        default=list(DEFAULT_EXTRA_DATABASES),
        help="Additional database names to scan when the row database has no hits",
    )
    return parser.parse_args()


def normalize_str(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> None:
    for col in columns:
        if col not in df.columns:
            df[col] = ""


def get_connection(server: str, database: str, driver: str) -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str, timeout=15)


class ConnectionPool:
    def __init__(self, server: str, driver: str) -> None:
        self.server = server
        self.driver = driver
        self._pool: Dict[str, pyodbc.Connection] = {}

    def get(self, database: str) -> Optional[pyodbc.Connection]:
        database = database.strip()
        if not database:
            return None
        if database not in self._pool:
            try:
                self._pool[database] = get_connection(self.server, database, self.driver)
            except pyodbc.Error as exc:
                print(f"[ERR] Cannot connect to {self.server}/{database}: {exc}")
                return None
        return self._pool[database]

    def close(self) -> None:
        for conn in self._pool.values():
            try:
                conn.close()
            except pyodbc.Error:
                pass
        self._pool.clear()


def classify_motivo(sql_definition: Optional[str], target_schema: str, target_table: str) -> str:
    if not sql_definition:
        return "Sconosciuto"
    lowered = sql_definition.lower()
    schema = re.escape(target_schema.lower())
    table = re.escape(target_table.lower())
    candidates = {
        f"{schema}\\.{table}",
        f"[{target_schema.lower()}].[{target_table.lower()}]",
        target_table.lower(),
    }
    for raw_pattern in WRITE_PATTERNS:
        for candidate in candidates:
            if re.search(raw_pattern.format(target=candidate), lowered):
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
        "   AND o.type_desc IN ("
        + ",".join(f"'{t}'" for t in OBJECT_TYPES_OF_INTEREST)
        + " )"
        "   AND o.is_ms_shipped = 0"
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


def aggregate_metadata(
    database: str,
    target_schema: str,
    target_table: str,
    objects: List[LineageObject],
) -> Dict[str, str]:
    if not objects:
        return {}
    db_values = {database}
    schemas = {obj.object_schema for obj in objects}
    names = [f"{obj.object_schema}.{obj.object_name}" for obj in objects]
    types = {obj.object_type for obj in objects}
    definitions = [obj.definition.strip() for obj in objects if obj.definition]
    motives = {
        classify_motivo(obj.definition, target_schema, target_table)
        for obj in objects
    }
    dep_tables = sorted({dep for obj in objects for dep in obj.dep_tables})
    dep_objects = sorted({dep for obj in objects for dep in obj.dep_objects})

    return {
        "oggetti_totali7.Database": " ; ".join(sorted(db_values)),
        "oggetti_totali7.Schema": " ; ".join(sorted(schemas)),
        "oggetti_totali7.ObjectName": " ; ".join(names),
        "oggetti_totali7.SQLDefinition": "\n\n".join(definitions),
        "oggetti_totali7.ObjectType": " ; ".join(sorted(types)),
        "oggetti_totali7.Motivo": " ; ".join(sorted(motives)),
        "oggetti_totali7.Dipendenze_Tabella": " ; ".join(dep_tables),
        "oggetti_totali7.Count_Dipendenza_Tabella": str(len(dep_tables)),
        "oggetti_totali7.Dipendenze_Oggetto": " ; ".join(dep_objects),
        "oggetti_totali7.Count_Dipendenza_Oggetto": str(len(dep_objects)),
    }


def determine_candidate_databases(row: pd.Series, extra: Iterable[str]) -> List[str]:
    ordered: List[str] = []
    candidates = [normalize_str(row.get("Database")), *extra]
    for value in candidates:
        if value and value not in ordered:
            ordered.append(value)
    return ordered


def enrich_row(
    row: pd.Series,
    conn_pool: ConnectionPool,
    extra_databases: Iterable[str],
) -> Dict[str, str]:
    schema = normalize_str(row.get("Schema")) or "dbo"
    table = normalize_str(row.get("Table"))
    if not table:
        return {}

    target_columns_needed = {
        col for col in TARGET_COLUMNS if not normalize_str(row.get(col))
    }
    if not target_columns_needed:
        return {}

    candidate_dbs = determine_candidate_databases(row, extra_databases)
    for database in candidate_dbs:
        conn = conn_pool.get(database)
        if conn is None:
            continue
        objects = fetch_referencing_objects(conn, schema, table)
        if objects:
            return aggregate_metadata(database, schema, table, objects)
    return {}


def main() -> None:
    args = parse_args()
    input_path = Path(args.excel)
    if not input_path.exists():
        raise FileNotFoundError(f"Input Excel not found: {input_path}")

    print("[1/4] Loading Excel ...")
    df = pd.read_excel(input_path, sheet_name=args.sheet)
    ensure_columns(df, TARGET_COLUMNS)

    conn_pool = ConnectionPool(args.server, args.driver)
    enriched_rows = 0

    try:
        print("[2/4] Enriching rows ...")
        for idx, row in df.iterrows():
            new_values = enrich_row(row, conn_pool, args.extra_db)
            if not new_values:
                continue
            for col, value in new_values.items():
                if not normalize_str(df.at[idx, col]):
                    df.at[idx, col] = value
            enriched_rows += 1
    finally:
        conn_pool.close()

    print(f"[3/4] Rows enriched: {enriched_rows}")
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)
    print(f"[4/4] Saved enriched workbook to {output_path}")


if __name__ == "__main__":
    main()
