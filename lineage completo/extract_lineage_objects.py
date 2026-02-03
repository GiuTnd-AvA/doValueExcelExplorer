"""Extract SQL objects feeding tables listed in a lineage Excel workbook.

The input workbook only contains the direct report-to-table mapping
(Path_File, File_name, Server, Database, Schema, Table). This script
creates a new workbook that also includes the SQL objects (views,
procedures, functions, triggers) referencing each table, mirroring the
`oggetti_totali7.*` columns produced by the previous enrichment flow.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pyodbc

DEFAULT_INPUT = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse engineering\Lineage completo\input_test_lineage.xlsx"
)
DEFAULT_OUTPUT = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse engineering\Lineage completo\input_test_lineage_enriched.xlsx"
)
DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"
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
    "oggetti_totali7.Count_Dipendenza_Oggetto",
]
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
    parser = argparse.ArgumentParser(description="Estrai oggetti derivati per il file input_test_lineage")
    parser.add_argument("--excel", default=DEFAULT_INPUT, help="Percorso del file Excel di input")
    parser.add_argument("--sheet", default=0, help="Indice o nome del foglio da elaborare")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Percorso del file Excel arricchito")
    parser.add_argument("--driver", default=DEFAULT_DRIVER, help="Driver ODBC da usare per pyodbc")
    return parser.parse_args()


def normalize(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "nat", "none"}:
        return ""
    return text


def ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> None:
    for col in columns:
        if col not in df.columns:
            df[col] = ""


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


def main() -> None:
    args = parse_args()
    excel_path = Path(args.excel)
    if not excel_path.exists():
        raise FileNotFoundError(f"File non trovato: {excel_path}")

    df = pd.read_excel(excel_path, sheet_name=args.sheet)
    df.rename(columns=lambda c: str(c).strip(), inplace=True)
    ensure_columns(df, TARGET_COLUMNS)

    pool = ConnectionPool(args.driver)
    cache: Dict[Tuple[str, str, str, str], Dict[str, str]] = {}
    enriched = 0

    try:
        for idx, row in df.iterrows():
            server = normalize(row.get("Server"))
            database = normalize(row.get("Database"))
            schema = normalize(row.get("Schema")) or "dbo"
            table = normalize(row.get("Table"))
            if not (server and database and table):
                continue
            cache_key = (server.lower(), database.lower(), schema.lower(), table.lower())
            if cache_key in cache:
                metadata = cache[cache_key]
            else:
                conn = pool.get(server, database)
                if conn is None:
                    cache[cache_key] = {}
                    continue
                objects = fetch_referencing_objects(conn, schema, table)
                metadata = aggregate_metadata(database, schema, table, objects)
                cache[cache_key] = metadata
            if not metadata:
                continue
            for col, value in metadata.items():
                df.at[idx, col] = value
            enriched += 1
            if enriched % 100 == 0:
                print(f"Elaborate {enriched} righe con oggetti trovati")
    finally:
        pool.close()

    print(f"Totale righe con oggetti derivati: {enriched}")
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)
    print(f"File salvato in {output_path}")


if __name__ == "__main__":
    main()
