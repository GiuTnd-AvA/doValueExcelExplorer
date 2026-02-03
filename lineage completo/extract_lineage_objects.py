"""Recursive lineage extraction with normalized outputs for graph analytics.

Given the Excel workbook that lists each report file and the tables it reads,
this script finds every database object (view/proc/function/trigger) referencing
those tables, recursively expands downstream dependencies, and writes three
flattened Excel datasets:
1. Report-to-object links (one row per report/table/object tuple)
2. Object-to-dependency edges (object -> table/object references)
3. Catalog of every SQL object that appears while traversing the lineage graph
"""

from __future__ import annotations

import argparse
import re
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pyodbc

DEFAULT_INPUT = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse engineering\Lineage completo\input_test_lineage.xlsx"
)
DEFAULT_REPORT_OUTPUT = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse engineering\Lineage completo\report_lineage_links.xlsx"
)
DEFAULT_DEPENDENCY_OUTPUT = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse engineering\Lineage completo\object_dependency_edges.xlsx"
)
DEFAULT_OBJECT_OUTPUT = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse engineering\Lineage completo\lineage_object_catalog.xlsx"
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

REPORT_COLUMNS = [
    "Path_File",
    "File_name",
    "Server",
    "Database",
    "Schema",
    "Table",
    "ObjectServer",
    "ObjectDatabase",
    "ObjectSchema",
    "ObjectName",
    "ObjectType",
    "Motivo",
    "ObjectDefinition",
    "DependencyTables",
    "DependencyObjects",
]

DEPENDENCY_COLUMNS = [
    "ObjectServer",
    "ObjectDatabase",
    "ObjectSchema",
    "ObjectName",
    "ObjectType",
    "DependencyType",
    "DependencySchema",
    "DependencyName",
]

OBJECT_CATALOG_COLUMNS = [
    "Server",
    "Database",
    "ObjectSchema",
    "ObjectName",
    "ObjectType",
    "ObjectDefinition",
]


@dataclass
class LineageObject:
    server: str
    database: str
    object_schema: str
    object_name: str
    object_type: str
    definition: str
    dep_tables: Tuple[Tuple[str, str], ...]
    dep_objects: Tuple[Tuple[str, str, str], ...]


def normalize(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "nat", "none"}:
        return ""
    return text


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


def list_server_databases(server: str, driver: str) -> List[str]:
    conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE=master;Trusted_Connection=yes;"
    try:
        with pyodbc.connect(conn_str, timeout=10) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sys.databases "
                "WHERE state_desc='ONLINE' AND database_id > 4"
            )
            return [row[0] for row in cursor.fetchall()]
    except pyodbc.Error as exc:
        print(f"[WARN] Impossibile leggere i database di {server}: {exc}")
        return []


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


def fetch_object_dependencies(
    conn: pyodbc.Connection,
    object_id: int,
) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str, str]]]:
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
    tables: List[Tuple[str, str]] = []
    objects: List[Tuple[str, str, str]] = []
    for ref_schema, ref_name, ref_type in cursor.fetchall():
        if not ref_name:
            continue
        schema_part = (ref_schema or "dbo").strip() or "dbo"
        if ref_type == "USER_TABLE":
            tables.append((schema_part, ref_name))
        else:
            objects.append((schema_part, ref_name, ref_type or "UNKNOWN"))
    cursor.close()
    return tables, objects


def build_lineage_object(
    server: str,
    database: str,
    object_schema: str,
    object_name: str,
    object_type: str,
    definition: Optional[str],
    conn: pyodbc.Connection,
    object_id: int,
) -> LineageObject:
    dep_tables, dep_objects = fetch_object_dependencies(conn, object_id)
    return LineageObject(
        server=server,
        database=database,
        object_schema=object_schema or "dbo",
        object_name=object_name,
        object_type=object_type,
        definition=(definition or ""),
        dep_tables=tuple(dep_tables),
        dep_objects=tuple(dep_objects),
    )


def fetch_referencing_objects(
    conn: pyodbc.Connection,
    server: str,
    database: str,
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
    results: List[LineageObject] = []
    for object_id, object_schema, object_name, object_type, definition in cursor.fetchall():
        results.append(
            build_lineage_object(
                server,
                database,
                object_schema,
                object_name,
                object_type,
                definition,
                conn,
                object_id,
            )
        )
    cursor.close()
    return results


def fetch_object_by_name(
    conn: pyodbc.Connection,
    server: str,
    database: str,
    schema: str,
    name: str,
) -> Optional[LineageObject]:
    lookup = (
        "SELECT o.object_id, o.type_desc, OBJECT_DEFINITION(o.object_id)"
        " FROM sys.objects o"
        " WHERE o.is_ms_shipped = 0"
        "   AND SCHEMA_NAME(o.schema_id) = ?"
        "   AND o.name = ?"
        "   AND o.type_desc IN ("
        + ",".join(f"'{t}'" for t in OBJECT_TYPES_OF_INTEREST)
        + ")"
    )
    cursor = conn.cursor()
    cursor.execute(lookup, schema or "dbo", name)
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None
    object_id, object_type, definition = row
    return build_lineage_object(
        server,
        database,
        schema or "dbo",
        name,
        object_type,
        definition,
        conn,
        object_id,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lineage ricorsivo normalizzato per costruire un grafo"
    )
    parser.add_argument("--excel", default=DEFAULT_INPUT, help="Percorso del file Excel di input")
    parser.add_argument("--sheet", default=0, help="Indice o nome del foglio da analizzare")
    parser.add_argument(
        "--report-output",
        default=DEFAULT_REPORT_OUTPUT,
        help="File Excel dei collegamenti Report→Oggetto",
    )
    parser.add_argument(
        "--dependency-output",
        default=DEFAULT_DEPENDENCY_OUTPUT,
        help="File Excel delle dipendenze Oggetto→(Tabella/Oggetto)",
    )
    parser.add_argument(
        "--object-output",
        default=DEFAULT_OBJECT_OUTPUT,
        help="File Excel con il catalogo degli oggetti rilevati",
    )
    parser.add_argument("--driver", default=DEFAULT_DRIVER, help="Driver ODBC da utilizzare")
    parser.add_argument(
        "--fallback-db",
        action="append",
        default=[],
        help="Database aggiuntivi da provare quando quello indicato nella riga non restituisce risultati",
    )
    parser.add_argument(
        "--scan-all-databases",
        action="store_true",
        help="Se impostato prova tutti i database online del server finche' non trova corrispondenze",
    )
    parser.add_argument(
        "--override-server",
        default="",
        help="Forza l'uso di un singolo server per tutte le righe (es. EPCP3)",
    )
    return parser.parse_args()


def make_object_key(server: str, database: str, schema: str, name: str) -> Tuple[str, str, str, str]:
    return (server.lower(), database.lower(), schema.lower(), name.lower())


def build_candidate_databases(
    server: str,
    primary: str,
    fallback: Iterable[str],
    scan_all: bool,
    driver: str,
    server_db_cache: Dict[str, List[str]],
) -> List[str]:
    ordered: List[str] = []
    seen: set[str] = set()

    def add_value(value: str) -> None:
        if not value:
            return
        key = value.lower()
        if key not in seen:
            ordered.append(value)
            seen.add(key)

    add_value(primary)
    for db in fallback:
        add_value(db.strip())

    if scan_all:
        cache_key = server.lower()
        if cache_key not in server_db_cache:
            server_db_cache[cache_key] = list_server_databases(server, driver)
        for db in server_db_cache[cache_key]:
            add_value(db)

    return ordered


def format_table_list(items: Iterable[Tuple[str, str]]) -> str:
    return " ; ".join(f"{schema}.{name}" for schema, name in items)


def format_object_list(items: Iterable[Tuple[str, str, str]]) -> str:
    return " ; ".join(f"{schema}.{name}" for schema, name, _ in items)


def main() -> None:
    args = parse_args()
    excel_path = Path(args.excel)
    if not excel_path.exists():
        raise FileNotFoundError(f"File non trovato: {excel_path}")

    df = pd.read_excel(excel_path, sheet_name=args.sheet)
    df.rename(columns=lambda c: str(c).strip(), inplace=True)

    pool = ConnectionPool(args.driver)
    object_catalog: Dict[Tuple[str, str, str, str], LineageObject] = {}
    pending: Deque[LineageObject] = deque()  # Queue drives breadth-first recursion
    report_records: List[Dict[str, str]] = []
    server_db_cache: Dict[str, List[str]] = {}

    def register_object(obj: LineageObject) -> LineageObject:
        key = make_object_key(obj.server, obj.database, obj.object_schema, obj.object_name)
        existing = object_catalog.get(key)
        if existing is None:
            object_catalog[key] = obj
            pending.append(obj)
            return obj
        return existing

    fallback_dbs = [normalize(db) for db in args.fallback_db if normalize(db)]

    try:
        for _, row in df.iterrows():
            server = normalize(args.override_server or row.get("Server"))
            database_hint = normalize(row.get("Database"))
            schema = normalize(row.get("Schema")) or "dbo"
            table = normalize(row.get("Table"))
            if not (server and table):
                continue

            candidate_dbs = build_candidate_databases(
                server,
                database_hint,
                fallback_dbs,
                args.scan_all_databases,
                args.driver,
                server_db_cache,
            )

            objects: List[LineageObject] = []
            matched_db = ""
            for candidate_db in candidate_dbs or [database_hint]:
                if not candidate_db:
                    continue
                conn = pool.get(server, candidate_db)
                if conn is None:
                    continue
                objects = fetch_referencing_objects(conn, server, candidate_db, schema, table)
                if objects:
                    matched_db = candidate_db
                    break

            if not objects:
                continue

            for obj in objects:
                stored = register_object(obj)
                report_records.append(
                    {
                        "Path_File": normalize(row.get("Path_File")),
                        "File_name": normalize(row.get("File_name")),
                        "Server": server,
                        "Database": matched_db or database_hint,
                        "Schema": schema,
                        "Table": table,
                        "ObjectServer": stored.server,
                        "ObjectDatabase": stored.database,
                        "ObjectSchema": stored.object_schema,
                        "ObjectName": stored.object_name,
                        "ObjectType": stored.object_type,
                        "Motivo": classify_motivo(stored.definition, schema, table),
                        "ObjectDefinition": stored.definition.strip(),
                        "DependencyTables": format_table_list(stored.dep_tables),
                        "DependencyObjects": format_object_list(stored.dep_objects),
                    }
                )

        processed: set[Tuple[str, str, str, str]] = set()
        while pending:
            current = pending.popleft()
            key = make_object_key(
                current.server,
                current.database,
                current.object_schema,
                current.object_name,
            )
            if key in processed:
                continue
            processed.add(key)
            conn = pool.get(current.server, current.database)
            if conn is None:
                continue
            for dep_schema, dep_name, dep_type in current.dep_objects:
                if dep_type not in OBJECT_TYPES_OF_INTEREST:
                    continue
                dep_key = make_object_key(current.server, current.database, dep_schema or "dbo", dep_name)
                if dep_key in object_catalog:
                    continue
                dep_obj = fetch_object_by_name(conn, current.server, current.database, dep_schema, dep_name)
                if dep_obj:
                    register_object(dep_obj)
    finally:
        pool.close()

    if not report_records:
        print("Nessun oggetto derivato individuato")
        return

    dep_rows: List[Dict[str, str]] = []
    for obj in object_catalog.values():
        for dep_schema, dep_name in obj.dep_tables:
            dep_rows.append(
                {
                    "ObjectServer": obj.server,
                    "ObjectDatabase": obj.database,
                    "ObjectSchema": obj.object_schema,
                    "ObjectName": obj.object_name,
                    "ObjectType": obj.object_type,
                    "DependencyType": "TABLE",
                    "DependencySchema": dep_schema,
                    "DependencyName": dep_name,
                }
            )
        for dep_schema, dep_name, dep_type in obj.dep_objects:
            dep_rows.append(
                {
                    "ObjectServer": obj.server,
                    "ObjectDatabase": obj.database,
                    "ObjectSchema": obj.object_schema,
                    "ObjectName": obj.object_name,
                    "ObjectType": obj.object_type,
                    "DependencyType": dep_type,
                    "DependencySchema": dep_schema,
                    "DependencyName": dep_name,
                }
            )

    report_df = pd.DataFrame(report_records, columns=REPORT_COLUMNS)
    report_path = Path(args.report_output)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_df.to_excel(report_path, index=False)
    print(f"Report→Oggetto: {len(report_records)} righe -> {report_path}")

    dep_df = pd.DataFrame(dep_rows, columns=DEPENDENCY_COLUMNS)
    dep_path = Path(args.dependency_output)
    dep_path.parent.mkdir(parents=True, exist_ok=True)
    dep_df.to_excel(dep_path, index=False)
    print(f"Dipendenze normalizzate: {len(dep_rows)} righe -> {dep_path}")

    obj_rows = [
        {
            "Server": obj.server,
            "Database": obj.database,
            "ObjectSchema": obj.object_schema,
            "ObjectName": obj.object_name,
            "ObjectType": obj.object_type,
            "ObjectDefinition": obj.definition.strip(),
        }
        for obj in object_catalog.values()
    ]
    obj_df = pd.DataFrame(obj_rows, columns=OBJECT_CATALOG_COLUMNS)
    obj_path = Path(args.object_output)
    obj_path.parent.mkdir(parents=True, exist_ok=True)
    obj_df.to_excel(obj_path, index=False)
    print(f"Catalogo oggetti: {len(obj_rows)} elementi -> {obj_path}")


if __name__ == "__main__":
    main()
