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
import time
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
DEFAULT_FAILURE_LOG = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse engineering\Lineage completo\lineage_failures_log.xlsx"
)
DEFAULT_SUMMARY_OUTPUT = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse engineering\Lineage completo\lineage_summary_metrics.xlsx"
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

INVALID_EXCEL_CHARS = {chr(i) for i in range(32)} - {"\t", "\n", "\r"}


def sanitize_for_excel(value: object) -> str:
    text = normalize(value)
    if not text:
        return ""
    return "".join(ch for ch in text if ch not in INVALID_EXCEL_CHARS)


REPORT_GUIDE = [
    "Ogni riga associa una tabella dell'Excel di input all'oggetto SQL trovato (vista/stored/funzione).",
    "ObjectExtractionLevel indica il livello di scoperta: 1 = oggetto direttamente collegato al report, livelli maggiori sono dipendenze successive.",
    "Motivo specifica se l'oggetto effettua lettura o scrittura sulla tabella sorgente (analisi del DDL).",
    "DependencyTables e DependencyObjects elencano le dipendenze immediate dell'oggetto per consultazioni rapide.",
]

DEPENDENCY_GUIDE = [
    "Rappresentazione normalizzata del grafo oggetto → tabella/oggetto.",
    "Ogni riga è un arco diretto; ObjectExtractionLevel è il livello dell'oggetto sorgente, DependencyExtractionLevel quello del nodo di destinazione se noto.",
    "Le schede L1, L2, ... filtrano automaticamente le dipendenze per livello dell'oggetto sorgente.",
]

CATALOG_GUIDE = [
    "Catalogo degli oggetti SQL incontrati durante l'espansione del lineage.",
    "ExtractionLevel permette di capire a quale distanza dal report si trova l'oggetto.",
    "ObjectDefinition contiene il DDL completo utile a verificare logiche e join.",
]

FAILURE_GUIDE = [
    "Elenco delle tabelle di input per cui non è stato possibile derivare il lineage.",
    "Reason indica il motivo del fallimento (input incompleto, permessi insufficienti, nessun oggetto SQL trovato).",
    "Utilizzare queste informazioni per correggere l'Excel o richiedere i privilegi necessari.",
]

SUMMARY_GUIDE = [
    "Workbook di sintesi con KPI, breakdown per tipologia di oggetto e peso dei database sulla reportistica.",
    "Usare il foglio KPIs per avere un colpo d'occhio, mentre gli altri fogli approfondiscono distribuzioni e pesi.",
    "Le schede DipendenzeTipo/DipendenzeServer e SintesiFallimenti evidenziano copertura e gap durante l'espansione.",
]


def write_guide_sheet(writer: pd.ExcelWriter, lines: List[str]) -> None:
    if not lines:
        return
    guide_df = pd.DataFrame({"Descrizione": lines})
    guide_df.to_excel(writer, sheet_name="Guida", index=False)


def export_single_sheet_with_guide(
    df: pd.DataFrame,
    path: Path,
    sheet_name: str,
    guide_lines: List[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path) as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        write_guide_sheet(writer, guide_lines)


REPORT_COLUMNS = [
    "Server",
    "Database",
    "Schema",
    "Table",
    "SourceObjectType",
    "ObjectServer",
    "ObjectDatabase",
    "ObjectSchema",
    "ObjectName",
    "ObjectType",
    "ObjectExtractionLevel",
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
    "ObjectExtractionLevel",
    "DependencyType",
    "Server estratto",
    "DependencyDatabase",
    "DependencySchema",
    "DependencyName",
    "DependencyExtractionLevel",
]

OBJECT_CATALOG_COLUMNS = [
    "Server",
    "Database",
    "ObjectSchema",
    "ObjectName",
    "ObjectType",
    "ExtractionLevel",
    "ObjectDefinition",
]

FAILURE_COLUMNS = [
    "Server",
    "Database",
    "Schema",
    "Table",
    "SourceObjectType",
    "Reason",
]


@dataclass
class LineageObject:
    server: str
    database: str
    object_schema: str
    object_name: str
    object_type: str
    definition: str
    level: int
    dep_tables: Tuple[Tuple[str, str, str, str], ...]  # (server, database, schema, name)
    dep_objects: Tuple[Tuple[str, str, str, str, str], ...]  # (server, database, schema, name, type)


def normalize(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "nat", "none"}:
        return ""
    return text


def format_two_part_name(schema: str, name: str) -> str:
    schema_clean = (schema or "dbo").strip() or "dbo"
    name_clean = (name or "").strip()
    schema_escaped = schema_clean.replace("]", "]]" )
    name_escaped = name_clean.replace("]", "]]" )
    return f"[{schema_escaped}].[{name_escaped}]"


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


def map_object_type(type_desc: str) -> str:
    mapping = {
        "USER_TABLE": "TABLE",
        "VIEW": "VIEW",
        "SYNONYM": "SYNONYM",
        "SQL_INLINE_TABLE_VALUED_FUNCTION": "FUNCTION",
        "SQL_SCALAR_FUNCTION": "FUNCTION",
        "SQL_TABLE_VALUED_FUNCTION": "FUNCTION",
    }
    return mapping.get(type_desc.upper(), type_desc.upper())


def lookup_object_type(
    conn: pyodbc.Connection,
    schema: str,
    name: str,
) -> Optional[str]:
    schema_clean = (schema or "dbo").strip() or "dbo"
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT TOP 1 type_desc FROM sys.objects "
            "WHERE name = ? AND SCHEMA_NAME(schema_id) = ?",
            name,
            schema_clean,
        )
        row = cursor.fetchone()
    except pyodbc.Error:
        row = None
    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception:
            pass
    if row:
        return map_object_type(row[0] or "")
    return None


def detect_source_object_type(conn: pyodbc.Connection, schema: str, table: str) -> Optional[str]:
    query = (
        "SELECT TOP 1 type_desc"
        " FROM sys.objects"
        " WHERE is_ms_shipped = 0"
        "   AND SCHEMA_NAME(schema_id) = ?"
        "   AND name = ?"
    )
    cursor = conn.cursor()
    cursor.execute(query, schema or "dbo", table)
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None
    return map_object_type(row[0] or "")


def resolve_dependency_type(ref_type: Optional[str], ref_class: Optional[str], ref_db: str) -> str:
    if ref_type:
        return ref_type
    if ref_db:
        return "EXTERNAL_OBJECT"
    if ref_class:
        return ref_class.upper()
    return "UNKNOWN"


def fetch_referenced_entities_fallback(
    conn: pyodbc.Connection,
    object_schema: str,
    object_name: str,
) -> List[Tuple[str, str, str, str, str, str]]:
    cursor = None
    rows: List[Tuple[str, str, str, str, str, str]] = []
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT"
            "    ISNULL(referenced_schema_name, '') AS ref_schema,"
            "    referenced_entity_name AS ref_name,"
            "    referenced_type_desc AS ref_type,"
            "    ISNULL(referenced_database_name, '') AS ref_db,"
            "    ISNULL(referenced_server_name, '') AS ref_server,"
            "    referenced_class_desc AS ref_class"
            " FROM sys.dm_sql_referenced_entities(?, 'OBJECT')",
            format_two_part_name(object_schema, object_name),
        )
        rows = [
            (
                (row[0] or ""),
                row[1],
                row[2],
                (row[3] or ""),
                (row[4] or ""),
                row[5],
            )
            for row in cursor.fetchall()
        ]
    except Exception as exc:  # include permission errors or unsupported contexts
        print(
            "[WARN] Impossibile eseguire sys.dm_sql_referenced_entities per"
            f" {object_schema}.{object_name}: {exc}"
        )
        rows = []
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except pyodbc.Error:
                pass
    return rows


def fetch_object_dependencies(
    conn: pyodbc.Connection,
    object_id: int,
    object_schema: str,
    object_name: str,
) -> Tuple[List[Tuple[str, str, str, str]], List[Tuple[str, str, str, str, str]]]:
    query = (
        "SELECT"
        "    ISNULL(d.referenced_schema_name, '') AS ref_schema,"
        "    d.referenced_entity_name AS ref_name,"
        "    obj.type_desc AS ref_type,"
        "    ISNULL(d.referenced_database_name, '') AS ref_db,"
        "    ISNULL(d.referenced_server_name, '') AS ref_server,"
        "    d.referenced_class_desc AS ref_class"
        " FROM sys.sql_expression_dependencies d"
        " LEFT JOIN sys.objects obj ON d.referenced_id = obj.object_id"
        " WHERE d.referencing_id = ?"
    )
    cursor = conn.cursor()
    cursor.execute(query, object_id)
    base_rows = [
        (
            (row[0] or ""),
            row[1],
            row[2],
            (row[3] or ""),
            (row[4] or ""),
            row[5],
        )
        for row in cursor.fetchall()
    ]
    cursor.close()
    if not base_rows:
        base_rows = fetch_referenced_entities_fallback(conn, object_schema, object_name)

    tables: List[Tuple[str, str, str, str]] = []
    objects: List[Tuple[str, str, str, str, str]] = []
    inference_cache: Dict[Tuple[str, str], Optional[str]] = {}
    for ref_schema, ref_name, ref_type, ref_database, ref_server, ref_class in base_rows:
        if not ref_name:
            continue
        schema_part = (ref_schema or "dbo").strip() or "dbo"
        database_part = (ref_database or "").strip()
        server_part = (ref_server or "").strip()
        if ref_type == "USER_TABLE":
            tables.append((server_part, database_part, schema_part, ref_name))
            continue

        resolved_type = resolve_dependency_type(ref_type, ref_class, database_part)
        if resolved_type in {"UNKNOWN", "OBJECT_OR_COLUMN"} and not database_part and not server_part:
            cache_key = (schema_part.lower(), ref_name.lower())
            if cache_key not in inference_cache:
                inference_cache[cache_key] = lookup_object_type(conn, schema_part, ref_name)
            inferred = inference_cache[cache_key]
            if inferred == "TABLE":
                tables.append((server_part, database_part, schema_part, ref_name))
                continue
            if inferred:
                resolved_type = inferred

        objects.append(
            (
                server_part,
                database_part,
                schema_part,
                ref_name,
                resolved_type,
            )
        )
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
    level: int,
) -> LineageObject:
    dep_tables, dep_objects = fetch_object_dependencies(conn, object_id, object_schema, object_name)
    return LineageObject(
        server=server,
        database=database,
        object_schema=object_schema or "dbo",
        object_name=object_name,
        object_type=object_type,
        definition=(definition or ""),
        level=level,
        dep_tables=tuple(dep_tables),
        dep_objects=tuple(dep_objects),
    )


def fetch_referencing_objects(
    conn: pyodbc.Connection,
    server: str,
    database: str,
    target_schema: str,
    target_table: str,
    level: int,
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
                    level,
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
    level: int,
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
        level,
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
    parser.add_argument(
        "--failure-log",
        default=DEFAULT_FAILURE_LOG,
        help="File Excel con l'elenco dei report per cui non e' stato trovato il lineage",
    )
    parser.add_argument(
        "--summary-output",
        default=DEFAULT_SUMMARY_OUTPUT,
        help="File Excel con metriche riepilogative",
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


def format_table_list(items: Iterable[Tuple[str, str, str, str]]) -> str:
    def format_entry(server: str, db: str, schema: str, name: str) -> str:
        if server and db:
            return f"{server}.{db}.{schema}.{name}"
        if server:
            return f"{server}.{schema}.{name}"
        if db:
            return f"{db}.{schema}.{name}"
        return f"{schema}.{name}"

    return " ; ".join(format_entry(server, db, schema, name) for server, db, schema, name in items)


def format_object_list(items: Iterable[Tuple[str, str, str, str, str]]) -> str:
    def format_entry(server: str, db: str, schema: str, name: str) -> str:
        if server and db:
            return f"{server}.{db}.{schema}.{name}"
        if server:
            return f"{server}.{schema}.{name}"
        if db:
            return f"{db}.{schema}.{name}"
        return f"{schema}.{name}"

    return " ; ".join(format_entry(server, db, schema, name) for server, db, schema, name, _ in items)


def main() -> None:
    args = parse_args()
    excel_path = Path(args.excel)
    if not excel_path.exists():
        raise FileNotFoundError(f"File non trovato: {excel_path}")

    df = pd.read_excel(excel_path, sheet_name=args.sheet)
    df.rename(columns=lambda c: str(c).strip(), inplace=True)
    total_rows = len(df)
    if total_rows == 0:
        print("Nessuna riga presente nel file di input, nulla da elaborare")
        return

    pool = ConnectionPool(args.driver)
    object_catalog: Dict[Tuple[str, str, str, str], LineageObject] = {}
    pending: Deque[LineageObject] = deque()  # Queue drives breadth-first recursion
    report_records: List[Dict[str, object]] = []
    failure_records: List[Dict[str, object]] = []
    server_db_cache: Dict[str, List[str]] = {}
    start_time = time.time()

    def log_progress(message: str) -> None:
        elapsed = int(time.time() - start_time)
        hours, remainder = divmod(elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"[{hours:02d}:{minutes:02d}:{seconds:02d}] {message}")

    progress_interval = max(1, total_rows // 20)

    def maybe_report_progress(current_row: int) -> None:
        if total_rows == 0:
            return
        if current_row % progress_interval != 0 and current_row != total_rows:
            return
        pct = (current_row / total_rows) * 100
        log_progress(
            "Processate "
            f"{current_row}/{total_rows} righe ({pct:.1f}%) - "
            f"oggetti catalogo: {len(object_catalog)} - fallimenti: {len(failure_records)}"
        )

    log_progress(
        f"Avvio lineage per {total_rows} righe da {excel_path}"
    )

    def register_object(obj: LineageObject) -> LineageObject:
        key = make_object_key(obj.server, obj.database, obj.object_schema, obj.object_name)
        existing = object_catalog.get(key)
        if existing is None or obj.level < existing.level:
            object_catalog[key] = obj
            pending.append(obj)
            return obj
        return existing

    fallback_dbs = [normalize(db) for db in args.fallback_db if normalize(db)]

    def add_failure_record(record: Dict[str, object]) -> None:
        failure_records.append(
            {
                key: sanitize_for_excel(value) if isinstance(value, str) else value
                for key, value in record.items()
            }
        )

    try:
        for row_idx, (_, row) in enumerate(df.iterrows(), start=1):
            try:
                server = normalize(args.override_server or row.get("Server"))
                database_hint = normalize(row.get("Database"))
                schema = normalize(row.get("Schema")) or "dbo"
                table = normalize(row.get("Table"))
                if not server or not table:
                    add_failure_record(
                        {
                            "Server": server,
                            "Database": database_hint,
                            "Schema": schema,
                            "Table": table,
                            "SourceObjectType": "UNKNOWN",
                            "Reason": "Server o tabella non valorizzati",
                        }
                    )
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
                detected_source_type = ""
                for candidate_db in candidate_dbs or [database_hint]:
                    if not candidate_db:
                        continue
                    conn = pool.get(server, candidate_db)
                    if conn is None:
                        continue
                    try:
                        if not detected_source_type:
                            detected = detect_source_object_type(conn, schema, table)
                            if detected:
                                detected_source_type = detected
                        if detected_source_type and detected_source_type != "TABLE":
                            fetched = fetch_object_by_name(
                                conn,
                                server,
                                candidate_db,
                                schema,
                                table,
                                level=1,
                            )
                            objects = [fetched] if fetched else []
                        else:
                            objects = fetch_referencing_objects(
                                conn,
                                server,
                                candidate_db,
                                schema,
                                table,
                                level=1,
                            )
                    except pyodbc.Error as exc:
                        add_failure_record(
                            {
                                "Server": server,
                                "Database": candidate_db,
                                "Schema": schema,
                                "Table": table,
                                "SourceObjectType": detected_source_type or "UNKNOWN",
                                "Reason": f"Permesso insufficiente: {exc}",
                            }
                        )
                        continue
                    if objects:
                        matched_db = candidate_db
                        break

                source_type = detected_source_type or "TABLE"

                if not objects:
                    add_failure_record(
                        {
                            "Server": server,
                            "Database": database_hint,
                            "Schema": schema,
                            "Table": table,
                            "SourceObjectType": source_type,
                            "Reason": "Nessun oggetto SQL trovato nel server/database indicato",
                        }
                    )
                    continue

                for obj in objects:
                    if obj is None:
                        continue
                    stored = register_object(obj)
                    report_records.append(
                        {
                            "Server": sanitize_for_excel(server),
                            "Database": sanitize_for_excel(matched_db or database_hint),
                            "Schema": sanitize_for_excel(schema),
                            "Table": sanitize_for_excel(table),
                            "SourceObjectType": sanitize_for_excel(source_type),
                            "ObjectServer": sanitize_for_excel(stored.server),
                            "ObjectDatabase": sanitize_for_excel(stored.database),
                            "ObjectSchema": sanitize_for_excel(stored.object_schema),
                            "ObjectName": sanitize_for_excel(stored.object_name),
                            "ObjectType": sanitize_for_excel(stored.object_type),
                            "ObjectExtractionLevel": stored.level,
                            "Motivo": sanitize_for_excel(classify_motivo(stored.definition, schema, table)),
                            "ObjectDefinition": sanitize_for_excel(stored.definition.strip()),
                            "DependencyTables": sanitize_for_excel(format_table_list(stored.dep_tables)),
                            "DependencyObjects": sanitize_for_excel(format_object_list(stored.dep_objects)),
                        }
                    )
            finally:
                maybe_report_progress(row_idx)

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
            current_conn = pool.get(current.server, current.database)
            if current_conn is None:
                continue
            for dep_server, dep_db, dep_schema, dep_name, dep_type in current.dep_objects:
                should_attempt = dep_type in OBJECT_TYPES_OF_INTEREST or dep_type in {"EXTERNAL_OBJECT", "UNKNOWN"}
                if not should_attempt:
                    continue
                schema_norm = (dep_schema or "dbo").strip() or "dbo"
                target_server = dep_server or current.server
                target_db = dep_db or (current.database if target_server.lower() == current.server.lower() else dep_db)
                if not target_db:
                    continue
                dep_key = make_object_key(target_server, target_db, schema_norm, dep_name)
                if dep_key in object_catalog:
                    continue
                if target_server.lower() == current.server.lower() and target_db == current.database:
                    conn = current_conn
                else:
                    conn = pool.get(target_server, target_db)
                if conn is None:
                    continue
                try:
                    dep_obj = fetch_object_by_name(
                        conn,
                        target_server,
                        target_db,
                        schema_norm,
                        dep_name,
                        current.level + 1,
                    )
                except pyodbc.Error as exc:
                    add_failure_record(
                        {
                            "Server": target_server,
                            "Database": target_db,
                            "Schema": schema_norm,
                            "Table": dep_name,
                            "SourceObjectType": dep_type,
                            "Reason": f"Permesso insufficiente durante BFS: {exc}",
                        }
                    )
                    continue
                if dep_obj:
                    register_object(dep_obj)
        log_progress(f"Espansione ricorsiva completata: {len(processed)} oggetti espansi")
    finally:
        pool.close()

    if not report_records:
        log_progress("Nessun oggetto derivato individuato")
        return

    dep_rows: List[Dict[str, object]] = []
    for obj in object_catalog.values():
        for dep_server, dep_db, dep_schema, dep_name in obj.dep_tables:
            target_server = dep_server or obj.server
            target_db = dep_db or obj.database
            schema_norm = (dep_schema or "dbo").strip() or "dbo"
            dep_rows.append(
                {
                    "ObjectServer": sanitize_for_excel(obj.server),
                    "ObjectDatabase": sanitize_for_excel(obj.database),
                    "ObjectSchema": sanitize_for_excel(obj.object_schema),
                    "ObjectName": sanitize_for_excel(obj.object_name),
                    "ObjectType": sanitize_for_excel(obj.object_type),
                    "ObjectExtractionLevel": obj.level,
                    "DependencyType": "TABLE",
                    "Server estratto": sanitize_for_excel(target_server),
                    "DependencyDatabase": sanitize_for_excel(target_db),
                    "DependencySchema": sanitize_for_excel(schema_norm),
                    "DependencyName": sanitize_for_excel(dep_name),
                    "DependencyExtractionLevel": "",
                }
            )
        for dep_server, dep_db, dep_schema, dep_name, dep_type in obj.dep_objects:
            target_server = dep_server or obj.server
            target_db = dep_db or obj.database
            schema_norm = (dep_schema or "dbo").strip() or "dbo"
            dep_key = make_object_key(target_server, target_db, schema_norm, dep_name)
            resolved_type = dep_type
            referenced_obj = object_catalog.get(dep_key)
            if referenced_obj is not None:
                resolved_type = referenced_obj.object_type
                dependency_level = referenced_obj.level
            else:
                dependency_level = ""
            dep_rows.append(
                {
                    "ObjectServer": sanitize_for_excel(obj.server),
                    "ObjectDatabase": sanitize_for_excel(obj.database),
                    "ObjectSchema": sanitize_for_excel(obj.object_schema),
                    "ObjectName": sanitize_for_excel(obj.object_name),
                    "ObjectType": sanitize_for_excel(obj.object_type),
                    "ObjectExtractionLevel": obj.level,
                    "DependencyType": sanitize_for_excel(resolved_type),
                    "Server estratto": sanitize_for_excel(target_server),
                    "DependencyDatabase": sanitize_for_excel(target_db),
                    "DependencySchema": sanitize_for_excel(schema_norm),
                    "DependencyName": sanitize_for_excel(dep_name),
                    "DependencyExtractionLevel": dependency_level,
                }
            )

    report_df = pd.DataFrame(report_records, columns=REPORT_COLUMNS)
    report_path = Path(args.report_output)
    export_single_sheet_with_guide(report_df, report_path, "ReportLineage", REPORT_GUIDE)
    log_progress(f"Report→Oggetto: {len(report_records)} righe -> {report_path}")

    dep_df = pd.DataFrame(dep_rows, columns=DEPENDENCY_COLUMNS)
    dep_path = Path(args.dependency_output)
    dep_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(dep_path) as writer:
        dep_df.to_excel(writer, sheet_name="All", index=False)
        if not dep_df.empty:
            level_values: List[int] = []
            for raw_value in dep_df["ObjectExtractionLevel"].unique():
                if raw_value in ("", None):
                    continue
                try:
                    level_values.append(int(raw_value))
                except (ValueError, TypeError):
                    continue
            for level_value in sorted(set(level_values)):
                sheet_name = f"L{level_value}"
                dep_df.loc[dep_df["ObjectExtractionLevel"] == level_value].to_excel(
                    writer,
                    sheet_name=sheet_name,
                    index=False,
                )
        write_guide_sheet(writer, DEPENDENCY_GUIDE)
    log_progress(f"Dipendenze normalizzate: {len(dep_rows)} righe -> {dep_path}")

    dep_metrics_df = dep_df.copy()
    if dep_metrics_df.empty:
        total_dependency_edges = 0
        resolved_dependency_edges = 0
        dependency_breakdown_df = pd.DataFrame(
            columns=["DependencyType", "TotalEdges", "DistinctTargets", "ResolvedEdges", "ResolvedPct"]
        )
        dependency_server_df = pd.DataFrame(
            columns=["TargetServer", "TotalEdges", "DistinctTargets", "SharePct"]
        )
    else:
        dep_metrics_df.loc[:, "DependencyExtractionLevel"] = (
            dep_metrics_df["DependencyExtractionLevel"].fillna("").astype(str).str.strip()
        )
        dep_metrics_df.loc[:, "DependencyType"] = (
            dep_metrics_df["DependencyType"].fillna("").astype(str).str.strip()
        )
        dep_metrics_df.loc[:, "DependencyType"] = dep_metrics_df["DependencyType"].replace("", "(non rilevato)")
        target_server = dep_metrics_df["Server estratto"].fillna("").astype(str).str.strip()
        target_db = dep_metrics_df["DependencyDatabase"].fillna("").astype(str).str.strip()
        target_schema = dep_metrics_df["DependencySchema"].fillna("").astype(str).str.strip()
        target_name = dep_metrics_df["DependencyName"].fillna("").astype(str).str.strip()
        dep_metrics_df.loc[:, "TargetServer"] = target_server.replace("", "(non rilevato)")
        dep_metrics_df.loc[:, "ResolvedTarget"] = dep_metrics_df["DependencyExtractionLevel"].str.len() > 0
        dep_metrics_df.loc[:, "TargetKey"] = (
            target_server.str.lower()
            + "|"
            + target_db.str.lower()
            + "|"
            + target_schema.str.lower()
            + "|"
            + target_name.str.lower()
        )
        total_dependency_edges = int(dep_metrics_df.shape[0])
        resolved_dependency_edges = int(dep_metrics_df["ResolvedTarget"].sum())
        dependency_breakdown_df = (
            dep_metrics_df.groupby("DependencyType")
            .agg(
                TotalEdges=("TargetKey", "size"),
                DistinctTargets=("TargetKey", "nunique"),
                ResolvedEdges=("ResolvedTarget", "sum"),
            )
            .reset_index()
            .sort_values("TotalEdges", ascending=False)
        )
        dependency_breakdown_df.loc[:, "ResolvedPct"] = (
            dependency_breakdown_df["ResolvedEdges"]
            .div(dependency_breakdown_df["TotalEdges"].replace(0, pd.NA))
            .fillna(0)
            .round(2)
        )
        dependency_server_df = (
            dep_metrics_df.groupby("TargetServer")
            .agg(
                TotalEdges=("TargetKey", "size"),
                DistinctTargets=("TargetKey", "nunique"),
            )
            .reset_index()
            .sort_values("TotalEdges", ascending=False)
        )
        dependency_server_df.loc[:, "SharePct"] = (
            dependency_server_df["TotalEdges"] / total_dependency_edges * 100
        ).round(2)

    obj_rows = [
        {
            "Server": sanitize_for_excel(obj.server),
            "Database": sanitize_for_excel(obj.database),
            "ObjectSchema": sanitize_for_excel(obj.object_schema),
            "ObjectName": sanitize_for_excel(obj.object_name),
            "ObjectType": sanitize_for_excel(obj.object_type),
            "ExtractionLevel": obj.level,
            "ObjectDefinition": sanitize_for_excel(obj.definition.strip()),
        }
        for obj in object_catalog.values()
    ]
    obj_df = pd.DataFrame(obj_rows, columns=OBJECT_CATALOG_COLUMNS)
    obj_path = Path(args.object_output)
    export_single_sheet_with_guide(obj_df, obj_path, "Catalogo", CATALOG_GUIDE)
    log_progress(f"Catalogo oggetti: {len(obj_rows)} elementi -> {obj_path}")

    failure_df = pd.DataFrame(failure_records, columns=FAILURE_COLUMNS)
    if not failure_df.empty:
        failure_path = Path(args.failure_log)
        export_single_sheet_with_guide(failure_df, failure_path, "Fallimenti", FAILURE_GUIDE)
        log_progress(f"Log fallimenti lineage: {len(failure_records)} righe -> {failure_path}")
    else:
        log_progress("Nessun fallimento di lineage da registrare")

    if failure_df.empty:
        failure_summary_df = pd.DataFrame(
            columns=["Reason", "Occurrences", "DistinctServers", "DistinctTables", "SharePct"]
        )
    else:
        total_failures = len(failure_df)
        failure_summary_df = (
            failure_df.groupby("Reason")
            .agg(
                Occurrences=("Table", "size"),
                DistinctServers=("Server", "nunique"),
                DistinctTables=("Table", "nunique"),
            )
            .reset_index()
            .sort_values("Occurrences", ascending=False)
        )
        failure_summary_df.loc[:, "SharePct"] = (
            failure_summary_df["Occurrences"] / total_failures * 100
        ).round(2)

    total_objects = len(object_catalog)
    max_level = max((obj.level for obj in object_catalog.values()), default=0)
    total_dep_tables = sum(len(obj.dep_tables) for obj in object_catalog.values())
    total_dep_objects = sum(len(obj.dep_objects) for obj in object_catalog.values())
    avg_dep_tables = (total_dep_tables / total_objects) if total_objects else 0
    avg_dep_objects = (total_dep_objects / total_objects) if total_objects else 0
    dependency_resolution_pct = (
        round((resolved_dependency_edges / total_dependency_edges) * 100, 2)
        if total_dependency_edges
        else 0
    )
    distinct_target_servers = (
        dependency_server_df["TargetServer"].nunique() if not dependency_server_df.empty else 0
    )
    unique_reports = (
        report_df[["Server", "Database", "Schema", "Table"]]
        .drop_duplicates()
        .shape[0]
    )
    processed_sources = report_df[["Server", "Schema", "Table"]].drop_duplicates().shape[0]
    kpi_rows = [
        {"Metric": "Oggetti SQL distinti", "Value": total_objects},
        {"Metric": "Livello massimo estrazione", "Value": max_level},
        {"Metric": "Media dependenze verso tabelle", "Value": round(avg_dep_tables, 2)},
        {"Metric": "Media dependenze verso oggetti", "Value": round(avg_dep_objects, 2)},
        {"Metric": "Tabelle sorgente con lineage", "Value": processed_sources},
        {"Metric": "Combinazioni report-server", "Value": unique_reports},
        {"Metric": "Righe report lineage", "Value": len(report_df)},
        {"Metric": "Fallimenti lineage", "Value": len(failure_records)},
        {"Metric": "Archi dipendenza totali", "Value": total_dependency_edges},
        {"Metric": "Archi dipendenza risolti", "Value": resolved_dependency_edges},
        {"Metric": "Copertura target dipendenze (%)", "Value": dependency_resolution_pct},
        {"Metric": "Server target dipendenze", "Value": distinct_target_servers},
    ]

    if obj_df.empty:
        objects_by_type_df = pd.DataFrame(columns=["ObjectType", "DistinctObjects"])
        objects_per_db_df = pd.DataFrame(
            columns=["Database", "DistinctObjects", "DistinctSchemas", "MaxExtractionLevel"]
        )
        levels_distribution_df = pd.DataFrame(columns=["ExtractionLevel", "DistinctObjects"])
    else:
        objects_by_type_df = (
            obj_df.groupby("ObjectType")
            .agg(DistinctObjects=("ObjectName", "nunique"))
            .reset_index()
            .sort_values("DistinctObjects", ascending=False)
        )

        objects_per_db_df = (
            obj_df.groupby("Database")
            .agg(
                DistinctObjects=("ObjectName", "nunique"),
                DistinctSchemas=("ObjectSchema", "nunique"),
                MaxExtractionLevel=("ExtractionLevel", "max"),
            )
            .reset_index()
            .sort_values("DistinctObjects", ascending=False)
        )

        levels_distribution_df = (
            obj_df.groupby("ExtractionLevel")
            .agg(DistinctObjects=("ObjectName", "nunique"))
            .reset_index()
            .sort_values("ExtractionLevel")
        )

    if report_df.empty:
        report_weight_df = pd.DataFrame(
            columns=[
                "Database",
                "ReportRows",
                "DistinctSourceTables",
                "DistinctObjects",
                "DistinctServers",
                "SharePct",
            ]
        )
    else:
        report_weight_source = report_df.copy()
        report_weight_source.loc[:, "EffectiveDatabase"] = report_weight_source["ObjectDatabase"].replace("", pd.NA)
        report_weight_source.loc[:, "EffectiveDatabase"] = report_weight_source["EffectiveDatabase"].fillna(
            report_weight_source["Database"]
        )
        report_weight_source.loc[:, "EffectiveDatabase"] = report_weight_source["EffectiveDatabase"].replace(
            "", "(non rilevato)"
        )
        report_weight_df = (
            report_weight_source.groupby("EffectiveDatabase")
            .agg(
                ReportRows=("Table", "size"),
                DistinctSourceTables=("Table", "nunique"),
                DistinctObjects=("ObjectName", "nunique"),
                DistinctServers=("ObjectServer", "nunique"),
            )
            .reset_index()
            .rename(columns={"EffectiveDatabase": "Database"})
        )
    report_weight_df["Database"].replace("", "(non rilevato)", inplace=True)
    total_report_rows = report_weight_df["ReportRows"].sum()
    if total_report_rows:
        report_weight_df["SharePct"] = (
            report_weight_df["ReportRows"] / total_report_rows * 100
        ).round(2)
    else:
        report_weight_df["SharePct"] = 0

    summary_path = Path(args.summary_output)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(summary_path) as writer:
        pd.DataFrame(kpi_rows).to_excel(writer, sheet_name="KPIs", index=False)
        objects_by_type_df.to_excel(writer, sheet_name="OggettiPerTipo", index=False)
        objects_per_db_df.to_excel(writer, sheet_name="DatabaseOggetti", index=False)
        report_weight_df.to_excel(writer, sheet_name="PesoDatabase", index=False)
        dependency_breakdown_df.to_excel(writer, sheet_name="DipendenzeTipo", index=False)
        dependency_server_df.to_excel(writer, sheet_name="DipendenzeServer", index=False)
        levels_distribution_df.to_excel(writer, sheet_name="DistribuzioneLivelli", index=False)
        failure_summary_df.to_excel(writer, sheet_name="SintesiFallimenti", index=False)
        write_guide_sheet(writer, SUMMARY_GUIDE)
    log_progress(f"Metriche riepilogative -> {summary_path}")
    log_progress("Script completato")


if __name__ == "__main__":
    main()
