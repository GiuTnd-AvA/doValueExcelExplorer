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
from typing import Callable, Deque, Dict, Iterable, List, Optional, Set, Tuple, TypedDict, Union, cast

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
DEFAULT_PATH_OUTPUT = (
    r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse engineering\Lineage completo\lineage_path_matrix.xlsx"
)
DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"
MAX_EXCEL_ROWS = 1_048_576
DEFAULT_FALLBACK_DBS = [
    "AMS",
    "AMS_MANUAL_TABLES",
    "ANALISI",
    "ANALYTICS",
    "BAD0_Online",
    "BAD0OnlineFM",
    "BASEDATI_BI",
    "BASEDATI_BI_STORICI_2015",
    "BASEDATI_BI_STORICI_ANTE2015",
    "CORESQL7",
    "CORESQL7ARK",
    "CORESQL7ARKFM",
    "DWH",
    "EPC_BI",
    "EPC_PCT",
    "EPC_STG",
    "GESTITO",
    "MASTER",
    "MSDB",
    "REPLICA",
    "S1040",
    "S1053",
    "S1057",
    "S1057B",
    "S1229",
    "S1242",
    "S1259",
    "TEMPDB",
    "UTIL",
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
    r"\binsert\s+into\s+{target}",
    r"\bupdate\s+{target}",
    r"\bdelete\s+from\s+{target}",
    r"\bmerge\s+into\s+{target}",
)

INVALID_EXCEL_CHARS = {chr(i) for i in range(32)} - {"\t", "\n", "\r"}

COMMENT_BLOCK_RE = re.compile(r"/\*.*?\*/", flags=re.S)
COMMENT_LINE_RE = re.compile(r"--[^\n\r]*")
SQL_IDENTIFIER_TRANSLATION = str.maketrans({"[": "", "]": "", "`": "", '"': ""})


def sanitize_for_excel(value: object) -> str:
    text = normalize(value)
    if not text:
        return ""
    return "".join(ch for ch in text if ch not in INVALID_EXCEL_CHARS)


def sanitize_for_filename(value: object, fallback: str = "file") -> str:
    text = normalize(value)
    if not text:
        text = fallback
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "_", text)
    return cleaned[:120] or fallback


def strip_sql_comments(sql: str) -> str:
    without_block = re.sub(COMMENT_BLOCK_RE, " ", sql)
    return re.sub(COMMENT_LINE_RE, " ", without_block)


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


def build_lineage_path_columns(max_depth: int) -> List[str]:
    columns = [
        "FileName",
        "Path_File",
        "RootServer",
        "RootDatabase",
        "RootSchema",
        "RootTable",
        "RootSourceType",
    ]
    first_prefix = "L1Object"
    columns.extend(
        [
            f"{first_prefix}Server",
            f"{first_prefix}Database",
            f"{first_prefix}Schema",
            f"{first_prefix}Name",
            f"{first_prefix}Type",
            f"{first_prefix}ExtractionLevel",
        ]
    )

    for level in range(1, max_depth + 1):
        dep_prefix = f"L{level}Dependency"
        columns.extend(
            [
                f"{dep_prefix}Server",
                f"{dep_prefix}Database",
                f"{dep_prefix}Schema",
                f"{dep_prefix}Name",
                f"{dep_prefix}Type",
                f"{dep_prefix}ExtractionLevel",
                f"{dep_prefix}Scope",
            ]
        )
    return columns


LINEAGE_PATH_GUIDE = [
    "Rappresentazione orizzontale del lineage, una riga per ogni percorso File → L1…Lmax.",
    "Per ogni livello vengono riportati sia l'oggetto trovato sia la dipendenza analizzata.",
    "Le colonne restano vuote se il percorso termina prima di raggiungere il livello massimo impostato.",
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
    "FileName",
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
    "DependencyServer",
    "Server estratto",
    "DependencyDatabase",
    "DependencySchema",
    "DependencyName",
    "DependencyExtractionLevel",
    "DependencyScope",
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


class DependencyRecord(TypedDict):
    server: str
    database: str
    schema: str
    name: str
    type: str
    extraction_level: Union[int, str]
    next_obj: Optional[LineageObject]


ObjectKey = Tuple[str, str, str, str]


def normalize(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "nat", "none"}:
        return ""
    return text


def normalize_db_name(value: object) -> str:
    text = normalize(value)
    if text.startswith("[") and text.endswith("]") and len(text) > 2:
        return text[1:-1]
    return text


def format_two_part_name(schema: str, name: str) -> str:
    schema_clean = (schema or "dbo").strip() or "dbo"
    name_clean = (name or "").strip()
    schema_escaped = schema_clean.replace("]", "]]" )
    name_escaped = name_clean.replace("]", "]]" )
    return f"[{schema_escaped}].[{name_escaped}]"


class ConnectionPool:
    def __init__(self, driver: str, timeout: int) -> None:
        self.driver = driver
        self.timeout = max(1, timeout)
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
                self._pool[key] = pyodbc.connect(conn_str, timeout=self.timeout)
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


def list_server_databases(server: str, driver: str, timeout: int) -> List[str]:
    conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE=master;Trusted_Connection=yes;"
    try:
        with pyodbc.connect(conn_str, timeout=timeout) as conn:
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
    table_raw = (target_table or "").strip().lower()
    if not table_raw:
        return "Sconosciuto"
    schema_raw = (target_schema or "dbo").strip().lower() or "dbo"
    normalized_sql = strip_sql_comments(sql_definition)
    normalized_sql = normalized_sql.lower().translate(SQL_IDENTIFIER_TRANSLATION)
    candidate_tokens: List[str] = []
    schema_table = f"{schema_raw}.{table_raw}"
    if schema_table not in candidate_tokens:
        candidate_tokens.append(schema_table)
    if table_raw not in candidate_tokens:
        candidate_tokens.append(table_raw)
    escaped_candidates = [re.escape(token) for token in candidate_tokens if token]
    for template in WRITE_PATTERNS:
        for candidate in escaped_candidates:
            target_pattern = f"(?<!\\w){candidate}(?!\\w)"
            if re.search(template.format(target=target_pattern), normalized_sql):
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


def resolve_dependency_type(
    ref_type: Optional[str],
    ref_class: Optional[str],
    ref_db: str,
    parent_db: str,
) -> str:
    if ref_type:
        return ref_type

    ref_db_norm = normalize(ref_db).lower()
    parent_db_norm = normalize(parent_db).lower()
    if ref_db_norm and parent_db_norm and ref_db_norm == parent_db_norm:
        ref_db_norm = ""

    if ref_db_norm:
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
    object_server: str,
    object_database: str,
    connection_resolver: Optional[Callable[[str, str], Optional[pyodbc.Connection]]] = None,
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
    parent_db_norm = normalize(object_database).lower()
    parent_server_norm = normalize(object_server).lower()

    for ref_schema, ref_name, ref_type, ref_database, ref_server, ref_class in base_rows:
        if not ref_name:
            continue
        schema_part = (ref_schema or "dbo").strip() or "dbo"
        database_part = normalize_db_name(ref_database)
        server_part = normalize(ref_server)
        server_part_norm = server_part.lower()
        if ref_type == "USER_TABLE":
            tables.append((server_part, database_part, schema_part, ref_name))
            continue

        resolved_type = resolve_dependency_type(ref_type, ref_class, database_part, object_database)
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

        if resolved_type == "EXTERNAL_OBJECT" and database_part:
            target_server = server_part or object_server
            target_server_norm = (server_part_norm or parent_server_norm)
            if connection_resolver and target_server and target_server_norm == parent_server_norm:
                cross_conn = connection_resolver(target_server, database_part)
                if cross_conn is not None:
                    inferred_type = lookup_object_type(cross_conn, schema_part, ref_name)
                    if inferred_type:
                        resolved_type = inferred_type
                        server_part = target_server
                        server_part_norm = target_server_norm

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
    connection_resolver: Optional[Callable[[str, str], Optional[pyodbc.Connection]]] = None,
) -> LineageObject:
    dep_tables, dep_objects = fetch_object_dependencies(
        conn,
        object_id,
        object_schema,
        object_name,
        server,
        database,
        connection_resolver,
    )
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
    connection_resolver: Optional[Callable[[str, str], Optional[pyodbc.Connection]]] = None,
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
                connection_resolver,
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
    connection_resolver: Optional[Callable[[str, str], Optional[pyodbc.Connection]]] = None,
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
        connection_resolver,
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
        "--path-output",
        default=DEFAULT_PATH_OUTPUT,
        help="File Excel con la matrice File→L1-L5 su un'unica riga",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=8,
        help="Profondità massima da riportare nel foglio LineagePaths",
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
        "--connect-timeout",
        type=int,
        default=5,
        help="Secondi massimi di attesa per aprire una connessione SQL",
    )
    parser.add_argument(
        "--fallback-db",
        action="append",
        default=DEFAULT_FALLBACK_DBS,
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


def make_object_key(server: str, database: str, schema: str, name: str) -> ObjectKey:
    return (
        (server or "").lower(),
        (database or "").lower(),
        (schema or "").lower(),
        (name or "").lower(),
    )


def make_object_key_from_values(server: object, database: object, schema: object, name: object) -> ObjectKey:
    return (
        normalize(server).lower(),
        normalize(database).lower(),
        normalize(schema).lower(),
        normalize(name).lower(),
    )


def build_candidate_databases(
    server: str,
    primary: str,
    fallback: Iterable[str],
    scan_all: bool,
    driver: str,
    server_db_cache: Dict[str, List[str]],
    connect_timeout: int,
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
            server_db_cache[cache_key] = list_server_databases(server, driver, connect_timeout)
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


def classify_dependency_scope(
    parent_server: object,
    parent_database: object,
    dependency_server: object,
    dependency_database: object,
) -> str:
    parent_server_norm = normalize(parent_server).lower()
    parent_db_norm = normalize(parent_database).lower()
    dep_server_norm = normalize(dependency_server).lower()
    dep_db_norm = normalize(dependency_database).lower()

    if dep_server_norm and parent_server_norm and dep_server_norm != parent_server_norm:
        return "CROSS_SERVER"
    if dep_db_norm and parent_db_norm:
        if dep_db_norm != parent_db_norm:
            return "CROSS_DB"
        return "SAME_DB"
    if dep_db_norm and not parent_db_norm:
        return "CROSS_DB"
    if not dep_db_norm and parent_db_norm:
        return "SAME_DB"
    return "UNKNOWN"


def generate_lineage_paths(
    roots: List[Dict[str, object]],
    catalog: Dict[ObjectKey, LineageObject],
    max_depth: int,
) -> Tuple[List[Dict[str, object]], List[str]]:
    columns = build_lineage_path_columns(max_depth)
    rows: List[Dict[str, object]] = []

    def enumerate_dependencies(obj: LineageObject) -> List[DependencyRecord]:
        records: List[DependencyRecord] = []
        parent_key = make_object_key(obj.server, obj.database, obj.object_schema, obj.object_name)
        for dep_server, dep_db, dep_schema, dep_name in obj.dep_tables:
            target_server = dep_server or obj.server
            target_db = dep_db or obj.database
            schema_norm = (dep_schema or "dbo").strip() or "dbo"
            dep_key = make_object_key(target_server, target_db, schema_norm, dep_name)
            if dep_key == parent_key:
                continue
            records.append(
                DependencyRecord(
                    server=dep_server or target_server,
                    database=target_db,
                    schema=schema_norm,
                    name=dep_name,
                    type="TABLE",
                    extraction_level="",
                    next_obj=None,
                )
            )
        for dep_server, dep_db, dep_schema, dep_name, dep_type in obj.dep_objects:
            target_server = dep_server or obj.server
            target_db = dep_db or obj.database
            schema_norm = (dep_schema or "dbo").strip() or "dbo"
            dep_key = make_object_key(target_server, target_db, schema_norm, dep_name)
            if dep_key == parent_key:
                continue
            next_obj = catalog.get(dep_key)
            records.append(
                DependencyRecord(
                    server=dep_server or target_server,
                    database=target_db,
                    schema=schema_norm,
                    name=dep_name,
                    type=next_obj.object_type if next_obj else dep_type,
                    extraction_level=next_obj.level if next_obj else "",
                    next_obj=next_obj,
                )
            )
        return records

    def traverse(
        current_obj: LineageObject,
        level: int,
        row_state: Dict[str, object],
        visited_keys: Set[ObjectKey],
    ) -> None:
        if level > max_depth:
            rows.append(row_state.copy())
            return

        if level == 1:
            row_state[f"L{level}ObjectServer"] = sanitize_for_excel(current_obj.server)
            row_state[f"L{level}ObjectDatabase"] = sanitize_for_excel(current_obj.database)
            row_state[f"L{level}ObjectSchema"] = sanitize_for_excel(current_obj.object_schema)
            row_state[f"L{level}ObjectName"] = sanitize_for_excel(current_obj.object_name)
            row_state[f"L{level}ObjectType"] = sanitize_for_excel(current_obj.object_type)
            row_state[f"L{level}ObjectExtractionLevel"] = current_obj.level

        dependencies = enumerate_dependencies(current_obj)
        if not dependencies:
            rows.append(row_state.copy())
            return

        for dep in dependencies:
            next_row = row_state.copy()
            next_row[f"L{level}DependencyServer"] = sanitize_for_excel(dep["server"])
            next_row[f"L{level}DependencyDatabase"] = sanitize_for_excel(dep["database"])
            next_row[f"L{level}DependencySchema"] = sanitize_for_excel(dep["schema"])
            next_row[f"L{level}DependencyName"] = sanitize_for_excel(dep["name"])
            next_row[f"L{level}DependencyType"] = sanitize_for_excel(dep["type"])
            next_row[f"L{level}DependencyExtractionLevel"] = dep["extraction_level"]
            scope = classify_dependency_scope(
                current_obj.server,
                current_obj.database,
                dep["server"] or current_obj.server,
                dep["database"] or current_obj.database,
            )
            next_row[f"L{level}DependencyScope"] = scope
            next_obj = dep["next_obj"]
            if next_obj is not None and level < max_depth:
                next_key = make_object_key(
                    next_obj.server,
                    next_obj.database,
                    next_obj.object_schema,
                    next_obj.object_name,
                )
                if next_key in visited_keys:
                    rows.append(next_row)
                    continue
                traverse(next_obj, level + 1, next_row, visited_keys | {next_key})
            else:
                rows.append(next_row)

    for root in roots:
        raw_key = root.get("RootObjectKey")
        if raw_key is None:
            continue
        if not isinstance(raw_key, tuple) or len(raw_key) != 4 or not all(isinstance(part, str) for part in raw_key):
            continue
        key = cast(ObjectKey, raw_key)
        lineage_obj = catalog.get(key)
        if lineage_obj is None:
            continue
        base_row: Dict[str, object] = {column: "" for column in columns}
        for column in ("FileName", "Path_File", "RootServer", "RootDatabase", "RootSchema", "RootTable", "RootSourceType"):
            base_row[column] = root.get(column, "")
        traverse(lineage_obj, 1, base_row, {key})

    return rows, columns


class LineageRunner:
    def __init__(self, args: argparse.Namespace, df: pd.DataFrame, excel_path: Path) -> None:
        self.args = args
        self.df = df
        self.excel_path = excel_path
        self.pool = ConnectionPool(args.driver, args.connect_timeout)
        self.object_catalog: Dict[ObjectKey, LineageObject] = {}
        self.pending: Deque[LineageObject] = deque()
        self.report_records: List[Dict[str, object]] = []
        self.lineage_roots: List[Dict[str, object]] = []
        self.failure_records: List[Dict[str, object]] = []
        self.server_db_cache: Dict[str, List[str]] = {}
        self.start_time = time.time()
        self.total_rows = len(df)
        self.progress_interval = max(1, self.total_rows // 20) if self.total_rows else 1
        self.fallback_dbs = [normalize(db) for db in args.fallback_db if normalize(db)]
        self.column_aliases: Dict[str, str] = {}
        for col in df.columns:
            display = str(col).strip()
            canonical = self._canonical_column_name(display)
            if canonical and canonical not in self.column_aliases:
                self.column_aliases[canonical] = display

    def log_progress(self, message: str) -> None:
        elapsed = int(time.time() - self.start_time)
        hours, remainder = divmod(elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"[{hours:02d}:{minutes:02d}:{seconds:02d}] {message}")

    def maybe_report_progress(self, current_row: int) -> None:
        if self.total_rows == 0:
            return
        if current_row % self.progress_interval != 0 and current_row != self.total_rows:
            return
        pct = (current_row / self.total_rows) * 100
        self.log_progress(
            "Processate "
            f"{current_row}/{self.total_rows} righe ({pct:.1f}%) - "
            f"oggetti catalogo: {len(self.object_catalog)} - fallimenti: {len(self.failure_records)}"
        )

    @staticmethod
    def _canonical_column_name(name: str) -> str:
        return re.sub(r"[^a-z0-9]", "", name.lower())

    def _get_column_value(self, row: pd.Series, column_name: str) -> Optional[object]:
        canonical = self._canonical_column_name(column_name)
        actual = self.column_aliases.get(canonical)
        if actual is None:
            return None
        return row.get(actual)

    def register_object(self, obj: LineageObject) -> LineageObject:
        key = make_object_key(obj.server, obj.database, obj.object_schema, obj.object_name)
        existing = self.object_catalog.get(key)
        if existing is None or obj.level < existing.level:
            self.object_catalog[key] = obj
            self.pending.append(obj)
            return obj
        return existing

    def add_failure_record(self, record: Dict[str, object]) -> None:
        self.failure_records.append(
            {
                key: sanitize_for_excel(value) if isinstance(value, str) else value
                for key, value in record.items()
            }
        )

    def process_input_rows(self) -> None:
        for row_idx, (_, row) in enumerate(self.df.iterrows(), start=1):
            try:
                file_name = normalize(self._get_column_value(row, "FileName"))
                path_file = normalize(self._get_column_value(row, "Path_File"))
                row_server = self._get_column_value(row, "Server")
                server = normalize(self.args.override_server or row_server)
                database_hint = normalize(self._get_column_value(row, "Database"))
                schema = normalize(self._get_column_value(row, "Schema")) or "dbo"
                table = normalize(self._get_column_value(row, "Table"))
                if not server or not table:
                    self.add_failure_record(
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
                    self.fallback_dbs,
                    self.args.scan_all_databases,
                    self.args.driver,
                    self.server_db_cache,
                    self.args.connect_timeout,
                )

                objects: List[LineageObject] = []
                matched_db = ""
                detected_source_type = ""
                for candidate_db in candidate_dbs or [database_hint]:
                    if not candidate_db:
                        continue
                    conn = self.pool.get(server, candidate_db)
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
                                connection_resolver=self.pool.get,
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
                                connection_resolver=self.pool.get,
                            )
                    except pyodbc.Error as exc:
                        self.add_failure_record(
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
                    self.add_failure_record(
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

                file_name_safe = sanitize_for_excel(file_name)
                path_file_safe = sanitize_for_excel(path_file)
                root_server_safe = sanitize_for_excel(server)
                root_database_safe = sanitize_for_excel(matched_db or database_hint)
                root_schema_safe = sanitize_for_excel(schema)
                root_table_safe = sanitize_for_excel(table)
                root_source_type_safe = sanitize_for_excel(source_type)

                for obj in objects:
                    if obj is None:
                        continue
                    stored = self.register_object(obj)
                    self.report_records.append(
                        {
                            "FileName": file_name_safe,
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
                    self.lineage_roots.append(
                        {
                            "FileName": file_name_safe,
                            "Path_File": path_file_safe,
                            "RootServer": root_server_safe,
                            "RootDatabase": root_database_safe,
                            "RootSchema": root_schema_safe,
                            "RootTable": root_table_safe,
                            "RootSourceType": root_source_type_safe,
                            "RootObjectKey": make_object_key(
                                stored.server,
                                stored.database,
                                stored.object_schema,
                                stored.object_name,
                            ),
                        }
                    )
            finally:
                self.maybe_report_progress(row_idx)

    def expand_dependencies(self) -> None:
        processed: Set[ObjectKey] = set()
        while self.pending:
            current = self.pending.popleft()
            key = make_object_key(
                current.server,
                current.database,
                current.object_schema,
                current.object_name,
            )
            if key in processed:
                continue
            processed.add(key)
            current_conn = self.pool.get(current.server, current.database)
            if current_conn is None:
                continue
            for dep_server, dep_db, dep_schema, dep_name, dep_type in current.dep_objects:
                should_attempt = dep_type in OBJECT_TYPES_OF_INTEREST or dep_type in {"EXTERNAL_OBJECT", "UNKNOWN"}
                if not should_attempt:
                    continue
                schema_norm = (dep_schema or "dbo").strip() or "dbo"
                target_server = dep_server or current.server
                target_db = dep_db or current.database
                if not target_db:
                    continue
                dep_key = make_object_key(target_server, target_db, schema_norm, dep_name)
                if dep_key == key or dep_key in self.object_catalog:
                    continue
                if target_server.lower() == current.server.lower() and target_db == current.database:
                    conn = current_conn
                else:
                    conn = self.pool.get(target_server, target_db)
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
                        connection_resolver=self.pool.get,
                    )
                except pyodbc.Error as exc:
                    self.add_failure_record(
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
                    self.register_object(dep_obj)
        self.log_progress(f"Espansione ricorsiva completata: {len(processed)} oggetti espansi")

    def build_dependency_rows(self) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []
        for obj in self.object_catalog.values():
            parent_key = make_object_key(obj.server, obj.database, obj.object_schema, obj.object_name)
            for dep_server, dep_db, dep_schema, dep_name in obj.dep_tables:
                target_server = dep_server or obj.server
                target_db = dep_db or obj.database
                schema_norm = (dep_schema or "dbo").strip() or "dbo"
                dep_key = make_object_key(target_server, target_db, schema_norm, dep_name)
                if dep_key == parent_key:
                    continue
                rows.append(
                    {
                        "ObjectServer": sanitize_for_excel(obj.server),
                        "ObjectDatabase": sanitize_for_excel(obj.database),
                        "ObjectSchema": sanitize_for_excel(obj.object_schema),
                        "ObjectName": sanitize_for_excel(obj.object_name),
                        "ObjectType": sanitize_for_excel(obj.object_type),
                        "ObjectExtractionLevel": obj.level,
                        "DependencyType": "TABLE",
                        "DependencyServer": sanitize_for_excel(dep_server or target_server),
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
                if dep_key == parent_key:
                    continue
                referenced_obj = self.object_catalog.get(dep_key)
                resolved_type = referenced_obj.object_type if referenced_obj else dep_type
                dependency_level = referenced_obj.level if referenced_obj else ""
                inferred_scope = classify_dependency_scope(
                    obj.server,
                    obj.database,
                    target_server,
                    target_db,
                )
                rows.append(
                    {
                        "ObjectServer": sanitize_for_excel(obj.server),
                        "ObjectDatabase": sanitize_for_excel(obj.database),
                        "ObjectSchema": sanitize_for_excel(obj.object_schema),
                        "ObjectName": sanitize_for_excel(obj.object_name),
                        "ObjectType": sanitize_for_excel(obj.object_type),
                        "ObjectExtractionLevel": obj.level,
                        "DependencyType": sanitize_for_excel(resolved_type),
                        "DependencyServer": sanitize_for_excel(dep_server or target_server),
                        "Server estratto": sanitize_for_excel(target_server),
                        "DependencyDatabase": sanitize_for_excel(target_db),
                        "DependencySchema": sanitize_for_excel(schema_norm),
                        "DependencyName": sanitize_for_excel(dep_name),
                        "DependencyExtractionLevel": dependency_level,
                        "DependencyScope": inferred_scope,
                    }
                )
        return rows

    def _root_keys_for_file(self, file_name: str) -> List[ObjectKey]:
        keys: List[ObjectKey] = []
        for root in self.lineage_roots:
            if root.get("FileName") != file_name:
                continue
            raw_key = root.get("RootObjectKey")
            if (
                isinstance(raw_key, tuple)
                and len(raw_key) == 4
                and all(isinstance(part, str) for part in raw_key)
            ):
                keys.append(cast(ObjectKey, raw_key))
        return keys

    def _collect_reachable_object_keys(self, root_keys: Iterable[ObjectKey]) -> Set[ObjectKey]:
        max_depth = max(1, self.args.max_depth)
        visited: Set[ObjectKey] = set()
        stack: List[Tuple[ObjectKey, int]] = [(key, 1) for key in root_keys]
        while stack:
            current_key, depth = stack.pop()
            if current_key in visited:
                continue
            visited.add(current_key)
            current_obj = self.object_catalog.get(current_key)
            if current_obj is None:
                continue
            if depth >= max_depth:
                continue
            for dep_server, dep_db, dep_schema, dep_name, _ in current_obj.dep_objects:
                target_server = dep_server or current_obj.server
                target_db = dep_db or current_obj.database
                schema_norm = (dep_schema or "dbo").strip() or "dbo"
                dep_key = make_object_key(target_server, target_db, schema_norm, dep_name)
                if dep_key not in visited:
                    stack.append((dep_key, depth + 1))
        return visited

    def export_per_file_lineage(
        self,
        report_df: pd.DataFrame,
        dep_df: pd.DataFrame,
        path_df: pd.DataFrame,
    ) -> None:
        if report_df.empty:
            self.log_progress("Export per-file saltato: nessun FileName disponibile")
            return

        per_file_dir = Path(self.args.report_output).parent / "lineage_by_file"
        per_file_dir.mkdir(parents=True, exist_ok=True)

        dep_object_keys: Optional[pd.Series] = None
        if not dep_df.empty:
            dep_object_keys = dep_df.apply(
                lambda row: make_object_key_from_values(
                    row.get("ObjectServer"),
                    row.get("ObjectDatabase"),
                    row.get("ObjectSchema"),
                    row.get("ObjectName"),
                ),
                axis=1,
            )

        file_names = report_df["FileName"].fillna("")
        unique_files = sorted(file_names.unique())
        path_file_names: Optional[pd.Series] = None
        if not path_df.empty:
            path_file_names = path_df["FileName"].fillna("")

        for file_name in unique_files:
            file_report = report_df[file_names == file_name]
            root_keys = self._root_keys_for_file(file_name)
            reachable_keys = self._collect_reachable_object_keys(root_keys) if root_keys else set()

            if dep_object_keys is not None and reachable_keys:
                dep_mask = dep_object_keys.isin(reachable_keys)
                file_dep = dep_df[dep_mask]
            else:
                file_dep = dep_df.iloc[0:0]

            if path_file_names is None:
                file_paths = path_df
            else:
                file_paths = path_df[path_file_names == file_name]

            safe_file_name = sanitize_for_filename(file_name or "senzanome")
            file_output = per_file_dir / f"Lineage_{safe_file_name}.xlsx"
            with pd.ExcelWriter(file_output) as writer:
                file_report.to_excel(writer, sheet_name="ReportLineage", index=False)
                file_dep.to_excel(writer, sheet_name="DependencyEdges", index=False)
                total_rows = len(file_paths)
                if total_rows <= MAX_EXCEL_ROWS:
                    file_paths.to_excel(writer, sheet_name="LineagePaths", index=False)
                else:
                    chunk_index = 1
                    for start in range(0, total_rows, MAX_EXCEL_ROWS):
                        end = min(start + MAX_EXCEL_ROWS, total_rows)
                        chunk = file_paths.iloc[start:end]
                        sheet_label = f"LineagePaths_{chunk_index}"
                        chunk.to_excel(writer, sheet_name=sheet_label, index=False)
                        chunk_index += 1
                    self.log_progress(
                        "LineagePaths per FileName '"
                        + (file_name or "(vuoto)")
                        + f"' suddiviso in {chunk_index - 1} fogli da max {MAX_EXCEL_ROWS} righe"
                    )
                write_guide_sheet(writer, REPORT_GUIDE)
            self.log_progress(
                f"Lineage per FileName '{file_name or '(vuoto)'}' -> {file_output}"
            )

    def export_outputs(self, dep_rows: List[Dict[str, object]]) -> None:
        args = self.args
        report_df = pd.DataFrame(self.report_records, columns=REPORT_COLUMNS)
        dep_df = pd.DataFrame(dep_rows, columns=DEPENDENCY_COLUMNS)

        path_rows, path_columns = generate_lineage_paths(
            self.lineage_roots,
            self.object_catalog,
            args.max_depth,
        )
        path_df = pd.DataFrame(path_rows, columns=path_columns)
        self.log_progress(
            "Export LineagePaths aggregato disattivato: mantenuti solo i workbook per FileName"
        )

        self.export_per_file_lineage(report_df, dep_df, path_df)

        report_path = Path(args.report_output)
        export_single_sheet_with_guide(report_df, report_path, "ReportLineage", REPORT_GUIDE)
        self.log_progress(f"Report→Oggetto: {len(self.report_records)} righe -> {report_path}")

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
        self.log_progress(f"Dipendenze normalizzate: {len(dep_rows)} righe -> {dep_path}")

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
            dep_metrics_df.loc[:, "DependencyServer"] = (
                dep_metrics_df["DependencyServer"].fillna("").astype(str).str.strip()
            )
            server_extracted = dep_metrics_df["Server estratto"].fillna("").astype(str).str.strip()
            target_server = dep_metrics_df["DependencyServer"].where(
                dep_metrics_df["DependencyServer"] != "",
                server_extracted,
            )
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
            for obj in self.object_catalog.values()
        ]
        obj_df = pd.DataFrame(obj_rows, columns=OBJECT_CATALOG_COLUMNS)
        obj_path = Path(args.object_output)
        export_single_sheet_with_guide(obj_df, obj_path, "Catalogo", CATALOG_GUIDE)
        self.log_progress(f"Catalogo oggetti: {len(obj_rows)} elementi -> {obj_path}")

        failure_df = pd.DataFrame(self.failure_records, columns=FAILURE_COLUMNS)
        if not failure_df.empty:
            failure_path = Path(args.failure_log)
            export_single_sheet_with_guide(failure_df, failure_path, "Fallimenti", FAILURE_GUIDE)
            self.log_progress(f"Log fallimenti lineage: {len(self.failure_records)} righe -> {failure_path}")
        else:
            self.log_progress("Nessun fallimento di lineage da registrare")

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

        total_objects = len(self.object_catalog)
        max_level = max((obj.level for obj in self.object_catalog.values()), default=0)
        total_dep_tables = sum(len(obj.dep_tables) for obj in self.object_catalog.values())
        total_dep_objects = sum(len(obj.dep_objects) for obj in self.object_catalog.values())
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
            {"Metric": "Fallimenti lineage", "Value": len(self.failure_records)},
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
        self.log_progress(f"Metriche riepilogative -> {summary_path}")
        self.log_progress("Script completato")

    def run(self) -> None:
        if self.total_rows == 0:
            self.log_progress("Nessuna riga presente nel file di input, nulla da elaborare")
            return

        self.log_progress(f"Avvio lineage per {self.total_rows} righe da {self.excel_path}")

        try:
            self.process_input_rows()
            self.expand_dependencies()

            if not self.report_records:
                self.log_progress("Nessun oggetto derivato individuato")
                return

            dep_rows = self.build_dependency_rows()
            self.export_outputs(dep_rows)
        finally:
            self.pool.close()


def main() -> None:
    args = parse_args()
    excel_path = Path(args.excel)
    if not excel_path.exists():
        raise FileNotFoundError(f"File non trovato: {excel_path}")

    df = pd.read_excel(excel_path, sheet_name=args.sheet)
    df.rename(columns=lambda c: str(c).strip(), inplace=True)

    runner = LineageRunner(args, df, excel_path)
    runner.run()


if __name__ == "__main__":
    main()
