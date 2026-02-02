"""
Esegue la scoperta ricorsiva delle dipendenze SQL (SP, funzioni, viste, trigger)
a partire dalle tabelle elencate in un file Excel (colonne Schema.Table e Join e SubQuery).

- Legge l'Excel e normalizza la lista di tabelle (rimuove duplicati e spazi).
- Interroga sys.sql_expression_dependencies per trovare oggetti che referenziano tali tabelle.
- Espande ricorsivamente le dipendenze fino a max_depth (default 5).
- Salva l'output in un Excel con le colonne:
    ObjectName, ObjectType, DependsOn, Depth, OriginTable, Path
"""

import argparse
import re
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import pyodbc

# ========================
# CONFIG
# ========================
DEFAULT_EXCEL = rf"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\7. Reverse engingeering\Lineage completo\Filepath+tabelle_dirette.xlsx"
DEFAULT_SHEET = 0           # oppure il nome del foglio
DEFAULT_SERVER = "EPCP3"
DEFAULT_DATABASE = "CORESQL7"
DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"
DEFAULT_MAX_DEPTH = 5
DEFAULT_OUTPUT = "lineage_layers.xlsx"


@dataclass(frozen=True)
class OriginContext:
    file_name: str
    server: str
    database: str
    origin_schema: str
    origin_name: str
    origin_display: str
    origin_key: str


@dataclass(frozen=True)
class LineageEdge:
    depth: int
    parent_schema: str
    parent_name: str
    parent_type: str
    child_schema: str
    child_name: str
    child_type: str


def get_connection(server: str, database: str, driver: str) -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str, timeout=15)


def parse_schema_table(raw_value: str) -> Tuple[str, str, str]:
    """Restituisce schema, nome (in uppercase originale) e display completo."""
    if not raw_value or str(raw_value).strip() in {"", "nan", "None"}:
        return "", "", ""
    raw_value = str(raw_value).strip()
    if "." in raw_value:
        schema, name = raw_value.split(".", 1)
    else:
        schema, name = "dbo", raw_value
    return schema.strip(), name.strip(), f"{schema.strip()}.{name.strip()}"


def extract_contexts_from_excel(path: Path, sheet) -> List[OriginContext]:
    df = pd.read_excel(path, sheet_name=sheet)
    df.rename(columns=lambda c: str(c).strip(), inplace=True)
    contexts: List[OriginContext] = []

    def append_context(raw_table: str, row: pd.Series) -> None:
        schema, name, display = parse_schema_table(raw_table)
        if not display:
            return
        file_name = str(row.get("File_name", "") or "").strip()
        server = str(row.get("Server", "") or "").strip() or DEFAULT_SERVER
        database = str(row.get("Database", "") or "").strip() or DEFAULT_DATABASE
        origin_key = f"{schema.lower()}::{name.lower()}"
        contexts.append(
            OriginContext(
                file_name=file_name,
                server=server,
                database=database,
                origin_schema=schema,
                origin_name=name,
                origin_display=display,
                origin_key=origin_key,
            )
        )

    for _, row in df.iterrows():
        seen_values = set()

        def try_append(value: str) -> None:
            normalized = (value or "").strip()
            if not normalized:
                return
            if normalized in seen_values:
                return
            seen_values.add(normalized)
            append_context(normalized, row)

        try_append(row.get("Schema.Table"))
        try_append(row.get("Tabella origine"))

        join_values = str(row.get("Join e SubQuery", "") or "")
        if join_values and join_values.lower() != "nan":
            for part in re.split(r"[;,]\s*", join_values):
                try_append(part)

    return contexts


def fetch_referencing_objects(
    conn: pyodbc.Connection, targets: List[Tuple[str, str]]
) -> List[Tuple[str, str, str]]:
    """
    Restituisce (schema, nome, type_desc) degli oggetti che referenziano
    gli elementi indicati da targets (lista di tuple schema/nome).
    """
    if not targets:
        return []

    conditions = []
    params: List[str] = []
    for schema, name in targets:
        conditions.append("(ISNULL(d.referenced_schema_name, 'dbo') = ? AND d.referenced_entity_name = ?)")
        params.extend([schema or 'dbo', name])

    where_clause = " OR ".join(conditions)
    query = f"""
        SELECT DISTINCT
            SCHEMA_NAME(o.schema_id) AS referencing_schema,
            o.name AS referencing_object,
            o.type_desc
        FROM sys.sql_expression_dependencies d
        JOIN sys.objects o ON d.referencing_id = o.object_id
        WHERE {where_clause}
    """

    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()
    return [(row[0] or 'dbo', row[1], row[2]) for row in rows]


def discover_layers_for_origin(
    conn: pyodbc.Connection,
    origin_schema: str,
    origin_name: str,
    max_depth: int,
) -> List[LineageEdge]:
    """Ritorna lista di archi (parent -> child) con profondità."""
    edges: List[LineageEdge] = []
    queue = deque()
    queue.append((origin_schema, origin_name, 'TABLE', 0))
    visited_nodes = set()

    while queue:
        parent_schema, parent_name, parent_type, depth = queue.popleft()
        if depth >= max_depth:
            continue
        targets = [(parent_schema, parent_name)]
        refs = fetch_referencing_objects(conn, targets)
        for child_schema, child_name, child_type in refs:
            child_key = (child_schema.lower(), child_name.lower(), depth + 1)
            edge = LineageEdge(
                depth=depth + 1,
                parent_schema=parent_schema,
                parent_name=parent_name,
                parent_type=parent_type,
                child_schema=child_schema,
                child_name=child_name,
                child_type=child_type,
            )
            edges.append(edge)
            if child_key in visited_nodes:
                continue
            visited_nodes.add(child_key)
            queue.append((child_schema, child_name, child_type, depth + 1))
    return edges


def parse_args():
    parser = argparse.ArgumentParser(
        description="Lineage ricorsivo da Excel (Schema.Table + Join/Subquery)"
    )
    parser.add_argument("--excel", default=DEFAULT_EXCEL)
    parser.add_argument("--sheet", default=DEFAULT_SHEET)
    parser.add_argument("--server", default=DEFAULT_SERVER)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--driver", default=DEFAULT_DRIVER)
    parser.add_argument("--max-depth", type=int, default=DEFAULT_MAX_DEPTH)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def build_excel_layers(
    contexts: List[OriginContext],
    edges_map: Dict[str, List[LineageEdge]],
    max_depth: int,
    output_path: Path,
):
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Sheet L1 (per file)
        l1_rows: List[Dict[str, str]] = []
        for ctx in contexts:
            edges = edges_map.get(ctx.origin_key, [])
            for edge in edges:
                if edge.depth != 1:
                    continue
                l1_rows.append({
                    'FileName': ctx.file_name,
                    'Server': ctx.server,
                    'Database': ctx.database,
                    'Tabella origine': ctx.origin_display,
                    'Object L1': f"{edge.child_schema}.{edge.child_name}",
                    'Object L1 Type': edge.child_type,
                })
        l1_df = pd.DataFrame(l1_rows, columns=[
            'FileName', 'Server', 'Database', 'Tabella origine', 'Object L1', 'Object L1 Type'
        ])
        l1_df.to_excel(writer, sheet_name='L1', index=False)

        # Sheets L2..Ln
        for depth in range(2, max_depth + 1):
            rows = []
            for edges in edges_map.values():
                for edge in edges:
                    if edge.depth == depth:
                        rows.append({
                            f'Object L{depth-1}': f"{edge.parent_schema}.{edge.parent_name}",
                            f'Object L{depth-1} Type': edge.parent_type,
                            f'Object L{depth}': f"{edge.child_schema}.{edge.child_name}",
                            f'Object L{depth} Type': edge.child_type,
                        })
            if rows:
                pd.DataFrame(rows).to_excel(writer, sheet_name=f'L{depth}', index=False)

    print(f"Report Excel generato: {output_path}")


def main():
    args = parse_args()
    excel_path = Path(args.excel)
    if not excel_path.exists():
        raise FileNotFoundError(f"Excel non trovato: {excel_path}")

    print("[1/4] Lettura Excel e normalizzazione tabelle...")
    contexts = extract_contexts_from_excel(excel_path, args.sheet)
    if not contexts:
        print("[WARN] Nessuna tabella trovata nel file.")
        return
    print(f"   Righe contestuali: {len(contexts)}")

    unique_origins: Dict[str, Tuple[str, str]] = {}
    for ctx in contexts:
        unique_origins.setdefault(ctx.origin_key, (ctx.origin_schema, ctx.origin_name))

    print(f"   Tabelle uniche da analizzare: {len(unique_origins)}")

    print("[2/4] Connessione a SQL Server...")
    conn = get_connection(args.server, args.database, args.driver)

    edges_map: Dict[str, List[LineageEdge]] = {}
    max_depth_found = 0
    for key, (schema, name) in unique_origins.items():
        edges = discover_layers_for_origin(conn, schema, name, args.max_depth)
        if edges:
            max_depth_found = max(max_depth_found, max(edge.depth for edge in edges))
        edges_map[key] = edges

    conn.close()

    if max_depth_found == 0:
        print("[WARN] Nessuna dipendenza trovata.")
    else:
        print(f"   Profondita massima trovata: L{max_depth_found}")

    print("[3/4] Salvataggio risultati...")
    resolved_max_depth = max_depth_found if int(max_depth_found) > 0 else int(args.max_depth)
    build_excel_layers(
        contexts,
        edges_map,
        resolved_max_depth,
        Path(args.output),
    )

    print("[4/4] Completato!")


if __name__ == "__main__":
    main()