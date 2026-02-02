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
from collections import deque
from pathlib import Path
from typing import List, Set, Tuple

import pandas as pd
import pyodbc

# ========================
# CONFIG
# ========================
DEFAULT_EXCEL = rf"\\dobank\progetti\S1\2025_pg_Unified_Data_Analytics_Tool\7. Reverse engingeering\Lineage completo\Filepath+tabelle_dirette.xlsx"
DEFAULT_SHEET = 0           # oppure il nome del foglio
DEFAULT_SERVER = "EPCP3"
DEFAULT_DATABASE = "CORESQL7"
DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"
DEFAULT_MAX_DEPTH = 5
DEFAULT_OUTPUT = "dipendenze_ricorsive.xlsx"


def get_connection(server: str, database: str, driver: str) -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str, timeout=15)


def normalize_table_name(name: str) -> str:
    """Restituisce solo il nome oggetto (senza schema) in lowercase."""
    if not name:
        return ""
    name = name.strip()
    if "." in name:
        return name.split(".")[-1].strip().lower()
    return name.lower()


def extract_tables_from_excel(path: Path, sheet) -> Set[str]:
    df = pd.read_excel(path, sheet_name=sheet)
    tables = set()

    if "Schema.Table" in df.columns:
        tables.update(
            normalize_table_name(val)
            for val in df["Schema.Table"].dropna().astype(str)
        )

    if "Join e SubQuery" in df.columns:
        for val in df["Join e SubQuery"].dropna().astype(str):
            parts = [normalize_table_name(p) for p in val.split(";")]
            tables.update(filter(None, parts))

    tables.discard("")  # rimuovi eventuali stringhe vuote
    return tables


def fetch_referencing_objects(
    conn: pyodbc.Connection, table_names: List[str]
) -> List[Tuple[str, str, str]]:
    """
    Ritorna (referencing_object, type_desc, referenced_entity) per gli oggetti
    che referenziano una qualunque tabella di table_names.
    """
    if not table_names:
        return []

    placeholders = ",".join("?" for _ in table_names)
    query = f"""
        SELECT DISTINCT
            o.name   AS referencing_object,
            o.type_desc,
            d.referenced_entity_name
        FROM sys.sql_expression_dependencies d
        JOIN sys.objects o ON d.referencing_id = o.object_id
        WHERE d.referenced_entity_name IN ({placeholders})
    """
    cursor = conn.cursor()
    cursor.execute(query, table_names)
    rows = cursor.fetchall()
    cursor.close()
    return [(row[0], row[1], row[2]) for row in rows]


def recursive_dependency_discovery(
    conn: pyodbc.Connection,
    starting_tables: Set[str],
    max_depth: int,
) -> List[Tuple[str, str, str, int, str, str]]:
    """
    Ritorna lista di tuple:
        (object_name, object_type, depends_on, depth, origin_table, path)
    """
    results = []
    visited = set()  # (name_lower, depth)
    queue = deque()

    for tbl in starting_tables:
        queue.append((tbl, 0, tbl, tbl))  # (current, depth, depends_on, path)

    while queue:
        target, depth, origin, path = queue.popleft()
        key = (target.lower(), depth)
        if key in visited or depth > max_depth:
            continue
        visited.add(key)

        refs = fetch_referencing_objects(conn, [target])
        for obj_name, obj_type, referenced in refs:
            obj_name_clean = obj_name.lower()
            results.append(
                (obj_name, obj_type, referenced, depth, origin, path)
            )
            queue.append(
                (
                    obj_name_clean,
                    depth + 1,
                    referenced,
                    f"{path} -> {obj_name_clean}",
                )
            )
    return results


def save_results(records: List[Tuple[str, str, str, int, str, str]], output_path: Path):
    columns = ["ObjectName", "ObjectType", "DependsOn", "Depth", "OriginTable", "Path"]
    df = pd.DataFrame(records, columns=columns)
    df.sort_values(by=["OriginTable", "Depth", "ObjectName"], inplace=True)
    df.to_excel(output_path, index=False)
    print(f"✅ Dipendenze salvate in: {output_path}")


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


def main():
    args = parse_args()
    excel_path = Path(args.excel)
    if not excel_path.exists():
        raise FileNotFoundError(f"Excel non trovato: {excel_path}")

    print("📥 Lettura Excel e normalizzazione tabelle...")
    tables = extract_tables_from_excel(excel_path, args.sheet)
    if not tables:
        print("⚠️ Nessuna tabella trovata nel file.")
        return
    print(f"   Tabelle iniziali: {len(tables)}")

    print("🔌 Connessione a SQL Server...")
    conn = get_connection(args.server, args.database, args.driver)

    print("🔁 Analisi ricorsiva dipendenze...")
    records = recursive_dependency_discovery(conn, tables, args.max_depth)
    print(f"   Oggetti trovati: {len(records)}")

    print("💾 Salvataggio risultati...")
    save_results(records, Path(args.output))

    conn.close()
    print("🏁 Completato!")


if __name__ == "__main__":
    main()