"""Populate the `Motivo` column by inspecting SQL definitions.

The input Excel must contain the columns:
    OriginTable | ObjectName | SQLDefinition | Motivo

For each row, the script analyses SQLDefinition to determine whether
OriginTable is referenced for write operations (INSERT/UPDATE/DELETE/
MERGE/SELECT INTO) or only read operations (FROM/JOIN). The result is
written back into the Motivo column (values: "Scrittura", "Lettura",
"Non rilevato"). By default the original Excel file is overwritten, but
an optional --output parameter can be provided to write to a new file.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd

DEFAULT_INPUT = (
    r"\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool"
    r"\7. Reverse Engingeering\Lineage completo\Check lettura_scrittura.xlsx"
)
DEFAULT_OUTPUT = DEFAULT_INPUT
REQUIRED_COLUMNS = ["OriginTable", "ObjectName", "SQLDefinition", "Motivo"]
WRITE_TEMPLATES = [
    r"\binsert\s+(?:into\s+)?{target}(?:\s|\(|$)",
    r"\bupdate\s+{target}(?:\s|\()",
    r"\bdelete\s+from\s+{target}(?:\s|\()",
    r"\bmerge\s+(?:into\s+)?{target}(?:\s|\()",
    r"\bselect\s+[^;]*?into\s+{target}(?:\s|\()",
]
READ_TEMPLATES = [
    r"\bfrom\s+{target}(?:\s|\(|,|$)",
    r"\bjoin\s+{target}(?:\s|\(|,|$)",
    r"\bfrom\b[^;]*?,\s*{target}(?:\s|\(|,|$)",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Classify Motivo (Lettura/Scrittura)")
    parser.add_argument("--excel", default=DEFAULT_INPUT, help="Excel file path")
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Output path (defaults to overwriting the input file)",
    )
    parser.add_argument("--sheet", default=0, help="Worksheet index or name")
    return parser.parse_args()


def normalize_identifier(value: str) -> str:
    return re.sub(r"[`\[\]\"]", "", value).strip()


def split_origin(value: str) -> Tuple[str, str, str]:
    text = normalize_identifier(value)
    if not text:
        return "", "", ""
    parts = [part.strip() for part in text.split(".") if part.strip()]
    if not parts:
        return "", "", ""
    if len(parts) == 1:
        return "", "", parts[0]
    if len(parts) == 2:
        return "", parts[0], parts[1]
    return parts[0], parts[1], parts[2]


def _token_variants(token: str) -> List[str]:
    clean = normalize_identifier(token).lower()
    if not clean:
        return []
    escaped = re.escape(clean)
    return [escaped, rf"\[\s*{escaped}\s*\]"]


def _wildcard_token() -> List[str]:
    return [r"(?:\w+|\[[^\]]+\])"]


def build_table_variants(database: str, schema: str, table: str) -> Tuple[List[str], List[str]]:
    table_clean = normalize_identifier(table).lower()
    if not table_clean:
        return [], []

    table_tokens = _token_variants(table_clean)
    bare_variants = [rf"(?<!\w){tok}(?!\w)" for tok in table_tokens]

    schema_tokens = _token_variants(schema) if schema else _wildcard_token()
    database_tokens = _token_variants(database) if database else _wildcard_token()

    qualified_variants: List[str] = []
    for schema_token in schema_tokens:
        for table_token in table_tokens:
            qualified_variants.append(rf"{schema_token}\s*\.\s*{table_token}")

    for database_token in database_tokens:
        for schema_token in schema_tokens:
            for table_token in table_tokens:
                qualified_variants.append(
                    rf"{database_token}\s*\.\s*{schema_token}\s*\.\s*{table_token}"
                )

    return bare_variants + qualified_variants, table_tokens


def matches_any(template_list: Iterable[str], variants: Iterable[str], sql: str) -> bool:
    for template in template_list:
        for variant in variants:
            pattern = template.format(target=variant)
            if re.search(pattern, sql, flags=re.IGNORECASE | re.DOTALL):
                return True
    return False


def matches_column_reference(column_tokens: Iterable[str], sql: str) -> bool:
    for token in column_tokens:
        pattern = rf"{token}\s*\."
        if re.search(pattern, sql, flags=re.IGNORECASE):
            return True
    return False


def normalize_sql(sql_definition: str) -> str:
    if not sql_definition:
        return ""
    return re.sub(r"\s+", " ", sql_definition).lower()


def classify(sql_definition: str, origin_table: str) -> str:
    database, schema, table = split_origin(origin_table)
    if not table:
        return "Non rilevato"
    sql = normalize_sql(sql_definition)
    variants, column_tokens = build_table_variants(database, schema, table)
    if not variants:
        return "Non rilevato"
    if matches_any(WRITE_TEMPLATES, variants, sql):
        return "Scrittura"
    if matches_any(READ_TEMPLATES, variants, sql) or matches_column_reference(column_tokens, sql):
        return "Lettura"
    return "Non rilevato"


def main() -> None:
    args = parse_args()
    input_path = Path(args.excel)
    if not input_path.exists():
        raise FileNotFoundError(f"File non trovato: {input_path}")

    print("[1/3] Caricamento Excel...")
    df = pd.read_excel(input_path, sheet_name=args.sheet)
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Colonne mancanti: {', '.join(missing)}")

    print("[2/3] Classificazione motivi...")
    for idx, row in df.iterrows():
        motivo = classify(str(row.get("SQLDefinition", "")), str(row.get("OriginTable", "")))
        df.at[idx, "Motivo"] = motivo
        if (idx + 1) % 200 == 0:
            print(f"   Processate {idx + 1} righe")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)
    print(f"[3/3] Salvataggio completato: {output_path}")


if __name__ == "__main__":
    main()
