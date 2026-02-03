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
    r"\bfrom\s+{target}(?:\s|\(|$)",
    r"\bjoin\s+{target}(?:\s|\(|$)",
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


def split_origin(value: str) -> Tuple[str, str]:
    text = normalize_identifier(value)
    if not text:
        return "", ""
    if "." in text:
        schema, table = text.split(".", 1)
    else:
        schema, table = "", text
    return schema.strip(), table.strip()


def build_table_variants(schema: str, table: str) -> List[str]:
    table = normalize_identifier(table).lower()
    schema = normalize_identifier(schema).lower()
    variants = []
    if not table:
        return variants
    variants.append(rf"(?<!\w){re.escape(table)}(?!\w)")
    variants.append(rf"\[\s*{re.escape(table)}\s*\]")
    if schema:
        variants.append(
            rf"(?<!\w){re.escape(schema)}\s*\.\s*{re.escape(table)}(?!\w)"
        )
        variants.append(
            rf"\[\s*{re.escape(schema)}\s*\]\s*\.\s*\[\s*{re.escape(table)}\s*\]"
        )
    return variants


def matches_any(template_list: Iterable[str], variants: Iterable[str], sql: str) -> bool:
    for template in template_list:
        for variant in variants:
            pattern = template.format(target=variant)
            if re.search(pattern, sql, flags=re.IGNORECASE | re.DOTALL):
                return True
    return False


def classify(sql_definition: str, origin_table: str) -> str:
    schema, table = split_origin(origin_table)
    if not table:
        return "Non rilevato"
    sql = (sql_definition or "").lower()
    variants = build_table_variants(schema, table)
    if not variants:
        return "Non rilevato"
    if matches_any(WRITE_TEMPLATES, variants, sql):
        return "Scrittura"
    if matches_any(READ_TEMPLATES, variants, sql):
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
