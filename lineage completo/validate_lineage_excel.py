"""Validate lineage Excel workbook for missing or inconsistent values.

The script inspects each row of the lineage catalogue and flags:
- missing mandatory metadata (report path, server, schema, table, etc.)
- table rows without any derived SQL object information
- mismatch between dependency lists and their recorded counts
- invalid "Motivo" values
- duplicated table definitions

Usage
=====
python validate_lineage_excel.py --excel <path> [--sheet Sheet1] [--report anomalies.xlsx]

The generated report contains one row per anomaly with the Excel row
number, the involved table/view, and the specific issue detected.
"""

from __future__ import annotations

import argparse
import re
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

MANDATORY_COLUMNS = [
    "Path_File",
    "File_name",
    "Server",
    "Database",
    "Schema",
    "Table",
]
OBJECT_COLUMNS = [
    "oggetti_totali7.Database",
    "oggetti_totali7.Schema",
    "oggetti_totali7.ObjectName",
    "oggetti_totali7.ObjectType",
    "oggetti_totali7.SQLDefinition",
]
COUNT_COLUMNS = [
    (
        "oggetti_totali7.Dipendenze_Tabella",
        "oggetti_totali7.Count_Dipendenza_Tabella",
    ),
    (
        "oggetti_totali7.Dipendenze_Oggetto",
        "oggetti_totali7.Count_Dipendenza_Oggetto",
    ),
]
VALID_MOTIVO = {"", "Lettura", "Scrittura", "Non rilevato", "Sconosciuto"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trova valori mancanti o incoerenti nel lineage Excel")
    parser.add_argument("--excel", required=True, help="Percorso del file Excel da validare")
    parser.add_argument("--sheet", default=0, help="Indice o nome del foglio da analizzare")
    parser.add_argument(
        "--report",
        default=None,
        help="Percorso opzionale per esportare il report delle anomalie (CSV o XLSX)",
    )
    return parser.parse_args()


def normalize(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "nat", "none"}:
        return ""
    return text


def split_multi(value: object) -> List[str]:
    text = normalize(value)
    if not text:
        return []
    parts = re.split(r"[;\n]+", text)
    return [part.strip() for part in parts if part.strip()]


def check_missing_columns(df: pd.DataFrame) -> List[str]:
    missing = [col for col in MANDATORY_COLUMNS + OBJECT_COLUMNS if col not in df.columns]
    return missing


def validate_row(row: pd.Series, excel_row: int) -> List[Dict[str, str]]:
    issues: List[Dict[str, str]] = []
    missing_base = [col for col in MANDATORY_COLUMNS if not normalize(row.get(col))]
    if missing_base:
        issues.append(
            {
                "Row": str(excel_row),
                "Table": f"{normalize(row.get('Schema'))}.{normalize(row.get('Table'))}",
                "Issue": f"Campi base mancanti: {', '.join(missing_base)}",
            }
        )

    has_table = normalize(row.get("Table")) != ""
    missing_objects = [col for col in OBJECT_COLUMNS if not normalize(row.get(col))]
    if has_table and len(missing_objects) == len(OBJECT_COLUMNS):
        issues.append(
            {
                "Row": str(excel_row),
                "Table": f"{normalize(row.get('Schema'))}.{normalize(row.get('Table'))}",
                "Issue": "Tabella senza oggetti derivati",
            }
        )

    for list_col, count_col in COUNT_COLUMNS:
        values = split_multi(row.get(list_col))
        count_value = normalize(row.get(count_col))
        if not values and not count_value:
            continue
        try:
            expected = int(count_value) if count_value else 0
        except ValueError:
            issues.append(
                {
                    "Row": str(excel_row),
                    "Table": f"{normalize(row.get('Schema'))}.{normalize(row.get('Table'))}",
                    "Issue": f"Valore non numerico in {count_col}: {count_value}",
                }
            )
            continue
        if expected != len(values):
            issues.append(
                {
                    "Row": str(excel_row),
                    "Table": f"{normalize(row.get('Schema'))}.{normalize(row.get('Table'))}",
                    "Issue": f"Mismatch tra {list_col} ({len(values)}) e {count_col} ({expected})",
                }
            )

    motivo = normalize(row.get("oggetti_totali7.Motivo"))
    if motivo and motivo not in VALID_MOTIVO:
        issues.append(
            {
                "Row": str(excel_row),
                "Table": f"{normalize(row.get('Schema'))}.{normalize(row.get('Table'))}",
                "Issue": f"Motivo non valido: {motivo}",
            }
        )

    return issues


def detect_duplicates(df: pd.DataFrame) -> List[Dict[str, str]]:
    subset = ["Database", "Schema", "Table"]
    missing_subset = [col for col in subset if col not in df.columns]
    if missing_subset:
        return []
    duplicated_mask = df.duplicated(subset=subset, keep=False)
    issues: List[Dict[str, str]] = []
    for idx in df[duplicated_mask].index:
        row = df.loc[idx]
        issues.append(
            {
                "Row": str(idx + 2),
                "Table": f"{normalize(row.get('Schema'))}.{normalize(row.get('Table'))}",
                "Issue": "Definizione duplicata per Database/Schema/Tabella",
            }
        )
    return issues


def write_report(anomalies: List[Dict[str, str]], report_path: Path) -> None:
    df_report = pd.DataFrame(anomalies)
    if report_path.suffix.lower() == ".xlsx":
        df_report.to_excel(report_path, index=False)
    else:
        df_report.to_csv(report_path, index=False)


def main() -> None:
    args = parse_args()
    excel_path = Path(args.excel)
    if not excel_path.exists():
        raise FileNotFoundError(f"File non trovato: {excel_path}")

    df = pd.read_excel(excel_path, sheet_name=args.sheet)
    df.rename(columns=lambda c: str(c).strip(), inplace=True)
    missing_columns = check_missing_columns(df)
    if missing_columns:
        print("[AVVISO] Colonne mancanti nel file:", ", ".join(sorted(missing_columns)))

    anomalies: List[Dict[str, str]] = []
    for idx, row in df.iterrows():
        excel_row = idx + 2  # header occupies first row
        anomalies.extend(validate_row(row, excel_row))

    anomalies.extend(detect_duplicates(df))

    summary = Counter(issue["Issue"] for issue in anomalies)
    print(f"Righe analizzate: {len(df)}")
    print(f"Anomalie trovate: {len(anomalies)}")
    for issue, count in summary.most_common():
        print(f" - {issue}: {count}")

    if args.report and anomalies:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        write_report(anomalies, report_path)
        print(f"Report salvato in {report_path}")
    elif args.report:
        print("Nessuna anomalia da salvare: report non creato")


if __name__ == "__main__":
    main()
