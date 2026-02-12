"""Load CSV files into Dz3_Analysis SQLite database."""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[1]
DB_PATH = WORKSPACE / "Dz3_Analysis.db"
CSV_DIR = Path(r"C:/Users/ciro.andreano/Desktop/CSV Lineage")
FILES = [
    ("File_name", CSV_DIR / "File_name.csv"),
    ("L0", CSV_DIR / "L0.csv"),
    ("L1", CSV_DIR / "L1.csv"),
    ("L2", CSV_DIR / "L2.csv"),
    ("L3", CSV_DIR / "L3.csv"),
    ("L4", CSV_DIR / "L4.csv"),
    ("L5", CSV_DIR / "L5.csv"),
]


def load_csvs() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        for table_name, file_path in FILES:
            if not file_path.exists():
                raise FileNotFoundError(f"CSV non trovato: {file_path}")

            with file_path.open(newline="", encoding="utf-8-sig") as csvfile:
                reader = csv.reader(csvfile, delimiter=";")
                try:
                    headers = next(reader)
                except StopIteration:
                    headers = []

                columns: list[str] = []
                for idx, header in enumerate(headers):
                    name = header.strip()
                    if not name:
                        name = f"Column_{idx + 1}"
                    columns.append(name)

                conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
                conn.execute(f'CREATE TABLE "{table_name}" ({col_defs})')

                insert_sql = (
                    f'INSERT INTO "{table_name}" '
                    f'({", ".join(f"\"{col}\"" for col in columns)}) '
                    f'VALUES ({", ".join(["?"] * len(columns))})'
                )

                for row in reader:
                    if not row or all(cell == "" for cell in row):
                        continue
                    if len(row) < len(columns):
                        row += [""] * (len(columns) - len(row))
                    elif len(row) > len(columns):
                        row = row[: len(columns)]
                    conn.execute(insert_sql, row)
        conn.commit()
        for table_name, _ in FILES:
            total = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
            print(f"{table_name}: {total} righe caricate")
    finally:
        conn.close()


if __name__ == "__main__":
    load_csvs()
