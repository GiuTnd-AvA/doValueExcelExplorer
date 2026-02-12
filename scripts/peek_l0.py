from __future__ import annotations
import csv
from pathlib import Path

fp = Path(r"C:/Users/ciro.andreano/Desktop/CSV Lineage/L0.csv")
with fp.open(encoding="utf-8-sig", newline="") as f:
    reader = csv.reader(f, delimiter=";")
    header = next(reader)
    print("Header:")
    print(header)
    print("Rows:")
    for _ in range(5):
        try:
            print(next(reader))
        except StopIteration:
            break
