"""Print row counts per SourceLayer for Lineage_All."""
from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "Dz3_Analysis.db"

conn = sqlite3.connect(DB_PATH)
try:
    cur = conn.cursor()
    cur.execute(
        "SELECT SourceLayer, COUNT(*) FROM Lineage_All GROUP BY SourceLayer ORDER BY SourceLayer"
    )
    for layer, count in cur.fetchall():
        print(f"{layer}: {count}")
finally:
    conn.close()
