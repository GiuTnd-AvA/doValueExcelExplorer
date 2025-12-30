import os
import sys
import types
import shutil

import pandas as pd
from openpyxl import Workbook

# Ensure workspace root in path
ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)

from Get_Table_Views_From_Excel import TableViewsExtractor

class DummyCursor:
    def __init__(self, rows_seq):
        # rows_seq is a list of lists for successive fetchall calls
        self._rows_seq = rows_seq
        self._idx = 0
        self.last_sql = None
        self.last_params = None

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.last_params = params
        return self

    def fetchall(self):
        if self._idx < len(self._rows_seq):
            rows = self._rows_seq[self._idx]
            self._idx += 1
            return rows
        return []

class DummyConn:
    def __init__(self, rows_seq):
        self._rows_seq = rows_seq
    def cursor(self):
        return DummyCursor(self._rows_seq)
    def close(self):
        pass

# Monkeypatch pyodbc in module
import Get_Table_Views_From_Excel as mod
mod.pyodbc = types.SimpleNamespace(connect=lambda conn_str, timeout=3: DummyConn([
    # First query (dependencies) returns two views
    [("v1", "CREATE VIEW v1 AS SELECT 1"), ("v2", "CREATE VIEW v2 AS SELECT 2")]
]))


def test_views_extractor_basic(tmp_path):
    # Prepare input Excel
    xlsx_in = tmp_path / "input.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.append(["Server", "DB", "Schema", "Table"])  # header
    ws.append(["EPCP3", "master", "dbo", "t1"])  # one table
    wb.save(str(xlsx_in))

    out_xlsx = tmp_path / "out.xlsx"

    extractor = TableViewsExtractor(str(xlsx_in), str(out_xlsx))
    out_path = extractor.run()

    # Validate output Excel
    assert os.path.exists(out_path)
    df = pd.read_excel(out_path, sheet_name="Viste")
    assert list(df.columns) == ["Server", "DB", "Schema", "Table", "Object_Name", "Definition"]
    assert len(df) == 2
    assert df.iloc[0]["Object_Name"] == "v1"
    assert df.iloc[1]["Object_Name"] == "v2"
