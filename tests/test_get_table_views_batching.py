import os
import sys
import types
import pandas as pd
from openpyxl import Workbook

# Ensure workspace root in path
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import Get_Table_Views_From_Excel as mod
from Get_Table_Views_From_Excel import TableViewsExtractor


def _write_input_excel(path, rows, header):
    wb = Workbook()
    ws = wb.active
    ws.append(header)
    for r in rows:
        ws.append(r)
    wb.save(path)


def test_partial_files_are_non_overlapping(tmp_path, monkeypatch):
    # Prepare input with 5 items
    xlsx_in = tmp_path / "input.xlsx"
    rows = [["EPCP3", "db", "dbo", f"t{i}"] for i in range(1, 6)]
    _write_input_excel(str(xlsx_in), rows, ["Server", "DB", "Schema", "Table"])

    out_xlsx = tmp_path / "out.xlsx"

    # Reduce batch size for test and avoid real DB calls
    monkeypatch.setattr(mod, "PARTIAL_SAVE_EVERY", 2, raising=False)

    # Stub pyodbc to bypass real connections
    if getattr(mod, "pyodbc", None) is None:
        monkeypatch.setattr(mod, "pyodbc", types.SimpleNamespace(connect=lambda *a, **k: types.SimpleNamespace(close=lambda: None)), raising=False)

    # Avoid using the connection in fetch; return one fake view per table
    def fake_fetch(self, conn, schema, table):
        return [(f"v_{table}", f"SELECT * FROM {schema}.{table}")]

    monkeypatch.setattr(TableViewsExtractor, "_fetch_views_for_table", fake_fetch, raising=False)

    # Also avoid real driver probing
    monkeypatch.setattr(TableViewsExtractor, "_build_conn_str", lambda self, s, d: "DRIVER={stub};", raising=False)

    # Monkeypatch connect to return a dummy object with close
    monkeypatch.setattr(mod.pyodbc, "connect", lambda *a, **k: types.SimpleNamespace(close=lambda: None), raising=False)

    extractor = TableViewsExtractor(str(xlsx_in), str(out_xlsx))
    extractor.run()

    base_dir = tmp_path
    base_name = os.path.splitext(os.path.basename(str(out_xlsx)))[0]

    p2 = base_dir / f"{base_name}_partial_2.xlsx"
    p4 = base_dir / f"{base_name}_partial_4.xlsx"

    assert os.path.exists(p2)
    assert os.path.exists(p4)

    df2 = pd.read_excel(p2, sheet_name="Viste")
    df4 = pd.read_excel(p4, sheet_name="Viste")

    # Each view corresponds to exactly one table
    assert set(df2["Table"].str.lower()) == {"t1", "t2"}
    assert set(df4["Table"].str.lower()) == {"t3", "t4"}

    # Ensure no overlap between partials
    assert set(df2["Table"].str.lower()).isdisjoint(set(df4["Table"].str.lower()))
