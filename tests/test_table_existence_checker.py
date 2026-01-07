import os
import sys
import types
import pandas as pd
from openpyxl import Workbook

# Ensure workspace root and BusinessLogic in path
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
BL = os.path.join(ROOT, "BusinessLogic")
if BL not in sys.path:
    sys.path.insert(0, BL)

import Table_Existence_Checker as mod
from Table_Existence_Checker import TableExistenceChecker


def _write_input_excel(path, rows, header):
    wb = Workbook()
    ws = wb.active
    ws.append(header)
    for r in rows:
        ws.append(r)
    wb.save(path)


def test_checker_with_db_and_schema(tmp_path):
    # Prepare input with explicit DB and schema
    xlsx_in = tmp_path / "input.xlsx"
    _write_input_excel(
        str(xlsx_in),
        rows=[["db1", "dbo", "t1"]],
        header=["DB", "Schema", "Table"],
    )

    out_xlsx = tmp_path / "out.xlsx"

    # Ensure module has a pyodbc-like placeholder to pass __init__
    if getattr(mod, "pyodbc", None) is None:
        mod.pyodbc = types.SimpleNamespace(connect=lambda *a, **k: None)

    # Patch fetching to avoid real DB
    def fake_fetch_tables(self, db: str):
        if db == "db1":
            return {("dbo", "t1"), ("sales", "t2")}
        return set()

    TableExistenceChecker._fetch_tables_in_db = fake_fetch_tables  # type: ignore

    checker = TableExistenceChecker(str(xlsx_in), str(out_xlsx), server="EPCP3")
    out = checker.run()
    assert os.path.exists(out)
    df = pd.read_excel(out, sheet_name="Tabelle")
    assert list(df.columns) == ["Server", "DB", "Schema", "Table"]
    assert len(df) == 1
    assert df.iloc[0].to_dict() == {
        "Server": "EPCP3",
        "DB": "db1",
        "Schema": "dbo",
        "Table": "t1",
    }


def test_checker_without_db_and_schema(tmp_path):
    # Only table name provided; no schema or db
    xlsx_in = tmp_path / "input.xlsx"
    _write_input_excel(
        str(xlsx_in),
        rows=[["t1"]],
        header=["Table"],
    )

    out_xlsx = tmp_path / "out.xlsx"

    if getattr(mod, "pyodbc", None) is None:
        mod.pyodbc = types.SimpleNamespace(connect=lambda *a, **k: None)

    def fake_list_dbs(self):
        return ["db1", "db2"]

    def fake_fetch_tables(self, db: str):
        if db == "db1":
            return {("dbo", "t1")}
        return set()

    TableExistenceChecker._list_user_databases = fake_list_dbs  # type: ignore
    TableExistenceChecker._fetch_tables_in_db = fake_fetch_tables  # type: ignore

    checker = TableExistenceChecker(str(xlsx_in), str(out_xlsx), server="EPCP3")
    out = checker.run()
    assert os.path.exists(out)
    df = pd.read_excel(out, sheet_name="Tabelle")
    assert len(df) == 1
    assert df.iloc[0]["DB"] == "db1"
    assert df.iloc[0]["Schema"].lower() == "dbo"
    assert df.iloc[0]["Table"].lower() == "t1"


def test_checker_no_matches_writes_empty_excel(tmp_path):
    xlsx_in = tmp_path / "input.xlsx"
    _write_input_excel(
        str(xlsx_in),
        rows=[["tX"]],
        header=["Table"],
    )

    out_xlsx = tmp_path / "out.xlsx"

    if getattr(mod, "pyodbc", None) is None:
        mod.pyodbc = types.SimpleNamespace(connect=lambda *a, **k: None)

    def fake_list_dbs(self):
        return ["db1", "db2"]

    def fake_fetch_tables(self, db: str):
        return {("dbo", "t1")}

    TableExistenceChecker._list_user_databases = fake_list_dbs  # type: ignore
    TableExistenceChecker._fetch_tables_in_db = fake_fetch_tables  # type: ignore

    checker = TableExistenceChecker(str(xlsx_in), str(out_xlsx), server="EPCP3")
    out = checker.run()
    assert os.path.exists(out)
    df = pd.read_excel(out, sheet_name="Tabelle")
    # No matches expected
    assert len(df) == 0
