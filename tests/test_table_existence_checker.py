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

import analisi_viste.Table_Existence_Checker as mod
from analisi_viste.Table_Existence_Checker import TableExistenceChecker


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
    assert list(df.columns) == ["Server", "DB", "Schema", "Table", "Error"]
    assert len(df) == 1
    row = df.iloc[0].to_dict()
    assert row["Server"] == "EPCP3"
    assert row["DB"] == "db1"
    assert row["Schema"] == "dbo"
    assert row["Table"] == "t1"
    # Celle vuote possono tornare NaN da read_excel
    assert pd.isna(row["Error"]) or str(row["Error"]) == ""


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
    # Celle vuote possono tornare NaN da read_excel
    assert pd.isna(df.iloc[0]["Error"]) or str(df.iloc[0]["Error"]) == ""


def test_checker_no_matches_writes_not_found_rows(tmp_path):
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
    # Now we expect a row with an error message for not found items
    assert len(df) == 1
    row = df.iloc[0]
    # DB blank when no DB specified
    assert (pd.isna(row["DB"]) or str(row["DB"]).strip() == "")
    assert str(row["Table"]).lower() == "tx"
    assert "trovata" in str(row["Error"]).lower()


def test_checker_includes_views(tmp_path, monkeypatch):
    # Input cerca una vista chiamata v1
    xlsx_in = tmp_path / "input.xlsx"
    _write_input_excel(
        str(xlsx_in),
        rows=[["v1"]],
        header=["Table"],
    )

    out_xlsx = tmp_path / "out.xlsx"

    # Ensure module has a pyodbc-like placeholder to pass __init__
    if getattr(mod, "pyodbc", None) is None:
        mod.pyodbc = types.SimpleNamespace(connect=lambda *a, **k: None)

    # Abilita ricerca viste (semantico per il test; logica mockata)
    try:
        import importlib
        import analisi_viste.Table_Existence_Checker as checker_mod
        checker_mod.INCLUDE_VIEWS = True
    except Exception:
        pass

    def fake_list_dbs(self):
        return ["db1"]

    # Simula che il DB contenga una vista v1 nello schema dbo
    def fake_fetch_objects(self, db: str):
        return {("dbo", "v1")}

    TableExistenceChecker._list_user_databases = fake_list_dbs  # type: ignore
    TableExistenceChecker._fetch_tables_in_db = fake_fetch_objects  # type: ignore

    checker = TableExistenceChecker(str(xlsx_in), str(out_xlsx), server="EPCP3")
    out = checker.run()
    assert os.path.exists(out)
    df = pd.read_excel(out, sheet_name="Tabelle")
    assert len(df) == 1
    assert df.iloc[0]["DB"] == "db1"
    assert df.iloc[0]["Schema"].lower() == "dbo"
    assert df.iloc[0]["Table"].lower() == "v1"
