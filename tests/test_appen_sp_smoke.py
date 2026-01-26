import os
from openpyxl import Workbook

# Target script imports
import importlib.util
import sys

# Resolve module path
MODULE_PATH = os.path.join(os.path.dirname(__file__), '..', 'analisi_viste', 'Appen_SP_From_Excel.py')
MODULE_PATH = os.path.abspath(MODULE_PATH)

spec = importlib.util.spec_from_file_location("appen_sp_module", MODULE_PATH)
appen_sp_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(appen_sp_module)  # type: ignore


def make_input_excel(path: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "Foglio1"
    headers = [
        "server",
        "database",
        "schema",
        "Table",
        "Nome Oggetto",
        "Tipo Oggetto",
        "Script Creazione",
    ]
    ws.append(headers)
    # Row 1: full info
    ws.append([
        "SRV1", "DB1", "dbo", "tbl1", "usp_DoThing", "Stored Procedure",
        "CREATE PROCEDURE [dbo].[usp_DoThing] AS SELECT 1;"
    ])
    # Row 2: missing Nome Oggetto but DDL present
    ws.append([
        "SRV1", "DB1", "dbo", "tbl1", "", "PROC",
        "CREATE PROCEDURE dbo.usp_Second AS SELECT 2;"
    ])
    # Row 3: non-proc, should be ignored
    ws.append([
        "SRV1", "DB1", "dbo", "tbl1", "dbo.vwSomething", "view",
        "CREATE VIEW [dbo].[vwSomething] AS SELECT 3;"
    ])
    wb.save(path)


def test_smoke_sp_append(tmp_path):
    input_xlsx = tmp_path / "input_sp.xlsx"
    make_input_excel(str(input_xlsx))

    output_txt = tmp_path / "SP_Append.txt"
    app = appen_sp_module.SPDDLAppender(str(input_xlsx), str(output_txt), None, create_sql_copy=False)
    out_path = app.run()

    assert os.path.exists(out_path), "Output txt should exist"
    with open(out_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()
    # Expect two blocks (Row1 + Row2)
    assert "usp_DoThing" in content
    assert "usp_Second" in content
    # Header comment format
    assert content.splitlines()[0].startswith("-- 1 SRV1\\DB1\\dbo\\usp_DoThing.sql")
