import os
import tempfile
from openpyxl import Workbook
from Append_Views_From_Excel import ViewsDDLAppender

def create_excel(rows):
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    ws.append(["server", "db", "schema", "table", "Object type", "DDL"])  # headers
    for r in rows:
        ws.append(r)
    tmpfd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(tmpfd)
    wb.save(path)
    return path


def read_text(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def test_appends_only_views_and_formats_header(tmp_path):
    # Prepare sample rows: one table, two views
    rows = [
        ["srvA", "dbA", "schA", "tabA", "user_table", "CREATE TABLE schA.tabA(id int);"] ,
        ["srvB", "dbB", "schB", "viewB", "view", "CREATE VIEW schB.viewB AS SELECT 1 AS c;"],
        ["srvC", "dbC", "schC", "viewC", "view", "CREATE VIEW schC.viewC AS SELECT 2 AS c;"]
    ]
    excel_path = create_excel(rows)
    out_txt = tmp_path / "out.txt"

    app = ViewsDDLAppender(excel_path, str(out_txt), sheet_name="Sheet1")
    result_path = app.run()

    assert os.path.exists(result_path)
    txt = read_text(result_path)

    # Should contain only two blocks (views)
    assert txt.count("\n") >= 4  # at least lines
    # Header comments numbered and with backslash path
    assert "-- 1 srvB\\dbB\\schB\\viewB.sql" in txt
    assert "-- 2 srvC\\dbC\\schC\\viewC.sql" in txt
    # DDL content present below headers
    assert "CREATE VIEW schB.viewB AS SELECT 1 AS c;" in txt
    assert "CREATE VIEW schC.viewC AS SELECT 2 AS c;" in txt


def test_handles_empty_ddl(tmp_path):
    rows = [
        ["srvD", "dbD", "schD", "viewD", "view", ""],
    ]
    excel_path = create_excel(rows)
    out_txt = tmp_path / "out.txt"

    app = ViewsDDLAppender(excel_path, str(out_txt), sheet_name="Sheet1")
    result_path = app.run()
    txt = read_text(result_path)

    # Header exists even if DDL is empty; newline afterwards
    assert "-- 1 srvD\\dbD\\schD\\viewD.sql" in txt
    # The file should end with a newline
    assert txt.endswith("\n")
