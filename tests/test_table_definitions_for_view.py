import os
import sys
from openpyxl import Workbook
import pandas as pd

BASE = os.path.abspath(os.path.dirname(__file__))
WORK = os.path.abspath(os.path.join(BASE, os.pardir))
if WORK not in sys.path:
    sys.path.insert(0, WORK)

import Get_Table_Definitions_From_Excel as mod
from Get_Table_Definitions_From_Excel import TableDefinitionExtractor

TMP = os.path.join(WORK, 'tmp_test_table_ddl')
os.makedirs(TMP, exist_ok=True)


def test_view_definition_is_used(tmp_path):
    # Build input Excel with one object that we'll mark as a view
    inp = tmp_path / 'tables.xlsx'
    wb = Workbook()
    ws = wb.active
    ws.title = 'Sheet1'
    ws.append(['Server','DB','Schema','Table'])
    ws.append(['EPCP3','master','dbo','v1'])
    wb.save(str(inp))

    outp = tmp_path / 'ddl.xlsx'

    # Fake connection & methods
    class FakeCursor:
        def __init__(self):
            self.calls = []
        def execute(self, *args, **kwargs):
            self.calls.append((args, kwargs))
        def fetchone(self):
            # Return a create view definition
            return ['CREATE VIEW [dbo].[v1] AS SELECT 1 AS c']
    class FakeConn:
        def cursor(self):
            return FakeCursor()
        def close(self):
            pass

    def fake_connect(*args, **kwargs):
        return FakeConn()

    # Ensure module has pyodbc.connect
    if getattr(mod, 'pyodbc', None) is None:
        import types
        mod.pyodbc = types.SimpleNamespace(connect=fake_connect)
    else:
        mod.pyodbc.connect = fake_connect

    # Bypass driver build
    TableDefinitionExtractor._build_conn_str = lambda self, s, d: 'DRIVER={test};'  # type: ignore

    # Force object type to 'Vista' and provide view definition directly
    TableDefinitionExtractor._get_object_type = lambda self, c, s, t: 'Vista'  # type: ignore
    # Allow the extractor to call its own method which uses FakeConn cursor to return a view
    TableDefinitionExtractor._fetch_view_definition = (
        lambda self, c, s, t: 'CREATE VIEW [dbo].[v1] AS SELECT 1 AS c'  # type: ignore
    )

    ext = TableDefinitionExtractor(str(inp), str(outp))
    out = ext.run()
    assert os.path.exists(out)
    df = pd.read_excel(out)
    assert list(df.columns) == ['Server','DB','Schema','Table','ObjectType','DDL']
    # Ensure the DDL contains CREATE VIEW and not CREATE TABLE
    ddl = str(df.loc[0,'DDL'])
    assert 'CREATE VIEW' in ddl.upper()
    assert 'CREATE TABLE' not in ddl.upper()
