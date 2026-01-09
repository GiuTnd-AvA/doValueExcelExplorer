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


def test_view_with_english_label_uses_view_definition(tmp_path):
    # Prepare input
    inp = tmp_path / 'tables.xlsx'
    wb = Workbook()
    ws = wb.active
    ws.title = 'Sheet1'
    ws.append(['Server','DB','Schema','Table'])
    ws.append(['EPCP3','master','dbo','v1'])
    wb.save(str(inp))

    outp = tmp_path / 'ddl.xlsx'

    # Fake connection that returns a view definition
    class FakeCursor:
        def execute(self, *args, **kwargs):
            pass
        def fetchone(self):
            return ['CREATE VIEW [dbo].[v1] AS SELECT 1']
    class FakeConn:
        def cursor(self):
            return FakeCursor()
        def close(self):
            pass

    def fake_connect(*args, **kwargs):
        return FakeConn()

    if getattr(mod, 'pyodbc', None) is None:
        import types
        mod.pyodbc = types.SimpleNamespace(connect=fake_connect)
    else:
        mod.pyodbc.connect = fake_connect

    # Bypass driver build
    TableDefinitionExtractor._build_conn_str = lambda self, s, d: 'DRIVER={test};'  # type: ignore

    # Force object type info to ('V','VIEW','VIEW')
    TableDefinitionExtractor._get_object_type_info = lambda self, c, s, t: ('V','VIEW','VIEW')  # type: ignore

    ext = TableDefinitionExtractor(str(inp), str(outp))
    out = ext.run()
    df = pd.read_excel(out)
    ddl = str(df.loc[0,'DDL']).upper()
    assert 'CREATE VIEW' in ddl
    assert 'CREATE TABLE' not in ddl
