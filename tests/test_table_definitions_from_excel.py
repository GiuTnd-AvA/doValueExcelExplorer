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

# Build input Excel with split columns
inp = os.path.join(TMP, 'tables.xlsx')
wb = Workbook()
ws = wb.active
ws.title = 'Sheet1'
ws.append(['Server','DB','Schema','Table'])
ws.append(['EPCP3','master','dbo','t1'])
wb.save(inp)

outp = os.path.join(TMP, 'ddl.xlsx')

# Fake pyodbc connect
class FakeCursor:
    def execute(self, *args, **kwargs):
        pass
    def fetchone(self):
        return ['CREATE TABLE [dbo].[t1] ( [id] int NOT NULL )']
class FakeConn:
    def cursor(self):
        return FakeCursor()
    def close(self):
        pass

orig_connect = getattr(mod.pyodbc, 'connect', None) if getattr(mod, 'pyodbc', None) is not None else None

def fake_connect(*args, **kwargs):
    return FakeConn()

# Ensure module has a pyodbc-like object with connect
if getattr(mod, 'pyodbc', None) is None:
    import types
    mod.pyodbc = types.SimpleNamespace(connect=fake_connect)
else:
    mod.pyodbc.connect = fake_connect

# Also bypass driver build test to not fail
def fake_build(self, server, db):
    return 'DRIVER={test};SERVER=%s;DATABASE=%s;' % (server, db)
TableDefinitionExtractor._build_conn_str = fake_build  # type: ignore

try:
    ext = TableDefinitionExtractor(inp, outp)
    out = ext.run()
    assert os.path.exists(out)
    df = pd.read_excel(out)
    assert list(df.columns) == ['Server','DB','Schema','Table','DDL']
    assert 'CREATE TABLE' in str(df.loc[0,'DDL'])
    print('Table DDL extractor test passed.')
finally:
    if getattr(mod, 'pyodbc', None) is not None and orig_connect is not None:
        mod.pyodbc.connect = orig_connect
