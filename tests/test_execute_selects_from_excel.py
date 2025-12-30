import os
import sys
from types import SimpleNamespace
from openpyxl import Workbook, load_workbook
import pandas as pd

BASE = os.path.abspath(os.path.dirname(__file__))
WORK = os.path.abspath(os.path.join(BASE, os.pardir))
if WORK not in sys.path:
    sys.path.insert(0, WORK)

import Execute_Selects_From_Excel as mod
from Execute_Selects_From_Excel import SelectsExecutor

TMP = os.path.join(WORK, 'tmp_test_exec')
os.makedirs(TMP, exist_ok=True)

# Create input Excel with two selects: one OK, one fails
input_xlsx = os.path.join(TMP, 'Selects.xlsx')
wb = Workbook()
ws = wb.active
ws.title = 'Sheet1'
ws.append(['Select'])
ws.append(['SELECT 1'])  # OK
ws.append(['SELECT FAIL'])  # Will simulate error
wb.save(input_xlsx)

output_xlsx = os.path.join(TMP, 'Esiti.xlsx')

# Build fake pyodbc connection
class FakeCursor:
    def __init__(self):
        self.timeout = None
    def execute(self, sql):
        if 'FAIL' in sql:
            raise Exception('Simulated error: invalid syntax near FAIL')
    def fetchmany(self, n):
        return [(1,)]

class FakeConn:
    def cursor(self):
        return FakeCursor()
    def close(self):
        pass

# Patch pyodbc.connect in our module
orig_connect = getattr(mod.pyodbc, 'connect', None) if getattr(mod, 'pyodbc', None) is not None else None

def fake_connect(conn_str, timeout=None):
    return FakeConn()
# Ensure module has a pyodbc-like object with connect
if getattr(mod, 'pyodbc', None) is None:
    mod.pyodbc = SimpleNamespace(connect=fake_connect)
else:
    mod.pyodbc.connect = fake_connect

try:
    ex = SelectsExecutor(input_xlsx, output_xlsx)
    out = ex.run()
    assert os.path.exists(out)
    # Validate output content
    df = pd.read_excel(out)
    assert list(df.columns) == ['Select', 'Errore']
    rows = df.to_dict('records')
    assert rows[0]['Select'] == 'SELECT 1' and (rows[0]['Errore'] == '' or pd.isna(rows[0]['Errore']))
    assert rows[1]['Select'] == 'SELECT FAIL' and 'invalid syntax' in rows[1]['Errore']
    print('Execute selects test passed.')
finally:
    # Restore
    if getattr(mod, 'pyodbc', None) is not None:
        if orig_connect is None:
            # Leave fake in place if there was no original
            pass
        else:
            mod.pyodbc.connect = orig_connect
