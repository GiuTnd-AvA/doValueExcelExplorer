import os
import sys
from openpyxl import Workbook

BASE = os.path.abspath(os.path.dirname(__file__))
WORK = os.path.abspath(os.path.join(BASE, os.pardir))
if WORK not in sys.path:
    sys.path.insert(0, WORK)

from analisi_viste.Append_Sql_Files_From_Excel import SqlFilesAppender

TMP = os.path.join(WORK, 'tmp_test_append_join')
os.makedirs(TMP, exist_ok=True)

# Create sample .sql files in a directory
sql_dir = os.path.join(TMP, 'sqls')
os.makedirs(sql_dir, exist_ok=True)
sql1 = os.path.join(sql_dir, 'j1.sql')
sql2 = os.path.join(sql_dir, 'j2.sql')
with open(sql1, 'w', encoding='utf-8') as f:
    f.write("SELECT 'J1' AS J;\n")
with open(sql2, 'w', encoding='utf-8') as f:
    f.write("SELECT 'J2' AS J;\n")

# Create Excel where column A is directory only, column B is filename
excel_path = os.path.join(TMP, 'Lista_file_SQL_join.xlsx')
wb = Workbook()
ws = wb.active
ws.title = 'Lista file SQL'
ws.append(['Percorsi', 'File'])
ws.append([sql_dir, 'j1.sql'])
ws.append([sql_dir, 'j2.sql'])
wb.save(excel_path)

# Run appender
out_txt = os.path.join(TMP, 'output_join.txt')
app = SqlFilesAppender(excel_path, out_txt)
result_path = app.run()

# Validate output
assert os.path.exists(result_path)
text = open(result_path, 'r', encoding='utf-8', errors='ignore').read()
assert f"--1 {os.path.join(sql_dir, 'j1.sql')}" in text
assert "SELECT 'J1' AS J;" in text
assert f"--2 {os.path.join(sql_dir, 'j2.sql')}" in text
assert "SELECT 'J2' AS J;" in text
print('Join-case test passed.')
