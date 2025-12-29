import os
import sys
from openpyxl import Workbook
BASE = os.path.abspath(os.path.dirname(__file__))
WORK = os.path.abspath(os.path.join(BASE, os.pardir))
# Ensure workspace root is on sys.path for imports
if WORK not in sys.path:
    sys.path.insert(0, WORK)
from Append_Sql_Files_From_Excel import SqlFilesAppender

TMP = os.path.join(WORK, 'tmp_test_append')
os.makedirs(TMP, exist_ok=True)

# Create sample .sql files
sql1 = os.path.join(TMP, 'a.sql')
sql2 = os.path.join(TMP, 'b.sql')
with open(sql1, 'w', encoding='utf-8') as f:
    f.write("SELECT 1 AS One;\n")
with open(sql2, 'w', encoding='utf-8') as f:
    f.write("-- comment in source\nSELECT 2 AS Two;\n")

# Create Excel listing the SQL paths
excel_path = os.path.join(TMP, 'Lista_file_SQL.xlsx')
wb = Workbook()
ws = wb.active
ws.title = 'Lista file SQL'
ws.append(['Percorsi', 'File'])
ws.append([sql1, os.path.basename(sql1)])
ws.append([sql2, os.path.basename(sql2)])
wb.save(excel_path)

# Run appender
out_txt = os.path.join(TMP, 'output.txt')
app = SqlFilesAppender(excel_path, out_txt)
result_path = app.run()

# Validate output
assert os.path.exists(result_path), "Output TXT not created"
text = open(result_path, 'r', encoding='utf-8', errors='ignore').read()
assert f"--1 {sql1}" in text, "Missing separator for first file"
assert f"--2 {sql2}" in text, "Missing separator for second file"
assert "SELECT 1 AS One" in text, "Missing content of first SQL"
assert "SELECT 2 AS Two" in text, "Missing content of second SQL"
print("Smoke test passed.")
