import sys
sys.path.insert(0, 'c:/Users/giuseppe.tanda/Desktop/doValueExcelExplorer')
import analisi_viste.ExtractSqlTables as M
sql = "SELECT * FROM USEPCPDZ2.amministrazione.dbo.AMM_COMPENSO_VARIABILE;"
from_pattern = None
for clause, pattern in M.CLAUSE_PATTERNS:
    if clause == 'FROM':
        from_pattern = pattern
        break
print('FROM pattern:', from_pattern.pattern)
for m in from_pattern.finditer(sql):
    print('MATCH:', m.group('table'))
