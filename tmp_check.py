import sys
sys.path.insert(0, 'c:/Users/giuseppe.tanda/Desktop/doValueExcelExplorer')
from analisi_viste.ExtractSqlTables import extract_matches, _strip_sql_comments
sql = "SELECT * FROM USEPCPDZ2.amministrazione.dbo.AMM_COMPENSO_VARIABILE;"
print(extract_matches(_strip_sql_comments(sql)))
