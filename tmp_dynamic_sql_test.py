import sys
sys.path.insert(0, 'c:/Users/giuseppe.tanda/Desktop/doValueExcelExplorer')
from ExtractSqlTables import extract_matches, _strip_sql_comments

# Test dynamic SQL pattern
sql = """set @v_sql = 'if object_id(''dbo.' + @v_nome_tabella_finale + ''') is not null drop table dbo.' + @v_nome_tabella_finale + ';';"""
result = extract_matches(_strip_sql_comments(sql))
print("Dynamic SQL test:")
print(f"  Input: {sql}")
print(f"  Result: {result}")
print(f"  Expected: [] (empty, no tables)")
print()

# Test real DROP TABLE
sql2 = "DROP TABLE dbo.actual_table_name;"
result2 = extract_matches(_strip_sql_comments(sql2))
print("Real DROP TABLE test:")
print(f"  Input: {sql2}")
print(f"  Result: {result2}")
print(f"  Expected: [{{'Clause': 'DROP TABLE', 'Table': 'dbo.actual_table_name'}}]")
