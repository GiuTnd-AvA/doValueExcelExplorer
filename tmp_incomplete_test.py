import sys
sys.path.insert(0, 'c:/Users/giuseppe.tanda/Desktop/doValueExcelExplorer')
from ExtractSqlTables import extract_matches, _strip_sql_comments

# Test case 1: dbo.CARTESIO_' + @variable
sql1 = "right outer join dbo.CARTESIO_' + @v_data_rif_aaaammgg + '_CLUSTER_PRATICA_T as CL"
result1 = extract_matches(_strip_sql_comments(sql1))
print("Test 1 - dbo.CARTESIO_' pattern:")
print(f"  Input: {sql1}")
print(f"  Result: {result1}")
print(f"  Expected: [] (empty, no tables)")
print()

# Test case 2: dbo.' + @variable (previous case)
sql2 = "drop table dbo.' + @v_nome_tabella_finale"
result2 = extract_matches(_strip_sql_comments(sql2))
print("Test 2 - dbo.' pattern:")
print(f"  Input: {sql2}")
print(f"  Result: {result2}")
print(f"  Expected: [] (empty, no tables)")
print()

# Test case 3: Real JOIN with full table name
sql3 = "right outer join dbo.CARTESIO_CLUSTER_PRATICA_T as CL"
result3 = extract_matches(_strip_sql_comments(sql3))
print("Test 3 - Real table name:")
print(f"  Input: {sql3}")
print(f"  Result: {result3}")
print(f"  Expected: [{{'Clause': 'RIGHT OUTER JOIN', 'Table': 'dbo.CARTESIO_CLUSTER_PRATICA_T'}}]")
print()

# Test case 4: PROGETTO_36_ pattern
sql4 = "right outer join dbo.PROGETTO_36_' + @v_data_rif_aaaammgg + '_CK_PRAT_PUNTO_01_T as P"
result4 = extract_matches(_strip_sql_comments(sql4))
print("Test 4 - dbo.PROGETTO_36_' pattern:")
print(f"  Input: {sql4}")
print(f"  Result: {result4}")
print(f"  Expected: [] (empty, no tables)")
