import sys
sys.path.insert(0, 'c:/Users/giuseppe.tanda/Desktop/doValueExcelExplorer')
import re
from ExtractSqlTables import IDENTIFIER, QUALIFIED, CLAUSE_PATTERNS

sql = """set @v_sql = 'if object_id(''dbo.' + @v_nome_tabella_finale + ''') is not null drop table dbo.' + @v_nome_tabella_finale + ';';"""

# Find DROP TABLE pattern
for clause, pattern in CLAUSE_PATTERNS:
    if clause == 'DROP TABLE':
        for m in pattern.finditer(sql):
            t = m.group('table').strip()
            match_end = m.end('table')
            following = sql[match_end:match_end+30]
            print(f"Matched table: '{t}'")
            print(f"Match end position: {match_end}")
            print(f"Following text (30 chars): '{following}'")
            print(f"Following starts with '.': {following.startswith('.')}")
            print(f"Following starts with quote: {following.startswith(chr(39))}")
            print()
