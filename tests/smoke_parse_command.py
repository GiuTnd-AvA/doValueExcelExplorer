import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from Connection.Get_Xml_Connection import GetXmlConnection

g = GetXmlConnection('x')

tests = [
    '"DB"."dbo"."T"',
    '[DB].[dbo].[T]',
    'dbo.T',
    'select * from [DB].[dbo].[T] a',
    'SELECT col FROM "DB"."sch"."Tab"',
]

for s in tests:
    print(s, '->', g._parse_command(s))
