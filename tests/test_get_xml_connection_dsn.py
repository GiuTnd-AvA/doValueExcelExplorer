import os
import tempfile
import zipfile
import unittest

# Allow imports from project root
import sys
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from Connection.Get_Xml_Connection import GetXmlConnection


CONNECTIONS_XML = '''<?xml version="1.0" encoding="utf-8" standalone="yes"?>
<connections xmlns:xr16="http://schemas.microsoft.com/office/spreadsheetml/2017/revision16" xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006" mc:Ignorable="xr16" xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <connection saveData="1" background="1" refreshedVersion="8" type="1" name="Query da ANALISI_H3" xr16:uid="{00000000-0015-0000-FFFF-FFFF00000000}" id="1">
    <dbPr command="SELECT * _x000d__x000a_ FROM s1057b.dbo.pmor_SVG_in_redazione" connection="DSN=ANALISI_H3;UID=s104406;Trusted_Connection=Yes;APP=Microsoft Office 2016;WSID=DOBXAMCREPD4103;DATABASE=ANALISI"/>
  </connection>
</connections>
'''


class TestGetXmlConnectionDSN(unittest.TestCase):
    def test_extract_with_dsn_and_uppercase_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            xlsx_path = os.path.join(tmp, 'sample.xlsx')
            with zipfile.ZipFile(xlsx_path, 'w', zipfile.ZIP_DEFLATED) as z:
                # Minimal structure: just xl/connections.xml
                z.writestr('xl/connections.xml', CONNECTIONS_XML)
            gx = GetXmlConnection(xlsx_path)
            results = gx.extract_connection_info()
            self.assertTrue(results, 'No connections found')
            self.assertEqual(len(results), 1)
            r = results[0]
            self.assertEqual(r['Database'], 'ANALISI')
            self.assertEqual(r['Server'], 'ANALISI_H3')
            self.assertEqual(r['Schema'], 'dbo')
            self.assertEqual(r['Tabella'], 'pmor_SVG_in_redazione')


if __name__ == '__main__':
    unittest.main()
