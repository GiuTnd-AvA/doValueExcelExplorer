import os, zipfile, tempfile, sys
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

with tempfile.TemporaryDirectory() as tmp:
    p = os.path.join(tmp, 'sample.xlsx')
    with zipfile.ZipFile(p, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('xl/connections.xml', CONNECTIONS_XML)
    gx = GetXmlConnection(p)
    res = gx.extract_connection_info()
    print('RESULTS:', res)
    with zipfile.ZipFile(p, 'r') as z:
        print('NAMES:', z.namelist())
        with z.open('xl/connections.xml') as f:
            data = f.read().decode('utf-8')
            print('XML:', data)
            import xml.etree.ElementTree as ET
            print('HEAD REPR:', repr(data[:120]))
            root = ET.fromstring(data)
            ns = {'ns': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
            found = root.findall('.//ns:connection', ns)
            print('FOUND VIA ET:', len(found))
