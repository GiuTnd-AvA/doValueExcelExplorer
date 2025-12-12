import zipfile
import os

import xml.etree.ElementTree as ET

class GetXmlConnection:
    def __init__(self, excel_path):
        self.excel_path = excel_path
        self.server = None
        self.database = None
        self.schema = None
        self.table = None
        self.xml_text = None

    def extract_connection_info(self):
        # Open the Excel file as a zip archive
        with zipfile.ZipFile(self.excel_path, 'r') as z:
            # Path to connections.xml inside the Excel file
            connections_path = 'xl/connections.xml'
            if connections_path not in z.namelist():
                raise FileNotFoundError("connections.xml not found in the Excel file.")
            with z.open(connections_path) as f:
                xml_data = f.read()
                self.xml_text = xml_data.decode('utf-8')
                root = ET.fromstring(self.xml_text)
                # Find the connection string
                conn = root.find('.//connection')
                if conn is not None:
                    conn_str = conn.attrib.get('connection', '')
                    # Example connection string parsing for SQL Server
                    # "Provider=SQLOLEDB.1;Integrated Security=SSPI;Persist Security Info=True;Initial Catalog=MyDatabase;Data Source=MyServer"
                    self.server = self._extract_value(conn_str, ['Data Source', 'Server'])
                    self.database = self._extract_value(conn_str, ['Initial Catalog', 'Database'])
                # Find the table and schema
                db_pr = root.find('.//dbPr')
                if db_pr is not None:
                    self.schema = db_pr.attrib.get('sschema')
                    self.table = db_pr.attrib.get('table')

    def _extract_value(self, conn_str, keys):
        for key in keys:
            for part in conn_str.split(';'):
                if part.strip().startswith(key + '='):
                    return part.split('=', 1)[1].strip()
        return None