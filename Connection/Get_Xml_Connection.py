import zipfile
import os

import xml.etree.ElementTree as ET

class GetXmlConnection:
    def __init__(self, excel_path):
        self.excel_path = excel_path
        self.file_name = os.path.basename(excel_path)
        self.server = None
        self.database = None
        self.schema = None
        self.table = None
        self.xml_text = None

    def extract_connection_info(self):
        results = []
        try:
            with zipfile.ZipFile(self.excel_path, 'r') as z:
                connections_path = 'xl/connections.xml'
                names = z.namelist()
                # Verifica robusta: presenza esatta o suffisso del percorso
                has_connections = any(n.endswith(connections_path) for n in names)
                if not has_connections:
                    return results  # no connections.xml, skip gracefully
                # Trova il nome esatto nel zip
                exact_name = next((n for n in names if n.endswith(connections_path)), connections_path)
                with z.open(exact_name) as f:
                    xml_data = f.read()
                    self.xml_text = xml_data.decode('utf-8', errors='ignore')
                    root = ET.fromstring(self.xml_text)
                    # Iterate all <connection> entries
                    for conn in root.findall('.//connection'):
                        info = {
                            'Server': None,
                            'Database': None,
                            'Schema': None,
                            'Tabella': None
                        }
                        conn_str = conn.attrib.get('connection', '')
                        info['Server'] = self._extract_value(conn_str, ['Data Source', 'Server'])
                        info['Database'] = self._extract_value(conn_str, ['Initial Catalog', 'Database'])
                        # dbPr often paired within the connection node
                        db_pr = conn.find('.//dbPr') if conn is not None else None
                        if db_pr is None:
                            db_pr = root.find('.//dbPr')
                        if db_pr is not None:
                            info['Schema'] = db_pr.attrib.get('sschema')
                            info['Tabella'] = db_pr.attrib.get('table')
                        # Solo connessioni SQL valide: richiede almeno Server e Database
                        if (info['Server'] and info['Database']):
                            results.append(info)
        except zipfile.BadZipFile:
            # Not a valid Excel zip; return no results
            return results
        except ET.ParseError:
            # Malformed XML; skip silently
            return results
        # Also set top-level attributes to the first connection for compatibility
        if results:
            first = results[0]
            self.server = first['Server']
            self.database = first['Database']
            self.schema = first['Schema']
            self.table = first['Tabella']
        return results

    def _extract_value(self, conn_str, keys):
        for key in keys:
            for part in conn_str.split(';'):
                if part.strip().startswith(key + '='):
                    return part.split('=', 1)[1].strip()
        return None