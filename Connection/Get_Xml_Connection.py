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
        """
        Legge xl/connections.xml dentro l'Excel e restituisce una lista di
        dict con chiavi: Server, Database, Schema, Tabella. Popola anche
        gli attributi della classe con la prima connessione valida.
        Gestisce namespace OpenXML e il caso "Multiple Tables" cercando in
        xl/workbook.xml i nomi delle tabelle collegate alla connection.
        """
        results = []
        try:
            with zipfile.ZipFile(self.excel_path, 'r') as z:
                names = z.namelist()
                conn_path = next((n for n in names if n.endswith('xl/connections.xml')), None)
                if not conn_path:
                    return results
                with z.open(conn_path) as f:
                    xml_text = f.read().decode('utf-8', errors='ignore')
                    self.xml_text = xml_text
                    #print(f"[GetXmlConnection] File: {self.file_name} - xl/connections.xml content (len={len(xml_text)}):\n{xml_text}\n--- END connections.xml ---\n")
                    try:
                        root = ET.fromstring(xml_text)
                    except ET.ParseError:
                        return results

                    ns = {'ns': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
                    connections = list(root.findall('.//ns:connection', ns))
                    if not connections:
                        connections = list(root.findall('.//connection'))

                    for conn in connections:
                        info = {
                            'ConnectionId': conn.attrib.get('id'),
                            'Name': conn.attrib.get('name'),
                            'Server': None,
                            'Database': None,
                            'Schema': None,
                            'Tabella': None
                        }
                        # dbPr con namespace
                        db_pr = conn.find('ns:dbPr', ns)
                        if db_pr is None:
                            db_pr = conn.find('dbPr')
                        conn_str = ''
                        command = ''
                        if db_pr is not None:
                            conn_str = db_pr.attrib.get('connection', '') or ''
                            command = db_pr.attrib.get('command', '') or ''

                        # Server/Database
                        info['Server'] = self._extract_value(conn_str, ['Data Source', 'Server'])
                        if not info['Server']:
                            # Fallback: usa DSN come identificatore "server" quando non presente Data Source/Server
                            info['Server'] = self._extract_value(conn_str, ['DSN'])
                        info['Database'] = self._extract_value(conn_str, ['Initial Catalog', 'Database'])

                        # Database/Schema/Tabella dal command (priorità al DB del command)
                        cmd_db, schema, table = self._parse_command(command)
                        # Se il DB è presente nel command, sovrascrive quello da Initial Catalog
                        if cmd_db:
                            info['Database'] = cmd_db
                        info['Schema'] = schema
                        info['Tabella'] = table

                        # print("\n"+
                        #     f"[GetXmlConnection] {self.file_name} -> "
                        #     f"Server={info['Server']}, Database={info['Database']}, "
                        #     f"Schema={info['Schema']}, Tabella={info['Tabella']}" +"\n"
                        # )
                        # In caso di SELECT con JOIN, mostra anche tutte le tabelle rilevate
                        for sch, tab in self._parse_all_tables(command):
                            if sch == info['Schema'] and tab == info['Tabella']:
                                continue
                            # print(
                            #     f"[GetXmlConnection] {self.file_name} -> Tabelle aggiuntive da JOIN/SELECT: "
                            #     f"Schema={sch}, Tabella={tab}"
                            # )
                        # Accetta e restituisce tutte le connessioni trovate,
                        # anche se alcune informazioni non sono presenti.
                        results.append(info)

                    # Multiple Tables: connection type 100 o name contenente 'Multiple Tables'
                    for conn in connections:
                        name_attr = conn.attrib.get('name', '') or ''
                        conn_type = conn.attrib.get('type', '') or ''
                        looks_multiple = ('multiple' in name_attr.lower()) or (conn_type == '100')
                        if not looks_multiple:
                            continue
                        tables = self._tables_from_workbook(z, name_attr)
                        srv, db = self._infer_server_database_from_name(name_attr)
                        for t in tables:
                            # print("\n"+
                            #     f"[GetXmlConnection] {self.file_name} (Multiple Tables '{name_attr}') -> "
                            #     f"Server={srv or '.'}, Database={db}, Schema=dbo, Tabella={t}"+"\n"
                            # )
                            results.append({
                                'ConnectionId': conn.attrib.get('id'),
                                'Name': name_attr,
                                'Server': srv or '.',
                                'Database': db,
                                'Schema': 'dbo',
                                'Tabella': t
                            })

        except zipfile.BadZipFile:
            return results

        if results:
            first = results[0]
            self.server = first['Server']
            self.database = first['Database']
            self.schema = first['Schema']
            self.table = first['Tabella']
        #print(f"[GetXmlConnection] Results for {self.file_name}: {results}")
        return results

    def _extract_value(self, conn_str, keys):
        if not conn_str:
            return None
        keys_lower = {k.lower() for k in keys}
        for part in conn_str.split(';'):
            part = part.strip()
            if '=' not in part:
                continue
            left, right = part.split('=', 1)
            if left.strip().lower() in keys_lower:
                return right.strip()
        return None

    def _parse_command(self, command):
        """
        Prova a estrarre (database, schema, tabella) dal valore di `command`.
        Supporta forme:
          - "DB"."Schema"."Tabella"
          - [DB].[Schema].[Tabella]
          - Schema.Tabella
          - SELECT ... FROM DB.Schema.Tabella [AS t]
        Se non trova il database, ritorna (None, schema, tabella).
        Se non riesce a riconoscere, ritorna (None, None, None).
        """
        import re
        if not command:
            return None, None, None

        cmd = command.replace('&quot;', '"').strip()

        def split_qualified(token: str):
            # Normalizza [x] in "x" e spazi attorno al punto
            token = re.sub(r'\[([^\]]+)\]', r'"\1"', token)
            token = re.sub(r'\s*\.\s*', '.', token)
            parts = [p for p in token.split('.') if p]
            clean = []
            for p in parts:
                p = p.strip()
                if len(p) >= 2 and p[0] == '"' and p[-1] == '"':
                    p = p[1:-1]
                clean.append(p)
            return [c for c in clean if c]

        # Caso semplice: il command è solo un identificatore qualificato (nessuno spazio/parola chiave)
        cmd_lower = cmd.lower()
        if not re.search(r'\s', cmd_lower) and all(k not in cmd_lower for k in ('select', 'from', 'join', 'where')):
            simple_parts = split_qualified(cmd)
            if len(simple_parts) == 3:
                return simple_parts[0], simple_parts[1], simple_parts[2]
            if len(simple_parts) == 2:
                return None, simple_parts[0], simple_parts[1]

        # Caso SELECT ... FROM ...
        if re.search(r'\bselect\b', cmd, flags=re.IGNORECASE):
            mfrom = re.search(r'\bfrom\b\s+([^\s;]+)', cmd, flags=re.IGNORECASE)
            if mfrom:
                token = mfrom.group(1)
                parts = split_qualified(token)
                if len(parts) == 3:
                    return parts[0], parts[1], parts[2]
                if len(parts) == 2:
                    return None, parts[0], parts[1]

        return None, None, None

    def _parse_all_tables(self, command):
        import re
        results = []
        if not command:
            return results
        cmd = command.replace('&quot;', '"')
        # Normalizza separatori e rimuove quoting [] e "
        def split_parts(token):
            token = token.strip()
            token = token.replace('[', '').replace(']', '').replace('"', '')
            # Rimuovi eventuali alias: es. schema.tabella AS t -> prendi prima parola
            token = token.split()[0]
            parts = [p for p in token.split('.') if p]
            return parts

        # FROM principale
        mfrom = re.search(r'\bfrom\b\s+([^\s;]+)', cmd, flags=re.IGNORECASE)
        if mfrom:
            parts = split_parts(mfrom.group(1))
            if len(parts) >= 2:
                results.append((parts[-2], parts[-1]))

        # Tutte le JOIN
        for m in re.finditer(r'\bjoin\b\s+([^\s;]+)', cmd, flags=re.IGNORECASE):
            parts = split_parts(m.group(1))
            if len(parts) >= 2:
                tup = (parts[-2], parts[-1])
                if tup not in results:
                    results.append(tup)

        return results
    def _infer_server_database_from_name(self, name_attr):
        if not name_attr:
            return None, None
        parts = [p for p in name_attr.replace('\\',' ').replace('/',' ').split() if p]
        server = None
        database = None
        if parts:
            first = parts[0]
            if first.startswith('.\\'):
                server = '.'
            elif first.lower().startswith('localhost'):
                server = 'localhost'
            else:
                server = first
        for token in parts[1:]:
            low = token.lower()
            if low in ('multiple','tables'):
                continue
            database = token
            break
        return server, database

    def _tables_from_workbook(self, zip_obj, connection_name):
        try:
            names = zip_obj.namelist()
            target = next((n for n in names if n.endswith('xl/workbook.xml')), None)
            if not target:
                return []
            with zip_obj.open(target) as f:
                xml_text = f.read().decode('utf-8', errors='ignore')
        except Exception:
            return []
        import re
        pattern = re.compile(r'name\s*=\s*"([^"]+)"\s+[^>]*connection\s*=\s*"' + re.escape(connection_name) + r'"', re.IGNORECASE)
        names = [m.group(1) for m in pattern.finditer(xml_text)]
        return [n for n in names if n and n != 'ThisWorkbookDataModel']

    
        

