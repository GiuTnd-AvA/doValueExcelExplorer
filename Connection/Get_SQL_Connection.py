from Connection.IConnection import IConnection
import re

class GetSqlConnection(IConnection):

    def __init__(self, txt_file):
        super().__init__(txt_file)

    def get_connection(self):
        with open(self.txt_file, 'r', encoding='utf-8') as f:
            content = f.read()


        # Source (riga con Source = Sql.Databases("...") oppure Sql.Database("...", "...") oppure Sql.Database("...", "...", [Query=...]))
        source_match = re.search(r'(Source|Origine)\s*=\s*(Sql\.Databases\([^)]+\)|Sql\.Database\([^)]+\))', content)
        self.source = source_match.group(2).strip() if source_match else None

        # Server, Database, Query
        if self.source:
            dbs_match = re.match(r'Sql\.Databases\("([^"]+)"\)', self.source)
            db_match = re.match(r'Sql\.Database\("([^"]+)",\s*"([^"]+)"\)', self.source)
            db_query_match = re.match(r'Sql\.Database\("([^"]+)",\s*"([^"]+)",\s*\[Query="([\s\S]*)"\]\)', self.source)
            if dbs_match:
                self.server = dbs_match.group(1)
                db_line = re.search(r'(Source|Origine)\{\[Name="([^"]+)"\]\}\[Data\]', content)
                if db_line:
                    self.database = db_line.group(2)
            elif db_match:
                self.server = db_match.group(1)
                self.database = db_match.group(2)
            elif db_query_match:
                self.server = db_query_match.group(1)
                self.database = db_query_match.group(2)
                query_sql = db_query_match.group(3)
                # Estrai tutte le tabelle da FROM o JOIN nella query SQL
                table_matches = re.findall(r'(?:FROM|JOIN)\s+([^\s,;]+)', query_sql, re.IGNORECASE)
                if table_matches:
                    tables = []
                    schemas = []
                    for table_full in table_matches:
                        table_full = table_full.strip('[]"`')
                        # Gestione temp table (##) o variabili Power Query (#)
                        if table_full.startswith('##') or table_full.startswith('#'):
                            schemas.append('')
                            tables.append(table_full)
                        else:
                            parts = table_full.split('.')
                            if len(parts) == 3:
                                # db.schema.table
                                # Puoi scegliere se salvare anche il db, qui prendo solo schema e table
                                schemas.append(parts[1])
                                tables.append(parts[2])
                            elif len(parts) == 2:
                                schemas.append(parts[0])
                                tables.append(parts[1])
                            else:
                                schemas.append('')
                                tables.append(table_full)
                    self.schema = ';'.join(schemas)
                    self.table = ';'.join(tables)

        # Schema e Table
        # self.schema = None
        # self.table = None
        # Cerca la riga che accede alla tabella
        table_match = re.search(r'\{\[Schema="([^"]+)",\s*Item="([^"]+)"\]\}\[Data\]', content)
        if table_match:
            self.schema = table_match.group(1)
            self.table = table_match.group(2)
        else:
            # Prova con schema "dbo" (alcuni file potrebbero non avere lo schema esplicito)
            table_match = re.search(r'\{\[Schema="([^"]+)",\s*Item="([^"]+)"\]\}\[Data\]', content)
            if table_match:
                self.schema = table_match.group(1)
                self.table = table_match.group(2)