from Connection.IConnection import IConnection
import re

class GetSqlConnection(IConnection):

    def __init__(self, txt_file):
        super().__init__(txt_file)

    def get_connection(self):
        with open(self.txt_file, 'r', encoding='utf-8') as f:
            content = f.read()


        # Source (riga con Source = Sql.Databases("...") oppure Sql.Database("...", "...") oppure Sql.Database("...", "...", [Query=...]))
        # Migliorato per gestire spazi e caratteri speciali
        source_match = re.search(r'(Source|Origine)\s*=\s*(Sql\.Databases\(([^)]*)\)|Sql\.Database\(([^)]*)\))', content)
        self.source = source_match.group(2).strip() if source_match else None

        # Server, Database, Query
        if self.source:
            # Estrai argomenti tra parentesi, gestendo anche spazi e escape
            args_match = re.match(r"Sql\.Database\((.*)\)", self.source)
            if args_match:
                # Split robusto sugli argomenti, gestisce anche [Query=...]
                args = [a.strip().strip('"') for a in re.split(r',\s*(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', args_match.group(1))]
                if len(args) >= 2:
                    self.server = args[0]
                    self.database = args[1]
                # Cerca Query
                query_match = re.search(r'Query\s*=\s*"([\s\S]*?)"', self.source)
                if query_match:
                    query_sql = query_match.group(1)
                    # Estrai tutte le tabelle da FROM o JOIN nella query SQL, gestendo anche escape, variabili e temp table
                    table_matches = re.findall(r'(?:FROM|JOIN)\s+((?:\[.*?\]|#\(.*?\)|##?\w+|\w+)(?:\.\w+){0,2})', query_sql, re.IGNORECASE)
                    if table_matches:
                        tables = []
                        schemas = []
                        for table_full in table_matches:
                            table_full = table_full.strip('[]"`')
                            # Variabili Power Query (#(...)), temp table (##), table semplice
                            if table_full.startswith('##') or table_full.startswith('#'):
                                schemas.append('')
                                tables.append(table_full)
                            else:
                                parts = table_full.split('.')
                                if len(parts) == 3:
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

        # Schema e Table da accesso diretto
        table_match = re.search(r'\{\[Schema="([^"]+)",\s*Item="([^"]+)"\]\}\[Data\]', content)
        if table_match:
            self.schema = table_match.group(1)
            self.table = table_match.group(2)