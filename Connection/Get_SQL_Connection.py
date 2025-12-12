from Connection.IConnection import IConnection
import re

class GetSqlConnection(IConnection):

    def __init__(self, txt_file):
        super().__init__(txt_file)

    def get_connection(self):
        with open(self.txt_file, 'r', encoding='utf-8') as f:
            content = f.read()


        # Source (riga con Source = Sql.Databases("...") oppure Sql.Database("...", "...") oppure Sql.Database("...", "...", [Query=...]))
        # Regex robusta: cattura tutto tra la parentesi aperta e la parentesi chiusa corrispondente, anche con #(tab), #(lf), ecc.
        source_match = re.search(r'(Source|Origine)\s*=\s*(Sql\.(?:Databases|Database)\((?:[^()]|\([^()]*\))*\))', content, re.DOTALL)
        self.source = source_match.group(2).strip() if source_match else None

        # Server, Database, Query
        if self.source:
            dbs_match = re.match(r'Sql\.Databases\("([^"]+)"\)', self.source)
            db_match = re.match(r'Sql\.Database\("([^"]+)",\s*"([^"]+)"\)', self.source)
            # Migliorata: matcha 3 argomenti, il terzo può essere qualsiasi cosa tra parentesi quadre, anche multilinea
            db_query_match = re.match(r'Sql\.Database\("([^"]+)",\s*"([^"]+)",\s*\[(.+)\]\)', self.source, re.DOTALL)
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
                third_arg = db_query_match.group(3)
                # Cerca Query="..." all'interno del terzo argomento, anche se contiene #(lf), #(tab), ecc.
                # Pulisci il terzo argomento da #(xxx) e spazi
                cleaned_third_arg = re.sub(r'#\([a-zA-Z0-9]+\)', '', third_arg)
                cleaned_third_arg = re.sub(r'\s+', ' ', cleaned_third_arg)
                # Parsing manuale per estrarre la query tra Query=" e la prima " prima della chiusura quadra
                idx_query = third_arg.find('Query="')
                if idx_query != -1:
                    idx_start = idx_query + len('Query="')
                    idx_end = third_arg.find('"', idx_start)
                    if idx_end != -1:
                        self.query = third_arg[idx_start:idx_end]
                        # Pulisci la query da tutte le sequenze #(xxx) e spazi
                        cleaned_query = re.sub(r'#\([a-zA-Z0-9]+\)', '', self.query)
                        cleaned_query = re.sub(r'\s+', ' ', cleaned_query)  # Normalizza spazi e a capo
                        # Fix: aggiungi spazio dopo SELECT se manca *
                        cleaned_query = re.sub(r'(SELECT)\s*\*', r'SELECT *', cleaned_query, flags=re.IGNORECASE)
                        # Fix: aggiungi spazio dopo SELECT * se manca prima di FROM
                        cleaned_query = re.sub(r'(SELECT \*)\s*(FROM)', r'SELECT * FROM', cleaned_query, flags=re.IGNORECASE)
                        # Fix: normalizza anche FROM e JOIN senza spazio
                        cleaned_query = re.sub(r'(FROM|JOIN)\s*', r'\1 ', cleaned_query, flags=re.IGNORECASE)
                        # Estrai tutte le tabelle da FROM o JOIN nella query SQL
                        table_matches = re.findall(r'(?:FROM|JOIN)\s+([#\w\[\].]+)', cleaned_query, re.IGNORECASE)
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
                else:
                    # fallback: regex pulita
                    cleaned_third_arg = re.sub(r'#\([a-zA-Z0-9]+\)', '', third_arg)
                    cleaned_third_arg = re.sub(r'\s+', ' ', cleaned_third_arg)
                    query_match = re.search(r'Query\s*=\s*"((?:.|\n)*?)"\s*\]', cleaned_third_arg)
                    if query_match:
                        self.query = query_match.group(1)
                        cleaned_query = re.sub(r'#\([a-zA-Z0-9]+\)', '', self.query)
                        cleaned_query = re.sub(r'\s+', ' ', cleaned_query)  # Normalizza spazi e a capo
                        # Fix: aggiungi spazio dopo SELECT se manca *
                        cleaned_query = re.sub(r'(SELECT)\s*\*', r'SELECT *', cleaned_query, flags=re.IGNORECASE)
                        # Fix: aggiungi spazio dopo SELECT * se manca prima di FROM
                        cleaned_query = re.sub(r'(SELECT \*)\s*(FROM)', r'SELECT * FROM', cleaned_query, flags=re.IGNORECASE)
                        # Fix: normalizza anche FROM e JOIN senza spazio
                        cleaned_query = re.sub(r'(FROM|JOIN)\s*', r'\1 ', cleaned_query, flags=re.IGNORECASE)
                        table_matches = re.findall(r'(?:FROM|JOIN)\s+([#\w\[\].]+)', cleaned_query, re.IGNORECASE)
                        if table_matches:
                            tables = []
                            schemas = []
                            for table_full in table_matches:
                                table_full = table_full.strip('[]"`')
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

        # DEBUG: stampa i valori estratti
        print(f"Server: {self.server}")
        print(f"Database: {self.database}")
        print(f"Schema: {self.schema}")
        print(f"Table: {self.table}")
        print(f"Query: {self.query}")
        print(f"Source: {self.source}")