from Connection.IConnection import IConnection
import re


class GetSqlConnection(IConnection):

    def __init__(self, txt_file):
        super().__init__(txt_file)
        self.join_tables = []  # Nuovo attributo: lista di tutte le tabelle di JOIN trovate

    def get_connection(self):
        try:
            with open(self.txt_file, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            with open(self.txt_file, 'r', encoding='latin-1') as f:
                content = f.read()


        # Source (riga con Source = Sql.Databases("...") oppure Sql.Database("...", "...") oppure Sql.Database("...", "...", [Query=...]))
        # Regex robusta: cattura tutto tra la parentesi aperta e la parentesi chiusa corrispondente, anche con #(tab), #(lf), ecc.
        # Cerca la chiamata Sql.Database/Databases e ne estrae tutto il contenuto tra parentesi (anche multilinea)
        sql_call = re.search(r'Sql\.(Databases|Database)\s*\(', content, re.IGNORECASE)
        if sql_call:
            start = sql_call.start()
            # Trova la parentesi chiusa corrispondente
            par_count = 0
            end = start
            found = False
            while end < len(content):
                if content[end] == '(': par_count += 1
                elif content[end] == ')': par_count -= 1
                if par_count == 0 and content[end] == ')':
                    found = True
                    break
                end += 1
            if found:
                self.source = content[start:end+1].strip()
            else:
                self.source = None
        else:
            self.source = None
        # self.source viene valorizzato dalla logica sottostante (sql_call)
        # ...existing code...

        # Server, Database, Query
        if self.source:
            # Estrai server e database anche se la query è multilinea o molto lunga
            dbs_match = re.match(r'Sql\.Databases\(["\']([^"\']+)["\']\)', self.source)
            # Regex robusta: estrae i primi due argomenti stringa anche se ci sono spazi, a capo o argomenti successivi
            db_match = re.match(r'Sql\.Database\(["\']([^"\']+)["\']\s*,\s*["\']([^"\']+)["\']', self.source, re.DOTALL)
            # Estrai tutto tra la prima [ e l'ultima ]
            # Estrai direttamente il contenuto tra la prima [ e l'ultima ]
            bracket_match = re.search(r'\[(.*)\]', self.source, re.DOTALL)
            if bracket_match:
                third_arg = bracket_match.group(1)
                # Estrai la query vera e propria
                query_match = re.search(r'Query\s*=\s*"([\s\S]*?)"', third_arg)
                if query_match:
                    self.query = query_match.group(1)
                    # Normalizzazione aggressiva della query (rafforzata)
                    cleaned_query = self.query
                    # Sostituisci #(lf), #(cr), #(tab), newline e tab con spazio
                    cleaned_query = re.sub(r'#\((lf|cr|tab)\)', ' ', cleaned_query, flags=re.IGNORECASE)
                    cleaned_query = re.sub(r'[\r\n\t]+', ' ', cleaned_query)
                    # Rimuovi altri caratteri speciali #(xxx)
                    cleaned_query = re.sub(r'#\([a-zA-Z0-9]+\)', '', cleaned_query)
                    # Rimuovi commenti SQL -- ... e /* ... */
                    cleaned_query = re.sub(r'--.*?\n', ' ', cleaned_query)
                    cleaned_query = re.sub(r'/\*.*?\*/', ' ', cleaned_query, flags=re.DOTALL)
                    # Normalizza spazi multipli
                    cleaned_query = re.sub(r'\s+', ' ', cleaned_query)

                    # Estrai tutte le tabelle coinvolte nei JOIN (oltre a FROM)
                    join_matches = re.findall(r'JOIN\s+((?:[\w\[\]]+\.){0,2}\[?[\w]+\]?)(?:\s+AS\s+\w+|\s+\w+)?', cleaned_query, re.IGNORECASE)
                    self.join_tables = []
                    for jm in join_matches:
                        table_full = jm.strip('[]"`()')
                        parts = re.split(r'\.', table_full)
                        parts = [p.strip('[]') for p in parts if p.strip('[]')]
                        # Prendi sempre l'ultimo elemento come nome tabella, il penultimo come schema (se esiste)
                        if len(parts) >= 2:
                            schema = parts[-2]
                            table = parts[-1]
                        elif len(parts) == 1:
                            schema = ''
                            table = parts[0]
                        else:
                            schema = ''
                            table = table_full
                        self.join_tables.append(f"{schema}.{table}" if schema else table)

                    # Regex aggiornata: gestisce anche schema..[table] e parentesi quadre, accetta caratteri speciali tra FROM/JOIN e la tabella
                    table_matches = re.findall(r'(?:FROM|JOIN)[^a-zA-Z]+([\w]+)\.\.[\[]?([\w]+)\]?', cleaned_query, re.IGNORECASE)
                    if table_matches:
                        self.schema = table_matches[0][0]
                        self.table = table_matches[0][1]
                    else:
                        # Fallback: pattern classici schema.table o solo table
                        table_matches = re.findall(r'(?:FROM|JOIN)\s+((?:[\w\[\]]+\.){0,2}\[?[\w]+\]?)(?:\s+AS\s+\w+|\s+\w+)?', cleaned_query, re.IGNORECASE)
                        if table_matches:
                            table_full = table_matches[0].strip('[]"`()')
                            parts = re.split(r'\.', table_full)
                            parts = [p.strip('[]') for p in parts if p.strip('[]')]
                            # Prendi sempre l'ultimo elemento come nome tabella, il penultimo come schema (se esiste)
                            if len(parts) >= 2:
                                self.schema = parts[-2]
                                self.table = parts[-1]
                            elif len(parts) == 1:
                                self.schema = ''
                                self.table = parts[0]
                            else:
                                self.schema = ''
                                self.table = table_full
                    # Nuova logica: se non è stata trovata una table, cerca temp table (# o ##)
                    if not getattr(self, 'table', None):
                        temp_table_match = re.search(r'(?:FROM|JOIN)\s+(#\#?[\w_]+)', cleaned_query, re.IGNORECASE)
                        if temp_table_match:
                            self.schema = ''
                            self.table = temp_table_match.group(1)
            if dbs_match:
                self.server = dbs_match.group(1)
                db_line = re.search(r'(Source|Origine)\{\[Name="([^"]+)"\]\}\[Data\]', content)
                if db_line:
                    self.database = db_line.group(2)
            elif db_match:
                self.server = db_match.group(1)
                self.database = db_match.group(2)

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
        # print(f"Server: {self.server}")
        # print(f"Database: {self.database}")
        # print(f"Schema: {self.schema}")
        # print(f"Table: {self.table}")
        # print(f"Query: {self.query}")
        # print(f"Source: {self.source}")
        print(f"File: {self.txt_file} | Table: {self.table}")