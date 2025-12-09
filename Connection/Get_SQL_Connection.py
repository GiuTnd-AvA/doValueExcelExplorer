from Connection.IConnection import IConnection
import re

class GetSqlConnection(IConnection):

    def __init__(self, txt_file):
        super().__init__(txt_file)

    def get_connection(self):
        with open(self.txt_file, 'r', encoding='utf-8') as f:
            content = f.read()

        # Source (riga con Source = Sql.Databases("...") oppure Sql.Database("...", "..."))
        source_match = re.search(r'(Source|Origine)\s*=\s*(Sql\.Databases\([^)]+\)|Sql\.Database\([^)]+\))', content)
        self.source = source_match.group(2).strip() if source_match else None

        # Server e Database
        # self.server = None
        # self.database = None
        if self.source:
            dbs_match = re.match(r'Sql\.Databases\("([^"]+)"\)', self.source)
            db_match = re.match(r'Sql\.Database\("([^"]+)",\s*"([^"]+)"\)', self.source)
            if dbs_match:
                self.server = dbs_match.group(1)
                # Cerca la riga che accede al database
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