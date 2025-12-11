from .IDBConnection import IDBConnection
from sqlalchemy import text

class SqlServerDatabaseConnection(IDBConnection):
    DRIVER = 'SQL Server'

    def __init__(self, server, database, schema=None, table=None):
        super().__init__(server, database, schema, table)

    def find_table_items(self):
        results = []
        schema_valid = self.schema not in ['', None]
        table_valid = self.table not in ['', None]
        if not table_valid:
            return results

        if schema_valid:
            query = f"""
            SELECT o.name, o.type_desc, sm.definition
            FROM sys.sql_modules sm
            JOIN sys.objects o ON sm.object_id = o.object_id
            WHERE CHARINDEX('FROM [{self.schema}].[{self.table}]', sm.definition) > 0
               OR CHARINDEX('JOIN [{self.schema}].[{self.table}]', sm.definition) > 0
               OR CHARINDEX('FROM {self.schema}.{self.table}', sm.definition) > 0
               OR CHARINDEX('JOIN {self.schema}.{self.table}', sm.definition) > 0
               OR CHARINDEX('FROM {self.table}', sm.definition) > 0
               OR CHARINDEX('JOIN {self.table}', sm.definition) > 0
            """
            table_label = f"{self.schema}.{self.table}"
        else:
            query = f"""
            SELECT o.name, o.type_desc, sm.definition
            FROM sys.sql_modules sm
            JOIN sys.objects o ON sm.object_id = o.object_id
            WHERE CHARINDEX('FROM {self.table}', sm.definition) > 0
               OR CHARINDEX('JOIN {self.table}', sm.definition) > 0
            """
            table_label = self.table

        try:
            conn = self._connect()  # Usa il metodo connessione della classe madre
            for r in conn.execute(text(query)):
                results.append({
                    "Table": table_label,
                    "ObjectName": r[0],
                    "ObjectType": r[1],
                    "SQLDefinition": r[2]
                })
        except Exception as e:
            print(f"Errore su tabella {table_label}: {e}")

        return results
