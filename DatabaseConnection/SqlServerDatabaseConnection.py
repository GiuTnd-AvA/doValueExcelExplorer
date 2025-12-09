from .IDBConnection import IDBConnection

class SqlServerDatabaseConnection(IDBConnection):
    DRIVER = 'SQL Server'

def find_table_items(connection, schema, table):
    """
    Restituisce tutti gli elementi (colonne) associati alla tabella specificata,
    inclusi nome colonna, tipo, se è nullable, e chiave primaria.
    """
    cursor = connection.cursor()
    query = f"""
        SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE,
            CASE WHEN k.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN (
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
                AND TABLE_SCHEMA = ?
                AND TABLE_NAME = ?
        ) k ON c.COLUMN_NAME = k.COLUMN_NAME
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
    """
    cursor.execute(query, (schema, table, schema, table))
    return cursor.fetchall()