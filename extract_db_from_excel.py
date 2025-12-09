from Config.config import EXCEL_INPUT_PATH, EXCEL_OUTPUT_PATH
import pandas as pd
import re
from sqlalchemy import create_engine, text

# Configurazione: modifica questi parametri
DRIVER = 'ODBC+Driver+17+for+SQL+Server'

# Leggi l'Excel di output
excel_path = EXCEL_INPUT_PATH
output_path = EXCEL_OUTPUT_PATH
df = pd.read_excel(excel_path)

results = []

for idx, row in df.iterrows():
    # Esegui solo se Type == 'Sql'
    if row.get('Type', '').lower() != 'sql':
        continue
    server = row.get('Server')
    db_name = row.get('Database')
    schema = row.get('Schema')
    table = row.get('Table')
    file_name = row.get('FileName')
    # Salta se server, db_name o table sono vuoti
    if not server or not db_name or not table:
        continue
    schema_valid = schema not in ['', None]
    table_valid = table not in ['', None]
    if table_valid:
        conn_str = f"mssql+pyodbc://@{server}/{db_name}?driver={DRIVER}&trusted_connection=yes"
        engine = create_engine(conn_str)
        if schema_valid:
            # Cerca con schema e tabella
            query = f"""
            SELECT o.name, o.type_desc, sm.definition
            FROM sys.sql_modules sm
            JOIN sys.objects o ON sm.object_id = o.object_id
            WHERE CHARINDEX('FROM [{schema}].[{table}]', sm.definition) > 0
               OR CHARINDEX('JOIN [{schema}].[{table}]', sm.definition) > 0
               OR CHARINDEX('FROM {schema}.{table}', sm.definition) > 0
               OR CHARINDEX('JOIN {schema}.{table}', sm.definition) > 0
               OR CHARINDEX('FROM {table}', sm.definition) > 0
               OR CHARINDEX('JOIN {table}', sm.definition) > 0
            """
            table_label = f"{schema}.{table}"
        else:
            # Cerca solo per tabella senza schema
            query = f"""
            SELECT o.name, o.type_desc, sm.definition
            FROM sys.sql_modules sm
            JOIN sys.objects o ON sm.object_id = o.object_id
            WHERE CHARINDEX('FROM {table}', sm.definition) > 0
               OR CHARINDEX('JOIN {table}', sm.definition) > 0
            """
            table_label = table
        try:
            with engine.connect() as conn:
                for r in conn.execute(text(query)):
                    results.append({
                        "FileName": file_name,
                        "Server": server,
                        "Database": db_name,
                        "Table": table_label,
                        "Type": row.get('Type'),
                        "ObjectName": r[0],
                        "ObjectType": r[1],
                        "SQLDefinition": r[2]
                    })
        except Exception as e:
            print(f"Errore su {file_name} ({db_name}) tabella {table_label}: {e}")
    else:
        print(f"[SKIP] Riga non valida: File={file_name}, Server={server}, Database={db_name}, Schema={schema}, Table={table}")

if results:
    pd.DataFrame(results).to_excel(output_path, index=False)
    print(f"Risultati esportati in: {output_path}")
else:
    print("Nessun risultato trovato.")
