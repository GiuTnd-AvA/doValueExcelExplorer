import pandas as pd
import re
from sqlalchemy import create_engine, text

# Configurazione: modifica questi parametri
DRIVER = 'ODBC+Driver+17+for+SQL+Server'

# Leggi l'Excel di output
excel_path = r'C:\Users\ciro.andreano\Desktop\Connessioni Trovate.xlsx'
output_path = r'C:\Users\ciro.andreano\Desktop\Risultati_SQL.xlsx'
df = pd.read_excel(excel_path)

results = []

# Funzione per estrarre il server dalla stringa ConnessioneSorgente

def extract_server(sorgente):
    s = str(sorgente)
    # Cerca sia Sql.Database("server", ...) che Sql.Databases("server")
    match = re.search(r'Sql\.(?:Database|Databases)\(["\'](.*?)["\']', s)
    return match.group(1) if match else None

for idx, row in df.iterrows():
    server = extract_server(row.get('ConnessioneSorgente'))
    schema = row.get('Schema')
    table = row.get('TableName')
    db_name = row.get('Sorgente')
    file_name = row.get('FileName')
    # Salta se db_name è vuoto, None o nan
    if not db_name or str(db_name).strip().lower() in ('', 'nan', 'none'):
        continue
    # Normalizza schema e table
    schema_valid = schema and schema not in ['Schema non individuato', '', None]
    table_valid = table and table not in ['Tabella non individuata', 'Item non individuato', '', None]
    if server and table_valid:
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
