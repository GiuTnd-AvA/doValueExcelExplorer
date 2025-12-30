
# --- INIZIALIZZAZIONI E IMPORT NECESSARI ---

# =========================
# IMPORT
# =========================
from Config.config import EXCEL_INPUT_PATH, EXCEL_OUTPUT_PATH
import pandas as pd
from sqlalchemy import create_engine, text
import pyodbc
import re

# =========================
# CONFIG & COSTANTI
# =========================
# Usa il driver ODBC 18 se disponibile, altrimenti fallback su 17
try:
    DRIVER = 'ODBC+Driver+18+for+SQL+Server'
    if not any('ODBC Driver 18 for SQL Server' in d for d in pyodbc.drivers()):
        DRIVER = 'ODBC+Driver+17+for+SQL+Server'
except Exception:
    DRIVER = 'ODBC+Driver+17+for+SQL+Server'

excel_path = EXCEL_INPUT_PATH
output_path = EXCEL_OUTPUT_PATH

# =========================
# FUNZIONI DI UTILITÀ
# =========================
def get_conn_params(row):
    return {
        "server": row.get('Server'),
        "db_name": row.get('Database'),
        "schema": row.get('Schema'),
        "table": row.get('Table'),
        "file_name": row.get('File_Name')
    }

def estrai_e_append_multi(engine, query, result_list, row_transform, error_msg):
    try:
        with engine.connect() as conn:
            for r in conn.execute(text(query)):
                res = row_transform(r)
                if isinstance(res, list):
                    result_list.extend(res)
                elif res is not None:
                    result_list.append(res)
    except Exception as e:
        print(f"{error_msg}: {e}\nQuery: {query}")

# =========================
# MAIN: ESTRAZIONE DATI
# =========================
df = pd.read_excel(excel_path, sheet_name=0)

total_rows = len(df)

# --- OTTIMIZZAZIONE: cache connessioni e export parziale ---
from collections import defaultdict

results = []
dipendenze = []
dipendenze_inverse = []
elenco_tabelle = []
struttura_colonne = []

engine_cache = dict()
def get_engine(server, db_name):
    key = (server, db_name)
    if key not in engine_cache:
        conn_str = f"mssql+pyodbc://@{server}/{db_name}?driver={DRIVER}&trusted_connection=yes"
        engine_cache[key] = create_engine(conn_str)
    return engine_cache[key]


def export_partial(results, dipendenze, output_path, batch_num):
    def export_large_dataframe(df, base_path, sheet_name, prefix):
        max_rows = 1000000
        for i in range(0, len(df), max_rows):
            chunk = df[i:i+max_rows]
            file_path = f"{base_path}_parziale_{prefix}_{batch_num}_{i//max_rows+1}.xlsx"
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                chunk.to_excel(writer, index=False, sheet_name=sheet_name)
            print(f"Export parziale: {file_path} ({len(chunk)} righe)")
    export_large_dataframe(pd.DataFrame(results), output_path, 'Risultati', 'results')
    export_large_dataframe(pd.DataFrame(dipendenze), output_path, 'Dipendenze', 'dipendenze')

total_rows = len(df)
batch_size = 100
for i, (idx, row) in enumerate(df.iterrows(), 1):
    print(f"Stato avanzamento: {i}/{total_rows}")
    params = get_conn_params(row)
    if not (params["server"] and params["db_name"] and params["table"]):
        print(f"SKIP: Parametri mancanti per riga {idx}")
        continue
    try:
        engine = get_engine(params['server'], params['db_name'])
        # Test connessione solo la prima volta
        if i == 1 or (i % batch_size == 1):
            with engine.connect() as test_conn:
                pass
    except Exception as e:
        print(f"ERRORE CONNESSIONE per riga {idx} (Server: {params['server']}, DB: {params['db_name']}): {e}")
        continue
    schema_valid = params["schema"] not in ['', None]
    table_valid = params["table"] not in ['', None]

    # --- RISULTATI: oggetti che usano la tabella ---
    # if table_valid:
    #     if schema_valid:
    #         query = f"""
    #         SELECT o.name, o.type_desc, sm.definition
    #         FROM sys.sql_modules sm
    #         JOIN sys.objects o ON sm.object_id = o.object_id
    #         WHERE CHARINDEX('FROM [{params['schema']}].[{params['table']}]', sm.definition) > 0
    #            OR CHARINDEX('JOIN [{params['schema']}].[{params['table']}]', sm.definition) > 0
    #            OR CHARINDEX('FROM {params['schema']}.{params['table']}', sm.definition) > 0
    #            OR CHARINDEX('JOIN {params['schema']}.{params['table']}', sm.definition) > 0
    #            OR CHARINDEX('FROM {params['table']}', sm.definition) > 0
    #            OR CHARINDEX('JOIN {params['table']}', sm.definition) > 0
    #         """
    #         table_label = f"{params['schema']}.{params['table']}"
    #     else:
    #         query = f"""
    #         SELECT o.name, o.type_desc, sm.definition
    #         FROM sys.sql_modules sm
    #         JOIN sys.objects o ON sm.object_id = o.object_id
    #         WHERE CHARINDEX('FROM {params['table']}', sm.definition) > 0
    #            OR CHARINDEX('JOIN {params['table']}', sm.definition) > 0
    #         """
    #         table_label = params['table']
    #     estrai_e_append_multi(
    #         engine,
    #         query,
    #         results,
    #         lambda r: {
    #             "FileName": params['file_name'],
    #             "Server": params['server'],
    #             "Database": params['db_name'],
    #             "Table": table_label,
    #             "ObjectName": r[0],
    #             "ObjectType": r[1],
    #             "SQLDefinition": r[2]
    #         },
    #         f"Errore su {params['file_name']} ({params['db_name']}) tabella {table_label}"
    #     )

    # --- DIPENDENZE: tutte le tabelle usate da ogni oggetto SQL ---
    dep_sql = """
    SELECT o.name, o.type_desc, sm.definition
    FROM sys.sql_modules sm
    JOIN sys.objects o ON sm.object_id = o.object_id
    """
    def dipendenze_row_transform(row):
        obj_name, obj_type, sql_def = row
        tables = set()
        if sql_def:
            for m in re.findall(r'(?:FROM|JOIN)\s+([\[\]\w]+)\.([\[\]\w]+)', sql_def, re.IGNORECASE):
                schema, table = m
                schema = schema.strip('[]')
                table = table.strip('[]')
                if table and table.lower() != schema.lower():
                    tables.add(f"{schema}.{table}")
            for m in re.findall(r'(?:FROM|JOIN)\s+([\[\]\w]+)', sql_def, re.IGNORECASE):
                t = m.strip('[]')
                if '.' not in t and t.lower() not in {"dbo", "sys", "information_schema", "guest", "db_owner", "db_accessadmin", "db_securityadmin", "db_ddladmin", "db_backupoperator", "db_datareader", "db_datawriter", "db_denydatareader", "db_denydatawriter"} and len(t) > 1:
                    tables.add(t)
        def get_dep_type(dep, db_name):
            # cross-database se inizia con un nome db diverso da quello corrente
            parts = dep.split('.')
            if len(parts) == 2:
                # schema.table oppure db.schema
                if parts[0].lower() != db_name.lower():
                    return 'cross-database'
                else:
                    return 'intra-database'
            elif len(parts) == 3:
                # db.schema.table
                if parts[0].lower() != db_name.lower():
                    return 'cross-database'
                else:
                    return 'intra-database'
            else:
                return 'intra-database'
        return [
            {
                "FileName": params['file_name'],
                "Database": params['db_name'],
                "Table": t,
                "ObjectName": obj_name,
                "ObjectType": obj_type,
                "Dipendenza": t,
                "DipendenzaType": get_dep_type(t, params['db_name'])
            }
            for t in tables
        ] if tables else None

    estrai_e_append_multi(
        engine,
        dep_sql,
        dipendenze,
        dipendenze_row_transform,
        f"Errore estrazione dipendenze complete per {params['db_name']}"
    )

    # Export parziale ogni batch_size record
    if i % batch_size == 0:
        export_partial(results, dipendenze, output_path, i // batch_size)
        results.clear()
        dipendenze.clear()


# Export finale in più file se necessario
def export_large_dataframe(df, base_path, sheet_name, prefix):
    max_rows = 1000000
    for i in range(0, len(df), max_rows):
        chunk = df[i:i+max_rows]
        file_path = f"{base_path}_{prefix}_{i//max_rows+1}.xlsx"
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            chunk.to_excel(writer, index=False, sheet_name=sheet_name)
        print(f"Esportato: {file_path} ({len(chunk)} righe)")

export_large_dataframe(pd.DataFrame(results), output_path, 'Risultati', 'final')
export_large_dataframe(pd.DataFrame(dipendenze), output_path, 'Dipendenze', 'final')

# =========================
# EXPORT RISULTATI
# =========================
with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    # pd.DataFrame(elenco_tabelle).to_excel(writer, index=False, sheet_name='ElencoTabelle')
    # pd.DataFrame(struttura_colonne).to_excel(writer, index=False, sheet_name='StrutturaColonne')
    pd.DataFrame(results).to_excel(writer, index=False, sheet_name='Risultati')
    pd.DataFrame(dipendenze).to_excel(writer, index=False, sheet_name='Dipendenze')
    # pd.DataFrame(dipendenze_inverse).to_excel(writer, index=False, sheet_name='DipendenzeTabella')
    print(f"Risultati esportati in: {output_path}")
    

    # # --- DIPENDENZE INVERSE ---
    # if schema_valid:
    #     tabella_full = f"{params['schema']}.{params['table']}"
    # else:
    #     tabella_full = params['table']
    # inv_query = f"""
    # SELECT OBJECT_NAME(referencing_id) AS referencing_entity_name, referencing_class_desc, referenced_entity_name, referenced_class_desc
    # FROM sys.sql_expression_dependencies
    # WHERE referenced_entity_name = '{params['table']}' OR referenced_entity_name = '{tabella_full}'
    # """
    # estrai_e_append(
    #     engine,
    #     inv_query,
    #     dipendenze_inverse,
    #     lambda inv: {
    #         "FileName": params['file_name'],
    #         "Database": params['db_name'],
    #         "Table": tabella_full,
    #         "ReferencingObject": inv[0],
    #         "ReferencingType": inv[1],
    #         "ReferencedEntity": inv[2],
    #         "ReferencedType": inv[3]
    #     },
    #     f"Errore dipendenze inverse per tabella {tabella_full} in {params['db_name']}"
    # )

    # # --- ELENCO TABELLE ---
    # tab_query = f"""
    # SELECT t.name AS NomeTabella, s.name AS SchemaName, t.type_desc AS TableType, ep.value AS TableDescription
    # FROM sys.tables t
    # JOIN sys.schemas s ON t.schema_id = s.schema_id
    # LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id AND ep.name = 'MS_Description'
    # WHERE t.name = '{params['table']}' AND s.name = '{params['schema']}'
    # """
    # estrai_e_append(
    #     engine,
    #     tab_query,
    #     elenco_tabelle,
    #     lambda tab: {
    #         "Nome Tabella": tab[0],
    #         "Schema": tab[1],
    #         "Tipo": tab[2],
    #         "Descrizione": tab[3]
    #     },
    #     f"Errore elenco tabelle per {params['table']}"
    # )

    # --- STRUTTURA COLONNE ---
    # col_query = f"""
    # SELECT c.table_name AS NomeTabella, c.column_name AS NomeColonna, c.data_type AS TipoDato, c.character_maximum_length AS Lunghezza,
    #        CASE WHEN kcu.column_name IS NOT NULL THEN 'PK' ELSE '' END AS PK,
    #        CASE WHEN fkcu.column_name IS NOT NULL THEN 'FK' ELSE '' END AS FK,
    #        c.is_nullable AS IsNullable,
    #        c.column_default AS DefaultValue
    # FROM INFORMATION_SCHEMA.COLUMNS c
    # LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    # FROM INFORMATION_SCHEMA.COLUMNS c
    # LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    #     ON c.table_name = kcu.table_name AND c.column_name = kcu.column_name AND kcu.constraint_name LIKE 'PK%'
    # LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE fkcu
    #     ON c.table_name = fkcu.table_name AND c.column_name = fkcu.column_name AND fkcu.constraint_name LIKE 'FK%'
    # WHERE c.table_name = '{params['table']}' AND c.table_schema = '{params['schema']}'
    # """
    # estrai_e_append(
    #     engine,
    #     col_query,
    #     struttura_colonne,
    #     lambda col: {
    #         "Nome Tabella": col[0],
    #         "Nome Colonna": col[1],
    #         "Tipo Dato": col[2],
    #         "Lunghezza": col[3],
    #         "PK": col[4],
    #         "FK": col[5],
    #         "IsNullable": col[6],
    #         "DefaultValue": col[7],
    #         "Descrizione": None
    #     },
    #     f"Errore struttura colonne per {params['table']}"
    # )

