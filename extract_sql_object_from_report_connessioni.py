from Config.config import EXCEL_INPUT_PATH, EXCEL_OUTPUT_PATH
import pandas as pd
from sqlalchemy import create_engine, text

# Usa il driver ODBC 18 se disponibile, altrimenti fallback su 17
try:
    DRIVER = 'ODBC+Driver+18+for+SQL+Server'
    # Test rapido per vedere se il driver 18 è disponibile
    import pyodbc
    if not any('ODBC Driver 18 for SQL Server' in d for d in pyodbc.drivers()):
        DRIVER = 'ODBC+Driver+17+for+SQL+Server'
except Exception:
    DRIVER = 'ODBC+Driver+17+for+SQL+Server'

excel_path = EXCEL_INPUT_PATH
output_path = EXCEL_OUTPUT_PATH
df = pd.read_excel(excel_path)

def get_conn_params(row):
    return {
        "server": row.get('Server'),
        "db_name": row.get('Database'),
        "schema": row.get('Schema'),
        "table": row.get('Table'),
        "file_name": row.get('File_Name'),
        "type": row.get('Type')
    }

def estrai_e_append(engine, query, result_list, row_transform, error_msg):
    try:
        print(f"Eseguo query: {query}")
        with engine.connect() as conn:
            for r in conn.execute(text(query)):
                result_list.append(row_transform(r))
    except Exception as e:
        print(f"{error_msg}: {e}\nQuery: {query}")

results = []
dipendenze = []
dipendenze_inverse = []
elenco_tabelle = []
struttura_colonne = []

for idx, row in df.iterrows():
    params = get_conn_params(row)
    # --- CONNESSIONE E VALIDAZIONE PARAMETRI ---
    if (
        not params["type"]
        or str(params["type"]).lower() != "sql"
        or not (params["server"] and params["db_name"] and params["table"])
    ):
        continue
    conn_str = f"mssql+pyodbc://@{params['server']}/{params['db_name']}?driver={DRIVER}&trusted_connection=yes"
    engine = create_engine(conn_str)
    schema_valid = params["schema"] not in ['', None]
    table_valid = params["table"] not in ['', None]

    # --- RISULTATI ---
    if table_valid:
        if schema_valid:
            query = f"""
            SELECT o.name, o.type_desc, sm.definition
            FROM sys.sql_modules sm
            JOIN sys.objects o ON sm.object_id = o.object_id
            WHERE CHARINDEX('FROM [{params['schema']}].[{params['table']}]', sm.definition) > 0
               OR CHARINDEX('JOIN [{params['schema']}].[{params['table']}]', sm.definition) > 0
               OR CHARINDEX('FROM {params['schema']}.{params['table']}', sm.definition) > 0
               OR CHARINDEX('JOIN {params['schema']}.{params['table']}', sm.definition) > 0
               OR CHARINDEX('FROM {params['table']}', sm.definition) > 0
               OR CHARINDEX('JOIN {params['table']}', sm.definition) > 0
            """
            table_label = f"{params['schema']}.{params['table']}"
        else:
            query = f"""
            SELECT o.name, o.type_desc, sm.definition
            FROM sys.sql_modules sm
            JOIN sys.objects o ON sm.object_id = o.object_id
            WHERE CHARINDEX('FROM {params['table']}', sm.definition) > 0
               OR CHARINDEX('JOIN {params['table']}', sm.definition) > 0
            """
            table_label = params['table']
        estrai_e_append(
            engine,
            query,
            results,
            lambda r: {
                "FileName": params['file_name'],
                "Server": params['server'],
                "Database": params['db_name'],
                "Table": table_label,
                "Type": params['type'],
                "ObjectName": r[0],
                "ObjectType": r[1],
                "SQLDefinition": r[2]
            },
            f"Errore su {params['file_name']} ({params['db_name']}) tabella {table_label}"
        )

    # --- DIPENDENZE ---
    dep_query = f"""
    SELECT referenced_entity_name, referenced_class_desc
    FROM sys.sql_expression_dependencies
    WHERE referencing_id = OBJECT_ID('{params['table']}')
    """
    estrai_e_append(
        engine,
        dep_query,
        dipendenze,
        lambda dep: {
            "FileName": params['file_name'],
            "Database": params['db_name'],
            "Table": params['table'],
            "ObjectName": params['table'],
            "ObjectType": None,
            "Dipendenza": dep[0],
            "DipendenzaType": dep[1]
        },
        f"Errore dipendenze per {params['table']} in {params['db_name']}"
    )

    # --- DIPENDENZE INVERSE ---
    if schema_valid:
        tabella_full = f"{params['schema']}.{params['table']}"
    else:
        tabella_full = params['table']
    inv_query = f"""
    SELECT OBJECT_NAME(referencing_id) AS referencing_entity_name, referencing_class_desc, referenced_entity_name, referenced_class_desc
    FROM sys.sql_expression_dependencies
    WHERE referenced_entity_name = '{params['table']}' OR referenced_entity_name = '{tabella_full}'
    """
    estrai_e_append(
        engine,
        inv_query,
        dipendenze_inverse,
        lambda inv: {
            "FileName": params['file_name'],
            "Database": params['db_name'],
            "Table": tabella_full,
            "ReferencingObject": inv[0],
            "ReferencingType": inv[1],
            "ReferencedEntity": inv[2],
            "ReferencedType": inv[3]
        },
        f"Errore dipendenze inverse per tabella {tabella_full} in {params['db_name']}"
    )

    # --- ELENCO TABELLE ---
    tab_query = f"""
    SELECT t.name AS NomeTabella, s.name AS SchemaName, t.type_desc AS TableType, ep.value AS TableDescription
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id AND ep.name = 'MS_Description'
    WHERE t.name = '{params['table']}' AND s.name = '{params['schema']}'
    """
    estrai_e_append(
        engine,
        tab_query,
        elenco_tabelle,
        lambda tab: {
            "Nome Tabella": tab[0],
            "Schema": tab[1],
            "Tipo": tab[2],
            "Descrizione": tab[3]
        },
        f"Errore elenco tabelle per {params['table']}"
    )

    # --- STRUTTURA COLONNE ---
    col_query = f"""
    SELECT c.table_name AS NomeTabella, c.column_name AS NomeColonna, c.data_type AS TipoDato, c.character_maximum_length AS Lunghezza,
           CASE WHEN kcu.column_name IS NOT NULL THEN 'PK' ELSE '' END AS PK,
           CASE WHEN fkcu.column_name IS NOT NULL THEN 'FK' ELSE '' END AS FK,
           c.is_nullable AS IsNullable,
           c.column_default AS DefaultValue
    FROM INFORMATION_SCHEMA.COLUMNS c
    LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    FROM INFORMATION_SCHEMA.COLUMNS c
    LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        ON c.table_name = kcu.table_name AND c.column_name = kcu.column_name AND kcu.constraint_name LIKE 'PK%'
    LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE fkcu
        ON c.table_name = fkcu.table_name AND c.column_name = fkcu.column_name AND fkcu.constraint_name LIKE 'FK%'
    WHERE c.table_name = '{params['table']}' AND c.table_schema = '{params['schema']}'
    """
    estrai_e_append(
        engine,
        col_query,
        struttura_colonne,
        lambda col: {
            "Nome Tabella": col[0],
            "Nome Colonna": col[1],
            "Tipo Dato": col[2],
            "Lunghezza": col[3],
            "PK": col[4],
            "FK": col[5],
            "IsNullable": col[6],
            "DefaultValue": col[7],
            "Descrizione": None
        },
        f"Errore struttura colonne per {params['table']}"
    )

with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    pd.DataFrame(elenco_tabelle).to_excel(writer, index=False, sheet_name='ElencoTabelle')
    pd.DataFrame(struttura_colonne).to_excel(writer, index=False, sheet_name='StrutturaColonne')
    pd.DataFrame(results).to_excel(writer, index=False, sheet_name='Risultati')
    pd.DataFrame(dipendenze).to_excel(writer, index=False, sheet_name='Dipendenze')
    pd.DataFrame(dipendenze_inverse).to_excel(writer, index=False, sheet_name='DipendenzeTabella')
    print(f"Risultati esportati in: {output_path}")
