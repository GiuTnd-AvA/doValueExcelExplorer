# =========================
# IMPORT
# =========================
from Config.config import EXCEL_INPUT_PATH, EXCEL_OUTPUT_PATH
import pandas as pd
from sqlalchemy import create_engine, text
import pyodbc

# =========================
# CONFIG & COSTANTI
# =========================
try:
    DRIVER = 'ODBC+Driver+18+for+SQL+Server'
    if not any('ODBC Driver 18 for SQL Server' in d for d in pyodbc.drivers()):
        DRIVER = 'ODBC+Driver+17+for+SQL+Server'
except Exception:
    DRIVER = 'ODBC+Driver+17+for+SQL+Server'

excel_path = EXCEL_INPUT_PATH
output_path = EXCEL_OUTPUT_PATH

def get_conn_params(row):
    """Estrae i parametri di connessione da una riga del DataFrame."""
    return {
        "server": row.get('Server'),
        "db_name": row.get('Database'),
        "schema": row.get('Schema'),
        "table": row.get('Table'),
    }

def get_engine(server, db_name, engine_cache, driver):
    key = (server, db_name)
    if key not in engine_cache:
        conn_str = f"mssql+pyodbc://@{server}/{db_name}?driver={driver}&trusted_connection=yes"
        engine_cache[key] = create_engine(conn_str)
    return engine_cache[key]

def get_variants(schema, table):
    variants = set()
    if schema and schema not in ['', None]:
        variants.add(f"[{schema}].[{table}]")
        variants.add(f"{schema}.{table}")
    variants.add(f"[{table}]")
    variants.add(table)
    return variants
    
def estrai_sql_objects(engine, query, params, table_label, error_msg):
    import re
    sql_objects = []
    # Clausole T-SQL da cercare
    clause_ops = ["INSERT INTO", "UPDATE", "DELETE FROM", "MERGE INTO", "CREATE TABLE", "ALTER TABLE", "FROM", "JOIN"]
    try:
        with engine.connect() as conn:
            for r in conn.execute(text(query)):
                obj_type = r[1]
                sql_def = r[2]
                found_clauses = set()
                if sql_def:
                    sql_def_l = sql_def.lower()
                    for v in get_variants(params['schema'], params['table']):
                        v_l = v.lower()
                        for op in clause_ops:
                            op_l = op.lower()
                            # Regex: op + spazi + v + (spazio o parentesi quadra o fine riga), tollera alias dopo
                            pattern = rf"{re.escape(op_l)}\s+{re.escape(v_l)}(\s|\[|$)"
                            if re.search(pattern, sql_def_l):
                                found_clauses.add(f"{op} {v}")
                base = {
                    "Server": params['server'],
                    "Database": params['db_name'],
                    "Table": table_label,
                    "ObjectName": r[0],
                    "ObjectType": obj_type,
                    "SQLDefinition": sql_def,
                    "SQL_CLAUSE": "; ".join(sorted(found_clauses)) if found_clauses else None
                }
                sql_objects.append(base)
    except Exception as e:
        print(f"{error_msg}: {e}\nQuery: {query}")
    return sql_objects

def export_large_dataframe(df, base_path, sheet_name, prefix, batch_num, max_rows=1000000):
    for i in range(0, len(df), max_rows):
        chunk = df[i:i+max_rows]
        file_path = f"{base_path}_parziale_{prefix}_{batch_num}_{i//max_rows+1}.xlsx"
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            chunk.to_excel(writer, index=False, sheet_name=sheet_name)
        print(f"Export parziale: {file_path} ({len(chunk)} righe)")

def main():
    df = pd.read_excel(excel_path, sheet_name=6)
    total_rows = len(df)
    batch_size = 50
    engine_cache = dict()
    sql_objects = []
    error_log = []

    for i, (idx, row) in enumerate(df.iloc[101:].iterrows(), 102):
        print(f"Stato avanzamento: {i}/{total_rows}")
        params = get_conn_params(row)
        if not (params["server"] and params["db_name"] and params["table"]):
            print(f"SKIP: Parametri mancanti per riga {idx}")
            error_log.append({
                "Riga": idx,
                "Server": params["server"],
                "Database": params["db_name"],
                "Schema": params["schema"],
                "Table": params["table"],
                "Errore": "Parametri mancanti"
            })
            continue
        try:
            engine = get_engine(params['server'], params['db_name'], engine_cache, DRIVER)
            if i == 1 or (i % batch_size == 1):
                with engine.connect() as test_conn:
                    pass
        except Exception as e:
            print(f"ERRORE CONNESSIONE per riga {idx} (Server: {params['server']}, DB: {params['db_name']}): {e}")
            error_log.append({
                "Riga": idx,
                "Server": params["server"],
                "Database": params["db_name"],
                "Schema": params["schema"],
                "Table": params["table"],
                "Errore": str(e)
            })
            continue
        schema = params["schema"]
        table = params["table"]
        variants = get_variants(schema, table)
        conditions = []
        clause_ops = ["INSERT INTO", "UPDATE", "DELETE FROM", "MERGE INTO", "CREATE TABLE", "ALTER TABLE", "FROM", "JOIN"]
        for v in variants:
            for op in clause_ops:
                conditions.append(f"CHARINDEX('{op} {v}', sm.definition) > 0")
        where_clause = "\n                OR ".join(conditions)
        query = f"""
            SELECT o.name, o.type_desc, sm.definition
            FROM sys.sql_modules sm
            JOIN sys.objects o ON sm.object_id = o.object_id
            WHERE (
                {where_clause}
            )
            AND o.type_desc <> 'VIEW'
        """
        table_label = f"{schema}.{table}" if schema and schema not in ['', None] else table
        try:
            batch_objects = estrai_sql_objects(engine, query, params, table_label, f"Errore su {params['db_name']} tabella {table_label}")
            sql_objects.extend(batch_objects)
        except Exception as e:
            print(f"ERRORE QUERY per riga {idx} (Server: {params['server']}, DB: {params['db_name']}): {e}")
            error_log.append({
                "Riga": idx,
                "Server": params["server"],
                "Database": params["db_name"],
                "Schema": params["schema"],
                "Table": params["table"],
                "Errore": f"Query: {e}"
            })
            continue
        if i % batch_size == 0:
            export_large_dataframe(pd.DataFrame(sql_objects), output_path, 'Oggetti T-Sql', 'oggetti', i // batch_size)
            sql_objects.clear()
    if sql_objects:
        export_large_dataframe(pd.DataFrame(sql_objects), output_path, 'Oggetti T-Sql', 'oggetti', (total_rows // batch_size) + 1)

    # Log riepilogativo errori
    if error_log:
        error_df = pd.DataFrame(error_log)
        error_file = f"{output_path}_error_log.xlsx"
        error_df.to_excel(error_file, index=False)
        print(f"\nLog errori esportato in: {error_file}")
    else:
        print("\nNessun errore di connessione o query.")

if __name__ == "__main__":
    main()
       
