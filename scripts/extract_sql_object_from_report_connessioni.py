# =========================
# IMPORT
# =========================
from config.config import EXCEL_INPUT_PATH, EXCEL_OUTPUT_PATH
import pandas as pd
from sqlalchemy import create_engine, text
import pyodbc
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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
SHEET_INDEX = 0 
BATCH_SIZE = 50
START_ROW = 102
MAX_WORKERS = 4  # Thread paralleli per query SQL  

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
        # Pool_pre_ping per verificare connessioni stale
        engine_cache[key] = create_engine(conn_str, pool_pre_ping=True, pool_size=10, max_overflow=20)
    return engine_cache[key]

def get_variants(schema, table, db_name=None):
    """Genera tutte le possibili varianti del nome tabella.
    Include database prefix per gestire cross-database references.
    """
    variants = set()
    
    # Database prefixes comuni da considerare
    db_prefixes = []
    if db_name and db_name not in ['', None]:
        db_prefixes = [
            db_name.upper(),  # ANALISI, CORESQL7, S1259
            db_name.lower(),  # analisi, coresql7, s1259
            db_name,          # Mixed case
        ]
    
    # Varianti base (senza database)
    if schema and schema not in ['', None]:
        variants.add(f"[{schema}].[{table}]")
        variants.add(f"{schema}.{table}")
        variants.add(f"[{schema}].{table}")
        variants.add(f"{schema}.[{table}]")
    variants.add(f"[{table}]")
    variants.add(table)
    
    # Aggiungi varianti con database prefix
    for db_prefix in db_prefixes:
        if schema and schema not in ['', None]:
            variants.add(f"{db_prefix}.{schema}.{table}")
            variants.add(f"[{db_prefix}].[{schema}].[{table}]")
            variants.add(f"{db_prefix}.[{schema}].[{table}]")
            variants.add(f"[{db_prefix}].{schema}.{table}")
            variants.add(f"{db_prefix}.{schema}.[{table}]")
            variants.add(f"{db_prefix}.[{schema}].{table}")
        variants.add(f"{db_prefix}..{table}")
        variants.add(f"{db_prefix}..[{table}]")
        variants.add(f"[{db_prefix}]..[{table}]")
        variants.add(f"[{db_prefix}]..{table}")
    
    return variants
    
def estrai_sql_objects(engine, query, params, table_label, error_msg):
    import re
    sql_objects = []
    # Clausole T-SQL da cercare (semplificate per performance)
    clause_ops = ["INSERT INTO", "UPDATE", "DELETE FROM", "MERGE INTO", "CREATE TABLE", "ALTER TABLE", "FROM", "JOIN"]
    
    try:
        with engine.connect() as conn:
            results = list(conn.execute(text(query)))  # Fetch tutti i risultati una volta
            
            for r in results:
                obj_type = r[1]
                sql_def = r[2]
                found_clauses = set()
                found_clause_types = set()
                
                if sql_def:
                    sql_def_l = sql_def.lower()
                    # Pre-compila le varianti una volta sola
                    variants_lower = [v.lower() for v in get_variants(params['schema'], params['table'], params.get('db_name'))]
                    
                    # Ottimizzazione: prima verifica se almeno una variante è presente
                    if any(v_l in sql_def_l for v_l in variants_lower):
                        for v, v_l in zip(get_variants(params['schema'], params['table'], params.get('db_name')), variants_lower):
                            for op in clause_ops:
                                op_l = op.lower()
                                # Verifica se l'operazione è presente nel codice
                                if op_l in sql_def_l and v_l in sql_def_l:
                                    # Pattern più flessibile: gestisce spazi multipli, tab, newline
                                    # Per tutte le clausole usiamo pattern flessibile per catturare anche con spazi/caratteri intermedi
                                    if op_l in ['join', 'from']:
                                        # Per JOIN/FROM: cerca la tabella anche se non immediatamente dopo
                                        # Pattern: (FROM|JOIN) ... tabella (con max 500 caratteri di distanza)
                                        pattern = rf"{re.escape(op_l)}\b[^;]{{0,500}}?\b{re.escape(v_l)}\b"
                                    else:
                                        # Per INSERT INTO, UPDATE, DELETE FROM, etc.: pattern più tollerante agli spazi
                                        # Gestisce: INSERT INTO  tabella, INSERT INTO\n\ttabella, etc.
                                        pattern = rf"{re.escape(op_l)}\s+{re.escape(v_l)}\b"
                                    
                                    if re.search(pattern, sql_def_l):
                                        found_clauses.add(f"{op} {v}")
                                        found_clause_types.add(op)
                
                base = {
                    "Server": params['server'],
                    "Database": params['db_name'],
                    "Table": table_label,
                    "ObjectName": r[0],
                    "ObjectType": obj_type,
                    "SQLDefinition": sql_def,
                    "SQL_CLAUSE": "; ".join(sorted(found_clauses)) if found_clauses else None,
                    "CLAUSE_TYPE": "; ".join(sorted(found_clause_types)) if found_clause_types else None
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

def process_single_table(idx, row, engine_cache, driver, error_log_lock):
    """Processa una singola tabella - usabile in parallelo."""
    local_error_log = []
    local_objects = []
    
    params = get_conn_params(row)
    if not (params["server"] and params["db_name"] and params["table"]):
        local_error_log.append({
            "Riga": idx,
            "Server": params["server"],
            "Database": params["db_name"],
            "Schema": params["schema"],
            "Table": params["table"],
            "Errore": "Parametri mancanti"
        })
        return local_objects, local_error_log
    
    try:
        engine = get_engine(params['server'], params['db_name'], engine_cache, driver)
        
        schema = params["schema"]
        table = params["table"]
        
        # Query ottimizzata: usa sys.sql_expression_dependencies invece di CHARINDEX
        query = f"""
            SELECT DISTINCT 
                o.name, 
                o.type_desc, 
                sm.definition
            FROM sys.sql_expression_dependencies sed
            INNER JOIN sys.objects o ON sed.referencing_id = o.object_id
            INNER JOIN sys.sql_modules sm ON o.object_id = sm.object_id
            INNER JOIN sys.objects ref_obj ON sed.referenced_id = ref_obj.object_id
            WHERE ref_obj.name = '{table}'
            {f"AND SCHEMA_NAME(ref_obj.schema_id) = '{schema}'" if schema and schema not in ['', None] else ''}
            AND o.type_desc <> 'VIEW'
            AND o.type IN ('P', 'FN', 'IF', 'TF', 'TR')
        """
        
        table_label = f"{schema}.{table}" if schema and schema not in ['', None] else table
        batch_objects = estrai_sql_objects(engine, query, params, table_label, 
                                          f"Errore su {params['db_name']} tabella {table_label}")
        local_objects.extend(batch_objects)
        
    except Exception as e:
        local_error_log.append({
            "Riga": idx,
            "Server": params["server"],
            "Database": params["db_name"],
            "Schema": params["schema"],
            "Table": params["table"],
            "Errore": f"Query: {e}"
        })
    
    return local_objects, local_error_log

def main():
    import time
    start_time = time.time()
    
    df = pd.read_excel(excel_path, sheet_name=SHEET_INDEX)
    total_rows = len(df)
    batch_size = BATCH_SIZE
    engine_cache = dict()
    sql_objects = []
    error_log = []
    error_log_lock = threading.Lock()
    
    print(f"Inizio processing di {total_rows - START_ROW + 1} tabelle con {MAX_WORKERS} thread paralleli")
    print(f"Query ottimizzata: sys.sql_expression_dependencies (no CHARINDEX)\n")
    
    # Processing parallelo per batch
    rows_to_process = list(df.iloc[START_ROW - 1:].iterrows())
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        
        for i, (idx, row) in enumerate(rows_to_process, START_ROW):
            future = executor.submit(process_single_table, idx, row, engine_cache, DRIVER, error_log_lock)
            futures.append((i, idx, future))
            
            # Limita il numero di future in memoria
            if len(futures) >= batch_size:
                # Processa batch completato
                completed_count = 0
                for curr_i, curr_idx, fut in futures:
                    try:
                        local_objects, local_errors = fut.result(timeout=30)
                        sql_objects.extend(local_objects)
                        error_log.extend(local_errors)
                        completed_count += 1
                    except Exception as e:
                        print(f"ERRORE THREAD per riga {curr_idx}: {e}")
                        error_log.append({
                            "Riga": curr_idx,
                            "Errore": f"Thread timeout/error: {e}"
                        })
                
                print(f"Stato: {i}/{total_rows} | Oggetti: {len(sql_objects)} | Completati: {completed_count}/{len(futures)}")
                
                # Export checkpoint
                if sql_objects:
                    export_large_dataframe(pd.DataFrame(sql_objects), output_path, 'Oggetti T-Sql', 'oggetti', i // batch_size)
                    sql_objects.clear()
                
                futures.clear()
        
        # Processa ultimi future rimasti
        if futures:
            print(f"\nProcessing ultimi {len(futures)} future...")
            for curr_i, curr_idx, fut in futures:
                try:
                    local_objects, local_errors = fut.result(timeout=30)
                    sql_objects.extend(local_objects)
                    error_log.extend(local_errors)
                except Exception as e:
                    print(f"ERRORE THREAD finale per riga {curr_idx}: {e}")
    
    # Export finale
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
    
    elapsed_time = time.time() - start_time
    print(f"\n⏱️  Tempo totale: {elapsed_time/60:.1f} minuti ({elapsed_time:.0f} secondi)")
    print(f"📊 Velocità media: {(total_rows - START_ROW + 1) / elapsed_time:.2f} tabelle/sec")

if __name__ == "__main__":
    main()
       
