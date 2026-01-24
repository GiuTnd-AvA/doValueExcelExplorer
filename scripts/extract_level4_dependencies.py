# =========================
# IMPORT
# =========================
import pandas as pd
import pyodbc
from pathlib import Path
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# =========================
# CONFIG
# =========================
INPUT_FILE_L3 = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\DIPENDENZE_LIVELLO_3.xlsx'
INPUT_FILE_L2 = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\DIPENDENZE_LIVELLO_2.xlsx'
INPUT_FILE_L1 = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\analisi_oggetti_critici.xlsx'
OUTPUT_FILE = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\DIPENDENZE_LIVELLO_4.xlsx'

SQL_SERVER = 'EPCP3'
# Lista completa dei 9 database sul server EPCP3
AVAILABLE_DATABASES = ['ams', 'CORESQL7', 'ANALISI', 's1057', 'BASEDATI_BI', 'EPC_BI', 'S1259', 'gestito', 'S1057B']
MAX_WORKERS = 4  # Parallelize processing
BATCH_SIZE = 100  # Process objects in batches

# Thread-safe print lock
print_lock = threading.Lock()

# =========================
# FUNZIONI SQL
# =========================

def get_sql_connection(database):
    """Connessione SQL Server."""
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
    )
    return pyodbc.connect(connection_string)

def extract_objects_for_table(database, table_name):
    """Trova TUTTI gli oggetti SQL che operano su una tabella (TRIGGER + SP/Functions con DML)."""
    conn = None
    objects_found = []
    
    try:
        conn = get_sql_connection(database)
        cursor = conn.cursor()
        
        # Query 1: Trigger sulla tabella
        trigger_query = """
        SELECT 
            t.name AS ObjectName,
            'SQL_TRIGGER' AS ObjectType,
            SCHEMA_NAME(tab.schema_id) AS SchemaName,
            tab.name AS ParentTable
        FROM sys.triggers t
        INNER JOIN sys.tables tab ON t.parent_id = tab.object_id
        WHERE LOWER(tab.name) = LOWER(?)
        """
        cursor.execute(trigger_query, table_name)
        trigger_rows = cursor.fetchall()
        
        for row in trigger_rows:
            objects_found.append({
                'name': row.ObjectName.lower(),
                'type': 'Trigger',
                'parent_table': row.ParentTable,
                'schema': row.SchemaName,
                'reason': f'Trigger su tabella {table_name}'
            })
        
        # Query 2: SP/Functions che referenziano la tabella con DML
        dml_query = """
        SELECT 
            o.name AS ObjectName,
            o.type_desc AS ObjectType,
            SCHEMA_NAME(o.schema_id) AS SchemaName,
            m.definition AS SQLDefinition
        FROM sys.sql_modules m
        INNER JOIN sys.objects o ON m.object_id = o.object_id
        WHERE o.type IN ('P', 'FN', 'IF', 'TF')
        AND (
            LOWER(m.definition) LIKE '%insert%into%' + LOWER(?) + '%'
            OR LOWER(m.definition) LIKE '%update%' + LOWER(?) + '%'
            OR LOWER(m.definition) LIKE '%delete%from%' + LOWER(?) + '%'
            OR LOWER(m.definition) LIKE '%merge%into%' + LOWER(?) + '%'
        )
        """
        cursor.execute(dml_query, table_name, table_name, table_name, table_name)
        dml_rows = cursor.fetchall()
        
        for row in dml_rows:
            obj_type = 'SP' if 'PROCEDURE' in row.ObjectType else 'Function'
            objects_found.append({
                'name': row.ObjectName.lower(),
                'type': obj_type,
                'parent_table': table_name,
                'schema': row.SchemaName,
                'reason': f'Modifica tabella {table_name} (DML)'
            })
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        if conn:
            try:
                conn.close()
            except:
                pass
    
    return objects_found

def extract_sql_definition(database, object_name):
    """Estrae SQLDefinition di un oggetto specifico (include TRIGGER)."""
    conn = None
    try:
        conn = get_sql_connection(database)
        
        # Prepara varianti del nome
        name_variants = []
        
        if '.' in object_name:
            parts = object_name.split('.')
            schema = parts[0]
            obj_name = parts[1] if len(parts) > 1 else parts[0]
            name_variants = [(schema, obj_name), (None, obj_name)]
        else:
            name_variants = [('dbo', object_name), (None, object_name)]
        
        cursor = conn.cursor()
        
        # sys.sql_modules include SP, Functions, Views, TRIGGERS
        for schema, obj_name in name_variants:
            if schema:
                query = """
                SELECT 
                    o.name AS ObjectName,
                    o.type_desc AS ObjectType,
                    m.definition AS SQLDefinition,
                    SCHEMA_NAME(o.schema_id) AS SchemaName
                FROM sys.sql_modules m
                INNER JOIN sys.objects o ON m.object_id = o.object_id
                WHERE LOWER(SCHEMA_NAME(o.schema_id)) = LOWER(?)
                AND LOWER(o.name) = LOWER(?)
                """
                cursor.execute(query, (schema, obj_name))
            else:
                query = """
                SELECT 
                    o.name AS ObjectName,
                    o.type_desc AS ObjectType,
                    m.definition AS SQLDefinition,
                    SCHEMA_NAME(o.schema_id) AS SchemaName
                FROM sys.sql_modules m
                INNER JOIN sys.objects o ON m.object_id = o.object_id
                WHERE LOWER(o.name) = LOWER(?)
                """
                cursor.execute(query, obj_name)
            
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            
            if rows:
                cursor.close()
                conn.close()
                return dict(zip(columns, rows[0]))
        
        cursor.close()
        conn.close()
        return None
        
    except Exception as e:
        if conn:
            try:
                conn.close()
            except:
                pass
        return None


def extract_sql_definitions_batch(database, object_names):
    """
    BATCH OPTIMIZED: Estrae SQLDefinition per lista di oggetti in una query sola.
    10-50x più veloce del loop sequenziale.
    """
    if not object_names:
        return {}
    
    conn = None
    results = {}
    
    try:
        conn = get_sql_connection(database)
        cursor = conn.cursor()
        
        # Prepara liste per schema.object e object-only
        schema_objects = []
        plain_objects = []
        
        for obj_name in object_names:
            if '.' in obj_name:
                parts = obj_name.split('.')
                schema = parts[0]
                obj = parts[1] if len(parts) > 1 else parts[0]
                schema_objects.append((schema.lower(), obj.lower(), obj_name))
            else:
                plain_objects.append((obj_name.lower(), obj_name))
        
        # Query 1: Oggetti con schema specificato
        if schema_objects:
            # Costruisci condizioni OR per ogni coppia schema.object
            conditions = []
            params = []
            for schema, obj, _ in schema_objects:
                conditions.append("(LOWER(SCHEMA_NAME(o.schema_id)) = ? AND LOWER(o.name) = ?)")
                params.extend([schema, obj])
            
            where_clause = " OR ".join(conditions)
            
            query = f"""
            SELECT 
                o.name AS ObjectName,
                o.type_desc AS ObjectType,
                m.definition AS SQLDefinition,
                SCHEMA_NAME(o.schema_id) AS SchemaName
            FROM sys.sql_modules m
            INNER JOIN sys.objects o ON m.object_id = o.object_id
            WHERE {where_clause}
            """
            cursor.execute(query, params)
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            
            for row in rows:
                result = dict(zip(columns, row))
                # Match con nome originale
                for _, _, orig_name in schema_objects:
                    if orig_name.lower() == f"{result['SchemaName']}.{result['ObjectName']}".lower():
                        results[orig_name] = result
                        break
        
        # Query 2: Oggetti senza schema (prova dbo e qualsiasi schema)
        if plain_objects:
            placeholders = ','.join(['?'] * len(plain_objects))
            params = [obj for obj, _ in plain_objects]
            
            query = f"""
            SELECT 
                o.name AS ObjectName,
                o.type_desc AS ObjectType,
                m.definition AS SQLDefinition,
                SCHEMA_NAME(o.schema_id) AS SchemaName
            FROM sys.sql_modules m
            INNER JOIN sys.objects o ON m.object_id = o.object_id
            WHERE LOWER(o.name) IN ({placeholders})
            """
            cursor.execute(query, params)
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            
            for row in rows:
                result = dict(zip(columns, row))
                # Match con nome originale (preferisci dbo, altrimenti first match)
                for _, orig_name in plain_objects:
                    if orig_name not in results and result['ObjectName'].lower() == orig_name.lower():
                        results[orig_name] = result
        
        cursor.close()
        conn.close()
        return results
        
    except Exception as e:
        with print_lock:
            print(f"⚠️ Batch query error DB {database}: {e}")
        if conn:
            try:
                conn.close()
            except:
                pass
        return results


def process_object_batch(batch_objects, databases_list, already_extracted):
    """
    Processa batch di oggetti L4 in parallelo con query batch ottimizzate.
    """
    results = []
    
    # Organizza oggetti per database di origine
    db_objects = {}
    
    for obj_info in batch_objects:
        db_found = obj_info.get('Database', None)
        
        # Se DB non disponibile da callers, prova tutti i DB
        if not db_found or pd.isna(db_found):
            db_found = 'Unknown'
        
        if db_found not in db_objects:
            db_objects[db_found] = []
        db_objects[db_found].append(obj_info)
    
    # Processa ogni database con batch query
    for db_name, objs in db_objects.items():
        if db_name == 'Unknown':
            # Fallback: prova tutti i database
            object_names = [obj['OggettoDipendente'] for obj in objs]
            found = False
            
            for try_db in databases_list:
                defs = extract_sql_definitions_batch(try_db, object_names)
                if defs:
                    found = True
                    for obj_info in objs:
                        obj_name = obj_info['OggettoDipendente']
                        if obj_name in defs:
                            def_info = defs[obj_name]
                            results.append({
                                'ObjectName': obj_name,
                                'ObjectType': def_info['ObjectType'],
                                'SQLDefinition': def_info['SQLDefinition'],
                                'Database': try_db,
                                'SchemaName': def_info['SchemaName'],
                                'Chiamante_L3': obj_info['Chiamanti'],
                                'Chiamante_L3_Database': obj_info['Chiamanti_Database'],
                                'DipendenzaOriginale': obj_info.get('DipendenzaOriginale', '')
                            })
                    break
            
            if not found:
                with print_lock:
                    print(f"⚠️ Oggetti non trovati in nessun DB: {[obj['OggettoDipendente'] for obj in objs]}")
        else:
            # Database noto: batch query diretta
            object_names = [obj['OggettoDipendente'] for obj in objs]
            defs = extract_sql_definitions_batch(db_name, object_names)
            
            for obj_info in objs:
                obj_name = obj_info['OggettoDipendente']
                if obj_name in defs:
                    def_info = defs[obj_name]
                    results.append({
                        'ObjectName': obj_name,
                        'ObjectType': def_info['ObjectType'],
                        'SQLDefinition': def_info['SQLDefinition'],
                        'Database': db_name,
                        'SchemaName': def_info['SchemaName'],
                        'Chiamante_L3': obj_info['Chiamanti'],
                        'Chiamante_L3_Database': obj_info['Chiamanti_Database'],
                        'DipendenzaOriginale': obj_info.get('DipendenzaOriginale', '')
                    })
                else:
                    with print_lock:
                        print(f"⚠️ Oggetto non trovato: {obj_name} in DB {db_name}")
    
    return results


def process_table_batch(table_batch_info, new_deps, already_extracted):
    """
    Processa batch di tabelle in parallelo.
    Returns lista oggetti trovati per le tabelle.
    """
    results = []
    
    for table_info in table_batch_info:
        table_name = table_info['table_name']
        databases_list = table_info['databases'].split('; ')
        critical_callers = table_info['critical_callers'].split('; ')
        
        for db in databases_list:
            objects_for_table = extract_objects_for_table(db, table_name)
            
            for obj in objects_for_table:
                # Evita duplicati con new_deps E oggetti già estratti
                obj_name_lower = obj['name'].lower()
                if not any(d['name'].lower() == obj_name_lower for d in new_deps) \
                   and obj_name_lower not in already_extracted:
                    results.append({
                        'name': obj['name'],
                        'object_type': obj['type'],
                        'total_callers': 0,
                        'critical_callers': len(critical_callers),
                        'caller_types': 'Tabella',
                        'critical_caller_types': 'Tabella',
                        'called_by_critical': obj['reason'],
                        'critical_caller_names': '; '.join(critical_callers[:5]),
                        'callers_list': [{'database': db, 'object_name': c, 'is_critical': 'SÌ'} for c in critical_callers],
                        'source': 'table_investigation'
                    })
    
    return results

# =========================
# FUNZIONI ANALISI
# =========================

def classify_dependency_type(dep_name):
    """Classifica tipo dipendenza."""
    dep_lower = dep_name.lower()
    
    if 'trigger' in dep_lower or 'tr_' in dep_lower or '_tr_' in dep_lower:
        return 'Trigger'
    
    if any(p in dep_lower for p in ['sp_', 'usp_', 'asp_', 'proc_', '_sp_']):
        return 'SP'
    
    if any(p in dep_lower for p in ['fn_', 'udf_', 'f_', '_fn_', '_udf_', 'tf_', 'if_', 'tvf_']):
        return 'Function'
    
    return 'Tabella'

def extract_dependencies_from_sql(sql_definition):
    """Estrae dipendenze da SQLDefinition separate per tipo."""
    if not sql_definition or not isinstance(sql_definition, str):
        return {'tables': [], 'objects': []}
    
    tables = set()
    objects = set()
    
    # Tabelle: FROM/JOIN
    table_pattern = r'(?:FROM|JOIN)\s+(?:\[?[\w]+\]?\.)?\[?([\w]+)\]?'
    for match in re.finditer(table_pattern, sql_definition, re.IGNORECASE):
        table_name = match.group(1).lower()
        if table_name not in ['select', 'deleted', 'inserted', 'dual']:
            if not table_name.startswith('#'):
                tables.add(table_name)
    
    # SP: EXEC/EXECUTE
    sp_pattern = r'(?:EXEC(?:UTE)?)\s+(?:\[?[\w]+\]?\.)?\[?([\w]+)\]?'
    for match in re.finditer(sp_pattern, sql_definition, re.IGNORECASE):
        sp_name = match.group(1).lower()
        objects.add(sp_name)
    
    # Functions
    fn_pattern = r'(?:\[?[\w]+\]?\.)?\[?(fn_[\w]+|udf_[\w]+|tf_[\w]+)\]?\s*\('
    for match in re.finditer(fn_pattern, sql_definition, re.IGNORECASE):
        fn_name = match.group(1).lower()
        objects.add(fn_name)
    
    return {'tables': list(tables), 'objects': list(objects)}

def extract_dependencies_with_context(df, dep_col='Dipendenze_Oggetti_L4'):
    """Estrae dipendenze oggetti L4 con contesto chiamante (da L3)."""
    dependency_map = {}
    
    for idx, row in df.iterrows():
        object_name = row.get('ObjectName', 'Unknown')
        object_type = row.get('ObjectType', 'Unknown')
        database = row.get('Database', '')
        catena_l1 = row.get('Catena_Origine_L1', '')
        dependencies_value = row.get(dep_col)
        
        if pd.isna(dependencies_value) or not isinstance(dependencies_value, str):
            continue
        
        if dependencies_value.lower() in ['nessuna', '']:
            continue
        
        deps = dependencies_value.split(';')
        for dep in deps:
            dep_clean = dep.strip().lower()
            if dep_clean and dep_clean != 'nessuna':
                if dep_clean not in dependency_map:
                    dependency_map[dep_clean] = []
                dependency_map[dep_clean].append({
                    'object_name': object_name,
                    'object_type': object_type,
                    'database': database,
                    'chain_l1': catena_l1
                })
    
    return dependency_map

def extract_tables_with_context(df, table_col='Dipendenze_Tabelle_L4'):
    """Estrae tabelle referenziate L4 con contesto chiamante."""
    table_map = {}
    
    for idx, row in df.iterrows():
        object_name = row.get('ObjectName', 'Unknown')
        object_type = row.get('ObjectType', 'Unknown')
        database = row.get('Database', '')
        catena_l1 = row.get('Catena_Origine_L1', '')
        tables_value = row.get(table_col)
        
        if pd.isna(tables_value) or not isinstance(tables_value, str):
            continue
        
        if tables_value.lower() in ['nessuna', '']:
            continue
        
        tables = tables_value.split(';')
        for table in tables:
            table_clean = table.strip().lower()
            if table_clean and table_clean != 'nessuna':
                if table_clean not in table_map:
                    table_map[table_clean] = []
                table_map[table_clean].append({
                    'object_name': object_name,
                    'object_type': object_type,
                    'database': database,
                    'chain_l1': catena_l1
                })
    
    return table_map

def find_new_dependencies_l4(already_extracted, dependency_map):
    """Trova nuove dipendenze L4 non già estratte in L1+L2+L3."""
    new_objects = []
    
    for dep_name, callers in dependency_map.items():
        if dep_name in already_extracted:
            continue
        
        obj_type = classify_dependency_type(dep_name)
        
        # Solo SP/Functions/Triggers
        if obj_type == 'Tabella':
            continue
        
        caller_names = [c['object_name'] for c in callers]
        all_chains_l1 = set()
        for c in callers:
            chain = c.get('chain_l1', '')
            if chain and chain != 'Non tracciato':
                all_chains_l1.update([x.strip() for x in chain.split(';')])
        
        new_objects.append({
            'name': dep_name,
            'object_type': obj_type,
            'total_callers': len(callers),
            'called_by': '; '.join([c['object_name'] for c in callers[:5]]),
            'caller_names': '; '.join(caller_names),
            'chain_l1': '; '.join(sorted(all_chains_l1)) if all_chains_l1 else 'Non tracciato',
            'callers_list': callers
        })
    
    return new_objects

# =========================
# MAIN
# =========================

def main():
    print("="*70)
    print("ESTRAZIONE DIPENDENZE LIVELLO 4 (L1→L2→L3→L4)")
    print("="*70)
    
    # 1. Carica oggetti già estratti (L1, L2, L3)
    print("\n1. Caricamento oggetti già estratti...")
    
    try:
        df_l1 = pd.read_excel(INPUT_FILE_L1)
        oggetti_l1 = set(df_l1['ObjectName'].str.lower().str.strip())
        print(f"   Oggetti L1: {len(oggetti_l1)}")
    except Exception as e:
        print(f"   ERRORE L1: {e}")
        return
    
    try:
        df_l2 = pd.read_excel(INPUT_FILE_L2, sheet_name='Oggetti Livello 2')
        oggetti_l2 = set(df_l2['ObjectName'].str.lower().str.strip())
        print(f"   Oggetti L2: {len(oggetti_l2)}")
    except Exception as e:
        print(f"   ERRORE L2: {e}")
        return
    
    try:
        df_l3 = pd.read_excel(INPUT_FILE_L3, sheet_name='Oggetti Livello 3')
        oggetti_l3 = set(df_l3['ObjectName'].str.lower().str.strip())
        print(f"   Oggetti L3: {len(oggetti_l3)}")
    except Exception as e:
        print(f"   ERRORE L3: {e}")
        return
    
    already_extracted = oggetti_l1 | oggetti_l2 | oggetti_l3
    print(f"   Totale già estratti (L1+L2+L3): {len(already_extracted)}")
    
    # 2. GAP ANALYSIS: trova nuove dipendenze OGGETTI SQL L4
    print("\n2. Gap Analysis - Identificazione nuove dipendenze Oggetti SQL L4...")
    dependency_map = extract_dependencies_with_context(df_l3)
    print(f"   Dipendenze oggetti L4 totali trovate: {len(dependency_map)}")
    
    new_deps_l4 = find_new_dependencies_l4(already_extracted, dependency_map)
    print(f"   Nuovi Oggetti SQL L4 da estrarre: {len(new_deps_l4)}")
    
    # 3. Traccia solo TABELLE referenziate da L3 (per report, non investigare oggetti)
    print("\n3. Tracciamento tabelle referenziate da L3...")
    table_map = extract_tables_with_context(df_l3)
    print(f"   Tabelle totali referenziate: {len(table_map)}")
    
    critical_tables = []
    
    for table_name, callers in table_map.items():
        all_chains_l1 = set()
        for c in callers:
            chain = c.get('chain_l1', '')
            if chain and chain != 'Non tracciato':
                all_chains_l1.update([x.strip() for x in chain.split(';')])
        
        # Usa i database dei chiamanti, filtrando valori vuoti
        databases_list = list(set([c['database'] for c in callers if c.get('database')]))
        if not databases_list:
            continue  # Skip se non ci sono database validi
        
        critical_tables.append({
            'table_name': table_name,
            'callers_count': len(callers),
            'callers_l3': '; '.join([c['object_name'] for c in callers[:10]]),
            'databases': '; '.join(databases_list),
            'critical_callers': '; '.join([c['object_name'] for c in callers[:10]])
        })
    
    print(f"   Tabelle referenziate da L3: {len(critical_tables)}")
    
    # Usa solo oggetti da gap analysis (no table investigation)
    new_deps_l4_total = new_deps_l4
    print(f"   Totale oggetti L4 da estrarre: {len(new_deps_l4_total)}")
    
    # 4. Estrai SQLDefinition oggetti livello 4 CON PARALLEL BATCH PROCESSING
    print("\n4. Estrazione SQLDefinition oggetti livello 4...\n")
    
    if not new_deps_l4_total:
        print("\n⚠ Nessuna nuova dipendenza L4 trovata - catena completa!")
        oggetti_l4 = []
    else:
        print(f"   Oggetti unici da estrarre: {len(new_deps_l4_total)}")
        print(f"   Usando {MAX_WORKERS} workers paralleli con batch size {BATCH_SIZE}\n")
        
        # Prepara dati per batch processing
        databases_l3 = list(df_l3['Database'].dropna().unique())
        
        # Prepara oggetti con database di origine
        objects_to_extract = []
        for obj_info in new_deps_l4_total:
            object_name = obj_info['name']
            clean_name = object_name.replace('[', '').replace(']', '').strip()
            
            # Trova database da chiamanti
            database_found = None
            caller_dbs = []
            for caller_info in obj_info['callers_list']:
                db_candidate = caller_info.get('database', '')
                if db_candidate:
                    caller_dbs.append(db_candidate)
            
            database_found = caller_dbs[0] if caller_dbs else None
            
            objects_to_extract.append({
                'OggettoDipendente': clean_name,
                'Database': database_found,
                'Chiamanti': obj_info['caller_names'],
                'Chiamanti_Database': '; '.join(caller_dbs) if caller_dbs else '',
                'DipendenzaOriginale': obj_info.get('called_by', '')
            })
        
        # Split in batches
        batches = []
        for i in range(0, len(objects_to_extract), BATCH_SIZE):
            batches.append(objects_to_extract[i:i + BATCH_SIZE])
        
        print(f"   Processing {len(batches)} batches...")
        
        # Process batches in parallel
        import time
        start_time = time.time()
        all_results = []
        processed = 0
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_object_batch, batch, databases_l3, already_extracted): i
                for i, batch in enumerate(batches)
            }
            
            for future in as_completed(futures):
                batch_idx = futures[future]
                try:
                    batch_results = future.result()
                    all_results.extend(batch_results)
                    processed += len(batches[batch_idx])
                    
                    elapsed = time.time() - start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    
                    with print_lock:
                        print(f"   Batch {batch_idx + 1}/{len(batches)} completato | "
                              f"Processati: {processed}/{len(objects_to_extract)} | "
                              f"Velocità: {rate:.1f} oggetti/sec")
                except Exception as e:
                    with print_lock:
                        print(f"   ⚠️ Errore batch {batch_idx}: {e}")
        
        elapsed = time.time() - start_time
        print(f"\n   ✓ Estrazione completata in {elapsed:.1f}s")
        print(f"   Oggetti trovati: {len(all_results)}/{len(objects_to_extract)}")
        
        # Converti risultati in formato finale
        oggetti_l4 = []
        for result in all_results:
            sql_def = result['SQLDefinition']
            
            # Estrai dipendenze livello 5
            deps_l5 = extract_dependencies_from_sql(sql_def)
            tables_l5 = deps_l5['tables']
            objects_l5 = deps_l5['objects']
            
            tables_l5_str = '; '.join(tables_l5) if tables_l5 else 'Nessuna'
            objects_l5_str = '; '.join(objects_l5) if objects_l5 else 'Nessuna'
            
            oggetti_l4.append({
                'Livello': 4,
                'Server': SQL_SERVER,
                'Database': result['Database'],
                'ObjectName': result['ObjectName'],
                'ObjectType': result['ObjectType'],
                'SchemaName': result['SchemaName'],
                'Oggetti_Chiamanti_L3': result['Chiamante_L3'],
                'Catena_Origine_L1': 'L1→L2→L3→L4',  # Simplified chain
                'N_Chiamanti_L3': result['Chiamante_L3'].count(';') + 1 if result['Chiamante_L3'] else 0,
                'Dipendenze_Tabelle_L5': tables_l5_str,
                'N_Tabelle_L5': len(tables_l5),
                'Dipendenze_Oggetti_L5': objects_l5_str,
                'N_Oggetti_L5': len(objects_l5),
                'SQLDefinition': sql_def
            })
    
    # 5. Export multi-sheet
    print(f"\n5. Export: {OUTPUT_FILE}")
    
    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        df_l3.to_excel(writer, sheet_name='Oggetti Livello 3', index=False)
        
        if oggetti_l4:
            pd.DataFrame(oggetti_l4).to_excel(writer, sheet_name='Oggetti Livello 4', index=False)
        
        if critical_tables:
            pd.DataFrame(critical_tables).to_excel(writer, sheet_name='Tabelle Referenziate L3', index=False)
        
        stats = [
            {'Metrica': 'LIVELLO 4 - SUMMARY', 'Valore': ''},
            {'Metrica': 'Oggetti Già Estratti (L1+L2+L3)', 'Valore': len(already_extracted)},
            {'Metrica': 'Nuove Dipendenze L4', 'Valore': len(new_deps_l4)},
            {'Metrica': 'Oggetti da Tabelle L3', 'Valore': len(table_objects_found)},
            {'Metrica': 'Totale Oggetti L4', 'Valore': len(new_deps_l4_total)},
            {'Metrica': 'Oggetti Estratti', 'Valore': len(oggetti_l4)},
            {'Metrica': 'Oggetti Non Trovati', 'Valore': len(new_deps_l4_total) - len(oggetti_l4) if new_deps_l4_total else 0},
            {'Metrica': '', 'Valore': ''},
            {'Metrica': 'COPERTURA TOTALE', 'Valore': ''},
            {'Metrica': 'Oggetti Totali (L1+L2+L3+L4)', 'Valore': len(already_extracted) + len(oggetti_l4)},
        ]
        pd.DataFrame(stats).to_excel(writer, sheet_name='Statistiche', index=False)
    
    print("\n" + "="*70)
    print("✅ COMPLETATO!")
    print("="*70)
    print(f"Oggetti L4 estratti: {len(oggetti_l4)}")
    print(f"Tabelle referenziate L3: {len(critical_tables)}")

if __name__ == "__main__":
    main()
