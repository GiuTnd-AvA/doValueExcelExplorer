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
ANALYZED_FILE_L1 = rf'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\analisi_oggetti_critici.xlsx'
OUTPUT_FILE = rf'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\DIPENDENZE_LIVELLO_2.xlsx'

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
        # Cerca in sys.sql_modules per INSERT/UPDATE/DELETE sulla tabella
        dml_query = """
        SELECT 
            o.name AS ObjectName,
            o.type_desc AS ObjectType,
            SCHEMA_NAME(o.schema_id) AS SchemaName,
            m.definition AS SQLDefinition
        FROM sys.sql_modules m
        INNER JOIN sys.objects o ON m.object_id = o.object_id
        WHERE o.type IN ('P', 'FN', 'IF', 'TF')  -- SP, Functions
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
        # Skip silently if database is not accessible (e.g., permission denied)
        error_msg = str(e).lower()
        if 'login failed' in error_msg or 'cannot open database' in error_msg:
            # Permission denied - skip without flooding output
            pass
        else:
            with print_lock:
                print(f"⚠️ Errore estrazione oggetti per {table_name} in {database}: {e}")
        if conn:
            try:
                conn.close()
            except:
                pass
    
    return objects_found


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
                # Evita duplicati con new_deps E oggetti già in L1
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
        
        # Prova tutte le varianti - sys.sql_modules include SP, Functions, Views, TRIGGERS
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


def process_object_batch(batch_objects, databases_list, already_extracted_l1):
    """
    Processa batch di oggetti L2 in parallelo con query batch ottimizzate.
    """
    results = []
    
    # Organizza oggetti per database di origine
    db_objects = {}
    
    for obj_info in batch_objects:
        db_found = obj_info.get('Database', None)
        
        # Se DB non disponibile da callers, prova tutti i DB L1
        if not db_found or pd.isna(db_found):
            db_found = 'Unknown'
        
        if db_found not in db_objects:
            db_objects[db_found] = []
        db_objects[db_found].append(obj_info)
    
    # Processa ogni database con batch query
    for db_name, objs in db_objects.items():
        if db_name == 'Unknown':
            # Fallback: cerca in tutti i database disponibili
            object_names = [obj['OggettoDipendente'] for obj in objs]
            objects_found_count = 0
            
            with print_lock:
                print(f"   Cercando {len(object_names)} oggetti in {len(databases_list)} database...")
            
            for try_db in databases_list:
                try:
                    defs = extract_sql_definitions_batch(try_db, object_names)
                    if defs:
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
                                    'Chiamante_L1': obj_info['Chiamanti'],
                                    'Chiamante_L1_Database': obj_info['Chiamanti_Database'],
                                    'DipendenzaOriginale': obj_info.get('DipendenzaOriginale', '')
                                })
                                objects_found_count += 1
                                # Rimuovi oggetto dalla lista per non cercarlo negli altri DB
                                object_names = [o for o in object_names if o != obj_name]
                except Exception as e:
                    with print_lock:
                        print(f"   ⚠️ Errore ricerca in DB {try_db}: {e}")
            
            with print_lock:
                print(f"   Trovati {objects_found_count}/{len(objs)} oggetti")
                if objects_found_count < len(objs):
                    not_found = [obj['OggettoDipendente'] for obj in objs if obj['OggettoDipendente'] in object_names]
                    print(f"   Non trovati: {not_found[:10]}")
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
                        'Chiamante_L1': obj_info['Chiamanti'],
                        'Chiamante_L1_Database': obj_info['Chiamanti_Database'],
                        'DipendenzaOriginale': obj_info.get('DipendenzaOriginale', '')
                    })
                else:
                    with print_lock:
                        print(f"⚠️ Oggetto non trovato: {obj_name} in DB {db_name}")
    
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
    """Estrae dipendenze da SQLDefinition."""
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

def extract_dependencies_with_context(df, dep_col='Dipendenze_Oggetti'):
    """Estrae dipendenze oggetti SQL con contesto chiamante."""
    dependency_map = {}
    
    for idx, row in df.iterrows():
        object_name = row.get('ObjectName', 'Unknown')
        object_type = row.get('ObjectType', 'Unknown')
        is_critical = row.get('Critico_Migrazione', 'NO')
        database = row.get('Database', '')
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
                    'is_critical': is_critical
                })
    
    return dependency_map

def extract_tables_with_context(df, table_col='Dipendenze_Tabelle'):
    """Estrae tabelle referenziate con contesto chiamante."""
    table_map = {}
    
    for idx, row in df.iterrows():
        object_name = row.get('ObjectName', 'Unknown')
        object_type = row.get('ObjectType', 'Unknown')
        is_critical = row.get('Critico_Migrazione', 'NO')
        database = row.get('Database', '')
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
                    'is_critical': is_critical
                })
    
    return table_map

def find_new_dependencies(already_extracted, dependency_map):
    """Trova nuove dipendenze SQL non già estratte in L1."""
    new_objects = []
    
    for dep_name, callers in dependency_map.items():
        # Escludi oggetti già estratti in L1
        if dep_name in already_extracted:
            continue
        
        obj_type = classify_dependency_type(dep_name)
        
        # Solo SP/Functions/Triggers (tabelle non hanno SQLDefinition)
        if obj_type == 'Tabella':
            continue
        
        critical_callers = [c for c in callers if c['is_critical'] == 'SÌ']
        
        if not critical_callers:
            continue
        
        caller_types = set([c['object_type'] for c in callers])
        critical_caller_types = set([c['object_type'] for c in critical_callers])
        critical_caller_names = [c['object_name'] for c in critical_callers]
        
        new_objects.append({
            'name': dep_name,
            'object_type': obj_type,
            'total_callers': len(callers),
            'critical_callers': len(critical_callers),
            'caller_types': '; '.join(sorted(caller_types)),
            'critical_caller_types': '; '.join(sorted(critical_caller_types)),
            'called_by_critical': '; '.join([c['object_name'] for c in critical_callers[:5]]),
            'critical_caller_names': '; '.join(critical_caller_names),
            'callers_list': critical_callers
        })
    
    return new_objects

# =========================
# MAIN
# =========================

def main():
    print("="*70)
    print("ESTRAZIONE DIPENDENZE LIVELLO 2 (solo GAP ANALYSIS)")
    print("="*70)
    
    # 1. Carica oggetti livello 1 (analizzati)
    print("\n1. Caricamento oggetti livello 1 analizzati...")
    try:
        df_l1 = pd.read_excel(ANALYZED_FILE_L1)
        # Crea set di oggetti già estratti (lowercase per confronto)
        already_extracted_l1 = set(df_l1['ObjectName'].str.lower().str.strip())
        print(f"   Oggetti livello 1 totali: {len(df_l1)}")
        print(f"   Set oggetti L1 per gap analysis: {len(already_extracted_l1)}")
        
        # Filtra solo oggetti critici per elaborazione
        df_l1_critical = df_l1[df_l1['Critico_Migrazione'] == 'SÌ'].copy()
        print(f"   Oggetti livello 1 critici: {len(df_l1_critical)}")
    except Exception as e:
        print(f"ERRORE: {e}")
        return
    
    # 2. GAP ANALYSIS: trova nuove dipendenze OGGETTI SQL (solo sys.sql_expression_dependencies)
    print("\n2. Gap Analysis - Identificazione nuove dipendenze Oggetti SQL...")
    dependency_map = extract_dependencies_with_context(df_l1_critical)
    print(f"   Dipendenze oggetti SQL totali: {len(dependency_map)}")
    
    new_deps = find_new_dependencies(already_extracted_l1, dependency_map)
    print(f"   Nuovi Oggetti SQL critici da estrarre: {len(new_deps)}")
    
    # 3. Traccia solo TABELLE referenziate (per report, non investigare oggetti)
    print("\n3. Tracciamento tabelle referenziate...")
    table_map = extract_tables_with_context(df_l1_critical)
    print(f"   Tabelle totali referenziate: {len(table_map)}")
    
    # Filtra tabelle critiche (chiamate da oggetti critici)
    critical_tables = []
    
    for table_name, callers in table_map.items():
        critical_callers = [c for c in callers if c['is_critical'] == 'SÌ']
        if critical_callers:
            # Usa i database dei chiamanti, filtrando valori vuoti
            databases_list = list(set([c['database'] for c in critical_callers if c.get('database')]))
            if not databases_list:
                continue  # Skip se non ci sono database validi
            
            critical_tables.append({
                'table_name': table_name,
                'critical_callers_count': len(critical_callers),
                'critical_callers': '; '.join([c['object_name'] for c in critical_callers[:10]]),
                'databases': '; '.join(databases_list)
            })
    
    print(f"   Tabelle critiche referenziate: {len(critical_tables)}")
    
    # Usa solo oggetti da gap analysis (no table investigation)
    new_deps_total = new_deps
    print(f"   Totale oggetti L2 da estrarre: {len(new_deps_total)}")
    
    if not new_deps_total:
        print("\n   Nessuna nuova dipendenza critica trovata!")
        # Esporta comunque le tabelle
    
    # 4. Estrai SQLDefinition oggetti livello 2 CON PARALLEL BATCH PROCESSING
    print("\n4. Estrazione SQLDefinition oggetti livello 2...\n")
    
    if not new_deps_total:
        print("\n   Nessuna nuova dipendenza critica trovata!")
        oggetti_l2 = []
    else:
        print(f"   Oggetti unici da estrarre: {len(new_deps_total)}")
        print(f"   Usando {MAX_WORKERS} workers paralleli con batch size {BATCH_SIZE}\n")
        
        # Prepara dati per batch processing
        # Usa la lista hardcoded dei 9 database disponibili
        databases_l1 = AVAILABLE_DATABASES
        print(f"   Database disponibili per ricerca: {len(databases_l1)} database")
        
        # Prepara oggetti - forza ricerca in tutti i database disponibili
        objects_to_extract = []
        for obj_info in new_deps_total:
            object_name = obj_info['name']
            clean_name = object_name.replace('[', '').replace(']', '').strip()
            
            # Non usare database specifico - forza ricerca in tutti i DB disponibili
            objects_to_extract.append({
                'OggettoDipendente': clean_name,
                'Database': None,  # Forza ricerca in tutti i database
                'Chiamanti': obj_info['critical_caller_names'],
                'Chiamanti_Database': '',
                'DipendenzaOriginale': obj_info.get('called_by_critical', '')
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
                executor.submit(process_object_batch, batch, databases_l1, already_extracted_l1): i
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
        oggetti_l2 = []
        for result in all_results:
            sql_def = result['SQLDefinition']
            
            # Estrai dipendenze (tabelle + oggetti) - non etichettare come L3
            deps = extract_dependencies_from_sql(sql_def)
            tables = deps['tables']
            objects = deps['objects']
            
            tables_str = '; '.join(tables) if tables else 'Nessuna'
            objects_str = '; '.join(objects) if objects else 'Nessuna'
            
            oggetti_l2.append({
                'Livello': 2,
                'Server': SQL_SERVER,
                'Database': result['Database'],
                'ObjectName': result['ObjectName'],
                'ObjectType': result['ObjectType'],
                'SchemaName': result['SchemaName'],
                'Oggetti_Chiamanti_L1': result['Chiamante_L1'],
                'N_Chiamanti_Critici': result['Chiamante_L1'].count(';') + 1 if result['Chiamante_L1'] else 0,
                'Dipendenze_Tabelle': tables_str,
                'N_Tabelle': len(tables),
                'Dipendenze_Oggetti': objects_str,
                'N_Oggetti': len(objects),
                'SQLDefinition': sql_def
            })
    
    # 5. Crea sheet dipendenze dettagliate
    print("\n5. Creazione dipendenze dettagliate...")
    
    dipendenze_dettagliate = []
    
    # Dipendenze L1 → L2 (OGGETTI)
    for i, row_l1 in df_l1_critical.iterrows():
        deps_objects = row_l1.get('Dipendenze_Oggetti', '')
        if isinstance(deps_objects, str) and deps_objects != 'Nessuna':
            deps = [d.strip() for d in deps_objects.split(';') if d.strip()]
            for dep in deps:
                dep_type = classify_dependency_type(dep)
                dipendenze_dettagliate.append({
                    'Livello_Parent': 1,
                    'Server': row_l1.get('Server', SQL_SERVER),
                    'Database': row_l1.get('Database', ''),
                    'ObjectName': row_l1['ObjectName'],
                    'ObjectType_Parent': row_l1['ObjectType'],
                    'Dipendenza': dep,
                    'Tipo_Dipendenza': 'Oggetto SQL',
                    'ObjectType_Dipendenza': dep_type,
                    'Livello_Dipendenza': 2
                })
    
    # Dipendenze L2 → potenziali L3 (OGGETTI)
    for obj_l2 in oggetti_l2:
        objects_str = obj_l2['Dipendenze_Oggetti']
        if objects_str != 'Nessuna':
            deps = [d.strip() for d in objects_str.split(';') if d.strip()]
            for dep in deps:
                dep_type = classify_dependency_type(dep)
                dipendenze_dettagliate.append({
                    'Livello_Parent': 2,
                    'Server': obj_l2['Server'],
                    'Database': obj_l2['Database'],
                    'ObjectName': obj_l2['ObjectName'],
                    'ObjectType_Parent': obj_l2['ObjectType'],
                    'Dipendenza': dep,
                    'Tipo_Dipendenza': 'Oggetto SQL',
                    'ObjectType_Dipendenza': dep_type,
                    'Livello_Dipendenza': 'Potenziale L3'
                })
    
    df_deps_dettagliate = pd.DataFrame(dipendenze_dettagliate)
    
    # 5.1 CREA SHEET ESPLOSI - L1 Oggetti → Tabelle
    print("\n5.1 Creazione sheet esplosi L1...")
    l1_obj_table_rows = []
    for _, row_l1 in df_l1_critical.iterrows():
        obj_name = row_l1.get('ObjectName', '')
        obj_type = row_l1.get('ObjectType', '')
        obj_server = row_l1.get('Server', SQL_SERVER)
        obj_db = row_l1.get('Database', '')
        critico_migr = row_l1.get('Critico_Migrazione', 'NO')
        
        # Esplode tabelle
        tables_str = row_l1.get('Dipendenze_Tabelle', '')
        if isinstance(tables_str, str) and tables_str != 'Nessuna':
            tables = [t.strip() for t in tables_str.split(';') if t.strip()]
            for table in tables:
                l1_obj_table_rows.append({
                    'Livello': 1,
                    'ObjectName': obj_name,
                    'ObjectType': obj_type,
                    'Server': obj_server,
                    'Database': obj_db,
                    'Critico_Migrazione': critico_migr,
                    'Tabella_Dipendente': table,
                    'Tipo_Relazione': 'DIPENDE_DA_TABELLA'
                })
    
    df_l1_obj_tables = pd.DataFrame(l1_obj_table_rows)
    print(f"  ✓ L1 Oggetti→Tabelle: {len(df_l1_obj_tables)} relazioni")
    
    # 5.2 CREA SHEET ESPLOSI - L1 Oggetti → Oggetti SQL
    l1_obj_obj_rows = []
    for _, row_l1 in df_l1_critical.iterrows():
        obj_name = row_l1.get('ObjectName', '')
        obj_type = row_l1.get('ObjectType', '')
        obj_server = row_l1.get('Server', SQL_SERVER)
        obj_db = row_l1.get('Database', '')
        critico_migr = row_l1.get('Critico_Migrazione', 'NO')
        
        # Esplode oggetti
        objects_str = row_l1.get('Dipendenze_Oggetti', '')
        if isinstance(objects_str, str) and objects_str != 'Nessuna':
            objects = [o.strip() for o in objects_str.split(';') if o.strip()]
            for obj_dep in objects:
                l1_obj_obj_rows.append({
                    'Livello': 1,
                    'ObjectName': obj_name,
                    'ObjectType': obj_type,
                    'Server': obj_server,
                    'Database': obj_db,
                    'Critico_Migrazione': critico_migr,
                    'Oggetto_Dipendente': obj_dep,
                    'Tipo_Relazione': 'DIPENDE_DA_OGGETTO'
                })
    
    df_l1_obj_objects = pd.DataFrame(l1_obj_obj_rows)
    print(f"  ✓ L1 Oggetti→Oggetti: {len(df_l1_obj_objects)} relazioni")
    
    # 5.3 CREA SHEET ESPLOSI - L2 Oggetti → Tabelle
    l2_obj_table_rows = []
    if oggetti_l2:
        for obj_l2 in oggetti_l2:
            obj_name = obj_l2.get('ObjectName', '')
            obj_type = obj_l2.get('ObjectType', '')
            obj_server = obj_l2.get('Server', SQL_SERVER)
            obj_db = obj_l2.get('Database', '')
            chiamanti = obj_l2.get('Oggetti_Chiamanti_L1', '')
            
            # Esplode tabelle
            tables_str = obj_l2.get('Dipendenze_Tabelle', '')
            if isinstance(tables_str, str) and tables_str != 'Nessuna':
                tables = [t.strip() for t in tables_str.split(';') if t.strip()]
                for table in tables:
                    l2_obj_table_rows.append({
                        'Livello': 2,
                        'ObjectName': obj_name,
                        'ObjectType': obj_type,
                        'Server': obj_server,
                        'Database': obj_db,
                        'Chiamanti_L1': chiamanti,
                        'Tabella_Dipendente': table,
                        'Tipo_Relazione': 'DIPENDE_DA_TABELLA'
                    })
    
    df_l2_obj_tables = pd.DataFrame(l2_obj_table_rows)
    print(f"  ✓ L2 Oggetti→Tabelle: {len(df_l2_obj_tables)} relazioni")
    
    # 5.4 CREA SHEET ESPLOSI - L2 Oggetti → Oggetti SQL
    l2_obj_obj_rows = []
    if oggetti_l2:
        for obj_l2 in oggetti_l2:
            obj_name = obj_l2.get('ObjectName', '')
            obj_type = obj_l2.get('ObjectType', '')
            obj_server = obj_l2.get('Server', SQL_SERVER)
            obj_db = obj_l2.get('Database', '')
            chiamanti = obj_l2.get('Oggetti_Chiamanti_L1', '')
            
            # Esplode oggetti
            objects_str = obj_l2.get('Dipendenze_Oggetti', '')
            if isinstance(objects_str, str) and objects_str != 'Nessuna':
                objects = [o.strip() for o in objects_str.split(';') if o.strip()]
                for obj_dep in objects:
                    l2_obj_obj_rows.append({
                        'Livello': 2,
                        'ObjectName': obj_name,
                        'ObjectType': obj_type,
                        'Server': obj_server,
                        'Database': obj_db,
                        'Chiamanti_L1': chiamanti,
                        'Oggetto_Dipendente': obj_dep,
                        'Tipo_Relazione': 'DIPENDE_DA_OGGETTO'
                    })
    
    df_l2_obj_objects = pd.DataFrame(l2_obj_obj_rows)
    print(f"  ✓ L2 Oggetti→Oggetti: {len(df_l2_obj_objects)} relazioni")
    
    # 6. Export multi-sheet
    print(f"\n6. Export: {OUTPUT_FILE}")
    
    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        # Sheet 1: Oggetti Livello 1
        df_l1_critical.to_excel(writer, sheet_name='Oggetti Livello 1', index=False)
        
        # Sheet 2: Oggetti Livello 2 (ESTRATTI)
        if oggetti_l2:
            pd.DataFrame(oggetti_l2).to_excel(writer, sheet_name='Oggetti Livello 2', index=False)
        
        # Sheet 3-4: L1 Esplosi
        if len(df_l1_obj_tables) > 0:
            df_l1_obj_tables.to_excel(writer, sheet_name='L1_Oggetti_Tabelle_Esploso', index=False)
        if len(df_l1_obj_objects) > 0:
            df_l1_obj_objects.to_excel(writer, sheet_name='L1_Oggetti_Oggetti_Esploso', index=False)
        
        # Sheet 5-6: L2 Esplosi
        if len(df_l2_obj_tables) > 0:
            df_l2_obj_tables.to_excel(writer, sheet_name='L2_Oggetti_Tabelle_Esploso', index=False)
        if len(df_l2_obj_objects) > 0:
            df_l2_obj_objects.to_excel(writer, sheet_name='L2_Oggetti_Oggetti_Esploso', index=False)
        
        # Sheet 7: Tabelle Referenziate L1
        if critical_tables:
            pd.DataFrame(critical_tables).to_excel(writer, sheet_name='Tabelle Referenziate L1', index=False)
        
        # Sheet 8: Dipendenze Dettagliate
        if not df_deps_dettagliate.empty:
            df_deps_dettagliate.to_excel(writer, sheet_name='Dipendenze Dettagliate', index=False)
        
        # Sheet 9: Statistiche
        stats = [
            {'Metrica': 'LIVELLO 1', 'Valore': ''},
            {'Metrica': 'Oggetti Totali L1', 'Valore': len(df_l1)},
            {'Metrica': 'Oggetti Critici L1', 'Valore': len(df_l1_critical)},
            {'Metrica': 'L1 Relazioni Tabelle', 'Valore': len(df_l1_obj_tables)},
            {'Metrica': 'L1 Relazioni Oggetti', 'Valore': len(df_l1_obj_objects)},
            {'Metrica': '', 'Valore': ''},
            {'Metrica': 'GAP ANALYSIS', 'Valore': ''},
            {'Metrica': 'Dipendenze Oggetti SQL Trovate', 'Valore': len(dependency_map)},
            {'Metrica': 'Nuove Dipendenze Critiche (Gap)', 'Valore': len(new_deps)},
            {'Metrica': 'Totale Oggetti L2 da Estrarre', 'Valore': len(new_deps_total)},
            {'Metrica': 'Tabelle Referenziate Totali', 'Valore': len(table_map)},
            {'Metrica': 'Tabelle Referenziate Critiche', 'Valore': len(critical_tables)},
            {'Metrica': '', 'Valore': ''},
            {'Metrica': 'LIVELLO 2', 'Valore': ''},
            {'Metrica': 'Oggetti Estratti', 'Valore': len(oggetti_l2)},
            {'Metrica': 'Oggetti Non Trovati', 'Valore': len(new_deps_total) - len(oggetti_l2) if new_deps_total else 0},
            {'Metrica': 'L2 Relazioni Tabelle', 'Valore': len(df_l2_obj_tables)},
            {'Metrica': 'L2 Relazioni Oggetti', 'Valore': len(df_l2_obj_objects)},
            {'Metrica': '', 'Valore': ''},
            {'Metrica': 'DIPENDENZE', 'Valore': ''},
            {'Metrica': 'Totale Relazioni Oggetti', 'Valore': len(df_deps_dettagliate)},
        ]
        pd.DataFrame(stats).to_excel(writer, sheet_name='Statistiche', index=False)
    
    print("\n" + "="*70)
    print("COMPLETATO!")
    print("="*70)
    print(f"Sheet creati:")
    print(f"  - Oggetti Livello 1: {len(df_l1_critical)} oggetti")
    print(f"  - Oggetti Livello 2: {len(oggetti_l2)} oggetti")
    print(f"  - L1 Oggetti→Tabelle Esploso: {len(df_l1_obj_tables)} relazioni")
    print(f"  - L1 Oggetti→Oggetti Esploso: {len(df_l1_obj_objects)} relazioni")
    print(f"  - L2 Oggetti→Tabelle Esploso: {len(df_l2_obj_tables)} relazioni")
    print(f"  - L2 Oggetti→Oggetti Esploso: {len(df_l2_obj_objects)} relazioni")
    print(f"  - Tabelle Referenziate L1: {len(critical_tables)} tabelle")
    print(f"  - Dipendenze Dettagliate: {len(df_deps_dettagliate)} relazioni")
    print(f"  - Statistiche")

if __name__ == "__main__":
    main()
