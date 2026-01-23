# =========================
# IMPORT
# =========================
import pandas as pd
import pyodbc
from pathlib import Path
import re

# =========================
# CONFIG
# =========================
ANALYZED_FILE_L1 = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\analisi_oggetti_critici.xlsx'
CONNESSIONI_FILE = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\Connessioni_verificate.xlsx'
OUTPUT_FILE = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\DIPENDENZE_LIVELLO_2.xlsx'

SQL_SERVER = 'EPCP3'

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

def find_new_dependencies(tables_set, dependency_map):
    """Trova nuove dipendenze SQL non presenti nella lista originale."""
    new_objects = []
    
    for dep_name, callers in dependency_map.items():
        if dep_name in tables_set:
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
    print("ESTRAZIONE DIPENDENZE LIVELLO 2 (con GAP ANALYSIS + TABELLE)")
    print("="*70)
    
    # 1. Carica lista tabelle originali da Connessioni_verificate
    print("\n1. Caricamento lista tabelle originali...")
    try:
        df_connessioni = pd.read_excel(CONNESSIONI_FILE)
        if 'Table' in df_connessioni.columns:
            original_tables = set(df_connessioni['Table'].str.lower().str.strip())
            print(f"   Tabelle originali: {len(original_tables)}")
        else:
            print("   ERRORE: Colonna 'Table' non trovata in Connessioni_verificate")
            return
    except Exception as e:
        print(f"   ERRORE caricamento Connessioni: {e}")
        return
    
    # 2. Carica oggetti livello 1 (analizzati)
    print("\n2. Caricamento oggetti livello 1 analizzati...")
    try:
        df_l1 = pd.read_excel(ANALYZED_FILE_L1)
        df_l1_critical = df_l1[df_l1['Critico_Migrazione'] == 'SÌ'].copy()
        print(f"   Oggetti livello 1 critici: {len(df_l1_critical)}")
    except Exception as e:
        print(f"ERRORE: {e}")
        return
    
    # 3. GAP ANALYSIS: trova nuove dipendenze OGGETTI SQL
    print("\n3. Gap Analysis - Identificazione nuove dipendenze Oggetti SQL...")
    dependency_map = extract_dependencies_with_context(df_l1_critical)
    print(f"   Dipendenze oggetti SQL totali: {len(dependency_map)}")
    
    new_deps = find_new_dependencies(original_tables, dependency_map)
    print(f"   Nuovi Oggetti SQL critici da estrarre: {len(new_deps)}")
    
    # 4. Traccia TABELLE referenziate E trova oggetti associati
    print("\n4. Tracciamento tabelle referenziate + estrazione oggetti associati...")
    table_map = extract_tables_with_context(df_l1_critical)
    print(f"   Tabelle totali referenziate: {len(table_map)}")
    
    # Filtra tabelle critiche (chiamate da oggetti critici)
    critical_tables = []
    table_objects_found = []  # Oggetti trovati per le tabelle
    
    for table_name, callers in table_map.items():
        critical_callers = [c for c in callers if c['is_critical'] == 'SÌ']
        if critical_callers:
            # Traccia tabella
            databases_list = list(set([c['database'] for c in critical_callers if c['database']]))
            
            critical_tables.append({
                'table_name': table_name,
                'critical_callers_count': len(critical_callers),
                'critical_callers': '; '.join([c['object_name'] for c in critical_callers[:10]]),
                'databases': '; '.join(databases_list)
            })
            
            # Estrai oggetti SQL associati alla tabella (TRIGGER + SP con DML)
            for db in databases_list:  # Tutti i database dove la tabella è referenziata
                objects_for_table = extract_objects_for_table(db, table_name)
                for obj in objects_for_table:
                    # Evita duplicati con oggetti già trovati in new_deps
                    obj_name_lower = obj['name'].lower()
                    if not any(d['name'].lower() == obj_name_lower for d in new_deps):
                        # Aggiungi alla lista con formato compatibile
                        caller_names = [c['object_name'] for c in critical_callers[:5]]
                        table_objects_found.append({
                            'name': obj['name'],
                            'object_type': obj['type'],
                            'total_callers': 0,  # Associato a tabella, non chiamato direttamente
                            'critical_callers': len(critical_callers),
                            'caller_types': 'Tabella',
                            'critical_caller_types': 'Tabella',
                            'called_by_critical': obj['reason'],
                            'critical_caller_names': '; '.join(caller_names),
                            'callers_list': critical_callers,
                            'source': 'table_investigation'
                        })
    
    print(f"   Tabelle critiche referenziate: {len(critical_tables)}")
    print(f"   Oggetti SQL trovati su tabelle: {len(table_objects_found)}")
    
    # Unisci oggetti trovati da gap analysis + investigazione tabelle
    new_deps_total = new_deps + table_objects_found
    print(f"   Totale oggetti L2 da estrarre: {len(new_deps_total)}")
    
    if not new_deps_total:
        print("\n   Nessuna nuova dipendenza critica trovata!")
        # Esporta comunque le tabelle
    
    # 5. Estrai SQLDefinition oggetti livello 2
    print("\n5. Estrazione SQLDefinition oggetti livello 2...\n")
    
    oggetti_l2 = []
    trovati = 0
    non_trovati = 0
    
    print(f"   Oggetti unici da estrarre: {len(new_deps_total)}\n")
    
    for i, obj_info in enumerate(new_deps_total):
        object_name = obj_info['name']
        clean_name = object_name.replace('[', '').replace(']', '').strip()
        
        if (i + 1) % 10 == 0:
            print(f"Progresso: {i + 1}/{len(new_deps_total)}")
        
        # Trova database da oggetti chiamanti
        database_found = None
        for caller_info in obj_info['callers_list']:
            db_candidate = caller_info.get('database', '')
            if db_candidate:
                database_found = db_candidate
                break
        
        if not database_found:
            # Fallback: cerca in tutti i database di L1
            databases_l1 = df_l1_critical['Database'].unique()
            for db in databases_l1:
                result = extract_sql_definition(db, clean_name)
                if result:
                    database_found = db
                    break
        
        if database_found:
            result = extract_sql_definition(database_found, clean_name)
            
            if result:
                trovati += 1
                sql_def = result['SQLDefinition']
                
                # Estrai dipendenze livello 3 (separate: tabelle + oggetti)
                deps_l3 = extract_dependencies_from_sql(sql_def)
                tables_l3 = deps_l3['tables']
                objects_l3 = deps_l3['objects']
                
                tables_l3_str = '; '.join(tables_l3) if tables_l3 else 'Nessuna'
                objects_l3_str = '; '.join(objects_l3) if objects_l3 else 'Nessuna'
                
                oggetti_l2.append({
                    'Livello': 2,
                    'Server': SQL_SERVER,
                    'Database': database_found,
                    'ObjectName': clean_name,
                    'ObjectType': result['ObjectType'],
                    'SchemaName': result['SchemaName'],
                    'Oggetti_Chiamanti_L1': obj_info['critical_caller_names'],
                    'N_Chiamanti_Critici': obj_info['critical_callers'],
                    'Dipendenze_Tabelle_L3': tables_l3_str,
                    'N_Tabelle_L3': len(tables_l3),
                    'Dipendenze_Oggetti_L3': objects_l3_str,
                    'N_Oggetti_L3': len(objects_l3),
                    'SQLDefinition': sql_def
                })
                
                if trovati <= 5:
                    print(f"  ✓ {clean_name} trovato in {database_found}")
            else:
                non_trovati += 1
                if non_trovati <= 5:
                    print(f"  ✗ {clean_name} non trovato in {database_found}")
        else:
            non_trovati += 1
            if non_trovati <= 5:
                print(f"  ✗ {clean_name} - database non determinato")
    
    print(f"\n   Oggetti trovati: {trovati}/{len(new_deps_total)}")
    print(f"   Oggetti non trovati: {non_trovati}/{len(new_deps_total)}")
    
    # 6. Crea sheet dipendenze dettagliate
    print("\n6. Creazione dipendenze dettagliate...")
    
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
    
    # Dipendenze L2 → L3 (OGGETTI)
    for obj_l2 in oggetti_l2:
        objects_l3_str = obj_l2['Dipendenze_Oggetti_L3']
        if objects_l3_str != 'Nessuna':
            deps = [d.strip() for d in objects_l3_str.split(';') if d.strip()]
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
                    'Livello_Dipendenza': 3
                })
    
    df_deps_dettagliate = pd.DataFrame(dipendenze_dettagliate)
    
    # 7. Export multi-sheet
    print(f"\n7. Export: {OUTPUT_FILE}")
    
    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        # Sheet 1: Oggetti Livello 1
        df_l1_critical.to_excel(writer, sheet_name='Oggetti Livello 1', index=False)
        
        # Sheet 2: Oggetti Livello 2 (ESTRATTI)
        if oggetti_l2:
            pd.DataFrame(oggetti_l2).to_excel(writer, sheet_name='Oggetti Livello 2', index=False)
        
        # Sheet 3: Tabelle Referenziate L1
        if critical_tables:
            pd.DataFrame(critical_tables).to_excel(writer, sheet_name='Tabelle Referenziate L1', index=False)
        
        # Sheet 4: Dipendenze Dettagliate
        if not df_deps_dettagliate.empty:
            df_deps_dettagliate.to_excel(writer, sheet_name='Dipendenze Dettagliate', index=False)
        
        # Sheet 5: Statistiche
        stats = [
            {'Metrica': 'LIVELLO 1', 'Valore': ''},
            {'Metrica': 'Oggetti Critici', 'Valore': len(df_l1_critical)},
            {'Metrica': '', 'Valore': ''},
            {'Metrica': 'GAP ANALYSIS', 'Valore': ''},
            {'Metrica': 'Tabelle Originali', 'Valore': len(original_tables)},
            {'Metrica': 'Dipendenze Oggetti SQL Trovate', 'Valore': len(dependency_map)},
            {'Metrica': 'Nuove Dipendenze Critiche', 'Valore': len(new_deps)},
            {'Metrica': 'Tabelle Referenziate Totali', 'Valore': len(table_map)},
            {'Metrica': 'Tabelle Referenziate Critiche', 'Valore': len(critical_tables)},
            {'Metrica': '', 'Valore': ''},
            {'Metrica': 'LIVELLO 2', 'Valore': ''},
            {'Metrica': 'Oggetti Estratti', 'Valore': trovati},
            {'Metrica': 'Oggetti Non Trovati', 'Valore': non_trovati},
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
    print(f"  - Tabelle Referenziate L1: {len(critical_tables)} tabelle")
    print(f"  - Dipendenze Dettagliate: {len(df_deps_dettagliate)} relazioni")
    print(f"  - Statistiche")

if __name__ == "__main__":
    main()
