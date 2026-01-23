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
INPUT_FILE_L3 = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\DIPENDENZE_LIVELLO_3.xlsx'
INPUT_FILE_L2 = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\DIPENDENZE_LIVELLO_2.xlsx'
INPUT_FILE_L1 = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\analisi_oggetti_critici.xlsx'
OUTPUT_FILE = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\DIPENDENZE_LIVELLO_4.xlsx'

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
    
    # 3. Traccia TABELLE referenziate da L3 E trova oggetti associati
    print("\n3. Tracciamento tabelle referenziate da L3 + estrazione oggetti associati...")
    table_map = extract_tables_with_context(df_l3)
    print(f"   Tabelle totali referenziate: {len(table_map)}")
    
    critical_tables = []
    table_objects_found = []
    
    for table_name, callers in table_map.items():
        all_chains_l1 = set()
        for c in callers:
            chain = c.get('chain_l1', '')
            if chain and chain != 'Non tracciato':
                all_chains_l1.update([x.strip() for x in chain.split(';')])
        
        databases_list = list(set([c['database'] for c in callers if c['database']]))
        
        critical_tables.append({
            'table_name': table_name,
            'callers_count': len(callers),
            'callers_l3': '; '.join([c['object_name'] for c in callers[:10]]),
            'chain_l1': '; '.join(sorted(all_chains_l1)) if all_chains_l1 else 'Non tracciato',
            'databases': '; '.join(databases_list)
        })
        
        # Estrai oggetti SQL associati alla tabella (TRIGGER + SP con DML)
        for db in databases_list:
            objects_for_table = extract_objects_for_table(db, table_name)
            for obj in objects_for_table:
                obj_name_lower = obj['name'].lower()
                if not any(d['name'].lower() == obj_name_lower for d in new_deps_l4) and obj_name_lower not in already_extracted:
                    caller_names = [c['object_name'] for c in callers[:5]]
                    chain_l1_str = '; '.join(sorted(all_chains_l1)) if all_chains_l1 else 'Non tracciato'
                    table_objects_found.append({
                        'name': obj['name'],
                        'object_type': obj['type'],
                        'total_callers': 0,
                        'called_by': obj['reason'],
                        'caller_names': '; '.join(caller_names[:5]),
                        'chain_l1': chain_l1_str,
                        'callers_list': callers,
                        'source': 'table_investigation'
                    })
    
    print(f"   Tabelle referenziate da L3: {len(critical_tables)}")
    print(f"   Oggetti SQL trovati su tabelle: {len(table_objects_found)}")
    
    new_deps_l4_total = new_deps_l4 + table_objects_found
    print(f"   Totale oggetti L4 da estrarre: {len(new_deps_l4_total)}")
    
    if not new_deps_l4_total:
        print("\n⚠ Nessuna nuova dipendenza L4 trovata - catena completa!")
    
    # 4. Estrai SQLDefinition oggetti livello 4
    print("\n4. Estrazione SQLDefinition oggetti livello 4...\n")
    
    oggetti_l4 = []
    trovati = 0
    non_trovati = 0
    
    print(f"   Oggetti unici da estrarre: {len(new_deps_l4_total)}\n")
    
    for i, obj_info in enumerate(new_deps_l4_total):
        object_name = obj_info['name']
        clean_name = object_name.replace('[', '').replace(']', '').strip()
        
        if (i + 1) % 10 == 0:
            print(f"Progresso: {i + 1}/{len(new_deps_l4_total)}")
        
        database_found = None
        for caller_info in obj_info['callers_list']:
            db_candidate = caller_info.get('database', '')
            if db_candidate:
                database_found = db_candidate
                break
        
        if not database_found:
            databases_l3 = df_l3['Database'].unique()
            for db in databases_l3:
                result = extract_sql_definition(db, clean_name)
                if result:
                    database_found = db
                    break
        
        if database_found:
            result = extract_sql_definition(database_found, clean_name)
            
            if result:
                trovati += 1
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
                    'Database': database_found,
                    'ObjectName': clean_name,
                    'ObjectType': result['ObjectType'],
                    'SchemaName': result['SchemaName'],
                    'Oggetti_Chiamanti_L3': obj_info['caller_names'],
                    'Catena_Origine_L1': obj_info['chain_l1'],
                    'N_Chiamanti_L3': obj_info['total_callers'],
                    'Dipendenze_Tabelle_L5': tables_l5_str,
                    'N_Tabelle_L5': len(tables_l5),
                    'Dipendenze_Oggetti_L5': objects_l5_str,
                    'N_Oggetti_L5': len(objects_l5),
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
    
    print(f"\n   Oggetti trovati: {trovati}/{len(new_deps_l4_total)}")
    print(f"   Oggetti non trovati: {non_trovati}/{len(new_deps_l4_total)}")
    
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
            {'Metrica': 'Oggetti Estratti', 'Valore': trovati},
            {'Metrica': 'Oggetti Non Trovati', 'Valore': non_trovati},
            {'Metrica': '', 'Valore': ''},
            {'Metrica': 'COPERTURA TOTALE', 'Valore': ''},
            {'Metrica': 'Oggetti Totali (L1+L2+L3+L4)', 'Valore': len(already_extracted) + trovati},
        ]
        pd.DataFrame(stats).to_excel(writer, sheet_name='Statistiche', index=False)
    
    print("\n" + "="*70)
    print("✅ COMPLETATO!")
    print("="*70)
    print(f"Oggetti L4 estratti: {trovati}")
    print(f"Tabelle referenziate L3: {len(critical_tables)}")

if __name__ == "__main__":
    main()
