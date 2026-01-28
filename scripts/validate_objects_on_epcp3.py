"""
Script per validare oggetti su EPCP3 e analizzare dipendenze cross-server per fase 0.
Input 1: LINEAGE_HYBRID_REPORT_MERGED.xlsx (2675 oggetti critici già analizzati)
Input 2: oggetti_da_validare.xlsx (5896 oggetti estratti da SQLDefinition dei 2675)
Output: Excel con validazione EPCP3 + analisi dipendenze cross-database/cross-server per TUTTI gli oggetti (8571 totali)
"""

import pandas as pd
import pyodbc
import re
from pathlib import Path
from collections import defaultdict
from sqlalchemy import create_engine, text
from datetime import datetime

# =========================
# CONFIG
# =========================
BASE_PATH = Path(r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\Report finali consolidati (Conn v. + viste)")

# File 1: 2675 oggetti critici già analizzati (con Livello, Motivo, ReferenceCount, etc.)
INPUT_MERGED_EXCEL = BASE_PATH / "LINEAGE_HYBRID_REPORT_MERGED.xlsx"

# File 2: 5896 oggetti estratti da SQLDefinition dei 2675
INPUT_DEPENDENCIES_EXCEL = BASE_PATH / "5896 Objects da validare in EPCP3" / "oggetti_da_validare.xlsx"

OUTPUT_EXCEL = BASE_PATH / "ALL_OBJECTS_VALIDATO_EPCP3_FASE0.xlsx"

SQL_SERVER = 'EPCP3'

# Lista completa 53 database su EPCP3 (dai tuoi screenshot + comuni)
EPCP3_DATABASES = [
    'master', 'AMS', 'ANALISI', 'BADO_OnLine', 'BASEDATI_BI', 'CORESQL7', 
    'EPC_BI', 'Gestito', 'REPLICA', 'S1057', 'S1057B', 'S1242', 'S1259',
    'amministrazione', 'AMS_MANUAL_TABLES', 'ANALYTICS', 'BADODB01',
    'BADOFLUSSI', 'badOflussi_STD', 'cdcsql', 'COESYS12000', 'CORESQL7ARK',
    'CORESQL7FM', 'DashboardDb', 'DBSettimmanale', 'DMLAV', 'DWH', 'EMP',
    'EPC_PCT', 'FlussiLavorazioni', 'GestCancipo', 'HCO', 'IDE_PRODUCTION',
    'LOGCDO', 'MngAssembly', 'msdb', 'protocollo', 'PSC', 'replicaFM',
    'report', 'reportDr_New', 'S1040', 'S3229', 'Salvataggio_dati_eliminati',
    'SEGNALAZIONI_BDI', 'Staging_ALL', 'Staging_Elaborazione', 'Staging_SST',
    'StagingEPC', 'tempdb', 'Templ_GBS', 'ugc_service', 'wh1'
]

# =========================
# FUNZIONI ANALISI SQL
# =========================

def strip_sql_comments(sql_def):
    """Rimuove commenti SQL (-- e /* */)."""
    if not sql_def or pd.isna(sql_def):
        return ''
    sql_def = str(sql_def)
    # Rimuovi commenti multilinea /* */
    sql_def = re.sub(r'/\*.*?\*/', ' ', sql_def, flags=re.DOTALL)
    # Rimuovi commenti singola linea --
    sql_def = re.sub(r'--[^\n]*', ' ', sql_def)
    return sql_def

def has_dml_operations(sql_def):
    """Verifica se ci sono operazioni DML (INSERT/UPDATE/DELETE/MERGE)."""
    if not sql_def:
        return False, []
    
    sql_def = strip_sql_comments(sql_def)
    sql_lower = sql_def.lower()
    
    operations = []
    if re.search(r'\binsert\s+into\b', sql_lower):
        operations.append('INSERT')
    if re.search(r'\bupdate\b(?!\s+statistics)', sql_lower):
        operations.append('UPDATE')
    if re.search(r'\bdelete\s+from\b', sql_lower):
        operations.append('DELETE')
    if re.search(r'\bmerge\s+into\b', sql_lower):
        operations.append('MERGE')
    
    return len(operations) > 0, operations

def has_ddl_operations(sql_def):
    """Verifica se ci sono operazioni DDL (CREATE/ALTER/DROP TABLE)."""
    if not sql_def:
        return False, []
    
    sql_def = strip_sql_comments(sql_def)
    sql_lower = sql_def.lower()
    
    operations = []
    if re.search(r'\bcreate\s+table\b', sql_lower):
        operations.append('CREATE TABLE')
    if re.search(r'\balter\s+table\b', sql_lower):
        operations.append('ALTER TABLE')
    if re.search(r'\bdrop\s+table\b', sql_lower):
        operations.append('DROP TABLE')
    if re.search(r'\btruncate\s+table\b', sql_lower):
        operations.append('TRUNCATE')
    
    return len(operations) > 0, operations

def has_complex_patterns(sql_def):
    """Identifica pattern T-SQL complessi."""
    if not sql_def:
        return []
    
    sql_def = strip_sql_comments(sql_def)
    sql_lower = sql_def.lower()
    
    patterns = []
    
    # Cursori
    if re.search(r'\bdeclare\s+\w+\s+cursor\b', sql_lower):
        patterns.append('CURSOR')
    
    # Dynamic SQL
    if re.search(r'\bexec\s*\(\s*@', sql_lower) or re.search(r'\bsp_executesql\b', sql_lower):
        patterns.append('DYNAMIC_SQL')
    
    # Transazioni esplicite
    if re.search(r'\bbegin\s+tran(saction)?\b', sql_lower):
        patterns.append('TRANSACTION')
    
    # TRY-CATCH
    if re.search(r'\bbegin\s+try\b', sql_lower):
        patterns.append('ERROR_HANDLING')
    
    # WHILE loops
    if re.search(r'\bwhile\b', sql_lower):
        patterns.append('LOOP')
    
    return patterns

def extract_cross_database_dependencies(sql_def):
    """Estrae riferimenti cross-database: [database].[schema].[object]."""
    if not sql_def:
        return set()
    
    sql_def = strip_sql_comments(sql_def)
    
    # Pattern per oggetti qualificati con database
    # Formato: [db].[schema].[object] o db.schema.object
    pattern = r'\[?(\w+)\]?\.\[?(\w+)\]?\.\[?(\w+)\]?'
    
    dependencies = set()
    for match in re.finditer(pattern, sql_def):
        db, schema, obj = match.groups()
        # Escludi pattern comuni non-database (tipo variabili)
        if db.lower() not in ['sys', 'information_schema', 'dbo']:
            dependencies.add(f"{db}.{schema}.{obj}")
    
    return dependencies

def extract_cross_server_dependencies(sql_def):
    """Estrae riferimenti cross-server: linked servers, OPENQUERY, OPENDATASOURCE."""
    if not sql_def:
        return set()
    
    sql_def = strip_sql_comments(sql_def)
    sql_lower = sql_def.lower()
    
    dependencies = set()
    
    # Pattern 1: [server].[database].[schema].[object]
    four_part_pattern = r'\[?(\w+)\]?\.\[?(\w+)\]?\.\[?(\w+)\]?\.\[?(\w+)\]?'
    for match in re.finditer(four_part_pattern, sql_def):
        server, db, schema, obj = match.groups()
        if server.lower() not in ['sys', 'dbo', 'information_schema']:
            dependencies.add(f"LINKED_SERVER:{server}.{db}.{schema}.{obj}")
    
    # Pattern 2: OPENQUERY
    if 'openquery' in sql_lower:
        openquery_matches = re.finditer(r'openquery\s*\(\s*([^,\)]+)', sql_lower)
        for match in openquery_matches:
            linked_server = match.group(1).strip()
            dependencies.add(f"OPENQUERY:{linked_server}")
    
    # Pattern 3: OPENDATASOURCE
    if 'opendatasource' in sql_lower:
        dependencies.add("OPENDATASOURCE:external_connection")
    
    # Pattern 4: OPENROWSET
    if 'openrowset' in sql_lower:
        dependencies.add("OPENROWSET:external_connection")
    
    return dependencies

def calculate_criticality_score(obj_type, has_dml, has_ddl, complex_patterns, cross_db_count, cross_server_count):
    """Calcola score criticità 0-100 per migrazione fase 0."""
    score = 0
    reasons = []
    
    # Trigger sono sempre critici (modificano dati automaticamente)
    if 'TRIGGER' in obj_type.upper():
        score += 50
        reasons.append("È un TRIGGER (modifica dati automaticamente)")
    
    # DML operations (molto critiche)
    if has_dml:
        score += 30
        reasons.append("Esegue operazioni DML critiche")
    
    # DDL operations (critiche per struttura)
    if has_ddl:
        score += 20
        reasons.append("Esegue operazioni DDL sulla struttura")
    
    # Pattern complessi
    if 'CURSOR' in complex_patterns:
        score += 15
        reasons.append("Usa cursori (performance critiche)")
    if 'DYNAMIC_SQL' in complex_patterns:
        score += 10
        reasons.append("Usa SQL dinamico")
    if 'LOOP' in complex_patterns:
        score += 5
        reasons.append("Contiene cicli iterativi")
    
    # Dipendenze cross-database (problema per fase 0)
    if cross_db_count > 0:
        score += min(20, cross_db_count * 5)
        reasons.append(f"Ha {cross_db_count} dipendenze cross-database")
    
    # Dipendenze cross-server (BLOCCA fase 0)
    if cross_server_count > 0:
        score += 50  # Molto critico!
        reasons.append(f"HA {cross_server_count} DIPENDENZE CROSS-SERVER - BLOCCA FASE 0")
    
    return min(100, score), reasons

def classify_migration_priority(score, has_cross_server):
    """Classifica priorità migrazione."""
    if has_cross_server:
        return 'BLOCCO_FASE0'  # Non può essere migrato in fase 0
    elif score >= 70:
        return 'ALTA'
    elif score >= 40:
        return 'MEDIA'
    else:
        return 'BASSA'

# =========================
# FUNZIONI SQL SERVER
# =========================

def get_connection_string(database):
    """Connection string per SQL Server."""
    return (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
    )

def find_object_in_database(database, object_name):
    """
    Cerca un oggetto in un database specifico.
    Returns: (found, object_type, schema_name, sql_definition)
    """
    try:
        conn_str = get_connection_string(database)
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        
        # Query per cercare l'oggetto
        query = """
        SELECT 
            o.name AS ObjectName,
            o.type_desc AS ObjectType,
            SCHEMA_NAME(o.schema_id) AS SchemaName,
            OBJECT_DEFINITION(o.object_id) AS SQLDefinition
        FROM sys.objects o
        WHERE LOWER(o.name) = LOWER(?)
          AND o.type IN ('P', 'FN', 'IF', 'TF', 'TR', 'V', 'U')  -- SP, Functions, Triggers, Views, Tables
        ORDER BY 
          CASE o.type 
            WHEN 'P' THEN 1  -- Stored Procedure
            WHEN 'TR' THEN 2  -- Trigger
            WHEN 'FN' THEN 3  -- Function
            WHEN 'V' THEN 4  -- View
            WHEN 'U' THEN 5  -- Table
            ELSE 6
          END
        """
        
        cursor.execute(query, object_name)
        row = cursor.fetchone()
        
        if row:
            obj_name = row.ObjectName
            obj_type = row.ObjectType
            schema = row.SchemaName
            sql_def = row.SQLDefinition if row.SQLDefinition else ''
            
            cursor.close()
            conn.close()
            return True, obj_type, schema, sql_def
        
        cursor.close()
        conn.close()
        return False, None, None, None
        
    except Exception as e:
        # Skip database silently se non accessibile
        return False, None, None, None

def parse_object_key(object_key):
    """
    Parsa chiave oggetto formato [Database].[Schema].[ObjectName].
    Returns: (database, schema, object_name)
    """
    if not object_key or pd.isna(object_key):
        return None, None, None
    
    # Rimuovi spazi e brackets
    object_key = str(object_key).strip()
    
    # Pattern per [Database].[Schema].[ObjectName]
    pattern = r'\[?([^\]\.]+)\]?\.\[?([^\]\.]+)\]?\.\[?([^\]\.]+)\]?'
    match = re.match(pattern, object_key)
    
    if match:
        return match.group(1), match.group(2), match.group(3)
    
    return None, None, None

def validate_and_analyze_objects(df):
    """
    Valida ogni oggetto su EPCP3 e arricchisce con analisi dipendenze cross-server.
    Mantiene tutti i metadati esistenti (Livello, Motivo, ReferenceCount per oggetti MERGED).
    La colonna 'Origine' distingue tra MERGED_CRITICAL e DISCOVERED_DEPENDENCY.
    """
    print("\n" + "="*80)
    print("VALIDAZIONE TUTTI GLI OGGETTI SU EPCP3")
    print("="*80)
    
    # Prepara liste per risultati
    databases_found_on_epcp3 = []
    found_on_epcp3_list = []
    epcp3_object_types = []
    epcp3_schemas = []
    epcp3_sql_definitions = []
    
    # Analisi dipendenze cross-server (il pezzo mancante!)
    cross_server_deps_list = []
    cross_server_count_list = []
    has_cross_server_list = []
    blocco_fase0_list = []
    
    # Statistiche
    stats = defaultdict(int)
    
    total = len(df)
    critical_count = len(df[df['Origine'] == 'MERGED_CRITICAL'])
    dependency_count = len(df[df['Origine'] == 'DISCOVERED_DEPENDENCY'])
    
    print(f"\nOggetti totali da validare: {total}")
    print(f"  • MERGED_CRITICAL:       {critical_count}")
    print(f"  • DISCOVERED_DEPENDENCY: {dependency_count}")
    print(f"Database EPCP3 da interrogare: {len(EPCP3_DATABASES)}")
    print("\nInizio validazione...\n")
    
    for row_num, (idx, row) in enumerate(df.iterrows(), start=1):
        # Estrai informazioni dall'oggetto (formato [Database].[Schema].[ObjectName])
        database_orig = str(row.get('Database', '')) if pd.notna(row.get('Database')) else ''
        schema_orig = str(row.get('Schema', '')) if pd.notna(row.get('Schema')) else ''
        object_name = str(row.get('ObjectName', '')) if pd.notna(row.get('ObjectName')) else ''
        
        # Progress
        if row_num % 100 == 0:
            print(f"  Processati {row_num}/{total} oggetti...")
        
        if not object_name:
            # Dati mancanti
            databases_found_on_epcp3.append(None)
            found_on_epcp3_list.append(False)
            epcp3_object_types.append(None)
            epcp3_schemas.append(None)
            epcp3_sql_definitions.append(None)
            cross_server_deps_list.append('')
            cross_server_count_list.append(0)
            has_cross_server_list.append(False)
            blocco_fase0_list.append('NO')
            stats['MISSING_DATA'] += 1
            continue
        
        # Cerca oggetto in tutti i database EPCP3
        found_on_epcp3 = False
        found_in_databases = []
        found_sql_def = None
        found_obj_type = None
        found_schema = None
        
        for database in EPCP3_DATABASES:
            is_found, obj_type, schema, sql_def = find_object_in_database(database, object_name)
            
            if is_found:
                found_on_epcp3 = True
                found_in_databases.append(database)
                
                # Prendi la prima occorrenza per analisi dettagliata
                if found_sql_def is None:
                    found_sql_def = sql_def
                    found_obj_type = obj_type
                    found_schema = schema
        
        if found_on_epcp3:
            # FOUND su EPCP3 - Analizza dipendenze cross-server
            databases_found_on_epcp3.append('; '.join(found_in_databases))
            found_on_epcp3_list.append(True)
            epcp3_object_types.append(found_obj_type)
            epcp3_schemas.append(found_schema)
            epcp3_sql_definitions.append(found_sql_def)
            
            stats['FOUND_EPCP3'] += 1
            
            # Analisi dipendenze cross-server (CRITICO per fase 0!)
            cross_server_deps = extract_cross_server_dependencies(found_sql_def)
            cross_server_count = len(cross_server_deps)
            cross_server_deps_list.append('; '.join(cross_server_deps) if cross_server_deps else '')
            cross_server_count_list.append(cross_server_count)
            has_cross_server_list.append(cross_server_count > 0)
            
            # Blocco fase 0?
            if cross_server_count > 0:
                blocco_fase0_list.append('SÌ')
                stats['BLOCCO_FASE0'] += 1
            else:
                blocco_fase0_list.append('NO')
                stats['OK_FASE0'] += 1
        else:
            # NOT_FOUND su EPCP3
            databases_found_on_epcp3.append(None)
            found_on_epcp3_list.append(False)
            epcp3_object_types.append(None)
            epcp3_schemas.append(None)
            epcp3_sql_definitions.append(None)
            cross_server_deps_list.append('')
            cross_server_count_list.append(0)
            has_cross_server_list.append(False)
            blocco_fase0_list.append('N/A')
            stats['NOT_FOUND_EPCP3'] += 1
    
    # Aggiungi nuove colonne al DataFrame (mantiene tutte le esistenti)
    df['EPCP3_Found'] = found_on_epcp3_list
    df['EPCP3_Databases'] = databases_found_on_epcp3
    df['EPCP3_ObjectType'] = epcp3_object_types
    df['EPCP3_Schema'] = epcp3_schemas
    df['EPCP3_SQLDefinition'] = epcp3_sql_definitions
    df['Cross_Server_Dependencies'] = cross_server_deps_list
    df['Cross_Server_Count'] = cross_server_count_list
    df['Has_Cross_Server'] = has_cross_server_list
    df['Blocco_Fase0'] = blocco_fase0_list
    
    return df, stats

def generate_excel_output(df, stats, output_path):
    """Genera Excel con multiple sheet di analisi per fase 0."""
    print(f"\n📊 Generazione Excel con analisi fase 0...")
    
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Sheet 1: Tutti gli oggetti con validazione EPCP3
            df.to_excel(writer, sheet_name='All_Objects_Validated', index=False)
            
            # Sheet 2: Solo oggetti FOUND su EPCP3
            found_df = df[df['EPCP3_Found'] == True].copy()
            if len(found_df) > 0:
                found_df.to_excel(writer, sheet_name='Found_on_EPCP3', index=False)
            
            # Sheet 3: OK per FASE 0 (no cross-server, trovati su EPCP3)
            ok_fase0_df = df[(df['EPCP3_Found'] == True) & (df['Blocco_Fase0'] == 'NO')].copy()
            if len(ok_fase0_df) > 0:
                ok_fase0_df.to_excel(writer, sheet_name='OK_FASE0', index=False)
            
            # Sheet 4: BLOCCO FASE 0 (hanno dipendenze cross-server)
            blocco_df = df[df['Blocco_Fase0'] == 'SÌ'].copy()
            if len(blocco_df) > 0:
                blocco_df.to_excel(writer, sheet_name='BLOCCO_FASE0', index=False)
            
            # Sheet 5: NOT_FOUND su EPCP3 (eliminati o su altro server?)
            not_found_df = df[df['EPCP3_Found'] == False].copy()
            if len(not_found_df) > 0:
                not_found_df.to_excel(writer, sheet_name='NOT_FOUND_EPCP3', index=False)
            
            # Sheet 6: Per Livello + Validazione EPCP3
            if len(found_df) > 0:
                for level in ['L1', 'L2', 'L3', 'L4']:
                    level_df = found_df[found_df['Livello'] == level].copy()
                    if len(level_df) > 0:
                        level_df.to_excel(writer, sheet_name=f'{level}_EPCP3', index=False)
            
            # Sheet 7: Summary statistiche FASE 0
            summary_data = {
                'Categoria': [
                    'Totale oggetti critici (MERGED)',
                    '',
                    'FOUND su EPCP3',
                    'NOT FOUND su EPCP3',
                    '',
                    '✅ OK per FASE 0 (no cross-server)',
                    '🚫 BLOCCO FASE 0 (cross-server)',
                    '',
                    'Per Livello (FOUND):',
                    '  L1 (Tabelle critiche)',
                    '  L2 (Dipendenze L1)',
                    '  L3 (Dipendenze L2)',
                    '  L4 (Dipendenze L3)'
                ],
                'Conteggio': [
                    len(df),
                    '',
                    stats['FOUND_EPCP3'],
                    stats['NOT_FOUND_EPCP3'],
                    '',
                    stats['OK_FASE0'],
                    stats['BLOCCO_FASE0'],
                    '',
                    '',
                    len(found_df[found_df['Livello'] == 'L1']) if len(found_df) > 0 else 0,
                    len(found_df[found_df['Livello'] == 'L2']) if len(found_df) > 0 else 0,
                    len(found_df[found_df['Livello'] == 'L3']) if len(found_df) > 0 else 0,
                    len(found_df[found_df['Livello'] == 'L4']) if len(found_df) > 0 else 0
                ],
                'Percentuale': [
                    '100%',
                    '',
                    f"{stats['FOUND_EPCP3']/len(df)*100:.1f}%" if len(df) > 0 else '0%',
                    f"{stats['NOT_FOUND_EPCP3']/len(df)*100:.1f}%" if len(df) > 0 else '0%',
                    '',
                    f"{stats['OK_FASE0']/stats['FOUND_EPCP3']*100:.1f}%" if stats['FOUND_EPCP3'] > 0 else '0%',
                    f"{stats['BLOCCO_FASE0']/stats['FOUND_EPCP3']*100:.1f}%" if stats['FOUND_EPCP3'] > 0 else '0%',
                    '',
                    '',
                    '',
                    '',
                    '',
                    ''
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary_Fase0', index=False)
            
            # Sheet 8: Breakdown per database EPCP3
            if len(found_df) > 0:
                # Esplodi le righe con multipli database
                db_list = []
                for idx, row in found_df.iterrows():
                    if pd.notna(row['EPCP3_Databases']):
                        dbs = str(row['EPCP3_Databases']).split('; ')
                        for db in dbs:
                            db_list.append({
                                'Database': db, 
                                'ObjectName': row['ObjectName'],
                                'Livello': row.get('Livello', 'N/A'),
                                'Blocco_Fase0': row['Blocco_Fase0']
                            })
                
                if db_list:
                    db_breakdown = pd.DataFrame(db_list)
                    db_counts = db_breakdown.groupby(['Database', 'Blocco_Fase0']).size().reset_index(name='Count')
                    db_counts.to_excel(writer, sheet_name='By_Database_EPCP3', index=False)
        
        print(f"✅ Excel generato: {output_path}")
        return True
        
    except Exception as e:
        print(f"✗ Errore generazione Excel: {e}")
        import traceback
        traceback.print_exc()
        return False

# =========================
# MAIN
# =========================

def main():
    print("\n" + "="*80)
    print("VALIDAZIONE TUTTI GLI OGGETTI SU EPCP3 - ANALISI FASE 0")
    print("="*80)
    print(f"\nInput 1 (CRITICI):      {INPUT_MERGED_EXCEL}")
    print(f"Input 2 (DIPENDENZE):   {INPUT_DEPENDENCIES_EXCEL}")
    print(f"Output:                 {OUTPUT_EXCEL}\n")
    
    # ============================
    # LEGGI FILE 1: OGGETTI CRITICI MERGED (2675)
    # ============================
    try:
        # Prova prima sheet "All Objects"
        try:
            df_merged = pd.read_excel(INPUT_MERGED_EXCEL, sheet_name='All Objects')
            print(f"✓ FILE 1: Lette {len(df_merged)} righe da sheet 'All Objects'")
        except:
            # Fallback su primo sheet
            df_merged = pd.read_excel(INPUT_MERGED_EXCEL)
            print(f"✓ FILE 1: Lette {len(df_merged)} righe dal primo sheet")
        
        # Aggiungi colonna Origine
        df_merged['Origine'] = 'MERGED_CRITICAL'
        print(f"  Colonne FILE 1: {list(df_merged.columns)[:8]}...")  # Prime 8 colonne
        
    except Exception as e:
        print(f"✗ Errore lettura FILE 1 (MERGED): {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ============================
    # LEGGI FILE 2: OGGETTI DA VALIDARE (5896)
    # ============================
    try:
        df_dependencies = pd.read_excel(INPUT_DEPENDENCIES_EXCEL)
        print(f"✓ FILE 2: Lette {len(df_dependencies)} righe")
        
        # Aggiungi colonna Origine
        df_dependencies['Origine'] = 'DISCOVERED_DEPENDENCY'
        print(f"  Colonne FILE 2: {list(df_dependencies.columns)[:8]}...")  # Prime 8 colonne
        
    except Exception as e:
        print(f"✗ Errore lettura FILE 2 (DEPENDENCIES): {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ============================
    # UNISCI I DUE DATASET
    # ============================
    print(f"\n🔗 Unione dataset...")
    
    # Identifica colonne comuni tra i due file
    common_cols = list(set(df_merged.columns) & set(df_dependencies.columns))
    print(f"  Colonne comuni: {len(common_cols)}")
    print(f"  {common_cols[:10]}..." if len(common_cols) > 10 else f"  {common_cols}")
    
    # Allinea le colonne: usa tutte le colonne di df_merged + aggiungi colonne mancanti da df_dependencies
    all_columns = list(df_merged.columns)
    for col in df_dependencies.columns:
        if col not in all_columns:
            all_columns.append(col)
            df_merged[col] = None  # Aggiungi colonna vuota a df_merged
    
    # Allinea df_dependencies alle stesse colonne
    for col in all_columns:
        if col not in df_dependencies.columns:
            df_dependencies[col] = None
    
    # Riordina colonne per entrambi i dataframe
    df_merged = df_merged[all_columns]
    df_dependencies = df_dependencies[all_columns]
    
    # Concatena
    df = pd.concat([df_merged, df_dependencies], ignore_index=True)
    
    print(f"✓ Dataset uniti: {len(df)} oggetti totali")
    print(f"  • MERGED_CRITICAL:       {len(df[df['Origine'] == 'MERGED_CRITICAL'])}")
    print(f"  • DISCOVERED_DEPENDENCY: {len(df[df['Origine'] == 'DISCOVERED_DEPENDENCY'])}")
    
    # Verifica colonne essenziali
    required_cols = ['Database', 'Schema', 'ObjectName', 'Origine']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"⚠️  Colonne mancanti: {missing_cols}")
        print(f"   Colonne disponibili: {list(df.columns)}")
        return
    
    # Valida e arricchisci con analisi EPCP3
    df_validated, stats = validate_and_analyze_objects(df)
    
    # Genera Excel output
    success = generate_excel_output(df_validated, stats, OUTPUT_EXCEL)
    
    if success:
        print("\n" + "="*80)
        print("✅ VALIDAZIONE COMPLETATA")
        print("="*80)
        
        critical_count = len(df[df['Origine'] == 'MERGED_CRITICAL'])
        dependency_count = len(df[df['Origine'] == 'DISCOVERED_DEPENDENCY'])
        
        print(f"\nRisultati FASE 0:")
        print(f"  • Totale oggetti:          {len(df)}")
        print(f"    - MERGED_CRITICAL:       {critical_count}")
        print(f"    - DISCOVERED_DEPENDENCY: {dependency_count}")
        print(f"\n  • FOUND su EPCP3:          {stats['FOUND_EPCP3']} ({stats['FOUND_EPCP3']/len(df)*100:.1f}%)")
        print(f"  • NOT FOUND su EPCP3:      {stats['NOT_FOUND_EPCP3']} ({stats['NOT_FOUND_EPCP3']/len(df)*100:.1f}%)")
        print(f"\n  • ✅ OK per FASE 0:        {stats['OK_FASE0']} oggetti")
        print(f"  • 🚫 BLOCCO FASE 0:        {stats['BLOCCO_FASE0']} oggetti (cross-server)")
        print(f"\n📁 Report dettagliato: {OUTPUT_EXCEL}\n")
        
        # Raccomandazioni strategiche
        print("="*80)
        print("RACCOMANDAZIONI MIGRAZIONE FASE 0:")
        print("="*80)
        
        if stats['BLOCCO_FASE0'] > 0:
            pct_blocco = stats['BLOCCO_FASE0'] / stats['FOUND_EPCP3'] * 100 if stats['FOUND_EPCP3'] > 0 else 0
            print(f"\n🚫 {stats['BLOCCO_FASE0']} oggetti ({pct_blocco:.1f}%) hanno dipendenze CROSS-SERVER")
            print("   → Questi BLOCCANO la migrazione FASE 0 (singolo server)")
            print("   → Opzioni:")
            print("     1. Escludere dalla fase 0, migrare in fase successiva")
            print("     2. Refactoring per eliminare linked server")
            print("     3. Valutare se necessario includere altri server")
            print("   → Vedi sheet 'BLOCCO_FASE0' per dettagli")
        
        if stats['OK_FASE0'] > 0:
            pct_ok = stats['OK_FASE0'] / stats['FOUND_EPCP3'] * 100 if stats['FOUND_EPCP3'] > 0 else 0
            print(f"\n✅ {stats['OK_FASE0']} oggetti ({pct_ok:.1f}%) sono OK per FASE 0")
            print("   → Nessuna dipendenza cross-server rilevata")
            print("   → Possono essere migrati su singolo server")
            print("   → Vedi sheet 'OK_FASE0' per lista completa")
        
        if stats['NOT_FOUND_EPCP3'] > 0:
            print(f"\n⚠️  {stats['NOT_FOUND_EPCP3']} oggetti NON TROVATI su EPCP3")
            print("   → Possibili cause:")
            print("     1. Oggetti eliminati dopo estrazione iniziale")
            print("     2. Oggetti su altro server (non EPCP3)")
            print("     3. Errori nei nomi (case sensitivity, typo)")
            print("   → Vedi sheet 'NOT_FOUND_EPCP3' per analisi")
        
        print("\n" + "="*80)
        print("PROSSIMI PASSI:")
        print("="*80)
        print("\n1. Analizza sheet 'OK_FASE0' → Oggetti pronti per migrazione")
        print("2. Analizza sheet 'BLOCCO_FASE0' → Decide strategia per cross-server")
        print("3. Usa sheet per Livello (L1_EPCP3, L2_EPCP3, etc.) per pianificazione")
        print("4. Verifica sheet 'By_Database_EPCP3' per scope database fase 0")
        print("\n" + "="*80 + "\n")

if __name__ == "__main__":
    main()
