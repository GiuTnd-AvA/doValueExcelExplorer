"""
Script ottimizzato per validare oggetti su EPCP3 e analizzare dipendenze per migrazione fase 0.

Input:
  - LINEAGE_HYBRID_REPORT_MERGED.xlsx (2675 oggetti critici con metadata)
  - oggetti_da_validare.xlsx (5896 oggetti estratti da SQL definitions)

Output:
  - Excel con validazione EPCP3 completa
  - Breakdown per tipo oggetto (SP, VIEW, TABLE, etc.)
  - Prioritizzazione fase 0 stratificata
  - Analisi cross-server dependencies

Ottimizzazioni:
  - Caricamento catalogo completo (53 query invece di ~450k)
  - Rimozione duplicati intelligente
  - Pulizia colonne automatica
"""

import pandas as pd
import pyodbc
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# =========================
# CONFIGURAZIONE
# =========================
BASE_PATH = Path(r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\Report finali consolidati (Conn v. + viste)")
INPUT_MERGED_EXCEL = BASE_PATH / "LINEAGE_HYBRID_REPORT_MERGED.xlsx"
INPUT_DEPENDENCIES_EXCEL = BASE_PATH / "5986 Objects da validare in EPCP3" / "oggetti_da_validare.xlsx"
OUTPUT_EXCEL = BASE_PATH / "ALL_OBJECTS_VALIDATO_EPCP3_FASE0.xlsx"

SQL_SERVER = 'EPCP3'
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
# FUNZIONI SQL
# =========================

def get_connection_string(database):
    return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={database};Trusted_Connection=yes;"

def get_all_objects_from_database(database):
    """Estrae TUTTI gli oggetti da un database (ottimizzato)."""
    try:
        conn = pyodbc.connect(get_connection_string(database), timeout=30)
        cursor = conn.cursor()
        
        query = """
        SELECT o.name, o.type_desc, SCHEMA_NAME(o.schema_id), OBJECT_DEFINITION(o.object_id)
        FROM sys.objects o
        WHERE o.type IN ('P', 'FN', 'IF', 'TF', 'TR', 'V', 'U') AND o.is_ms_shipped = 0
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        objects_dict = {row[0].lower(): (row[0], row[1], row[2], row[3] or '') for row in rows}
        
        cursor.close()
        conn.close()
        return objects_dict
    except:
        return {}

def extract_cross_server_dependencies(sql_def):
    """Rileva dipendenze cross-server (linked servers, OPENQUERY, etc.)."""
    if not sql_def or pd.isna(sql_def):
        return set()
    
    sql_def = str(sql_def)
    dependencies = set()
    
    # Pattern 4-part name: [server].[database].[schema].[object]
    four_part = r'\[?(\w+)\]?\.\[?(\w+)\]?\.\[?(\w+)\]?\.\[?(\w+)\]?'
    for match in re.finditer(four_part, sql_def):
        server = match.group(1)
        if server.lower() not in ['sys', 'dbo', 'information_schema']:
            dependencies.add(f"LINKED:{server}")
    
    # OPENQUERY/OPENDATASOURCE/OPENROWSET
    if 'openquery' in sql_def.lower():
        dependencies.add("OPENQUERY")
    if 'opendatasource' in sql_def.lower():
        dependencies.add("OPENDATASOURCE")
    if 'openrowset' in sql_def.lower():
        dependencies.add("OPENROWSET")
    
    return dependencies

# =========================
# VALIDAZIONE
# =========================

def validate_objects(df):
    """Valida oggetti su EPCP3 con analisi cross-server."""
    print("\n🚀 Caricamento catalogo EPCP3...")
    import sys
    sys.stdout.flush()
    
    database_catalogs = {}
    for db_num, database in enumerate(EPCP3_DATABASES, start=1):
        print(f"  [{db_num}/{len(EPCP3_DATABASES)}] {database}...", end=' ', flush=True)
        objects_dict = get_all_objects_from_database(database)
        database_catalogs[database] = objects_dict
        print(f"✓ {len(objects_dict)}", flush=True)
    
    print(f"\n✓ Catalogo caricato!\n🔍 Validazione {len(df)} oggetti...")
    
    # Liste risultati
    results = {
        'databases': [], 'found': [], 'types': [], 'schemas': [], 
        'sql_defs': [], 'cross_server_deps': [], 'cross_server_count': [],
        'has_cross_server': [], 'blocco_fase0': []
    }
    
    stats = defaultdict(int)
    
    for idx, row in df.iterrows():
        if (idx + 1) % 1000 == 0:
            print(f"  {idx + 1}/{len(df)}...", flush=True)
        
        object_name = str(row.get('ObjectName', '')).strip() if pd.notna(row.get('ObjectName')) else ''
        
        if not object_name:
            for key in results:
                results[key].append(None if key in ['databases', 'types', 'schemas', 'sql_defs', 'cross_server_deps'] else (False if key in ['found', 'has_cross_server'] else (0 if key == 'cross_server_count' else 'N/A')))
            stats['MISSING_DATA'] += 1
            continue
        
        # Lookup in memoria
        found_on_epcp3 = False
        found_in_dbs = []
        sql_def = None
        obj_type = None
        schema = None
        
        object_name_lower = object_name.lower()
        for database, objects_dict in database_catalogs.items():
            if object_name_lower in objects_dict:
                found_on_epcp3 = True
                found_in_dbs.append(database)
                if sql_def is None:
                    _, obj_type, schema, sql_def = objects_dict[object_name_lower]
        
        if found_on_epcp3:
            stats['FOUND_EPCP3'] += 1
            cross_server_deps = extract_cross_server_dependencies(sql_def)
            cross_count = len(cross_server_deps)
            has_cross = cross_count > 0
            
            results['databases'].append('; '.join(found_in_dbs))
            results['found'].append(True)
            results['types'].append(obj_type)
            results['schemas'].append(schema)
            results['sql_defs'].append(sql_def)
            results['cross_server_deps'].append('; '.join(cross_server_deps) if cross_server_deps else '')
            results['cross_server_count'].append(cross_count)
            results['has_cross_server'].append(has_cross)
            results['blocco_fase0'].append('SÌ' if has_cross else 'NO')
            
            if has_cross:
                stats['BLOCCO_FASE0'] += 1
            else:
                stats['OK_FASE0'] += 1
        else:
            stats['NOT_FOUND_EPCP3'] += 1
            results['databases'].append(None)
            results['found'].append(False)
            results['types'].append(None)
            results['schemas'].append(None)
            results['sql_defs'].append(None)
            results['cross_server_deps'].append('')
            results['cross_server_count'].append(0)
            results['has_cross_server'].append(False)
            results['blocco_fase0'].append('N/A')
    
    # Aggiungi colonne
    df['EPCP3_Found'] = results['found']
    df['EPCP3_Databases'] = results['databases']
    df['EPCP3_ObjectType'] = results['types']
    df['EPCP3_Schema'] = results['schemas']
    df['EPCP3_SQLDefinition'] = results['sql_defs']
    df['Cross_Server_Dependencies'] = results['cross_server_deps']
    df['Cross_Server_Count'] = results['cross_server_count']
    df['Has_Cross_Server'] = results['has_cross_server']
    df['Blocco_Fase0'] = results['blocco_fase0']
    
    # Pulizia colonne
    cols_to_remove = [col for col in df.columns if col.startswith('Unnamed:') or df[col].isna().all() or 
                     (col in ['SERVER', 'DATABASE', 'SCHEMA', 'OGGETTO'] and col not in ['Database', 'Schema', 'ObjectName'])]
    if cols_to_remove:
        df = df.drop(columns=cols_to_remove)
    
    print(f"✓ Validazione completata: {stats['FOUND_EPCP3']} trovati, {stats['NOT_FOUND_EPCP3']} non trovati")
    return df, stats

# =========================
# GENERAZIONE EXCEL
# =========================

def generate_excel(df, stats, output_path):
    """Genera Excel con analisi stratificata."""
    print(f"\n📊 Generazione Excel...")
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Sheet 1: ALL OBJECTS
        df.to_excel(writer, sheet_name='All_Objects_Validated', index=False)
        
        # Sheet 2: FOUND
        found_df = df[df['EPCP3_Found'] == True].copy()
        if len(found_df) > 0:
            found_df.to_excel(writer, sheet_name='Found_on_EPCP3', index=False)
        
        # Sheet 3-4: FASE 0
        ok_df = df[(df['EPCP3_Found'] == True) & (df['Blocco_Fase0'] == 'NO')].copy()
        if len(ok_df) > 0:
            ok_df.to_excel(writer, sheet_name='OK_FASE0', index=False)
        
        blocco_df = df[df['Blocco_Fase0'] == 'SÌ'].copy()
        if len(blocco_df) > 0:
            blocco_df.to_excel(writer, sheet_name='BLOCCO_FASE0', index=False)
        
        # Sheet 5: NOT FOUND
        not_found_df = df[df['EPCP3_Found'] == False].copy()
        if len(not_found_df) > 0:
            not_found_df.to_excel(writer, sheet_name='NOT_FOUND_EPCP3', index=False)
        
        # Sheet 6-9: Per Livello (ALL + EPCP3)
        if 'Livello' in df.columns:
            for level in ['L1', 'L2', 'L3', 'L4']:
                level_df = df[df['Livello'] == level].copy()
                if len(level_df) > 0:
                    level_df.to_excel(writer, sheet_name=f'{level}_ALL', index=False)
                    level_found = level_df[level_df['EPCP3_Found'] == True].copy()
                    if len(level_found) > 0:
                        level_found.to_excel(writer, sheet_name=f'{level}_EPCP3', index=False)
        
        # Sheet 10-11: New Objects
        new_df = df[(df['Livello'].isna()) | (df['Livello'] == '')].copy()
        if len(new_df) > 0:
            new_df.to_excel(writer, sheet_name='New_Objects_ALL', index=False)
            new_found = new_df[new_df['EPCP3_Found'] == True].copy()
            if len(new_found) > 0:
                new_found.to_excel(writer, sheet_name='New_Objects_EPCP3', index=False)
        
        # Sheet 12: BREAKDOWN PER TIPO
        if len(found_df) > 0 and 'EPCP3_ObjectType' in found_df.columns:
            type_breakdown = found_df.groupby(['EPCP3_ObjectType', 'Blocco_Fase0']).size().reset_index(name='Count')
            type_breakdown.to_excel(writer, sheet_name='Breakdown_Per_Tipo', index=False)
        
        # Sheet 13: PRIORITIZZAZIONE FASE 0 (L1 + Alta criticità)
        if len(ok_df) > 0:
            # Core: L1 + ReferenceCount alto + Criticità Alta
            if 'Livello' in ok_df.columns:
                if 'ReferenceCount' in ok_df.columns:
                    core_criteria = (ok_df['Livello'] == 'L1') | (ok_df['ReferenceCount'] >= 50)
                else:
                    core_criteria = (ok_df['Livello'] == 'L1')
                core_df = ok_df[core_criteria].copy()
                if len(core_df) > 0:
                    core_df.to_excel(writer, sheet_name='CORE_FASE0_Priority', index=False)
        
        # Sheet 14: SUMMARY
        summary_data = {
            'Categoria': [
                'Totale oggetti',
                '', 'FOUND su EPCP3', 'NOT FOUND su EPCP3',
                '', 'OK FASE 0 (no cross-server)', 'BLOCCO FASE 0 (cross-server)',
                '', 'Per Livello (ALL):', 'L1', 'L2', 'L3', 'L4', 'New Objects'
            ],
            'Conteggio': [
                len(df),
                '', stats['FOUND_EPCP3'], stats['NOT_FOUND_EPCP3'],
                '', stats['OK_FASE0'], stats['BLOCCO_FASE0'],
                '',
                '',
                len(df[df['Livello'] == 'L1']) if 'Livello' in df.columns else 0,
                len(df[df['Livello'] == 'L2']) if 'Livello' in df.columns else 0,
                len(df[df['Livello'] == 'L3']) if 'Livello' in df.columns else 0,
                len(df[df['Livello'] == 'L4']) if 'Livello' in df.columns else 0,
                len(new_df)
            ],
            'Percentuale': [
                '100%',
                '', f"{stats['FOUND_EPCP3']/len(df)*100:.1f}%", f"{stats['NOT_FOUND_EPCP3']/len(df)*100:.1f}%",
                '', f"{stats['OK_FASE0']/stats['FOUND_EPCP3']*100:.1f}%" if stats['FOUND_EPCP3'] > 0 else '0%',
                f"{stats['BLOCCO_FASE0']/stats['FOUND_EPCP3']*100:.1f}%" if stats['FOUND_EPCP3'] > 0 else '0%',
                '', '', '', '', '', '', ''
            ]
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary_Fase0', index=False)
    
    print(f"✅ Excel generato: {output_path}")

# =========================
# MAIN
# =========================

def main():
    print("="*80)
    print("VALIDAZIONE OGGETTI SU EPCP3 - FASE 0")
    print("="*80)
    
    # Leggi file MERGED
    try:
        df_merged = pd.read_excel(INPUT_MERGED_EXCEL, sheet_name='All Objects')
        df_merged['Origine'] = 'MERGED_CRITICAL'
        print(f"✓ FILE 1: {len(df_merged)} oggetti critici")
    except:
        df_merged = pd.read_excel(INPUT_MERGED_EXCEL)
        df_merged['Origine'] = 'MERGED_CRITICAL'
        print(f"✓ FILE 1: {len(df_merged)} oggetti")
    
    # Leggi file DEPENDENCIES
    df_dependencies = pd.read_excel(INPUT_DEPENDENCIES_EXCEL)
    df_dependencies = df_dependencies.rename(columns={'SERVER': 'Server_Origin', 'DATABASE': 'Database', 'SCHEMA': 'Schema', 'OGGETTO': 'ObjectName'})
    df_dependencies['Origine'] = 'DISCOVERED_DEPENDENCY'
    print(f"✓ FILE 2: {len(df_dependencies)} oggetti")
    
    # Allinea colonne e unisci
    all_columns = list(df_merged.columns)
    for col in df_dependencies.columns:
        if col not in all_columns:
            all_columns.append(col)
            df_merged[col] = None
    for col in all_columns:
        if col not in df_dependencies.columns:
            df_dependencies[col] = None
    
    df_merged = df_merged[all_columns]
    df_dependencies = df_dependencies[all_columns]
    df = pd.concat([df_merged, df_dependencies], ignore_index=True)
    
    # Rimuovi duplicati
    merged_objects = set(df_merged['ObjectName'].str.lower())
    df_dependencies_unique = df_dependencies[~df_dependencies['ObjectName'].str.lower().isin(merged_objects)]
    df = pd.concat([df_merged, df_dependencies_unique], ignore_index=True)
    
    print(f"✓ Dataset: {len(df)} oggetti ({len(df_merged)} MERGED + {len(df_dependencies_unique)} nuovi)")
    
    # Valida
    df_validated, stats = validate_objects(df)
    
    # Genera Excel
    generate_excel(df_validated, stats, OUTPUT_EXCEL)
    
    print(f"\n{'='*80}")
    print("RISULTATI:")
    print(f"  • Trovati su EPCP3: {stats['FOUND_EPCP3']}/{len(df)} ({stats['FOUND_EPCP3']/len(df)*100:.1f}%)")
    print(f"  • OK FASE 0: {stats['OK_FASE0']} ({stats['OK_FASE0']/stats['FOUND_EPCP3']*100:.1f}%)")
    print(f"  • BLOCCO FASE 0: {stats['BLOCCO_FASE0']} ({stats['BLOCCO_FASE0']/stats['FOUND_EPCP3']*100:.1f}%)")
    print(f"\n📁 {OUTPUT_EXCEL}\n{'='*80}")

if __name__ == "__main__":
    main()
