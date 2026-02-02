"""
Script per analisi LINEAGE completa degli oggetti SQL.

Per ogni oggetto:
  - Estrae tutte le dipendenze (tabelle, viste, SP, funzioni di cui dipende)
  - Costruisce albero di dipendenze completo
  - Garantisce che migrazione include TUTTE le dipendenze necessarie
  - Identifica oggetti "root" vs "leaf" (chi dipende da chi)

Input: 
  - Excel con validazione EPCP3 (qualsiasi sheet)

Output:
  - Excel con lineage completo per ogni oggetto
  - Grafo di dipendenze navigabile
  - Lista di migrazione ordinata per dependencies

Utilizza sys.sql_expression_dependencies per analisi completa.
"""

import pandas as pd
import pyodbc
import re
from pathlib import Path
from collections import defaultdict, deque
from datetime import datetime
import json

# =========================
# CONFIGURAZIONE
# =========================
BASE_PATH = Path(r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\Report finali consolidati (Conn v. + viste)")
INPUT_VALIDATION_EXCEL = BASE_PATH / "ALL_OBJECTS_VALIDATO_EPCP3_FASE0.xlsx"
OUTPUT_LINEAGE_EXCEL = BASE_PATH / "LINEAGE_COMPLETE_FASE0.xlsx"

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

def get_object_dependencies_from_database(database, object_name, schema_name='dbo'):
    """
    Estrae dipendenze di un oggetto usando sys.sql_expression_dependencies.
    Ritorna lista di tuple: (referenced_database, referenced_schema, referenced_entity, referenced_type)
    """
    try:
        conn = pyodbc.connect(get_connection_string(database), timeout=10)
        cursor = conn.cursor()
        
        # Query per estrarre dipendenze
        query = """
        SELECT DISTINCT
            ISNULL(d.referenced_database_name, DB_NAME()) AS Referenced_Database,
            ISNULL(d.referenced_schema_name, 'dbo') AS Referenced_Schema,
            d.referenced_entity_name AS Referenced_Object,
            o2.type_desc AS Referenced_Type
        FROM sys.sql_expression_dependencies d
        INNER JOIN sys.objects o ON d.referencing_id = o.object_id
        LEFT JOIN sys.objects o2 ON 
            d.referenced_id = o2.object_id AND 
            d.referenced_database_name IS NULL
        WHERE o.name = ?
            AND SCHEMA_NAME(o.schema_id) = ?
            AND d.referenced_entity_name IS NOT NULL
        ORDER BY Referenced_Database, Referenced_Schema, Referenced_Object
        """
        
        cursor.execute(query, (object_name, schema_name))
        rows = cursor.fetchall()
        
        dependencies = [(row[0], row[1], row[2], row[3]) for row in rows]
        
        cursor.close()
        conn.close()
        return dependencies
    except Exception as e:
        return []

def get_reverse_dependencies_from_database(database, object_name, schema_name='dbo'):
    """
    Estrae oggetti che dipendono dall'oggetto specificato (reverse dependencies).
    Ritorna lista di tuple: (referencing_database, referencing_schema, referencing_entity, referencing_type)
    """
    try:
        conn = pyodbc.connect(get_connection_string(database), timeout=10)
        cursor = conn.cursor()
        
        # Query per reverse dependencies
        query = """
        SELECT DISTINCT
            DB_NAME() AS Referencing_Database,
            SCHEMA_NAME(o.schema_id) AS Referencing_Schema,
            o.name AS Referencing_Object,
            o.type_desc AS Referencing_Type
        FROM sys.sql_expression_dependencies d
        INNER JOIN sys.objects o ON d.referencing_id = o.object_id
        INNER JOIN sys.objects o2 ON d.referenced_id = o2.object_id
        WHERE o2.name = ?
            AND SCHEMA_NAME(o2.schema_id) = ?
        ORDER BY Referencing_Database, Referencing_Schema, Referencing_Object
        """
        
        cursor.execute(query, (object_name, schema_name))
        rows = cursor.fetchall()
        
        reverse_deps = [(row[0], row[1], row[2], row[3]) for row in rows]
        
        cursor.close()
        conn.close()
        return reverse_deps
    except Exception as e:
        return []

def build_dependency_tree(database, object_name, schema_name, max_depth=10):
    """
    Costruisce albero completo di dipendenze (recursive).
    Ritorna dizionario con:
      - direct_dependencies: lista dipendenze dirette
      - all_dependencies: set di tutte le dipendenze (transitive)
      - dependency_levels: dict {dependency: level} per visualizzazione gerarchica
    """
    visited = set()
    all_deps = set()
    level_map = {}
    
    def traverse(db, obj, sch, current_level):
        if current_level > max_depth:
            return
        
        key = (db.lower(), sch.lower(), obj.lower())
        if key in visited:
            return
        visited.add(key)
        
        deps = get_object_dependencies_from_database(db, obj, sch)
        
        for dep_db, dep_sch, dep_obj, dep_type in deps:
            dep_key = (dep_db.lower(), dep_sch.lower(), dep_obj.lower())
            all_deps.add((dep_db, dep_sch, dep_obj, dep_type))
            
            if dep_key not in level_map:
                level_map[dep_key] = current_level + 1
            
            # Ricorsione
            traverse(dep_db, dep_obj, dep_sch, current_level + 1)
    
    # Start traversal
    direct_deps = get_object_dependencies_from_database(database, object_name, schema_name)
    traverse(database, object_name, schema_name, 0)
    
    return {
        'direct_dependencies': direct_deps,
        'all_dependencies': all_deps,
        'dependency_levels': level_map,
        'total_depth': max(level_map.values()) if level_map else 0
    }

# =========================
# ANALISI LINEAGE
# =========================

def analyze_lineage_for_sheet(df_sheet, sheet_name):
    """
    Analizza lineage completo per tutti gli oggetti in uno sheet.
    Aggiunge colonne:
      - Direct_Dependencies_Count: numero dipendenze dirette
      - Direct_Dependencies_List: lista dipendenze dirette
      - All_Dependencies_Count: numero totale dipendenze (transitive)
      - All_Dependencies_List: lista completa dipendenze
      - Max_Dependency_Depth: profondità massima albero dipendenze
      - Reverse_Dependencies_Count: quanti oggetti dipendono da questo
      - Reverse_Dependencies_List: lista oggetti che dipendono
      - Is_Root_Object: TRUE se non ha dipendenze (root)
      - Is_Leaf_Object: TRUE se nessuno dipende da questo (leaf)
    """
    print(f"\n📊 Analisi lineage per sheet: {sheet_name}")
    print(f"   Oggetti da analizzare: {len(df_sheet)}")
    
    results = {
        'direct_deps_count': [],
        'direct_deps_list': [],
        'all_deps_count': [],
        'all_deps_list': [],
        'max_depth': [],
        'reverse_deps_count': [],
        'reverse_deps_list': [],
        'is_root': [],
        'is_leaf': []
    }
    
    for idx, row in df_sheet.iterrows():
        if (idx + 1) % 10 == 0:
            print(f"     {idx + 1}/{len(df_sheet)}...", flush=True)
        
        object_name = str(row.get('ObjectName', '')).strip() if pd.notna(row.get('ObjectName')) else ''
        
        # Gestione database: priorità EPCP3_Databases, poi Database, poi default
        database = ''
        if pd.notna(row.get('EPCP3_Databases')):
            databases_str = str(row.get('EPCP3_Databases', '')).strip()
            if databases_str and databases_str != '':
                databases_list = databases_str.split(';')
                if databases_list and databases_list[0].strip():
                    database = databases_list[0].strip()
        
        if not database and pd.notna(row.get('Database')):
            database = str(row.get('Database', '')).strip()
        
        if not database or database == 'None':
            database = 'master'
        
        schema = str(row.get('EPCP3_Schema', 'dbo')).strip() if pd.notna(row.get('EPCP3_Schema')) else 'dbo'
        
        # Solo oggetti trovati su EPCP3
        if not row.get('EPCP3_Found', False):
            results['direct_deps_count'].append(0)
            results['direct_deps_list'].append('')
            results['all_deps_count'].append(0)
            results['all_deps_list'].append('')
            results['max_depth'].append(0)
            results['reverse_deps_count'].append(0)
            results['reverse_deps_list'].append('')
            results['is_root'].append(False)
            results['is_leaf'].append(False)
            continue
        
        # Build dependency tree (max_depth=2 per performance)
        try:
            tree = build_dependency_tree(database, object_name, schema, max_depth=2)
            
            direct_deps = tree['direct_dependencies']
            all_deps = tree['all_dependencies']
            
            # Reverse dependencies
            reverse_deps = get_reverse_dependencies_from_database(database, object_name, schema)
            
            # Format lists
            direct_deps_str = '; '.join([f"{d[0]}.{d[1]}.{d[2]} ({d[3]})" for d in direct_deps])
            all_deps_str = '; '.join([f"{d[0]}.{d[1]}.{d[2]} ({d[3]})" for d in all_deps])
            reverse_deps_str = '; '.join([f"{r[0]}.{r[1]}.{r[2]} ({r[3]})" for r in reverse_deps])
            
            results['direct_deps_count'].append(len(direct_deps))
            results['direct_deps_list'].append(direct_deps_str)
            results['all_deps_count'].append(len(all_deps))
            results['all_deps_list'].append(all_deps_str)
            results['max_depth'].append(tree['total_depth'])
            results['reverse_deps_count'].append(len(reverse_deps))
            results['reverse_deps_list'].append(reverse_deps_str)
            results['is_root'].append(len(direct_deps) == 0)
            results['is_leaf'].append(len(reverse_deps) == 0)
        except Exception as e:
            print(f"     ⚠️  Error on {object_name}: {str(e)[:80]}", flush=True)
            results['direct_deps_count'].append(0)
            results['direct_deps_list'].append(f"ERROR: {str(e)[:100]}")
            results['all_deps_count'].append(0)
            results['all_deps_list'].append('')
            results['max_depth'].append(0)
            results['reverse_deps_count'].append(0)
            results['reverse_deps_list'].append('')
            results['is_root'].append(False)
            results['is_leaf'].append(False)
    
    # Aggiungi colonne
    df_sheet['Direct_Dependencies_Count'] = results['direct_deps_count']
    df_sheet['Direct_Dependencies_List'] = results['direct_deps_list']
    df_sheet['All_Dependencies_Count'] = results['all_deps_count']
    df_sheet['All_Dependencies_List'] = results['all_deps_list']
    df_sheet['Max_Dependency_Depth'] = results['max_depth']
    df_sheet['Reverse_Dependencies_Count'] = results['reverse_deps_count']
    df_sheet['Reverse_Dependencies_List'] = results['reverse_deps_list']
    df_sheet['Is_Root_Object'] = results['is_root']
    df_sheet['Is_Leaf_Object'] = results['is_leaf']
    
    print(f"   ✓ Analisi completata!")
    return df_sheet

# =========================
# ANALISI MIGRATION ORDER
# =========================

def calculate_migration_order(df):
    """
    Calcola ordine di migrazione ottimale basato su dependency depth.
    Oggetti root (depth=0) vanno migrati per primi.
    """
    print("\n🔄 Calcolo ordine migrazione...")
    
    df['Migration_Priority'] = 0
    
    # Root objects: max priority
    df.loc[df['Is_Root_Object'] == True, 'Migration_Priority'] = 1000
    
    # By depth (inverted: shallow = high priority)
    max_depth = df['Max_Dependency_Depth'].max()
    if max_depth > 0:
        df['Migration_Priority'] += (max_depth - df['Max_Dependency_Depth']) * 100
    
    # By reverse dependencies (more dependents = higher priority)
    df['Migration_Priority'] += df['Reverse_Dependencies_Count'] * 10
    
    # Sort by priority
    df = df.sort_values('Migration_Priority', ascending=False)
    df['Migration_Order'] = range(1, len(df) + 1)
    
    print(f"   ✓ Ordine calcolato per {len(df)} oggetti")
    return df

# =========================
# GENERAZIONE EXCEL
# =========================

def generate_lineage_excel(sheets_dict, output_path):
    """Genera Excel con lineage completo per tutti gli sheet."""
    print(f"\n📊 Generazione Excel lineage...")
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for sheet_name, df_sheet in sheets_dict.items():
            df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"   ✓ Sheet: {sheet_name} ({len(df_sheet)} righe)")
        
        # Summary sheet
        summary_data = []
        for sheet_name, df_sheet in sheets_dict.items():
            if len(df_sheet) == 0:
                continue
            summary_data.append({
                'Sheet': sheet_name,
                'Total_Objects': len(df_sheet),
                'Root_Objects': len(df_sheet[df_sheet['Is_Root_Object'] == True]),
                'Leaf_Objects': len(df_sheet[df_sheet['Is_Leaf_Object'] == True]),
                'Avg_Dependencies': df_sheet['All_Dependencies_Count'].mean(),
                'Max_Depth': df_sheet['Max_Dependency_Depth'].max()
            })
        
        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Lineage_Summary', index=False)
    
    print(f"✅ Excel lineage generato: {output_path}")

# =========================
# MAIN
# =========================

def main():
    print("="*80)
    print("LINEAGE TRACKER - ANALISI DIPENDENZE COMPLETE")
    print("="*80)
    
    # Leggi file validazione
    excel_file = pd.ExcelFile(INPUT_VALIDATION_EXCEL)
    print(f"✓ File validazione caricato: {INPUT_VALIDATION_EXCEL.name}")
    print(f"   Sheet disponibili: {', '.join([str(s) for s in excel_file.sheet_names])}")
    
    # Seleziona sheet da analizzare (solo prioritari - evita OK_FASE0 con 6939 oggetti)
    sheets_to_analyze = [
        'CORE_FASE0_Priority',  # Oggetti prioritari (~500 oggetti)
        'BLOCCO_FASE0',         # Oggetti con cross-server deps (~384 oggetti)
        'L1_EPCP3'              # Solo L1 (tabelle base - ~2014 oggetti)
    ]
    
    sheets_dict = {}
    
    for sheet_name in sheets_to_analyze:
        if sheet_name not in excel_file.sheet_names:
            print(f"⚠️  Sheet '{sheet_name}' non trovato, skip...")
            continue
        
        df_sheet = pd.read_excel(INPUT_VALIDATION_EXCEL, sheet_name=sheet_name)
        print(f"\n🔍 Processing: {sheet_name} ({len(df_sheet)} oggetti)")
        
        # Analizza lineage
        df_analyzed = analyze_lineage_for_sheet(df_sheet, sheet_name)
        
        # Calcola ordine migrazione
        df_ordered = calculate_migration_order(df_analyzed)
        
        sheets_dict[f"{sheet_name}_Lineage"] = df_ordered
    
    # Genera Excel output
    generate_lineage_excel(sheets_dict, OUTPUT_LINEAGE_EXCEL)
    
    print(f"\n{'='*80}")
    print("LINEAGE ANALYSIS COMPLETATA!")
    print(f"📁 Output: {OUTPUT_LINEAGE_EXCEL}")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
