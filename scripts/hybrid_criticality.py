# =========================
# IMPORT
# =========================
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# =========================
# CONFIG
# =========================
SUMMARY_PATH = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\SUMMARY_REPORT.xlsx'
TOP_REF_PATH = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\TOP_REFERENCED_ANALYSIS.xlsx'
OUTPUT_PATH = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\SUMMARY_REPORT_HYBRID.xlsx'

# SQL Server
SERVER = 'EPCP3'
DRIVER = 'ODBC+Driver+17+for+SQL+Server'
TOP_N = 1500

DATABASES = [
    'ANALISI', 'AMS', 'BASEDATI_BI', 'CORESQL7', 'DWH',
    'EPC_BI', 'GESTITO', 'MORTGAGE', 'S1057', 'S1057B',
    'S1259', 'STAGING', 'STG', 'UTIL'
]

# =========================
# SOGLIE CRITICITÀ IBRIDA
# =========================
# Un oggetto è critico se:
#   1. Ha operazioni DML/DDL (logica originale)
#   2. OPPURE ha ReferenceCount >= soglia (nuova logica)

REFERENCE_COUNT_THRESHOLD = 50  # Oggetti con 50+ riferimenti = CRITICI per dipendenze

# =========================
# FUNZIONI
# =========================

def get_engine(server, db_name, driver):
    """Crea engine SQLAlchemy."""
    conn_str = f"mssql+pyodbc://@{server}/{db_name}?driver={driver}&trusted_connection=yes"
    return create_engine(conn_str, pool_pre_ping=True)

def get_reference_counts_for_database(server, database, driver):
    """
    Ottiene ReferenceCount per tutti gli oggetti nel database.
    Ritorna DataFrame con Schema, ObjectName, ObjectType, ReferenceCount.
    """
    print(f"  Query {database}...")
    
    try:
        engine = get_engine(server, database, driver)
        
        # Query per contare TUTTI i riferimenti (non solo TOP N)
        query = """
        SELECT 
            OBJECT_SCHEMA_NAME(sed.referenced_id) AS SchemaName,
            OBJECT_NAME(sed.referenced_id) AS ObjectName,
            o.type_desc AS ObjectType,
            COUNT(DISTINCT sed.referencing_id) AS ReferenceCount
        FROM sys.sql_expression_dependencies sed
        INNER JOIN sys.objects o ON sed.referenced_id = o.object_id
        WHERE o.type IN ('P', 'FN', 'IF', 'TF', 'TR', 'V')
            AND o.is_ms_shipped = 0
        GROUP BY sed.referenced_id, o.type_desc
        """
        
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        
        df['Database'] = database
        print(f"    ✓ {len(df)} oggetti con riferimenti")
        return df
        
    except Exception as e:
        print(f"    ✗ Errore: {e}")
        return pd.DataFrame()

def get_all_reference_counts(server, databases, driver):
    """Ottiene ReferenceCount per tutti gli oggetti in tutti i database."""
    print("\n" + "="*80)
    print("QUERY SQL - REFERENCE COUNT per tutti gli oggetti")
    print("="*80 + "\n")
    
    all_refs = []
    
    for database in databases:
        df_refs = get_reference_counts_for_database(server, database, driver)
        if not df_refs.empty:
            all_refs.append(df_refs)
    
    if not all_refs:
        print("\n✗ Nessun reference count trovato")
        return pd.DataFrame()
    
    df_all = pd.concat(all_refs, ignore_index=True)
    
    # Normalizza
    df_all['Database'] = df_all['Database'].str.upper()
    df_all['SchemaName'] = df_all['SchemaName'].str.upper()
    df_all['ObjectName'] = df_all['ObjectName'].str.upper()
    
    print(f"\n✓ Totale: {len(df_all)} oggetti con riferimenti")
    print(f"  Range ReferenceCount: {df_all['ReferenceCount'].min():.0f} - {df_all['ReferenceCount'].max():.0f}")
    print(f"  Media ReferenceCount: {df_all['ReferenceCount'].mean():.1f}")
    
    # Oggetti sopra soglia
    high_refs = df_all[df_all['ReferenceCount'] >= REFERENCE_COUNT_THRESHOLD]
    print(f"\n  Oggetti con {REFERENCE_COUNT_THRESHOLD}+ riferimenti: {len(high_refs)}")
    
    return df_all

def load_summary_report(summary_path):
    """Carica SUMMARY_REPORT esistente."""
    print("\n" + "="*80)
    print("CARICAMENTO SUMMARY_REPORT ORIGINALE")
    print("="*80 + "\n")
    
    sheets = {}
    
    try:
        xl_file = pd.ExcelFile(summary_path)
        
        for sheet_name in xl_file.sheet_names:
            sheets[sheet_name] = pd.read_excel(summary_path, sheet_name=sheet_name)
            
            # Normalizza Database
            if 'Database' in sheets[sheet_name].columns:
                sheets[sheet_name]['Database'] = sheets[sheet_name]['Database'].str.upper()
            
            # Normalizza Schema
            if 'Schema' in sheets[sheet_name].columns:
                sheets[sheet_name]['Schema'] = sheets[sheet_name]['Schema'].str.upper()
            
            # Normalizza ObjectName
            if 'ObjectName' in sheets[sheet_name].columns:
                sheets[sheet_name]['ObjectName'] = sheets[sheet_name]['ObjectName'].str.upper()
            
            print(f"✓ {sheet_name}: {len(sheets[sheet_name])} righe")
        
        return sheets
        
    except Exception as e:
        print(f"✗ Errore: {e}")
        return {}

def load_missing_high_ref_objects(top_ref_path, threshold):
    """
    Carica oggetti con ReferenceCount >= soglia NON presenti nel lineage originale.
    Questi verranno aggiunti a L1 come critici standalone.
    """
    print("\n" + "="*80)
    print(f"CARICAMENTO OGGETTI MANCANTI CON {threshold}+ REFERENCES")
    print("="*80 + "\n")
    
    try:
        xl_file = pd.ExcelFile(top_ref_path)
        print(f"Sheet disponibili: {xl_file.sheet_names}")
        
        # Prova diversi nomi di sheet
        sheet_candidates = ['DA_AGGIUNGERE_Critici_Deps', 'Top_Non_Critici', 'Top_Non_Critici_Analysis']
        
        df_missing = None
        sheet_used = None
        
        for sheet_name in sheet_candidates:
            if sheet_name in xl_file.sheet_names:
                df_missing = pd.read_excel(top_ref_path, sheet_name=sheet_name)
                sheet_used = sheet_name
                break
        
        if df_missing is None:
            print(f"✗ Nessuno sheet trovato tra: {sheet_candidates}")
            print(f"  Oggetti mancanti NON aggiunti (il file potrebbe non esistere ancora)")
            return pd.DataFrame()
        
        print(f"✓ Caricato sheet: {sheet_used} ({len(df_missing)} righe)")
        
        # Filtra solo oggetti con ReferenceCount >= soglia
        if 'ReferenceCount' in df_missing.columns:
            df_missing = df_missing[df_missing['ReferenceCount'] >= threshold].copy()
            print(f"✓ Filtrati {len(df_missing)} oggetti con {threshold}+ refs")
        elif 'Criticità_Dipendenze' in df_missing.columns:
            df_missing = df_missing[df_missing['Criticità_Dipendenze'] == f'CRITICA ({threshold}+)'].copy()
            print(f"✓ Filtrati {len(df_missing)} oggetti CRITICA ({threshold}+)")
        else:
            print(f"⚠ Colonne: {df_missing.columns.tolist()}")
            print(f"✗ Impossibile filtrare per criticità")
            return pd.DataFrame()
        
        if df_missing.empty:
            print(f"✓ Nessun oggetto mancante con {threshold}+ refs (già tutti inclusi)")
            return pd.DataFrame()
        
        # Normalizza
        if 'Database' in df_missing.columns:
            df_missing['Database'] = df_missing['Database'].str.upper()
        if 'Schema' not in df_missing.columns and 'SchemaName' in df_missing.columns:
            df_missing['Schema'] = df_missing['SchemaName']
        if 'Schema' in df_missing.columns:
            df_missing['Schema'] = df_missing['Schema'].str.upper()
        if 'ObjectName' in df_missing.columns:
            df_missing['ObjectName'] = df_missing['ObjectName'].str.upper()
        
        # Stats
        print(f"\n✓ TOTALE oggetti mancanti critici: {len(df_missing)}")
        
        if 'ObjectType' in df_missing.columns:
            print(f"\nPer tipo:")
            for tipo, count in df_missing['ObjectType'].value_counts().items():
                print(f"  • {tipo}: {count}")
        
        if 'Database' in df_missing.columns:
            print(f"\nPer database:")
            for db, count in df_missing['Database'].value_counts().head(5).items():
                print(f"  • {db}: {count}")
        
        return df_missing
        
    except Exception as e:
        print(f"✗ Errore: {e}")
        print(f"  Oggetti mancanti NON aggiunti")
        return pd.DataFrame()

def apply_hybrid_criticality(df_level, df_reference_counts, level_name):
    """
    Applica logica ibrida di criticità:
    - L1: Critico se ha DML/DDL OPPURE ReferenceCount >= soglia
    - L2-L4: TUTTI critici per definizione (dipendenze di L1) + aggiungi ReferenceCount
    """
    print(f"\n  Elaborazione {level_name}...")
    
    # Controlla se esiste colonna Critico_Migrazione
    has_criticality_column = 'Critico_Migrazione' in df_level.columns and df_level['Critico_Migrazione'].notna().any()
    
    # Backup colonne originali
    if has_criticality_column:
        df_level['Critico_Migrazione_Original'] = df_level['Critico_Migrazione']
    else:
        # L2-L4: tutti sono critici per definizione
        df_level['Critico_Migrazione_Original'] = 'SÌ'
    
    if 'Criticità_Tecnica' in df_level.columns:
        df_level['Criticità_Tecnica_Original'] = df_level['Criticità_Tecnica']
    else:
        df_level['Criticità_Tecnica_Original'] = ''
    
    # Gestisci colonna Schema (potrebbe non esistere o chiamarsi diversamente)
    if 'Schema' not in df_level.columns:
        if 'SchemaName' in df_level.columns:
            df_level['Schema'] = df_level['SchemaName']
        else:
            df_level['Schema'] = 'dbo'  # Default
    
    # Normalizza per merge (uppercase)
    df_level['Database_Merge'] = df_level['Database'].str.upper()
    df_level['Schema_Merge'] = df_level['Schema'].str.upper()
    df_level['ObjectName_Merge'] = df_level['ObjectName'].str.upper()
    
    # Merge con reference counts
    df_merged = df_level.merge(
        df_reference_counts[['Database', 'SchemaName', 'ObjectName', 'ReferenceCount']],
        left_on=['Database_Merge', 'Schema_Merge', 'ObjectName_Merge'],
        right_on=['Database', 'SchemaName', 'ObjectName'],
        how='left',
        suffixes=('', '_ref')
    )
    
    # Rimuovi colonne temporanee merge
    df_merged = df_merged.drop(columns=['Database_Merge', 'Schema_Merge', 'ObjectName_Merge'], errors='ignore')
    
    # Rimuovi colonne duplicate dal merge
    for col in ['Database_ref', 'SchemaName', 'ObjectName_ref']:
        if col in df_merged.columns:
            df_merged = df_merged.drop(columns=[col])
    
    # Riempie NaN in ReferenceCount con 0
    df_merged['ReferenceCount'] = df_merged['ReferenceCount'].fillna(0).astype(int)
    
    # Logica ibrida
    if has_criticality_column:
        # L1: logica completa DML/DDL OR Dipendenze
        was_critical_dml = df_merged['Critico_Migrazione_Original'] == 'SÌ'
        is_critical_deps = df_merged['ReferenceCount'] >= REFERENCE_COUNT_THRESHOLD
        
        # Applica OR logico
        df_merged['Critico_Migrazione'] = 'NO'
        df_merged.loc[was_critical_dml | is_critical_deps, 'Critico_Migrazione'] = 'SÌ'
        
        # Motivazione criticità
        df_merged['Motivo_Criticità'] = ''
        df_merged.loc[was_critical_dml & is_critical_deps, 'Motivo_Criticità'] = 'DML/DDL + Dipendenze'
        df_merged.loc[was_critical_dml & ~is_critical_deps, 'Motivo_Criticità'] = 'DML/DDL'
        df_merged.loc[~was_critical_dml & is_critical_deps, 'Motivo_Criticità'] = f'Dipendenze ({REFERENCE_COUNT_THRESHOLD}+ refs)'
        
        # Aggiorna Criticità_Tecnica per nuovi critici
        df_merged['Criticità_Tecnica'] = df_merged['Criticità_Tecnica_Original']
        newly_critical = (~was_critical_dml) & is_critical_deps
        df_merged.loc[newly_critical, 'Criticità_Tecnica'] = 'DIPENDENZE_CRITICHE'
        
        # Stats
        original_critical = df_merged['Critico_Migrazione_Original'].eq('SÌ').sum()
        new_critical = df_merged['Critico_Migrazione'].eq('SÌ').sum()
        added_critical = new_critical - original_critical
        
    else:
        # L2-L4: TUTTI critici per definizione (dipendenze di L1)
        df_merged['Critico_Migrazione'] = 'SÌ'
        
        # Motivazione basata solo su dipendenze se presenti
        df_merged['Motivo_Criticità'] = 'Dipendenza L1 (Bottom-Up)'
        high_deps = df_merged['ReferenceCount'] >= REFERENCE_COUNT_THRESHOLD
        df_merged.loc[high_deps, 'Motivo_Criticità'] = f'Dipendenza L1 + Dipendenze ({REFERENCE_COUNT_THRESHOLD}+ refs)'
        
        # Criticità tecnica
        df_merged['Criticità_Tecnica'] = 'DIPENDENZA_L1'
        df_merged.loc[high_deps, 'Criticità_Tecnica'] = 'DIPENDENZA_L1 + REFS_CRITICHE'
        
        # Stats
        original_critical = 0  # Non c'erano critici marcati originalmente
        new_critical = len(df_merged)  # Tutti sono critici ora
        added_critical = new_critical
    
    print(f"    Critici originali (DML/DDL):     {original_critical}")
    print(f"    Critici nuovi (Dipendenze):      {added_critical}")
    print(f"    Critici totali (Ibrido):         {new_critical}")
    
    return df_merged

def generate_hybrid_summary(sheets_original, df_reference_counts):
    """Genera nuovo SUMMARY_REPORT con logica ibrida."""
    print("\n" + "="*80)
    print("APPLICAZIONE LOGICA IBRIDA ai Livelli")
    print("="*80)
    
    sheets_hybrid = {}
    stats = {}
    
    for level in ['L1', 'L2', 'L3', 'L4']:
        if level in sheets_original:
            df_hybrid = apply_hybrid_criticality(
                sheets_original[level].copy(),
                df_reference_counts,
                level
            )
            sheets_hybrid[level] = df_hybrid
            
            stats[level] = {
                'total': len(df_hybrid),
                'critical_original': (df_hybrid['Critico_Migrazione_Original'] == 'SÌ').sum(),
                'critical_hybrid': (df_hybrid['Critico_Migrazione'] == 'SÌ').sum(),
                'added': (df_hybrid['Critico_Migrazione'] == 'SÌ').sum() - (df_hybrid['Critico_Migrazione_Original'] == 'SÌ').sum()
            }
    
    # Copia altri sheet invariati
    for sheet_name, df in sheets_original.items():
        if sheet_name not in ['L1', 'L2', 'L3', 'L4']:
            sheets_hybrid[sheet_name] = df
    
    return sheets_hybrid, stats

def export_hybrid_report(sheets_hybrid, stats, df_reference_counts):
    """Esporta nuovo SUMMARY_REPORT con logica ibrida."""
    print("\n" + "="*80)
    print("EXPORT SUMMARY_REPORT_HYBRID")
    print("="*80 + "\n")
    
    with pd.ExcelWriter(OUTPUT_PATH, engine='openpyxl') as writer:
        
        # Sheet Summary con statistiche ibrido
        summary_data = []
        summary_data.append({
            'Livello': 'GENERALE',
            'Metrica': 'Criterio Criticità',
            'Valore': f'IBRIDO: (L1) DML/DDL OR ReferenceCount >= {REFERENCE_COUNT_THRESHOLD} | (L2-L4) Tutti critici (dipendenze L1)'
        })
        summary_data.append({
            'Livello': 'GENERALE',
            'Metrica': 'Data Generazione',
            'Valore': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        summary_data.append({'Livello': '', 'Metrica': '', 'Valore': ''})
        
        for level in ['L1', 'L2', 'L3', 'L4']:
            if level in stats:
                summary_data.append({
                    'Livello': level,
                    'Metrica': 'Totale oggetti',
                    'Valore': stats[level]['total']
                })
                summary_data.append({
                    'Livello': level,
                    'Metrica': 'Critici totali (ibrido)',
                    'Valore': stats[level]['critical_hybrid']
                })
                summary_data.append({
                    'Livello': level,
                    'Metrica': 'Critici originali',
                    'Valore': stats[level]['critical_original']
                })
                summary_data.append({
                    'Livello': level,
                    'Metrica': 'Aggiunti (dipendenze)',
                    'Valore': stats[level]['added']
                })
                summary_data.append({'Livello': '', 'Metrica': '', 'Valore': ''})
        
        df_summary = pd.DataFrame(summary_data)
        df_summary.to_excel(writer, sheet_name='Summary_Hybrid', index=False)
        print("✓ Sheet: Summary_Hybrid")
        
        # Sheet L1, L2, L3, L4 con logica ibrida
        for level in ['L1', 'L2', 'L3', 'L4']:
            if level in sheets_hybrid:
                sheets_hybrid[level].to_excel(writer, sheet_name=level, index=False)
                print(f"✓ Sheet: {level} ({len(sheets_hybrid[level])} oggetti, {stats[level]['critical_hybrid']} critici)")
        
        # Sheet altri (exploded, etc.)
        for sheet_name, df in sheets_hybrid.items():
            if sheet_name not in ['L1', 'L2', 'L3', 'L4', 'Summary_Hybrid']:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"✓ Sheet: {sheet_name}")
        
        # Sheet nuovo: CRITICI_AGGIUNTI (solo oggetti che diventano critici per dipendenze)
        all_newly_critical = []
        for level in ['L1', 'L2', 'L3', 'L4']:
            if level in sheets_hybrid:
                df_level = sheets_hybrid[level]
                newly_critical = df_level[
                    (df_level['Critico_Migrazione'] == 'SÌ') &
                    (df_level['Critico_Migrazione_Original'] != 'SÌ')
                ].copy()
                newly_critical['Livello'] = level
                all_newly_critical.append(newly_critical)
        
        if all_newly_critical:
            df_newly_critical = pd.concat(all_newly_critical, ignore_index=True)
            df_newly_critical = df_newly_critical.sort_values('ReferenceCount', ascending=False)
            df_newly_critical.to_excel(writer, sheet_name='CRITICI_AGGIUNTI_Deps', index=False)
            print(f"✓ Sheet: CRITICI_AGGIUNTI_Deps ({len(df_newly_critical)} oggetti)")
    
    print(f"\n✓ Report salvato: {OUTPUT_PATH}")
    
    # Print summary
    print("\n" + "="*80)
    print("RIEPILOGO CRITICITÀ IBRIDA")
    print("="*80 + "\n")
    
    total_original = sum(stats[level]['critical_original'] for level in stats)
    total_added = sum(stats[level]['added'] for level in stats)
    total_hybrid = sum(stats[level]['critical_hybrid'] for level in stats)
    
    print(f"Critici originali (DML/DDL):          {total_original}")
    print(f"Critici aggiunti (Dipendenze {REFERENCE_COUNT_THRESHOLD}+):    {total_added}")
    print(f"Critici TOTALI (Ibrido):              {total_hybrid}")
    print(f"\nIncremento:                           +{total_added} oggetti ({total_added/total_original*100:.1f}%)")
    print("")

# =========================
# MAIN
# =========================

def main():
    print("\n")
    print("="*80)
    print("HYBRID CRITICALITY - Integrazione DML/DDL + Dipendenze")
    print("="*80)
    print("")
    print(f"Input:  {SUMMARY_PATH}")
    print(f"Output: {OUTPUT_PATH}")
    print(f"\nCriterio ibrido:")
    print(f"  Critico_Migrazione = SÌ se:")
    print(f"    • Ha operazioni DML/DDL (INSERT/UPDATE/DELETE/CREATE/ALTER)")
    print(f"    • OPPURE")
    print(f"    • Ha ReferenceCount >= {REFERENCE_COUNT_THRESHOLD} (dipendenze critiche)")
    print("")
    
    # 1. Query SQL per ReferenceCount
    df_reference_counts = get_all_reference_counts(SERVER, DATABASES, DRIVER)
    
    if df_reference_counts.empty:
        print("\n✗ Impossibile ottenere reference counts. Terminazione.")
        return
    
    # 2. Carica SUMMARY_REPORT originale
    sheets_original = load_summary_report(SUMMARY_PATH)
    
    if not sheets_original:
        print("\n✗ Impossibile caricare SUMMARY_REPORT. Terminazione.")
        return
    
    # 3. Carica oggetti mancanti con 50+ refs (se esistono)
    df_missing = load_missing_high_ref_objects(TOP_REF_PATH, REFERENCE_COUNT_THRESHOLD)
    
    # 4. Aggiungi oggetti mancanti a L1 prima della logica hybrid
    if not df_missing.empty and 'L1' in sheets_original:
        print("\n" + "="*80)
        print(f"AGGIUNTA {len(df_missing)} OGGETTI MANCANTI A L1")
        print("="*80 + "\n")
        
        df_l1_original = sheets_original['L1']
        
        # Prepara oggetti mancanti per integrazione
        df_missing_prep = df_missing.copy()
        
        # Aggiungi/adatta colonne per compatibilità L1
        df_missing_prep['Critico_Migrazione'] = 'SÌ'
        df_missing_prep['Criticità_Tecnica'] = 'DIPENDENZE_CRITICHE'
        
        # Assicura colonne compatibili
        for col in df_l1_original.columns:
            if col not in df_missing_prep.columns:
                df_missing_prep[col] = None
        
        # Riordina colonne come L1
        df_missing_prep = df_missing_prep[df_l1_original.columns]
        
        # Combina
        sheets_original['L1'] = pd.concat([df_l1_original, df_missing_prep], ignore_index=True)
        
        print(f"✓ L1 originale: {len(df_l1_original)} oggetti")
        print(f"✓ Oggetti aggiunti: {len(df_missing_prep)} oggetti")
        print(f"✓ L1 nuovo: {len(sheets_original['L1'])} oggetti")
    
    # 5. Applica logica ibrida
    sheets_hybrid, stats = generate_hybrid_summary(sheets_original, df_reference_counts)
    
    # 6. Export nuovo report
    export_hybrid_report(sheets_hybrid, stats, df_reference_counts)
    
    print("\n" + "="*80)
    print("HYBRID CRITICALITY COMPLETATO")
    print("="*80)
    print("")
    print("📊 File generato:")
    print(f"   {OUTPUT_PATH}")
    print("")
    print("📋 Nuovo criterio applicato:")
    print(f"   • Critici DML/DDL: mantiene logica originale")
    print(f"   • Critici Dipendenze: aggiunge oggetti con {REFERENCE_COUNT_THRESHOLD}+ riferimenti")
    print(f"   • Colonna 'Motivo_Criticità': indica perché è critico")
    print(f"   • Sheet 'CRITICI_AGGIUNTI_Deps': nuovi critici da dipendenze")
    print("")
    print("✅ Prossimi passi:")
    print("   1. Aprire SUMMARY_REPORT_HYBRID.xlsx")
    print("   2. Verificare sheet 'CRITICI_AGGIUNTI_Deps'")
    print("   3. Verificare colonna 'Motivo_Criticità' nei livelli L1-L4")
    print("   4. Confrontare con VALIDATION_REPORT.xlsx")
    print("")

if __name__ == "__main__":
    main()
