# =========================
# IMPORT
# =========================
import pandas as pd
from pathlib import Path
from datetime import datetime
import warnings
import pyodbc
warnings.filterwarnings('ignore')

# =========================
# CONFIG
# =========================
HYBRID_SUMMARY_PATH = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\SUMMARY_REPORT_HYBRID.xlsx'
OUTPUT_TXT = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\TABLES_MIGRATION_REPORT.txt'
OUTPUT_MD = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\TABLES_MIGRATION_REPORT.md'
SQL_SERVER = 'EPCP3'

# =========================
# FUNZIONI
# =========================

def verify_object_types_sql(df_tables, server_name):
    """Verifica su SQL Server se ogni oggetto è TABLE o VIEW."""
    print("\n" + "="*80)
    print("VERIFICA OBJECT TYPE su SQL Server")
    print("="*80 + "\n")
    
    if df_tables.empty:
        print("✗ Nessuna tabella da verificare")
        return df_tables
    
    # Raggruppa per database
    databases = df_tables['Database'].unique()
    print(f"Database da verificare: {len(databases)}")
    
    # Query per verificare tipo oggetto
    query_template = """
    SELECT 
        s.name AS SchemaName,
        o.name AS ObjectName,
        o.type_desc AS ObjectType
    FROM sys.objects o
    INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
    WHERE o.type IN ('U', 'V')  -- U=TABLE, V=VIEW
        AND o.name IN ({placeholders})
    """
    
    results = []
    
    for db in databases:
        print(f"\n  Database: {db}")
        
        # Tabelle per questo database
        db_tables = df_tables[df_tables['Database'] == db]['Table'].unique()
        
        if len(db_tables) == 0:
            continue
        
        try:
            # Connessione
            conn_str = f"DRIVER={{SQL Server}};SERVER={server_name};DATABASE={db};Trusted_Connection=yes;"
            conn = pyodbc.connect(conn_str, timeout=10)
            cursor = conn.cursor()
            
            # Batch di 500 per volta (limite SQL Server IN clause)
            batch_size = 500
            for i in range(0, len(db_tables), batch_size):
                batch = db_tables[i:i+batch_size]
                placeholders = ','.join(['?' for _ in batch])
                query = query_template.format(placeholders=placeholders)
                
                cursor.execute(query, list(batch))
                rows = cursor.fetchall()
                
                for row in rows:
                    results.append({
                        'Database': db,
                        'Schema': row.SchemaName,
                        'Table': row.ObjectName,
                        'ObjectType': 'TABLE' if row.ObjectType == 'USER_TABLE' else 'VIEW'
                    })
            
            conn.close()
            print(f"    ✓ Verificati {len([r for r in results if r['Database'] == db])} oggetti")
            
        except Exception as e:
            print(f"    ✗ Errore connessione/query: {e}")
    
    # Merge risultati con dataframe originale
    if results:
        df_results = pd.DataFrame(results)
        df_tables = df_tables.merge(
            df_results[['Database', 'Table', 'ObjectType']], 
            on=['Database', 'Table'], 
            how='left'
        )
        
        # Oggetti non trovati = UNKNOWN
        df_tables['ObjectType'] = df_tables['ObjectType'].fillna('UNKNOWN')
        
        # Stats
        type_counts = df_tables['ObjectType'].value_counts()
        print("\n" + "="*80)
        print("RISULTATO VERIFICA:")
        for obj_type, count in type_counts.items():
            pct = count / len(df_tables) * 100
            print(f"  • {obj_type:10s}: {count:4d} ({pct:5.1f}%)")
        print("="*80)
    else:
        df_tables['ObjectType'] = 'UNKNOWN'
        print("\n✗ Nessun risultato dalla verifica SQL")
    
    return df_tables

def load_tables_data(summary_path):
    """Carica dati tabelle dal SUMMARY_REPORT_HYBRID."""
    print("="*80)
    print("CARICAMENTO TABELLE da SUMMARY_REPORT_HYBRID")
    print("="*80 + "\n")
    
    tables_data = {}
    
    try:
        xl_file = pd.ExcelFile(summary_path)
        available_sheets = xl_file.sheet_names
        
        print(f"Sheet disponibili nel file: {len(available_sheets)}")
        print(f"  {', '.join(str(s) for s in available_sheets[:10])}{'...' if len(available_sheets) > 10 else ''}")
        print()
        
        # Sheet da caricare con varianti possibili
        sheets_to_load = [
            (['L1'], 'L1'),
            (['Dipendenze_Tabelle_L1', 'Dipendenze_L1', 'Oggetti_Esplosi_L1'], 'Dipendenze_L1'),
            (['L2'], 'L2'),
            (['Dipendenze_Tabelle_L2', 'Dipendenze_L2', 'Oggetti_Esplosi_L2'], 'Dipendenze_L2'),
            (['L3'], 'L3'),
            (['Dipendenze_Tabelle_L3', 'Dipendenze_L3', 'Oggetti_Esplosi_L3'], 'Dipendenze_L3'),
            (['L4'], 'L4'),
            (['Dipendenze_Tabelle_L4', 'Dipendenze_L4', 'Oggetti_Esplosi_L4'], 'Dipendenze_L4')
        ]
        
        for possible_names, label in sheets_to_load:
            loaded = False
            for sheet_name in possible_names:
                if sheet_name in available_sheets:
                    try:
                        df = pd.read_excel(summary_path, sheet_name=sheet_name)
                        tables_data[label] = df
                        print(f"✓ {sheet_name}: {len(df)} righe")
                        loaded = True
                        break
                    except Exception as e:
                        print(f"✗ {sheet_name}: errore lettura - {e}")
            
            if not loaded:
                print(f"⚠ {label}: nessun sheet trovato tra {possible_names}")
        
        print(f"\n✓ Caricati {len(tables_data)} sheet")
        return tables_data
        
    except Exception as e:
        print(f"✗ Errore caricamento: {e}")
        return {}

def extract_tables_from_level(df_level, df_dependencies, level_name):
    """Estrae tabelle da un livello.
    
    LOGICA:
    - L1: Colonna 'Table' (tabella iniziale) + Colonna 'Dipendenze_Tabelle' (separate da ;)
    - L2-L4: Solo colonna 'Dipendenze_Tabelle' (separate da ;)
    """
    print(f"\n  Analisi {level_name}...")
    
    tables_info = []
    
    # 1. Tabelle INIZIALI (solo L1 - dalla colonna Table)
    if 'Table' in df_level.columns and 'Database' in df_level.columns:
        for _, row in df_level.iterrows():
            table_name = str(row['Table']).strip()
            db_name = str(row['Database']).strip().upper()
            
            if table_name and table_name != 'nan' and db_name and db_name != 'NAN':
                tables_info.append({
                    'Database': db_name,
                    'Table': table_name,
                    'Source': 'Tabella Iniziale',
                    'Level': level_name
                })
        print(f"    Tabelle iniziali (da colonna Table): {len(tables_info)}")
    
    # 2. Tabelle REFERENZIATE (dalla colonna Dipendenze_Tabelle - separati da ;)
    deps_tables_count = 0
    if 'Dipendenze_Tabelle' in df_level.columns and 'Database' in df_level.columns:
        for _, row in df_level.iterrows():
            db_name = str(row['Database']).strip().upper()
            dipendenze = str(row.get('Dipendenze_Tabelle', '')).strip()
            
            if dipendenze and dipendenze != 'nan' and dipendenze != 'Nessuna':
                # Split per ;
                tabelle_deps = [t.strip() for t in dipendenze.split(';') if t.strip()]
                
                for table_dep in tabelle_deps:
                    # Rimuovi dbo. se presente
                    if table_dep.startswith('dbo.'):
                        table_dep = table_dep[4:]
                    
                    # FILTRO: Skippa nomi invalidi
                    if table_dep.lower() in ['dbo', 'objects', 'sysobjects', 'sysindexes', 'syscolumns']:
                        continue  # Schema o system tables
                    if len(table_dep) < 2:
                        continue  # Nome troppo corto
                    
                    # Evita duplicati
                    if not any(t['Database'] == db_name and t['Table'] == table_dep for t in tables_info):
                        tables_info.append({
                            'Database': db_name,
                            'Table': table_dep,
                            'Source': 'Dipendenza',
                            'Level': level_name
                        })
                        deps_tables_count += 1
        
        print(f"    Tabelle da dipendenze (separate da ;): {deps_tables_count}")
    
    # Converti in DataFrame
    df_tables = pd.DataFrame(tables_info)
    
    if not df_tables.empty:
        # Normalizza
        df_tables['Database'] = df_tables['Database'].str.upper()
        df_tables = df_tables.drop_duplicates(subset=['Database', 'Table'])
        
        print(f"    ✓ Totale tabelle uniche: {len(df_tables)}")
        
        # Stats per database
        db_dist = df_tables['Database'].value_counts()
        return {
            'level': level_name,
            'total': len(df_tables),
            'db_distribution': db_dist,
            'df_tables': df_tables
        }
    else:
        print(f"    ✗ Nessuna tabella trovata")
        return {
            'level': level_name,
            'total': 0,
            'db_distribution': pd.Series(),
            'df_tables': pd.DataFrame()
        }

def generate_txt_report(analyses):
    """Genera report TXT per tabelle."""
    print("\n" + "="*80)
    print("GENERAZIONE TABLES_MIGRATION_REPORT.txt")
    print("="*80 + "\n")
    
    lines = []
    
    # Header
    lines.append("="*100)
    lines.append("TABLES MIGRATION REPORT - Tabelle da Migrare")
    lines.append("="*100)
    lines.append("")
    lines.append(f"Data generazione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Source: SUMMARY_REPORT_HYBRID.xlsx")
    lines.append("")
    lines.append("LOGICA ESTRAZIONE:")
    lines.append("  ✓ L1: Tabelle di partenza (Table iniziale) + Tabelle referenziate (Dipendenze)")
    lines.append("  ✓ L2-L4: Tabelle referenziate dagli oggetti (Dipendenze)")
    lines.append("")
    lines.append("="*100)
    lines.append("")
    
    # =====================
    # SEZIONE 1: SUMMARY ESECUTIVO
    # =====================
    lines.append("1. SUMMARY ESECUTIVO")
    lines.append("="*100)
    lines.append("")
    
    # Combina tutte le tabelle
    all_tables = []
    for level, analysis in analyses.items():
        if not analysis['df_tables'].empty:
            all_tables.append(analysis['df_tables'])
    
    # Inizializza variabili
    df_all_tables = pd.DataFrame()
    total_tables = 0
    total_databases = 0
    db_totals = pd.Series(dtype=int)
    
    if all_tables:
        df_all_tables = pd.concat(all_tables, ignore_index=True)
        df_all_tables = df_all_tables.drop_duplicates(subset=['Database', 'Table'])
        
        total_tables = len(df_all_tables)
        total_databases = df_all_tables['Database'].nunique()
        
        lines.append(f"Tabelle totali da migrare:               {total_tables}")
        lines.append(f"Database coinvolti:                       {total_databases}")
        lines.append("")
        
        # Distribuzione per tipo oggetto (TABLE/VIEW)
        if 'ObjectType' in df_all_tables.columns:
            lines.append("Distribuzione per tipo oggetto:")
            type_counts = df_all_tables['ObjectType'].value_counts()
            for obj_type, count in type_counts.items():
                pct = count / total_tables * 100
                lines.append(f"  • {obj_type:10s}: {count:3d} ({pct:5.1f}%)")
            lines.append("")
        
        # Distribuzione per livello
        lines.append("Distribuzione per livello:")
        for level in ['L1', 'L2', 'L3', 'L4']:
            if level in analyses:
                a = analyses[level]
                lines.append(f"  {level}: {a['total']:3d} tabelle")
        lines.append("")
        
        # Top database
        lines.append("Distribuzione per database:")
        db_totals = df_all_tables['Database'].value_counts().sort_values(ascending=False)
        for db, count in db_totals.items():
            pct = count / total_tables * 100
            lines.append(f"  • {db:20s}: {count:3d} tabelle ({pct:5.1f}%)")
        lines.append("")
    else:
        lines.append("✗ Nessuna tabella trovata")
        lines.append("")
    
    # =====================
    # SEZIONE 2: DETTAGLIO PER LIVELLO
    # =====================
    lines.append("")
    lines.append("2. DETTAGLIO PER LIVELLO")
    lines.append("="*100)
    lines.append("")
    
    for level in ['L1', 'L2', 'L3', 'L4']:
        if level not in analyses:
            continue
        
        a = analyses[level]
        
        lines.append(f"{'─'*100}")
        lines.append(f"LIVELLO {level}")
        lines.append(f"{'─'*100}")
        lines.append("")
        
        lines.append(f"Tabelle totali:                    {a['total']}")
        lines.append("")
        
        if not a['db_distribution'].empty:
            lines.append("Per database:")
            for db, count in a['db_distribution'].sort_values(ascending=False).items():
                pct = count / a['total'] * 100 if a['total'] > 0 else 0
                lines.append(f"  • {db:20s}: {count:3d} ({pct:5.1f}%)")
            lines.append("")
        
        lines.append("")
    
    # =====================
    # SEZIONE 3: LISTA COMPLETA TABELLE
    # =====================
    lines.append("")
    lines.append("3. LISTA COMPLETA TABELLE DA MIGRARE")
    lines.append("="*100)
    lines.append("")
    
    if all_tables:
        # Ordina per Database, poi Table
        df_all_tables_sorted = df_all_tables.sort_values(['Database', 'Table'])
        
        current_db = None
        for _, row in df_all_tables_sorted.iterrows():
            if current_db != row['Database']:
                if current_db is not None:
                    lines.append("")
                current_db = row['Database']
                lines.append(f"{'─'*100}")
                lines.append(f"DATABASE: {current_db}")
                lines.append(f"{'─'*100}")
                lines.append("")
            
            table_full = f"[{row['Database']}].[dbo].[{row['Table']}]"
            level_info = row.get('Level', 'N/A')
            source_info = row.get('Source', 'N/A')
            obj_type = row.get('ObjectType', 'UNKNOWN')
            lines.append(f"  • {table_full:60s} | {obj_type:7s} | Lvl: {level_info} | {source_info}")
        
        lines.append("")
    
    # =====================
    # SEZIONE 4: RACCOMANDAZIONI
    # =====================
    lines.append("")
    lines.append("4. RACCOMANDAZIONI MIGRAZIONE TABELLE")
    lines.append("="*100)
    lines.append("")
    
    lines.append("STRATEGIA MIGRAZIONE:")
    lines.append("")
    lines.append("1️⃣  FASE 0 - Schema e Tabelle")
    lines.append(f"   • Migrare TUTTE le {total_tables if all_tables else 0} oggetti (tabelle + viste) PRIMA degli oggetti")
    
    if all_tables and 'ObjectType' in df_all_tables.columns:
        type_counts = df_all_tables['ObjectType'].value_counts()
        if 'TABLE' in type_counts:
            lines.append(f"     - {type_counts['TABLE']} TABLES: struttura, indici, constraint, FK, dati")
        if 'VIEW' in type_counts:
            lines.append(f"     - {type_counts['VIEW']} VIEWS: definizione (DOPO le tables)")
        if 'UNKNOWN' in type_counts:
            lines.append(f"     - {type_counts['UNKNOWN']} UNKNOWN: verificare manualmente su SQL Server")
    else:
        lines.append("   • Includere: struttura, indici, constraint, FK")
    
    lines.append("   • Verificare: data types compatibility, collation")
    lines.append("")
    
    lines.append("2️⃣  PRIORITÀ PER DATABASE")
    if all_tables and not db_totals.empty:
        lines.append("   Ordine consigliato basato su volume:")
        for i, (db, count) in enumerate(db_totals.head(5).items(), start=1):
            lines.append(f"   {i}. {db}: {count} tabelle")
    lines.append("")
    
    lines.append("3️⃣  VALIDAZIONE POST-MIGRAZIONE")
    lines.append("   • Row count verification (source vs target)")
    lines.append("   • Schema comparison (struttura)")
    lines.append("   • Referential integrity check (FK)")
    lines.append("   • Performance baseline (query sample)")
    lines.append("")
    
    lines.append("⚠️  ATTENZIONE:")
    lines.append("   • Le tabelle DEVONO essere migrate PRIMA degli oggetti L1-L4")
    lines.append("   • Verificare dipendenze cross-database prima della migrazione")
    lines.append(f"   • Coinvolti {total_databases if all_tables else 0} database - coordinare accessi")
    lines.append("")
    
    # Footer
    lines.append("")
    lines.append("="*100)
    lines.append("FINE REPORT")
    lines.append("="*100)
    
    # Scrivi file
    with open(OUTPUT_TXT, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    print(f"✓ Report salvato: {OUTPUT_TXT}")

def generate_md_report(analyses):
    """Genera report Markdown per tabelle."""
    print(f"Generazione {OUTPUT_MD}...")
    
    lines = []
    
    # Header
    lines.append("# TABLES MIGRATION REPORT - Tabelle da Migrare")
    lines.append("")
    lines.append(f"**Data generazione:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("## Logica Estrazione")
    lines.append("")
    lines.append("- **L1:** Tabelle di partenza (Table iniziale) + Tabelle referenziate (Dipendenze)")
    lines.append("- **L2-L4:** Tabelle referenziate dagli oggetti (Dipendenze)")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # Summary
    all_tables = []
    for level, analysis in analyses.items():
        if not analysis['df_tables'].empty:
            all_tables.append(analysis['df_tables'])
    
    # Inizializza variabili
    df_all_tables = pd.DataFrame()
    total_tables = 0
    total_databases = 0
    db_totals = pd.Series(dtype=int)
    
    if all_tables:
        df_all_tables = pd.concat(all_tables, ignore_index=True)
        df_all_tables = df_all_tables.drop_duplicates(subset=['Database', 'Table'])
        
        total_tables = len(df_all_tables)
        total_databases = df_all_tables['Database'].nunique()
        
        lines.append("## 1. Summary Esecutivo")
        lines.append("")
        lines.append(f"- **Tabelle totali da migrare:** {total_tables}")
        lines.append(f"- **Database coinvolti:** {total_databases}")
        lines.append("")
        
        # Per livello
        lines.append("### Per livello:")
        lines.append("")
        lines.append("| Livello | Tabelle |")
        lines.append("|---------|--------:|")
        for level in ['L1', 'L2', 'L3', 'L4']:
            if level in analyses:
                lines.append(f"| {level} | {analyses[level]['total']} |")
        lines.append("")
        
        # Per database
        lines.append("### Per database:")
        lines.append("")
        db_totals = df_all_tables['Database'].value_counts().sort_values(ascending=False)
        lines.append("| Database | Tabelle | % |")
        lines.append("|----------|--------:|--:|")
        for db, count in db_totals.items():
            pct = count / total_tables * 100
            lines.append(f"| {db} | {count} | {pct:.1f}% |")
        lines.append("")
    
    # Raccomandazioni
    lines.append("---")
    lines.append("")
    lines.append("## 2. Raccomandazioni Migrazione")
    lines.append("")
    lines.append("### Strategia:")
    lines.append("")
    lines.append("1. **FASE 0 - Schema e Tabelle**")
    lines.append(f"   - Migrare TUTTE le {total_tables if all_tables else 0} tabelle PRIMA degli oggetti")
    lines.append("   - Includere: struttura, indici, constraint, FK")
    lines.append("")
    lines.append("2. **FASE 1-4 - Oggetti L1→L4**")
    lines.append("   - Dopo migrazione tabelle, procedere con oggetti")
    lines.append("")
    lines.append("### ⚠️ Attenzione:")
    lines.append("")
    lines.append("- Le tabelle DEVONO essere migrate PRIMA degli oggetti")
    lines.append("- Verificare dipendenze cross-database")
    lines.append(f"- Coordinare accessi su {total_databases if all_tables else 0} database")
    lines.append("")
    
    # Footer
    lines.append("---")
    lines.append("")
    lines.append("*Fine Report*")
    
    # Scrivi file
    with open(OUTPUT_MD, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    print(f"✓ Report salvato: {OUTPUT_MD}")

# =========================
# MAIN
# =========================

def main():
    print("\n")
    print("="*80)
    print("GENERAZIONE TABLES MIGRATION REPORT")
    print("="*80)
    print("")
    print(f"Source: {HYBRID_SUMMARY_PATH}")
    print(f"Output TXT: {OUTPUT_TXT}")
    print(f"Output MD:  {OUTPUT_MD}")
    print("")
    
    # Carica dati
    tables_data = load_tables_data(HYBRID_SUMMARY_PATH)
    
    if not tables_data:
        print("\n✗ Impossibile caricare dati. Terminazione.")
        return
    
    # Analizza ogni livello
    print("\n" + "="*80)
    print("ANALISI TABELLE PER LIVELLO")
    print("="*80)
    
    analyses = {}
    
    for level in ['L1', 'L2', 'L3', 'L4']:
        df_level = tables_data.get(level)
        df_deps = tables_data.get(f'Dipendenze_{level}')
        
        if df_level is not None:
            analyses[level] = extract_tables_from_level(df_level, df_deps, level)
    
    # Combina tutte le tabelle per verifica SQL
    all_tables_list = []
    for level, analysis in analyses.items():
        if not analysis['df_tables'].empty:
            all_tables_list.append(analysis['df_tables'])
    
    if all_tables_list:
        df_all_combined = pd.concat(all_tables_list, ignore_index=True)
        df_all_combined = df_all_combined.drop_duplicates(subset=['Database', 'Table'])
        
        # Verifica su SQL Server
        df_all_combined = verify_object_types_sql(df_all_combined, SQL_SERVER)
        
        # Aggiorna analyses con ObjectType
        for level in analyses:
            if not analyses[level]['df_tables'].empty:
                analyses[level]['df_tables'] = analyses[level]['df_tables'].merge(
                    df_all_combined[['Database', 'Table', 'ObjectType']],
                    on=['Database', 'Table'],
                    how='left'
                )
    
    # Genera report TXT
    generate_txt_report(analyses)
    
    # Genera report MD
    generate_md_report(analyses)
    
    print("\n" + "="*80)
    print("GENERAZIONE COMPLETATA")
    print("="*80)
    print("")
    print("📊 File generati:")
    print(f"   • TXT:  {OUTPUT_TXT}")
    print(f"   • MD:   {OUTPUT_MD}")
    print("")
    print("📋 Contenuto:")
    print("   1. Summary esecutivo con totale tabelle")
    print("   2. Distribuzione per database e livello")
    print("   3. Lista completa tabelle da migrare")
    print("   4. Raccomandazioni strategia migrazione")
    print("")
    print("✅ Report pronto per condivisione!")
    print("")

if __name__ == "__main__":
    main()
