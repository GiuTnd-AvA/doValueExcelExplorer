# =========================
# IMPORT
# =========================
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import pyodbc

# =========================
# CONFIG
# =========================
SUMMARY_PATH = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\SUMMARY_REPORT.xlsx'
OUTPUT_PATH = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\VALIDATION_REPORT.xlsx'

# SQL Server
SERVER = 'EPCP3'
DRIVER = '{ODBC Driver 17 for SQL Server}'
TOP_N = 1500  # Top N oggetti più referenziati da validare

# Database da analizzare (dalla lista nel progetto)
DATABASES = [
    'ANALISI', 'BASEDATI_BI', 'CORESQL7', 'DWH', 
    'MORTGAGE', 'S1259', 'STAGING', 'STG', 'UTIL'
]

# =========================
# FUNZIONI
# =========================

def load_critical_objects(summary_path):
    """Carica oggetti critici da L1."""
    print(f"Caricamento oggetti critici da: {summary_path}")
    
    try:
        df_l1 = pd.read_excel(summary_path, sheet_name='L1')
        
        # Filtra solo critici per migrazione
        df_critical = df_l1[df_l1['Critico_Migrazione'] == 'SÌ'].copy()
        
        print(f"  ✓ Caricati {len(df_critical)} oggetti critici da L1")
        print(f"    - Totale L1: {len(df_l1)}")
        print(f"    - Critici: {len(df_critical)}")
        
        return df_critical
        
    except Exception as e:
        print(f"  ✗ Errore caricamento: {e}")
        return pd.DataFrame()

def get_engine(server, db_name, driver):
    """Crea engine SQLAlchemy."""
    conn_str = f"mssql+pyodbc://@{server}/{db_name}?driver={driver}&trusted_connection=yes"
    return create_engine(conn_str, pool_pre_ping=True)

def get_top_referenced_objects(server, database, driver, top_n):
    """
    Query SQL per trovare gli oggetti più referenziati nel database.
    Conta quanti altri oggetti dipendono da ciascun oggetto.
    """
    print(f"\n  Analisi database: {database}")
    
    try:
        engine = get_engine(server, database, driver)
        
        # Query per contare dipendenze
        # Trova oggetti più referenziati usando sys.sql_expression_dependencies
        query = f"""
        SELECT TOP {top_n}
            OBJECT_SCHEMA_NAME(sed.referenced_id) AS SchemaName,
            OBJECT_NAME(sed.referenced_id) AS ObjectName,
            o.type_desc AS ObjectType,
            COUNT(DISTINCT sed.referencing_id) AS ReferenceCount,
            '{database}' AS DatabaseName
        FROM sys.sql_expression_dependencies sed
        INNER JOIN sys.objects o ON sed.referenced_id = o.object_id
        WHERE o.type IN ('P', 'FN', 'IF', 'TF', 'TR', 'V')  -- SP, Functions, Triggers, Views
            AND o.is_ms_shipped = 0  -- Escludi oggetti di sistema
        GROUP BY sed.referenced_id, o.type_desc
        HAVING COUNT(DISTINCT sed.referencing_id) > 0
        ORDER BY ReferenceCount DESC
        """
        
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        
        print(f"    ✓ Trovati {len(df)} oggetti referenziati")
        
        if len(df) > 0:
            print(f"    Top 5 più referenziati:")
            for i, (idx, row) in enumerate(df.head(5).iterrows(), start=1):
                obj_full = f"[{row['SchemaName']}].[{row['ObjectName']}]"
                print(f"      {i}. {obj_full} ({row['ObjectType']}) - {row['ReferenceCount']} riferimenti")
        
        return df
        
    except Exception as e:
        print(f"    ✗ Errore: {e}")
        return pd.DataFrame()

def normalize_object_name(row):
    """Normalizza nome oggetto per confronto."""
    schema = row.get('Schema', '') or row.get('SchemaName', '') or 'dbo'
    obj_name = row.get('ObjectName', '')
    db = row.get('Database', '') or row.get('DatabaseName', '')
    
    # Formato: DATABASE.SCHEMA.OBJECTNAME
    return f"{db.upper()}.{schema.upper()}.{obj_name.upper()}"

def compare_objects(df_critical, df_top_referenced):
    """Confronta oggetti critici con oggetti più referenziati."""
    print("\n" + "="*80)
    print("CONFRONTO OGGETTI CRITICI vs TOP REFERENZIATI")
    print("="*80 + "\n")
    
    # Normalizza nomi per confronto
    critical_set = set(df_critical.apply(normalize_object_name, axis=1))
    referenced_set = set(df_top_referenced.apply(normalize_object_name, axis=1))
    
    print(f"Oggetti critici identificati: {len(critical_set)}")
    print(f"Oggetti top referenziati SQL: {len(referenced_set)}")
    print("")
    
    # Match: presenti in entrambi
    match = critical_set.intersection(referenced_set)
    print(f"✓ Match (critici E top referenced): {len(match)}")
    print(f"  Percentuale copertura: {len(match)/len(critical_set)*100:.1f}%")
    print("")
    
    # Critici NON nei top (possibili falsi positivi o oggetti poco usati ma critici per DML)
    critical_not_in_top = critical_set - referenced_set
    print(f"⚠ Critici NON nei top referenced: {len(critical_not_in_top)}")
    print(f"  (Potrebbero essere oggetti critici per DML ma poco referenziati)")
    print("")
    
    # Top NON critici (possibili oggetti mancanti)
    top_not_critical = referenced_set - critical_set
    print(f"❌ Top referenced NON critici: {len(top_not_critical)}")
    print(f"  (Oggetti molto usati ma NON identificati come critici)")
    print("")
    
    return {
        'match': match,
        'critical_not_in_top': critical_not_in_top,
        'top_not_critical': top_not_critical
    }

def generate_validation_report(df_critical, df_top_referenced, comparison):
    """Genera report Excel con risultati validazione."""
    print("\n" + "="*80)
    print("GENERAZIONE REPORT VALIDAZIONE")
    print("="*80 + "\n")
    
    # Sheet 1: Summary
    summary_data = {
        'Metrica': [
            'Oggetti Critici Identificati (L1)',
            'Oggetti Top Referenced SQL',
            'Match (presenti in entrambi)',
            'Critici NON nei top',
            'Top NON critici',
            'Percentuale Copertura'
        ],
        'Valore': [
            len(df_critical),
            len(df_top_referenced),
            len(comparison['match']),
            len(comparison['critical_not_in_top']),
            len(comparison['top_not_critical']),
            f"{len(comparison['match'])/len(df_critical)*100:.1f}%"
        ]
    }
    df_summary = pd.DataFrame(summary_data)
    
    # Sheet 2: Match - Oggetti validati
    match_list = []
    for obj_normalized in comparison['match']:
        # Trova dettagli da df_critical
        for idx, row in df_critical.iterrows():
            if normalize_object_name(row) == obj_normalized:
                match_list.append({
                    'Database': row['Database'],
                    'Schema': row.get('Schema', 'dbo'),
                    'ObjectName': row['ObjectName'],
                    'ObjectType': row['ObjectType'],
                    'Critico_Migrazione': row.get('Critico_Migrazione', ''),
                    'Criticità_Tecnica': row.get('Criticità_Tecnica', ''),
                    'Status': '✓ VALIDATO (Critico E Top Referenced)'
                })
                break
    df_match = pd.DataFrame(match_list)
    
    # Sheet 3: Critici NON nei top
    critical_not_top_list = []
    for obj_normalized in comparison['critical_not_in_top']:
        for idx, row in df_critical.iterrows():
            if normalize_object_name(row) == obj_normalized:
                critical_not_top_list.append({
                    'Database': row['Database'],
                    'Schema': row.get('Schema', 'dbo'),
                    'ObjectName': row['ObjectName'],
                    'ObjectType': row['ObjectType'],
                    'Critico_Migrazione': row.get('Critico_Migrazione', ''),
                    'Criticità_Tecnica': row.get('Criticità_Tecnica', ''),
                    'DML_Count': row.get('DML_Count', 0),
                    'Note': 'Critico per DML/DDL ma poco referenziato'
                })
                break
    df_critical_not_top = pd.DataFrame(critical_not_top_list)
    
    # Sheet 4: Top NON critici - possibili oggetti mancanti
    top_not_critical_list = []
    for obj_normalized in comparison['top_not_critical']:
        for idx, row in df_top_referenced.iterrows():
            if normalize_object_name(row) == obj_normalized:
                top_not_critical_list.append({
                    'Database': row['DatabaseName'],
                    'Schema': row['SchemaName'],
                    'ObjectName': row['ObjectName'],
                    'ObjectType': row['ObjectType'],
                    'ReferenceCount': row['ReferenceCount'],
                    'Note': 'Molto referenziato ma NON identificato come critico'
                })
                break
    df_top_not_critical = pd.DataFrame(top_not_critical_list)
    
    # Sheet 5: Dettaglio oggetti critici
    df_critical_export = df_critical.copy()
    
    # Sheet 6: Dettaglio top referenced
    df_top_referenced_export = df_top_referenced.copy()
    
    # Export Excel
    with pd.ExcelWriter(OUTPUT_PATH, engine='openpyxl') as writer:
        df_summary.to_excel(writer, sheet_name='Summary', index=False)
        if len(df_match) > 0:
            df_match.to_excel(writer, sheet_name='Match_Validati', index=False)
        if len(df_critical_not_top) > 0:
            df_critical_not_top.to_excel(writer, sheet_name='Critici_Non_Top', index=False)
        if len(df_top_not_critical) > 0:
            df_top_not_critical.to_excel(writer, sheet_name='Top_Non_Critici', index=False)
        df_critical_export.to_excel(writer, sheet_name='Dettaglio_Critici', index=False)
        df_top_referenced_export.to_excel(writer, sheet_name='Dettaglio_Top_Referenced', index=False)
    
    print(f"✓ Report salvato: {OUTPUT_PATH}")
    print(f"\nSheet generate:")
    print(f"  1. Summary - Metriche validazione")
    print(f"  2. Match_Validati - {len(df_match)} oggetti validati")
    print(f"  3. Critici_Non_Top - {len(df_critical_not_top)} oggetti critici poco referenziati")
    print(f"  4. Top_Non_Critici - {len(df_top_not_critical)} oggetti molto usati NON critici")
    print(f"  5. Dettaglio_Critici - Tutti gli oggetti critici L1")
    print(f"  6. Dettaglio_Top_Referenced - Top {TOP_N} oggetti più referenziati")

# =========================
# MAIN
# =========================

def main():
    print("="*80)
    print("VALIDAZIONE OGGETTI CRITICI con SQL SERVER")
    print("="*80)
    print(f"\nServer: {SERVER}")
    print(f"Database da analizzare: {len(DATABASES)}")
    print(f"Top N oggetti per database: {TOP_N}")
    print("")
    
    # Carica oggetti critici
    df_critical = load_critical_objects(SUMMARY_PATH)
    
    if df_critical.empty:
        print("\n✗ Nessun oggetto critico trovato. Terminazione.")
        return
    
    # Raggruppa per database
    print(f"\nDistribuzione oggetti critici per database:")
    for db, count in df_critical['Database'].value_counts().items():
        print(f"  • {db}: {count} oggetti")
    
    # Query SQL per trovare top referenced objects in ogni database
    print("\n" + "="*80)
    print("QUERY SQL - TOP REFERENCED OBJECTS")
    print("="*80)
    
    all_top_referenced = []
    
    for database in DATABASES:
        df_top = get_top_referenced_objects(SERVER, database, DRIVER, TOP_N)
        if not df_top.empty:
            all_top_referenced.append(df_top)
    
    if not all_top_referenced:
        print("\n✗ Nessun oggetto referenziato trovato da SQL. Terminazione.")
        return
    
    # Combina tutti i risultati
    df_all_top_referenced = pd.concat(all_top_referenced, ignore_index=True)
    print(f"\n✓ Totale oggetti top referenced: {len(df_all_top_referenced)}")
    
    # Confronto
    comparison = compare_objects(df_critical, df_all_top_referenced)
    
    # Genera report
    generate_validation_report(df_critical, df_all_top_referenced, comparison)
    
    print("\n" + "="*80)
    print("VALIDAZIONE COMPLETATA")
    print("="*80)
    print(f"\n✓ Report Excel: {OUTPUT_PATH}")
    print("\nRaccomandazioni:")
    print("  1. Verifica 'Top_Non_Critici' per possibili oggetti mancanti")
    print("  2. Analizza 'Critici_Non_Top' per validare criticità")
    print(f"  3. 'Match_Validati' conferma {len(comparison['match'])} oggetti critici")
    print("")

if __name__ == "__main__":
    main()
