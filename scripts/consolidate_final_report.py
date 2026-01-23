# =========================
# IMPORT
# =========================
import pandas as pd
from pathlib import Path
import re

# =========================
# CONFIG
# =========================
# File consolidato con correzioni manuali
ANALYZED_FILE = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\analisi_oggetti_critici.xlsx'
NEW_DEPS_FILE = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\analisi_sqldefinition_criticità_nuove_dipendenze.xlsx'
OUTPUT_FILE = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\REPORT_FINALE_MIGRAZIONE.xlsx'

# =========================
# FUNZIONI
# =========================

def classify_dependency_type(dep_name):
    """Classifica una dipendenza come Tabella, SP, Trigger o Function."""
    dep_lower = dep_name.lower()
    
    # Trigger
    if 'trigger' in dep_lower or dep_lower.startswith('tr_') or '_tr_' in dep_lower:
        return 'Trigger'
    
    # Stored Procedures
    if any(p in dep_lower for p in ['sp_', 'usp_', 'asp_', 'proc_', 'p_', '_sp_']):
        return 'SP'
    
    # Functions
    if any(p in dep_lower for p in ['fn_', 'udf_', 'tf_', 'if_', 'f_', '_fn_', '_udf_']):
        return 'Function'
    
    # Default: Tabella
    return 'Tabella'

def parse_dependencies(dep_string):
    """Analizza la stringa dipendenze e conta per tipo."""
    if not dep_string or not isinstance(dep_string, str) or dep_string.lower() == 'nessuna':
        return {
            'total': 0,
            'tables': 0,
            'sp': 0,
            'triggers': 0,
            'functions': 0,
            'list': []
        }
    
    # Split per punto e virgola
    deps = [d.strip() for d in dep_string.split(';') if d.strip()]
    
    counts = {
        'total': len(deps),
        'tables': 0,
        'sp': 0,
        'triggers': 0,
        'functions': 0,
        'list': deps
    }
    
    for dep in deps:
        dep_type = classify_dependency_type(dep)
        if dep_type == 'Tabella':
            counts['tables'] += 1
        elif dep_type == 'SP':
            counts['sp'] += 1
        elif dep_type == 'Trigger':
            counts['triggers'] += 1
        elif dep_type == 'Function':
            counts['functions'] += 1
    
    return counts

def load_critical_objects():
    """Carica gli oggetti critici dal file consolidato con correzioni manuali."""
    try:
        print(f"Caricamento file: {ANALYZED_FILE}")
        df = pd.read_excel(ANALYZED_FILE)
        
        # Filtra solo oggetti critici (con correzioni manuali dell'utente)
        critical = df[df['Critico_Migrazione'] == 'SÌ'].copy()
        
        # Rimuovi eventuali duplicati per ObjectName
        critical = critical.drop_duplicates(subset=['ObjectName'], keep='first')
        
        print(f"Totale oggetti critici caricati: {len(critical)}\n")
        return critical
        
    except Exception as e:
        print(f"ERRORE leggendo il file consolidato: {e}")
        return pd.DataFrame()

def create_main_sheet(df_critical):
    """Crea lo sheet principale con oggetti critici e analisi dipendenze."""
    if df_critical.empty:
        return pd.DataFrame()
    
    result_rows = []
    
    for idx, row in df_critical.iterrows():
        # Parse dipendenze
        dep_info = parse_dependencies(row.get('Dipendenze', ''))
        
        result_row = {
            'ObjectName': row.get('ObjectName', ''),
            'ObjectType': row.get('ObjectType', ''),
            'Descrizione': row.get('Descrizione_Comportamento', ''),
            'Complessità_Score': row.get('Complessità_Score', 0),
            'Criticità_Tecnica': row.get('Criticità_Tecnica', ''),
            'Pattern_Identificati': row.get('Pattern_Identificati', ''),
            'N_Dipendenze_Totali': dep_info['total'],
            'N_Tabelle': dep_info['tables'],
            'N_SP': dep_info['sp'],
            'N_Trigger': dep_info['triggers'],
            'N_Functions': dep_info['functions'],
            'Dipendenze_Lista': row.get('Dipendenze', ''),
            'DML_Count': row.get('DML_Count', 0),
            'JOIN_Count': row.get('JOIN_Count', 0),
            'Righe_Codice': row.get('Righe_Codice', 0),
            'SQLDefinition': row.get('SQLDefinition', '')
        }
        
        result_rows.append(result_row)
    
    result_df = pd.DataFrame(result_rows)
    
    # Ordina per complessità e criticità
    result_df = result_df.sort_values(['Criticità_Tecnica', 'Complessità_Score'], 
                                       ascending=[False, False])
    
    return result_df

def extract_all_tables(df_critical):
    """Estrae tutte le tabelle uniche dalle dipendenze degli oggetti critici."""
    all_tables = set()
    
    for idx, row in df_critical.iterrows():
        dep_info = parse_dependencies(row.get('Dipendenze', ''))
        for dep in dep_info['list']:
            if classify_dependency_type(dep) == 'Tabella':
                all_tables.add(dep.lower())
    
    # Crea DataFrame
    tables_df = pd.DataFrame({
        'Tabella': sorted(all_tables),
        'Tipo': ['Tabella Referenziata' for _ in all_tables],
        'Note': ['Usata da oggetti critici' for _ in all_tables]
    })
    
    return tables_df

def load_new_tables():
    """Carica le nuove tabelle dal file dipendenze."""
    try:
        df = pd.read_excel(NEW_DEPS_FILE, sheet_name='Nuove Tabelle')
        print(f"Caricate {len(df)} nuove tabelle")
        return df
    except Exception as e:
        print(f"ATTENZIONE: Non posso caricare nuove tabelle: {e}")
        return pd.DataFrame()

def load_new_sp_functions():
    """Carica le nuove SP/Functions dal file dipendenze."""
    try:
        df = pd.read_excel(NEW_DEPS_FILE, sheet_name='Nuove SP-Functions')
        print(f"Caricate {len(df)} nuove SP/Functions")
        return df
    except Exception as e:
        print(f"ATTENZIONE: Non posso caricare nuove SP/Functions: {e}")
        return pd.DataFrame()

def create_statistics(df_critical, df_tables, df_new_tables, df_new_sp):
    """Crea sheet con statistiche riepilogative."""
    
    # Conta per tipo di oggetto
    type_counts = df_critical['ObjectType'].value_counts()
    
    # Conta per criticità tecnica
    tech_counts = df_critical['Criticità_Tecnica'].value_counts()
    
    # Conta tipi nelle nuove SP/Functions se esiste la colonna ObjectType
    new_sp_type_counts = {}
    if 'ObjectType' in df_new_sp.columns and not df_new_sp.empty:
        new_sp_type_counts = df_new_sp['ObjectType'].value_counts()
    
    stats = [
        {'Metrica': 'OGGETTI CRITICI', 'Valore': ''},
        {'Metrica': 'Totale Oggetti Critici', 'Valore': len(df_critical)},
        {'Metrica': '  - Stored Procedures', 'Valore': type_counts.get('SQL_STORED_PROCEDURE', 0)},
        {'Metrica': '  - Trigger', 'Valore': type_counts.get('SQL_TRIGGER', 0)},
        {'Metrica': '  - Scalar Functions', 'Valore': type_counts.get('SQL_SCALAR_FUNCTION', 0)},
        {'Metrica': '  - Table-Valued Functions', 'Valore': type_counts.get('SQL_TABLE_VALUED_FUNCTION', 0)},
        {'Metrica': '', 'Valore': ''},
        {'Metrica': 'CRITICITÀ TECNICA', 'Valore': ''},
        {'Metrica': '  - Alta', 'Valore': tech_counts.get('ALTA', 0)},
        {'Metrica': '  - Media', 'Valore': tech_counts.get('MEDIA', 0)},
        {'Metrica': '  - Bassa', 'Valore': tech_counts.get('BASSA', 0)},
        {'Metrica': '', 'Valore': ''},
        {'Metrica': 'COMPLESSITÀ', 'Valore': ''},
        {'Metrica': 'Score Medio Complessità', 'Valore': f"{df_critical['Complessità_Score'].mean():.1f}"},
        {'Metrica': 'DML Operations Medie', 'Valore': f"{df_critical['DML_Count'].mean():.1f}"},
        {'Metrica': 'Righe Codice Medie', 'Valore': f"{df_critical['Righe_Codice'].mean():.0f}"},
        {'Metrica': '', 'Valore': ''},
        {'Metrica': 'DIPENDENZE', 'Valore': ''},
        {'Metrica': 'Tabelle Totali Referenziate', 'Valore': len(df_tables)},
        {'Metrica': 'Nuove Tabelle da Analizzare', 'Valore': len(df_new_tables)},
        {'Metrica': 'Nuove SP/Functions da Analizzare', 'Valore': len(df_new_sp)},
        {'Metrica': '  - Stored Procedures', 'Valore': new_sp_type_counts.get('SQL_STORED_PROCEDURE', 0)},
        {'Metrica': '  - Scalar Functions', 'Valore': new_sp_type_counts.get('SQL_SCALAR_FUNCTION', 0)},
        {'Metrica': '  - Table-Valued Functions', 'Valore': new_sp_type_counts.get('SQL_TABLE_VALUED_FUNCTION', 0)},
        {'Metrica': '  - Triggers', 'Valore': new_sp_type_counts.get('SQL_TRIGGER', 0)},
        {'Metrica': 'Dipendenze Medie per Oggetto', 'Valore': f"{df_critical['N_Dipendenze_Totali'].mean():.1f}"},
    ]
    
    return pd.DataFrame(stats)

def create_denormalized_dependencies_sheet(df_critical):
    """Crea sheet denormalizzato dove ogni dipendenza ha una riga separata."""
    if df_critical.empty:
        return pd.DataFrame()
    
    denorm_rows = []
    
    for idx, row in df_critical.iterrows():
        server = row.get('Server', '')
        database = row.get('Database', '')
        object_name = row.get('ObjectName', '')
        object_type = row.get('ObjectType', '')
        critico = row.get('Critico_Migrazione', '')
        dependencies_str = row.get('Dipendenze', '')
        
        # Parse dipendenze
        if pd.isna(dependencies_str) or not isinstance(dependencies_str, str) or dependencies_str.lower() == 'nessuna':
            # Oggetto senza dipendenze - crea comunque una riga
            denorm_rows.append({
                'Server': server,
                'Database': database,
                'ObjectName': object_name,
                'ObjectType_Parent': object_type,
                'Dipendenza': 'NESSUNA',
                'ObjectType_Dipendenza': '',
                'Critico_Migrazione': critico
            })
        else:
            # Split dipendenze per ';'
            deps = [d.strip() for d in dependencies_str.split(';') if d.strip()]
            
            for dep in deps:
                dep_type = classify_dependency_type(dep)
                
                denorm_rows.append({
                    'Server': server,
                    'Database': database,
                    'ObjectName': object_name,
                    'ObjectType_Parent': object_type,
                    'Dipendenza': dep,
                    'ObjectType_Dipendenza': dep_type,
                    'Critico_Migrazione': critico
                })
    
    result_df = pd.DataFrame(denorm_rows)
    
    # Ordina per Server, Database, ObjectName, Dipendenza
    result_df = result_df.sort_values(['Server', 'Database', 'ObjectName', 'Dipendenza'])
    
    return result_df

# =========================
# MAIN
# =========================

def main():
    print("=== Consolidamento Report Finale Migrazione ===\n")
    
    # 1. Carica oggetti critici
    print("1. Caricamento oggetti critici...")
    df_critical = load_critical_objects()
    
    if df_critical.empty:
        print("ERRORE: Nessun oggetto critico trovato!")
        return
    
    # 2. Crea sheet principale
    print("2. Creazione sheet oggetti critici...")
    main_sheet = create_main_sheet(df_critical)
    
    # 3. Estrai tabelle
    print("3. Estrazione tabelle referenziate...")
    tables_sheet = extract_all_tables(df_critical)
    
    # 4. Carica nuove tabelle
    print("4. Caricamento nuove tabelle...")
    new_tables_sheet = load_new_tables()
    
    # 5. Carica nuove SP/Functions
    print("5. Caricamento nuove SP/Functions...")
    new_sp_sheet = load_new_sp_functions()
    
    # 6. Crea statistiche
    print("6. Creazione statistiche...")
    stats_sheet = create_statistics(main_sheet, tables_sheet, new_tables_sheet, new_sp_sheet)
    
    # 7. Crea sheet denormalizzato dipendenze
    print("7. Creazione sheet denormalizzato dipendenze...")
    denorm_sheet = create_denormalized_dependencies_sheet(df_critical)
    
    # 8. Esporta tutto
    print(f"\n8. Esportazione report finale: {OUTPUT_FILE}")
    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        main_sheet.to_excel(writer, sheet_name='Oggetti Critici', index=False)
        denorm_sheet.to_excel(writer, sheet_name='Dipendenze Dettagliate', index=False)
        tables_sheet.to_excel(writer, sheet_name='Tabelle Referenziate', index=False)
        new_tables_sheet.to_excel(writer, sheet_name='Nuove Tabelle', index=False)
        new_sp_sheet.to_excel(writer, sheet_name='Nuove SP-Functions', index=False)
        stats_sheet.to_excel(writer, sheet_name='Statistiche', index=False)
    
    print("\n=== Report Finale Completato ===")
    print(f"\nRiepilogo:")
    print(f"  - Oggetti critici: {len(main_sheet)}")
    print(f"  - Dipendenze dettagliate (righe): {len(denorm_sheet)}")
    print(f"  - Tabelle referenziate: {len(tables_sheet)}")
    print(f"  - Nuove tabelle: {len(new_tables_sheet)}")
    print(f"  - Nuove SP/Functions: {len(new_sp_sheet)}")
    print(f"\nFile creato: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
