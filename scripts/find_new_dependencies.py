# =========================
# IMPORT
# =========================
import pandas as pd
from pathlib import Path

# =========================
# CONFIG
# =========================
INPUT_FILE = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\analisi_sqldefinition_criticità.xlsx'
SHEET_NAME = 0  # Primo sheet
OUTPUT_SUFFIX = "_nuove_dipendenze"

# =========================
# FUNZIONI
# =========================

def extract_unique_tables(df, column_name='Table'):
    """Estrae tutte le tabelle uniche dalla colonna specificata."""
    if column_name not in df.columns:
        print(f"ATTENZIONE: Colonna '{column_name}' non trovata!")
        return set()
    
    tables = set()
    for value in df[column_name].dropna():
        if isinstance(value, str):
            tables.add(value.strip().lower())
    
    return tables

def extract_dependencies(df, column_name='Dipendenze'):
    """Estrae tutte le dipendenze dalla colonna, gestendo valori separati da punto e virgola."""
    if column_name not in df.columns:
        print(f"ATTENZIONE: Colonna '{column_name}' non trovata!")
        return set()
    
    dependencies = set()
    for value in df[column_name].dropna():
        if isinstance(value, str):
            # Split per ';' se ci sono più dipendenze nella stessa cella
            deps = value.split(';')
            for dep in deps:
                dep_clean = dep.strip().lower()
                if dep_clean and dep_clean != 'nessuna':
                    dependencies.add(dep_clean)
    
    return dependencies

def classify_object_type(obj_name):
    """Classifica un oggetto come SP/Function o Tabella basandosi sul nome."""
    obj_lower = obj_name.lower()
    
    # Pattern comuni per SP e Functions
    sp_function_patterns = [
        'sp_', 'usp_', 'asp_', 'proc_', 'p_',  # Stored Procedures
        'fn_', 'udf_', 'tf_', 'if_', 'f_',     # Functions
        '_sp_', '_fn_', '_udf_'                # Pattern nel mezzo
    ]
    
    # Se contiene uno di questi pattern, è probabilmente SP/Function
    for pattern in sp_function_patterns:
        if pattern in obj_lower:
            return 'SP/Function'
    
    # Se ha parentesi quadre e inizia con schema dbo./schema., è più probabile una Function/SP
    if 'dbo.' in obj_lower or '[dbo].' in obj_lower:
        # Verifica se ha pattern tipici di programmable objects
        if any(p in obj_lower for p in ['get', 'set', 'calc', 'exec', 'run', 'process']):
            return 'SP/Function'
    
    # Default: probabilmente una tabella
    return 'Tabella'

def find_new_objects(tables, dependencies):
    """Trova oggetti in dipendenze che non sono nelle tabelle e li classifica."""
    new_objects = dependencies - tables
    
    # Classifica ogni oggetto
    classified = {
        'tables': [],
        'sp_functions': []
    }
    
    for obj in sorted(new_objects):
        obj_type = classify_object_type(obj)
        if obj_type == 'Tabella':
            classified['tables'].append(obj)
        else:
            classified['sp_functions'].append(obj)
    
    return classified

# =========================
# MAIN
# =========================

def main():
    print("=== Analisi Nuove Dipendenze ===\n")
    
    try:
        # Leggi file Excel
        print(f"Lettura file: {INPUT_FILE}")
        df = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME)
        print(f"Righe lette: {len(df)}")
        print(f"Colonne: {', '.join(df.columns)}\n")
        
        # Estrai tabelle e dipendenze
        print("Estrazione tabelle uniche...")
        tables = extract_unique_tables(df, 'Table')
        print(f"  - Tabelle trovate: {len(tables)}")
        
        print("\nEstrazione dipendenze...")
        dependencies = extract_dependencies(df, 'Dipendenze')
        print(f"  - Dipendenze totali: {len(dependencies)}")
        
        # Trova nuovi oggetti
        print("\nConfrontando dipendenze con tabelle...")
        classified_objects = find_new_objects(tables, dependencies)
        new_tables = classified_objects['tables']
        new_sp_functions = classified_objects['sp_functions']
        
        print(f"  - Nuove TABELLE da analizzare: {len(new_tables)}")
        print(f"  - Nuove SP/FUNCTIONS da analizzare: {len(new_sp_functions)}\n")
        
        if new_tables or new_sp_functions:
            # Esporta risultati
            input_path = Path(INPUT_FILE)
            output_path = input_path.parent / f"{input_path.stem}{OUTPUT_SUFFIX}.xlsx"
            
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Sheet 1: Nuove Tabelle
                if new_tables:
                    tables_df = pd.DataFrame({
                        'Nuova_Tabella': new_tables,
                        'Azione': ['Aggiungere all\'estrazione' for _ in new_tables],
                        'Note': ['Tabella referenziata ma non analizzata' for _ in new_tables]
                    })
                    tables_df.to_excel(writer, sheet_name='Nuove Tabelle', index=False)
                
                # Sheet 2: Nuove SP/Functions
                if new_sp_functions:
                    sp_df = pd.DataFrame({
                        'Nuovo_Oggetto': new_sp_functions,
                        'Tipo_Stimato': ['SP/Function' for _ in new_sp_functions],
                        'Azione': ['Analizzare dipendenze' for _ in new_sp_functions],
                        'Note': ['Oggetto chiamato da altri ma non estratto' for _ in new_sp_functions]
                    })
                    sp_df.to_excel(writer, sheet_name='Nuove SP-Functions', index=False)
                
                # Sheet 3: Statistiche
                stats_df = pd.DataFrame({
                    'Metrica': [
                        'Tabelle Analizzate', 
                        'Dipendenze Totali', 
                        'Nuove Tabelle',
                        'Nuove SP/Functions',
                        '% Nuove Tabelle',
                        '% Nuove SP/Functions'
                    ],
                    'Valore': [
                        len(tables), 
                        len(dependencies), 
                        len(new_tables),
                        len(new_sp_functions),
                        f"{len(new_tables)/len(dependencies)*100:.1f}%" if dependencies else "0%",
                        f"{len(new_sp_functions)/len(dependencies)*100:.1f}%" if dependencies else "0%"
                    ]
                })
                stats_df.to_excel(writer, sheet_name='Statistiche', index=False)
            
            print(f"Risultati esportati in: {output_path}\n")
            
            # Mostra risultati
            if new_tables:
                print(f"Prime 10 NUOVE TABELLE:")
                for i, obj in enumerate(new_tables[:10], 1):
                    print(f"  {i}. {obj}")
                if len(new_tables) > 10:
                    print(f"  ... e altre {len(new_tables) - 10} tabelle\n")
            
            if new_sp_functions:
                print(f"Prime 10 NUOVE SP/FUNCTIONS:")
                for i, obj in enumerate(new_sp_functions[:10], 1):
                    print(f"  {i}. {obj}")
                if len(new_sp_functions) > 10:
                    print(f"  ... e altri {len(new_sp_functions) - 10} oggetti\n")
        else:
            print("Nessun nuovo oggetto trovato! Tutte le dipendenze sono già nella lista delle tabelle analizzate.")
    
    except FileNotFoundError:
        print(f"ERRORE: File non trovato: {INPUT_FILE}")
        print("Verifica che il percorso sia corretto e che il file esista.")
    except Exception as e:
        print(f"ERRORE durante l'elaborazione: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
