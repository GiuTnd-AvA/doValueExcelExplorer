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

def find_new_objects(tables, dependencies):
    """Trova oggetti in dipendenze che non sono nelle tabelle."""
    new_objects = dependencies - tables
    return sorted(new_objects)

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
        print("\nConfronting dipendenze con tabelle...")
        new_objects = find_new_objects(tables, dependencies)
        print(f"  - Nuovi oggetti da analizzare: {len(new_objects)}\n")
        
        if new_objects:
            # Crea DataFrame con risultati
            result_df = pd.DataFrame({
                'Nuovo_Oggetto': new_objects,
                'Tipo_Stimato': ['Function/SP' for _ in new_objects],
                'Note': ['Da verificare e analizzare' for _ in new_objects]
            })
            
            # Esporta risultati
            input_path = Path(INPUT_FILE)
            output_path = input_path.parent / f"{input_path.stem}{OUTPUT_SUFFIX}.xlsx"
            
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                result_df.to_excel(writer, sheet_name='Nuove Dipendenze', index=False)
                
                # Aggiungi anche statistiche
                stats_df = pd.DataFrame({
                    'Metrica': ['Tabelle Analizzate', 'Dipendenze Totali', 'Nuovi Oggetti', 'Percentuale Nuovi'],
                    'Valore': [len(tables), len(dependencies), len(new_objects), 
                              f"{len(new_objects)/len(dependencies)*100:.1f}%" if dependencies else "0%"]
                })
                stats_df.to_excel(writer, sheet_name='Statistiche', index=False)
            
            print(f"Risultati esportati in: {output_path}\n")
            
            # Mostra primi 10 nuovi oggetti
            print("Primi 10 nuovi oggetti trovati:")
            for i, obj in enumerate(new_objects[:10], 1):
                print(f"  {i}. {obj}")
            
            if len(new_objects) > 10:
                print(f"  ... e altri {len(new_objects) - 10} oggetti")
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
