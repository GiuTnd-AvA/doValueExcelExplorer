"""
Script per consolidare tutti i file *_analyzed.xlsx in un unico file analisi_oggetti_critici.xlsx
"""

import pandas as pd
from pathlib import Path

# =========================
# CONFIG
# =========================
BASE_PATH = Path(r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL')
ANALYZED_PATTERN = "*_analyzed.xlsx"
OUTPUT_FILE = BASE_PATH / "analisi_oggetti_critici.xlsx"

def main():
    print("=" * 80)
    print("CONSOLIDAMENTO FILE ANALIZZATI")
    print("=" * 80)
    
    # Trova tutti i file _analyzed.xlsx
    print(f"\n1. Ricerca file con pattern: {ANALYZED_PATTERN}")
    print(f"   Directory: {BASE_PATH}\n")
    
    analyzed_files = list(BASE_PATH.glob(ANALYZED_PATTERN))
    
    if not analyzed_files:
        print(f"❌ ERRORE: Nessun file trovato con pattern '{ANALYZED_PATTERN}'")
        print(f"   Verifica che analyze_sql_complexity.py sia stato eseguito")
        return
    
    print(f"✓ Trovati {len(analyzed_files)} file da consolidare:")
    for f in sorted(analyzed_files):
        print(f"  - {f.name}")
    
    # Carica e concatena tutti i file
    print(f"\n2. Caricamento e concatenamento file...")
    
    all_dataframes_oggetti = []
    all_dataframes_tabelle = []
    all_dataframes_oggetti_dep = []
    total_rows = 0
    
    for file_path in sorted(analyzed_files):
        try:
            # Sheet principale "Oggetti"
            df_oggetti = pd.read_excel(file_path, sheet_name='Oggetti')
            all_dataframes_oggetti.append(df_oggetti)
            total_rows += len(df_oggetti)
            print(f"  ✓ {file_path.name}: {len(df_oggetti)} oggetti")
            
            # Sheet "Oggetti_Tabelle_Esploso" (se esiste)
            try:
                df_tabelle = pd.read_excel(file_path, sheet_name='Oggetti_Tabelle_Esploso')
                all_dataframes_tabelle.append(df_tabelle)
                print(f"    - Oggetti_Tabelle_Esploso: {len(df_tabelle)} relazioni")
            except:
                pass
            
            # Sheet "Oggetti_Oggetti_Esploso" (se esiste)
            try:
                df_oggetti_dep = pd.read_excel(file_path, sheet_name='Oggetti_Oggetti_Esploso')
                all_dataframes_oggetti_dep.append(df_oggetti_dep)
                print(f"    - Oggetti_Oggetti_Esploso: {len(df_oggetti_dep)} relazioni")
            except:
                pass
                
        except Exception as e:
            print(f"  ❌ ERRORE caricamento {file_path.name}: {e}")
    
    if not all_dataframes_oggetti:
        print("\n❌ ERRORE: Nessun file caricato con successo")
        return
    
    # Concatena tutti i DataFrame
    print(f"\n3. Concatenamento DataFrame...")
    df_consolidated = pd.concat(all_dataframes_oggetti, ignore_index=True)
    print(f"  ✓ Sheet Oggetti: {len(df_consolidated)} righe")
    
    df_consolidated_tabelle = pd.DataFrame()
    if all_dataframes_tabelle:
        df_consolidated_tabelle = pd.concat(all_dataframes_tabelle, ignore_index=True)
        print(f"  ✓ Sheet Oggetti_Tabelle_Esploso: {len(df_consolidated_tabelle)} relazioni")
    
    df_consolidated_oggetti = pd.DataFrame()
    if all_dataframes_oggetti_dep:
        df_consolidated_oggetti = pd.concat(all_dataframes_oggetti_dep, ignore_index=True)
        print(f"  ✓ Sheet Oggetti_Oggetti_Esploso: {len(df_consolidated_oggetti)} relazioni")
    
    # Verifica colonne critiche
    print(f"\n4. Verifica colonne critiche...")
    required_cols = ['ObjectName', 'Dipendenze_Tabelle', 'Dipendenze_Oggetti', 
                     'Critico_Migrazione', 'Server', 'Database']
    
    missing_cols = [col for col in required_cols if col not in df_consolidated.columns]
    
    if missing_cols:
        print(f"  ⚠ ATTENZIONE: Colonne mancanti: {', '.join(missing_cols)}")
    else:
        print(f"  ✓ Tutte le colonne critiche presenti")
    
    # Rimuovi duplicati per ObjectName (se presenti)
    initial_count = len(df_consolidated)
    df_consolidated = df_consolidated.drop_duplicates(subset=['ObjectName'], keep='first')
    duplicates_removed = initial_count - len(df_consolidated)
    
    if duplicates_removed > 0:
        print(f"\n  ⚠ Rimossi {duplicates_removed} duplicati per ObjectName")
        print(f"  ✓ Righe uniche finali: {len(df_consolidated)}")
    
    # Statistiche pre-export
    print(f"\n5. Statistiche consolidate:")
    if 'Critico_Migrazione' in df_consolidated.columns:
        critici = len(df_consolidated[df_consolidated['Critico_Migrazione'] == 'SÌ'])
        print(f"  - Oggetti critici per migrazione: {critici}")
    
    if 'Criticità_Tecnica' in df_consolidated.columns:
        crit_counts = df_consolidated['Criticità_Tecnica'].value_counts()
        print(f"  - Criticità Tecnica: ALTA={crit_counts.get('ALTA', 0)}, MEDIA={crit_counts.get('MEDIA', 0)}, BASSA={crit_counts.get('BASSA', 0)}")
    
    if 'ObjectType' in df_consolidated.columns or 'TipoOggetto' in df_consolidated.columns:
        type_col = 'ObjectType' if 'ObjectType' in df_consolidated.columns else 'TipoOggetto'
        type_counts = df_consolidated[type_col].value_counts()
        print(f"  - Distribuzione per tipo:")
        for obj_type, count in type_counts.head(5).items():
            print(f"    * {obj_type}: {count}")
    
    # Export file consolidato
    print(f"\n6. Export file consolidato: {OUTPUT_FILE.name}")
    
    try:
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            # Sheet principale
            df_consolidated.to_excel(writer, sheet_name='Oggetti', index=False)
            
            # Sheet esplosi (se disponibili)
            if len(df_consolidated_tabelle) > 0:
                df_consolidated_tabelle.to_excel(writer, sheet_name='Oggetti_Tabelle_Esploso', index=False)
            
            if len(df_consolidated_oggetti) > 0:
                df_consolidated_oggetti.to_excel(writer, sheet_name='Oggetti_Oggetti_Esploso', index=False)
        
        print(f"  ✓ File salvato con successo")
        print(f"  ✓ Sheet Oggetti: {len(df_consolidated)} righe")
        if len(df_consolidated_tabelle) > 0:
            print(f"  ✓ Sheet Oggetti_Tabelle_Esploso: {len(df_consolidated_tabelle)} relazioni")
        if len(df_consolidated_oggetti) > 0:
            print(f"  ✓ Sheet Oggetti_Oggetti_Esploso: {len(df_consolidated_oggetti)} relazioni")
        print(f"  ✓ Percorso: {OUTPUT_FILE}")
    except Exception as e:
        print(f"  ❌ ERRORE durante export: {e}")
        return
    
    print("\n" + "=" * 80)
    print("✅ CONSOLIDAMENTO COMPLETATO CON SUCCESSO!")
    print("=" * 80)
    print(f"\nFile consolidato: analisi_oggetti_critici.xlsx")
    print(f"Oggetti totali: {len(df_consolidated)}")
    print(f"\n➡️  Ora puoi eseguire: extract_level2_dependencies.py")


if __name__ == "__main__":
    main()
