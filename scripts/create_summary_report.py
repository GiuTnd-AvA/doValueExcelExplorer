"""
Script per generare un report di summary consolidato con 3 sheet (L1, L2, L3)
Ogni sheet contiene: Server | DB | Tabella origine | Oggetti associati | Tipo oggetto | 
                     Dipendenze Oggetti | Tipo oggetti | Dipendenze Tabelle
"""

import pandas as pd
import sys
from pathlib import Path

# Aggiungi la directory principale al path per importare i moduli
current_dir = Path(__file__).resolve().parent
root_dir = current_dir.parent
sys.path.insert(0, str(root_dir))

def create_summary_report():
    """
    Crea un file Excel di summary consolidato con 3 sheet (L1, L2, L3)
    """
    
    # Percorsi dei file input
    base_path = Path(r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL")
    
    analisi_path = base_path / "analisi_oggetti_critici.xlsx"
    l2_path = base_path / "DIPENDENZE_LIVELLO_2.xlsx"
    l3_path = base_path / "DIPENDENZE_LIVELLO_3.xlsx"
    l4_path = base_path / "DIPENDENZE_LIVELLO_4.xlsx"
    
    print("=" * 80)
    print("CREAZIONE REPORT DI SUMMARY")
    print("=" * 80)
    
    # Verifica esistenza file obbligatori
    for path, name in [(analisi_path, "Analisi"), (l2_path, "L2")]:
        if not path.exists():
            print(f"❌ File non trovato: {path}")
            return
        print(f"✓ File trovato: {name}")
    
    # Verifica L3 (opzionale)
    has_l3 = l3_path.exists()
    if has_l3:
        print(f"✓ File trovato: L3")
    else:
        print(f"⚠ File L3 non trovato (verrà saltato): {l3_path}")
    
    # Verifica L4 (opzionale)
    has_l4 = l4_path.exists()
    if has_l4:
        print(f"✓ File trovato: L4")
    else:
        print(f"⚠ File L4 non trovato (verrà saltato): {l4_path}")
    
    print("\n" + "=" * 80)
    print("CARICAMENTO DATI")
    print("=" * 80)
    
    # Carica i dati dai file DIPENDENZE (L1 già contiene solo critici)
    df_l1 = pd.read_excel(l2_path, sheet_name="Oggetti Livello 1")
    df_l2 = pd.read_excel(l2_path, sheet_name="Oggetti Livello 2")
    
    # Carica sheet esplosi L1 e L2
    try:
        df_l1_tab_espl = pd.read_excel(l2_path, sheet_name="L1_Oggetti_Tabelle_Esploso")
        df_l1_obj_espl = pd.read_excel(l2_path, sheet_name="L1_Oggetti_Oggetti_Esploso")
        print(f"✓ L1 Tabelle Esploso: {len(df_l1_tab_espl)} relazioni")
        print(f"✓ L1 Oggetti Esploso: {len(df_l1_obj_espl)} relazioni")
    except:
        df_l1_tab_espl = pd.DataFrame()
        df_l1_obj_espl = pd.DataFrame()
        print(f"⚠ Sheet esplosi L1 non trovati")
    
    try:
        df_l2_tab_espl = pd.read_excel(l2_path, sheet_name="L2_Oggetti_Tabelle_Esploso")
        df_l2_obj_espl = pd.read_excel(l2_path, sheet_name="L2_Oggetti_Oggetti_Esploso")
        print(f"✓ L2 Tabelle Esploso: {len(df_l2_tab_espl)} relazioni")
        print(f"✓ L2 Oggetti Esploso: {len(df_l2_obj_espl)} relazioni")
    except:
        df_l2_tab_espl = pd.DataFrame()
        df_l2_obj_espl = pd.DataFrame()
        print(f"⚠ Sheet esplosi L2 non trovati")
    
    if has_l3:
        df_l3 = pd.read_excel(l3_path, sheet_name="Oggetti Livello 3")
        print(f"✓ Oggetti L3: {len(df_l3)}")
        # Carica sheet esplosi L3
        try:
            df_l3_tab_espl = pd.read_excel(l3_path, sheet_name="L3_Oggetti_Tabelle_Esploso")
            df_l3_obj_espl = pd.read_excel(l3_path, sheet_name="L3_Oggetti_Oggetti_Esploso")
            print(f"✓ L3 Tabelle Esploso: {len(df_l3_tab_espl)} relazioni")
            print(f"✓ L3 Oggetti Esploso: {len(df_l3_obj_espl)} relazioni")
        except:
            df_l3_tab_espl = pd.DataFrame()
            df_l3_obj_espl = pd.DataFrame()
    else:
        df_l3 = None
        df_l3_tab_espl = pd.DataFrame()
        df_l3_obj_espl = pd.DataFrame()
    
    if has_l4:
        df_l4 = pd.read_excel(l4_path, sheet_name="Oggetti Livello 4")
        print(f"✓ Oggetti L4: {len(df_l4)}")
        # Carica sheet esplosi L4
        try:
            df_l4_tab_espl = pd.read_excel(l4_path, sheet_name="L4_Oggetti_Tabelle_Esploso")
            df_l4_obj_espl = pd.read_excel(l4_path, sheet_name="L4_Oggetti_Oggetti_Esploso")
            print(f"✓ L4 Tabelle Esploso: {len(df_l4_tab_espl)} relazioni")
            print(f"✓ L4 Oggetti Esploso: {len(df_l4_obj_espl)} relazioni")
        except:
            df_l4_tab_espl = pd.DataFrame()
            df_l4_obj_espl = pd.DataFrame()
    else:
        df_l4 = None
        df_l4_tab_espl = pd.DataFrame()
        df_l4_obj_espl = pd.DataFrame()
    
    print(f"✓ Oggetti L1: {len(df_l1)}")
    print(f"✓ Oggetti L2: {len(df_l2)}")
    
    print("\n" + "=" * 80)
    print("CREAZIONE SHEET CONTEGGI")
    print("=" * 80)
    
    # Calcola statistiche aggregate da dataframe
    conteggi_rows = []
    
    # Header
    conteggi_rows.append({'Livello': 'SOMMARIO', 'Metrica': 'Oggetti Totali', 'Valore': '', 'Dettaglio': ''})
    
    # Conta oggetti per livello
    for level, df_level in [('L1', df_l1), ('L2', df_l2), ('L3', df_l3), ('L4', df_l4)]:
        if df_level is not None and len(df_level) > 0:
            count_total = len(df_level)
            conteggi_rows.append({
                'Livello': level,
                'Metrica': 'Numero Oggetti',
                'Valore': count_total,
                'Dettaglio': f'{count_total} oggetti SQL'
            })
            
            # Conta per tipo oggetto
            if 'ObjectType' in df_level.columns:
                type_counts = df_level['ObjectType'].value_counts()
                for obj_type, count in type_counts.items():
                    conteggi_rows.append({
                        'Livello': level,
                        'Metrica': f'  - {obj_type}',
                        'Valore': count,
                        'Dettaglio': ''
                    })
    
    # Header dipendenze
    conteggi_rows.append({'Livello': '', 'Metrica': '', 'Valore': '', 'Dettaglio': ''})
    conteggi_rows.append({'Livello': 'DIPENDENZE', 'Metrica': 'Relazioni Tabelle/Oggetti', 'Valore': '', 'Dettaglio': ''})
    
    # Conta dipendenze da exploded sheets
    for level, df_tab, df_obj in [
        ('L1', df_l1_tab_espl, df_l1_obj_espl),
        ('L2', df_l2_tab_espl, df_l2_obj_espl),
        ('L3', df_l3_tab_espl, df_l3_obj_espl),
        ('L4', df_l4_tab_espl, df_l4_obj_espl)
    ]:
        if df_tab is not None and len(df_tab) > 0:
            count_tab_relations = len(df_tab)
            unique_tables = df_tab['Tabella_Dipendente'].nunique() if 'Tabella_Dipendente' in df_tab.columns else 0
            conteggi_rows.append({
                'Livello': level,
                'Metrica': 'Relazioni → Tabelle',
                'Valore': count_tab_relations,
                'Dettaglio': f'{unique_tables} tabelle uniche'
            })
        
        if df_obj is not None and len(df_obj) > 0:
            count_obj_relations = len(df_obj)
            unique_objects = df_obj['Oggetto_Dipendente'].nunique() if 'Oggetto_Dipendente' in df_obj.columns else 0
            conteggi_rows.append({
                'Livello': level,
                'Metrica': 'Relazioni → Oggetti SQL',
                'Valore': count_obj_relations,
                'Dettaglio': f'{unique_objects} oggetti unici'
            })
    
    df_conteggi = pd.DataFrame(conteggi_rows)
    print(f"✓ Sheet Conteggi creato: {len(df_conteggi)} righe di statistiche")
    
    # Prepara i nomi degli sheet per l'export
    sheets_to_export = []
    
    # L1 + exploded sheets
    sheets_to_export.append(('L1', df_l1))
    if df_l1_tab_espl is not None and len(df_l1_tab_espl) > 0:
        sheets_to_export.append(('Tabelle_Esplose_L1', df_l1_tab_espl))
    if df_l1_obj_espl is not None and len(df_l1_obj_espl) > 0:
        sheets_to_export.append(('Oggetti_Esplosi_L1', df_l1_obj_espl))
    
    # L2 + exploded sheets
    sheets_to_export.append(('L2', df_l2))
    if df_l2_tab_espl is not None and len(df_l2_tab_espl) > 0:
        sheets_to_export.append(('Tabelle_Esplose_L2', df_l2_tab_espl))
    if df_l2_obj_espl is not None and len(df_l2_obj_espl) > 0:
        sheets_to_export.append(('Oggetti_Esplosi_L2', df_l2_obj_espl))
    
    # L3 + exploded sheets
    if has_l3 and df_l3 is not None and len(df_l3) > 0:
        sheets_to_export.append(('L3', df_l3))
        if df_l3_tab_espl is not None and len(df_l3_tab_espl) > 0:
            sheets_to_export.append(('Tabelle_Esplose_L3', df_l3_tab_espl))
        if df_l3_obj_espl is not None and len(df_l3_obj_espl) > 0:
            sheets_to_export.append(('Oggetti_Esplosi_L3', df_l3_obj_espl))
    
    # L4 + exploded sheets
    if has_l4 and df_l4 is not None and len(df_l4) > 0:
        sheets_to_export.append(('L4', df_l4))
        if df_l4_tab_espl is not None and len(df_l4_tab_espl) > 0:
            sheets_to_export.append(('Tabelle_Esplose_L4', df_l4_tab_espl))
        if df_l4_obj_espl is not None and len(df_l4_obj_espl) > 0:
            sheets_to_export.append(('Oggetti_Esplosi_L4', df_l4_obj_espl))
    
    # Conteggi come ultimo sheet
    sheets_to_export.append(('Conteggi', df_conteggi))
    
    print(f"✓ Preparati {len(sheets_to_export)} sheet per l'export")
    
    print("\n" + "=" * 80)
    print("SALVATAGGIO FILE EXCEL")
    print("=" * 80)
    
    # Salva il file Excel
    output_path = base_path / "SUMMARY_REPORT.xlsx"
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for sheet_name, df_sheet in sheets_to_export:
            if df_sheet is not None and len(df_sheet) > 0:
                df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"  ✓ Sheet '{sheet_name}': {len(df_sheet)} righe")
    
    print(f"\n✓ File salvato: {output_path}")
    
    print("\n" + "=" * 80)
    print("STATISTICHE FINALI")
    print("=" * 80)
    
    # Calcola totale oggetti
    total_objects = 0
    if df_l1 is not None: total_objects += len(df_l1)
    if df_l2 is not None: total_objects += len(df_l2)
    if df_l3 is not None: total_objects += len(df_l3)
    if df_l4 is not None: total_objects += len(df_l4)
    
    # Calcola totale relazioni
    total_relations = 0
    for _, df_sheet in sheets_to_export:
        if df_sheet is not None and 'Esplose' in _:
            total_relations += len(df_sheet)
    
    print(f"Oggetti unici totali: {total_objects}")
    print(f"Relazioni di dipendenza: {total_relations}")
    print(f"Sheet totali nel report: {len(sheets_to_export)}")
    print("\n✅ Report di summary completato con successo!")


if __name__ == "__main__":
    try:
        create_summary_report()
    except Exception as e:
        print(f"\n❌ Errore durante l'esecuzione: {str(e)}")
        import traceback
        traceback.print_exc()
