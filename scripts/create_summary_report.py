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
    
    if has_l3:
        df_l3 = pd.read_excel(l3_path, sheet_name="Oggetti Livello 3")
        print(f"✓ Oggetti L3: {len(df_l3)}")
    else:
        df_l3 = None
    
    if has_l4:
        df_l4 = pd.read_excel(l4_path, sheet_name="Oggetti Livello 4")
        print(f"✓ Oggetti L4: {len(df_l4)}")
    else:
        df_l4 = None
    
    print(f"✓ Oggetti L1: {len(df_l1)}")
    print(f"✓ Oggetti L2: {len(df_l2)}")
    
    print("\n" + "=" * 80)
    print("CREAZIONE SHEET L1")
    print("=" * 80)
    
    # Sheet L1: Oggetti L1 (già solo critici da DIPENDENZE_LIVELLO_2.xlsx)
    summary_l1_rows = []
    
    for _, obj_row in df_l1.iterrows():
        obj_name = obj_row.get('ObjectName', '')
        obj_type = obj_row.get('ObjectType', '')
        obj_server = obj_row.get('Server', '')
        obj_db = obj_row.get('Database', '')
        
        # Dipendenze tabelle
        dip_tabelle = str(obj_row.get('Dipendenze_Tabelle', ''))
        if pd.isna(dip_tabelle) or dip_tabelle == 'nan':
            dip_tabelle = ''
        
        # Dipendenze oggetti
        dip_oggetti = str(obj_row.get('Dipendenze_Oggetti', ''))
        if pd.isna(dip_oggetti) or dip_oggetti == 'nan':
            dip_oggetti = ''
        
        # Estrai tipi oggetti dalle dipendenze
        tipo_oggetti = extract_object_types(dip_oggetti)
        
        # Tabella origine (dalla prima dipendenza tabella se disponibile)
        tabella_origine = ''
        if dip_tabelle and dip_tabelle not in ['Nessuna', '']:
            tabelle_list = [t.strip() for t in dip_tabelle.split(';')]
            if tabelle_list:
                tabella_origine = tabelle_list[0]
        
        summary_l1_rows.append({
            'Server': obj_server,
            'DB': obj_db,
            'Tabella origine': tabella_origine,
            'Oggetti associati': obj_name,
            'Tipo oggetto': obj_type,
            'Dipendenze Oggetti': dip_oggetti,
            'Tipo oggetti': tipo_oggetti,
            'Dipendenze Tabelle': dip_tabelle
        })
    
    df_summary_l1 = pd.DataFrame(summary_l1_rows)
    print(f"✓ Righe create per L1: {len(df_summary_l1)}")
    
    print("\n" + "=" * 80)
    print("CREAZIONE SHEET L2")
    print("=" * 80)
    
    # Sheet L2: Oggetti L1 -> Oggetti L2
    summary_l2_rows = []
    
    for _, obj_l2_row in df_l2.iterrows():
        obj_name = obj_l2_row.get('ObjectName', '')
        obj_type = obj_l2_row.get('ObjectType', '')
        obj_server = obj_l2_row.get('Server', '')
        obj_db = obj_l2_row.get('Database', '')
        
        # Oggetti chiamanti L1
        chiamanti = str(obj_l2_row.get('Oggetti_Chiamanti_L1', ''))
        if pd.isna(chiamanti) or chiamanti == 'nan':
            chiamanti = ''
        
        # Dipendenze oggetti
        dip_oggetti = str(obj_l2_row.get('Dipendenze_Oggetti', ''))
        if pd.isna(dip_oggetti) or dip_oggetti == 'nan':
            dip_oggetti = ''
        
        # Dipendenze tabelle
        dip_tabelle = str(obj_l2_row.get('Dipendenze_Tabelle', ''))
        if pd.isna(dip_tabelle) or dip_tabelle == 'nan':
            dip_tabelle = ''
        
        # Estrai tipi oggetti
        tipo_oggetti = extract_object_types(dip_oggetti)
        
        # Se ha chiamanti, crea una riga per ogni chiamante
        if chiamanti:
            for chiamante in chiamanti.split(','):
                chiamante = chiamante.strip()
                if chiamante:
                    summary_l2_rows.append({
                        'Server': obj_server,
                        'DB': obj_db,
                        'Tabella origine': chiamante,  # L'oggetto L1 chiamante
                        'Oggetti associati': obj_name,
                        'Tipo oggetto': obj_type,
                        'Dipendenze Oggetti': dip_oggetti,
                        'Tipo oggetti': tipo_oggetti,
                        'Dipendenze Tabelle': dip_tabelle
                    })
        else:
            # Nessun chiamante specifico
            summary_l2_rows.append({
                'Server': obj_server,
                'DB': obj_db,
                'Tabella origine': '',
                'Oggetti associati': obj_name,
                'Tipo oggetto': obj_type,
                'Dipendenze Oggetti': dip_oggetti,
                'Tipo oggetti': tipo_oggetti,
                'Dipendenze Tabelle': dip_tabelle
            })
    
    df_summary_l2 = pd.DataFrame(summary_l2_rows)
    print(f"✓ Righe create per L2: {len(df_summary_l2)}")
    
    print("\n" + "=" * 80)
    print("CREAZIONE SHEET L3")
    print("=" * 80)
    
    # Sheet L3: Oggetti L2 -> Oggetti L3
    summary_l3_rows = []
    
    if has_l3 and df_l3 is not None:
        for _, obj_l3_row in df_l3.iterrows():
            obj_name = obj_l3_row.get('ObjectName', '')
            obj_type = obj_l3_row.get('ObjectType', '')
            obj_server = obj_l3_row.get('Server', '')
            obj_db = obj_l3_row.get('Database', '')
            
            # Oggetti chiamanti L2
            chiamanti = str(obj_l3_row.get('Oggetti_Chiamanti_L2', ''))
            if pd.isna(chiamanti) or chiamanti == 'nan':
                chiamanti = ''
            
            # Dipendenze oggetti
            dip_oggetti = str(obj_l3_row.get('Dipendenze_Oggetti', ''))
            if pd.isna(dip_oggetti) or dip_oggetti == 'nan':
                dip_oggetti = ''
            
            # Dipendenze tabelle
            dip_tabelle = str(obj_l3_row.get('Dipendenze_Tabelle', ''))
            if pd.isna(dip_tabelle) or dip_tabelle == 'nan':
                dip_tabelle = ''
            
            # Estrai tipi oggetti
            tipo_oggetti = extract_object_types(dip_oggetti)
            
            # Se ha chiamanti, crea una riga per ogni chiamante
            if chiamanti:
                for chiamante in chiamanti.split(','):
                    chiamante = chiamante.strip()
                    if chiamante:
                        summary_l3_rows.append({
                            'Server': obj_server,
                            'DB': obj_db,
                            'Tabella origine': chiamante,  # L'oggetto L2 chiamante
                            'Oggetti associati': obj_name,
                            'Tipo oggetto': obj_type,
                            'Dipendenze Oggetti': dip_oggetti,
                            'Tipo oggetti': tipo_oggetti,
                            'Dipendenze Tabelle': dip_tabelle
                        })
            else:
                # Nessun chiamante specifico
                summary_l3_rows.append({
                    'Server': obj_server,
                    'DB': obj_db,
                    'Tabella origine': '',
                    'Oggetti associati': obj_name,
                    'Tipo oggetto': obj_type,
                    'Dipendenze Oggetti': dip_oggetti,
                    'Tipo oggetti': tipo_oggetti,
                    'Dipendenze Tabelle': dip_tabelle
                })
    else:
        print("⚠ Sheet L3 non disponibile (file mancante)")
    
    df_summary_l3 = pd.DataFrame(summary_l3_rows)
    print(f"✓ Righe create per L3: {len(df_summary_l3)}")
    
    # Sheet L4 (se disponibile)
    if has_l4 and df_l4 is not None:
        print("\n" + "=" * 80)
        print("CREAZIONE SHEET L4")
        print("=" * 80)
        
        summary_l4_rows = []
        for _, obj_l4_row in df_l4.iterrows():
            obj_name = obj_l4_row.get('ObjectName', '')
            obj_type = obj_l4_row.get('ObjectType', '')
            obj_server = obj_l4_row.get('Server', '')
            obj_db = obj_l4_row.get('Database', '')
            
            chiamanti = str(obj_l4_row.get('Oggetti_Chiamanti_L3', ''))
            if pd.isna(chiamanti) or chiamanti == 'nan':
                chiamanti = ''
            
            dip_oggetti = str(obj_l4_row.get('Dipendenze_Oggetti', ''))
            if pd.isna(dip_oggetti) or dip_oggetti == 'nan':
                dip_oggetti = ''
            
            dip_tabelle = str(obj_l4_row.get('Dipendenze_Tabelle', ''))
            if pd.isna(dip_tabelle) or dip_tabelle == 'nan':
                dip_tabelle = ''
            
            tipo_oggetti = extract_object_types(dip_oggetti)
            
            if chiamanti:
                for chiamante in chiamanti.split(','):
                    chiamante = chiamante.strip()
                    if chiamante:
                        summary_l4_rows.append({
                            'Server': obj_server,
                            'DB': obj_db,
                            'Tabella origine': chiamante,
                            'Oggetti associati': obj_name,
                            'Tipo oggetto': obj_type,
                            'Dipendenze Oggetti': dip_oggetti,
                            'Tipo oggetti': tipo_oggetti,
                            'Dipendenze Tabelle': dip_tabelle
                        })
            else:
                summary_l4_rows.append({
                    'Server': obj_server,
                    'DB': obj_db,
                    'Tabella origine': '',
                    'Oggetti associati': obj_name,
                    'Tipo oggetto': obj_type,
                    'Dipendenze Oggetti': dip_oggetti,
                    'Tipo oggetti': tipo_oggetti,
                    'Dipendenze Tabelle': dip_tabelle
                })
        
        df_summary_l4 = pd.DataFrame(summary_l4_rows)
        print(f"✓ Righe create per L4: {len(df_summary_l4)}")
    else:
        df_summary_l4 = pd.DataFrame()
    
    print("\n" + "=" * 80)
    print("CREAZIONE STATISTICHE GLOBALI")
    print("=" * 80)
    
    # Calcola statistiche avanzate
    total_objects = len(df_l1) + len(df_l2)
    if has_l3 and df_l3 is not None:
        total_objects += len(df_l3)
    if has_l4 and df_l4 is not None:
        total_objects += len(df_l4)
    
    # Raggruppa per tipo oggetto
    type_stats = {}
    for df, level in [(df_l1, 'L1'), (df_l2, 'L2'), 
                       (df_l3 if has_l3 else None, 'L3'), 
                       (df_l4 if has_l4 else None, 'L4')]:
        if df is not None and len(df) > 0:
            obj_type_col = 'ObjectType'
            if obj_type_col in df.columns:
                for obj_type in df[obj_type_col].unique():
                    count = len(df[df[obj_type_col] == obj_type])
                    if obj_type not in type_stats:
                        type_stats[obj_type] = {'L1': 0, 'L2': 0, 'L3': 0, 'L4': 0}
                    type_stats[obj_type][level] = count
    
    stats_rows = [
        {'Categoria': 'COPERTURA TOTALE', 'Valore': '', 'Dettaglio': ''},
        {'Categoria': 'Oggetti Totali', 'Valore': total_objects, 'Dettaglio': f'L1+L2+L3+L4'},
        {'Categoria': 'Oggetti L1', 'Valore': len(df_l1), 'Dettaglio': 'Critici iniziali'},
        {'Categoria': 'Oggetti L2', 'Valore': len(df_l2), 'Dettaglio': 'Dipendenze L1'},
    ]
    
    if has_l3 and df_l3 is not None:
        stats_rows.append({'Categoria': 'Oggetti L3', 'Valore': len(df_l3), 'Dettaglio': 'Dipendenze L2'})
    
    if has_l4 and df_l4 is not None:
        stats_rows.append({'Categoria': 'Oggetti L4', 'Valore': len(df_l4), 'Dettaglio': 'Dipendenze L3'})
    
    stats_rows.append({'Categoria': '', 'Valore': '', 'Dettaglio': ''})
    stats_rows.append({'Categoria': 'DISTRIBUZIONE PER TIPO', 'Valore': '', 'Dettaglio': ''})
    
    for obj_type, counts in sorted(type_stats.items()):
        total_type = sum(counts.values())
        detail = ', '.join([f"{k}:{v}" for k, v in counts.items() if v > 0])
        stats_rows.append({'Categoria': obj_type, 'Valore': total_type, 'Dettaglio': detail})
    
    df_stats_global = pd.DataFrame(stats_rows)
    print(f"✓ Statistiche globali create: {len(df_stats_global)} metriche")
    
    print("\n" + "=" * 80)
    print("CREAZIONE SHEET DIPENDENZE RELAZIONI")
    print("=" * 80)
    
    # Crea uno sheet con dipendenze "esplode" - una riga per ogni relazione
    relation_rows = []
    
    # L1: Oggetto → Tabelle/Oggetti dipendenti
    for _, obj_row in df_l1.iterrows():
        obj_name = obj_row.get('ObjectName', '')
        obj_type = obj_row.get('ObjectType', '')
        obj_server = obj_row.get('Server', '')
        obj_db = obj_row.get('Database', '')
        
        # Tabelle
        dip_tabelle = str(obj_row.get('Dipendenze_Tabelle', ''))
        if dip_tabelle and dip_tabelle not in ['nan', 'Nessuna', '']:
            for table in dip_tabelle.split(';'):
                table = table.strip()
                if table:
                    relation_rows.append({
                        'Livello': 'L1',
                        'Server': obj_server,
                        'DB': obj_db,
                        'Oggetto Origine': obj_name,
                        'Tipo Origine': obj_type,
                        'Relazione': 'Dipende da',
                        'Oggetto Destinazione': table,
                        'Tipo Destinazione': 'TABELLA',
                        'Tipo Dipendenza': 'Tabella'
                    })
        
        # Oggetti
        dip_oggetti = str(obj_row.get('Dipendenze_Oggetti', ''))
        if dip_oggetti and dip_oggetti not in ['nan', 'Nessuna', '']:
            for obj in dip_oggetti.split(';'):
                obj = obj.strip()
                if obj:
                    relation_rows.append({
                        'Livello': 'L1',
                        'Server': obj_server,
                        'DB': obj_db,
                        'Oggetto Origine': obj_name,
                        'Tipo Origine': obj_type,
                        'Relazione': 'Dipende da',
                        'Oggetto Destinazione': obj,
                        'Tipo Destinazione': 'OGGETTO SQL',
                        'Tipo Dipendenza': 'Oggetto'
                    })
    
    # L2: Oggetti_Chiamanti_L1 → L2 Object
    for _, obj_row in df_l2.iterrows():
        obj_name = obj_row.get('ObjectName', '')
        obj_type = obj_row.get('ObjectType', '')
        obj_server = obj_row.get('Server', '')
        obj_db = obj_row.get('Database', '')
        
        chiamanti = str(obj_row.get('Oggetti_Chiamanti_L1', ''))
        if chiamanti and chiamanti not in ['nan', '']:
            for chiamante in chiamanti.split(';'):
                chiamante = chiamante.strip()
                if chiamante:
                    relation_rows.append({
                        'Livello': 'L2',
                        'Server': obj_server,
                        'DB': obj_db,
                        'Oggetto Origine': chiamante,
                        'Tipo Origine': 'L1',
                        'Relazione': 'Chiama',
                        'Oggetto Destinazione': obj_name,
                        'Tipo Destinazione': obj_type,
                        'Tipo Dipendenza': 'Chiamata L1→L2'
                    })
    
    # L3: Similar pattern
    if has_l3 and df_l3 is not None:
        for _, obj_row in df_l3.iterrows():
            obj_name = obj_row.get('ObjectName', '')
            obj_type = obj_row.get('ObjectType', '')
            obj_server = obj_row.get('Server', '')
            obj_db = obj_row.get('Database', '')
            
            chiamanti = str(obj_row.get('Oggetti_Chiamanti_L2', ''))
            if chiamanti and chiamanti not in ['nan', '']:
                for chiamante in chiamanti.split(';'):
                    chiamante = chiamante.strip()
                    if chiamante:
                        relation_rows.append({
                            'Livello': 'L3',
                            'Server': obj_server,
                            'DB': obj_db,
                            'Oggetto Origine': chiamante,
                            'Tipo Origine': 'L2',
                            'Relazione': 'Chiama',
                            'Oggetto Destinazione': obj_name,
                            'Tipo Destinazione': obj_type,
                            'Tipo Dipendenza': 'Chiamata L2→L3'
                        })
    
    # L4: Similar pattern
    if has_l4 and df_l4 is not None:
        for _, obj_row in df_l4.iterrows():
            obj_name = obj_row.get('ObjectName', '')
            obj_type = obj_row.get('ObjectType', '')
            obj_server = obj_row.get('Server', '')
            obj_db = obj_row.get('Database', '')
            
            chiamanti = str(obj_row.get('Oggetti_Chiamanti_L3', ''))
            if chiamanti and chiamanti not in ['nan', '']:
                for chiamante in chiamanti.split(';'):
                    chiamante = chiamante.strip()
                    if chiamante:
                        relation_rows.append({
                            'Livello': 'L4',
                            'Server': obj_server,
                            'DB': obj_db,
                            'Oggetto Origine': chiamante,
                            'Tipo Origine': 'L3',
                            'Relazione': 'Chiama',
                            'Oggetto Destinazione': obj_name,
                            'Tipo Destinazione': obj_type,
                            'Tipo Dipendenza': 'Chiamata L3→L4'
                        })
    
    df_relations = pd.DataFrame(relation_rows)
    print(f"✓ Relazioni create: {len(df_relations)} righe")
    
    print("\n" + "=" * 80)
    print("SALVATAGGIO FILE EXCEL")
    print("=" * 80)
    
    # Salva il file Excel
    output_path = base_path / "SUMMARY_REPORT.xlsx"
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df_summary_l1.to_excel(writer, sheet_name='L1', index=False)
        df_summary_l2.to_excel(writer, sheet_name='L2', index=False)
        if has_l3 and len(df_summary_l3) > 0:
            df_summary_l3.to_excel(writer, sheet_name='L3', index=False)
        if has_l4 and len(df_summary_l4) > 0:
            df_summary_l4.to_excel(writer, sheet_name='L4', index=False)
        df_stats_global.to_excel(writer, sheet_name='Statistiche Globali', index=False)
        df_relations.to_excel(writer, sheet_name='Dipendenze Relazioni', index=False)
    print(f"✓ File salvato: {output_path}")
    
    print("\n" + "=" * 80)
    print("STATISTICHE FINALI")
    print("=" * 80)
    print(f"Sheet L1: {len(df_summary_l1)} righe")
    print(f"Sheet L2: {len(df_summary_l2)} righe")
    if has_l3:
        print(f"Sheet L3: {len(df_summary_l3)} righe")
    if has_l4:
        print(f"Sheet L4: {len(df_summary_l4)} righe")
    print(f"Sheet Statistiche Globali: {len(df_stats_global)} metriche")
    
    total_rows = len(df_summary_l1) + len(df_summary_l2)
    if has_l3:
        total_rows += len(df_summary_l3)
    if has_l4:
        total_rows += len(df_summary_l4)
    
    print(f"Totale righe: {total_rows}")
    print(f"Totale oggetti unici: {total_objects}")
    print("\n✅ Report di summary completato con successo!")


def extract_object_types(dependencies_str):
    """
    Estrae i tipi di oggetti da una stringa di dipendenze
    Esempio: "dbo.usp_GetData, dbo.fn_Calculate" -> "P, FN"
    """
    if not dependencies_str or pd.isna(dependencies_str) or dependencies_str == 'nan':
        return ''
    
    types = []
    deps = [d.strip() for d in str(dependencies_str).split(',')]
    
    for dep in deps:
        if not dep:
            continue
        
        # Identifica il tipo basandosi sul prefisso
        if dep.startswith('usp_') or dep.startswith('sp_'):
            types.append('P')  # Stored Procedure
        elif dep.startswith('fn_') or dep.startswith('udf_'):
            types.append('FN')  # Function
        elif dep.startswith('tr_') or dep.startswith('trig_'):
            types.append('TR')  # Trigger
        elif dep.startswith('vw_') or dep.startswith('v_'):
            types.append('V')  # View
        else:
            types.append('?')  # Tipo sconosciuto
    
    return ', '.join(types)


if __name__ == "__main__":
    try:
        create_summary_report()
    except Exception as e:
        print(f"\n❌ Errore durante l'esecuzione: {str(e)}")
        import traceback
        traceback.print_exc()
