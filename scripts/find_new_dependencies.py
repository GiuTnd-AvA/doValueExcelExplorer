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

def extract_dependencies_with_context(df, table_col='Table', object_col='ObjectName', type_col='ObjectType', critical_col='Critico_Migrazione', dep_col='Dipendenze'):
    """Estrae dipendenze mantenendo il contesto dell'oggetto chiamante."""
    if dep_col not in df.columns:
        print(f"ATTENZIONE: Colonna '{dep_col}' non trovata!")
        return {}
    
    # Mappa: dipendenza → lista di (object_name, object_type, is_critical)
    dependency_map = {}
    
    for idx, row in df.iterrows():
        object_name = row.get(object_col, 'Unknown')
        object_type = row.get(type_col, 'Unknown')
        is_critical = row.get(critical_col, 'NO')
        dependencies_value = row.get(dep_col)
        
        if pd.isna(dependencies_value) or not isinstance(dependencies_value, str):
            continue
        
        # Split per ';' se ci sono più dipendenze
        deps = dependencies_value.split(';')
        for dep in deps:
            dep_clean = dep.strip().lower()
            if dep_clean and dep_clean != 'nessuna':
                if dep_clean not in dependency_map:
                    dependency_map[dep_clean] = []
                dependency_map[dep_clean].append({
                    'object_name': object_name,
                    'object_type': object_type,
                    'is_critical': is_critical
                })
    
    return dependency_map

def find_new_objects_with_context(tables, dependency_map):
    """Trova oggetti in dipendenze che non sono nelle tabelle, mantenendo il contesto."""
    results = {
        'tables': [],
        'sp_functions': []
    }
    
    for dep_name, callers in dependency_map.items():
        # Controlla se questa dipendenza è già nelle tabelle analizzate
        if dep_name in tables:
            continue
        
        # Classifica il tipo
        obj_type = classify_object_type(dep_name)
        
        # Conta chiamanti critici
        critical_callers = [c for c in callers if c['is_critical'] == 'SÌ']
        
        # Estrai tipi di oggetti chiamanti
        caller_types = set([c['object_type'] for c in callers])
        critical_caller_types = set([c['object_type'] for c in critical_callers]) if critical_callers else set()
        
        obj_info = {
            'name': dep_name,
            'total_callers': len(callers),
            'critical_callers': len(critical_callers),
            'caller_types': '; '.join(sorted(caller_types)),
            'critical_caller_types': '; '.join(sorted(critical_caller_types)) if critical_caller_types else 'Nessuno',
            'called_by': '; '.join([c['object_name'] for c in callers[:5]]),  # Primi 5
            'called_by_critical': '; '.join([c['object_name'] for c in critical_callers[:5]]) if critical_callers else 'Nessuno',
            'is_critical_dependency': 'SÌ' if critical_callers else 'NO'
        }
        
        if obj_type == 'Tabella':
            results['tables'].append(obj_info)
        else:
            results['sp_functions'].append(obj_info)
    
    return results

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
        
        print("\nEstrazione dipendenze con contesto...")
        dependency_map = extract_dependencies_with_context(df, 'Table', 'ObjectName', 'ObjectType', 'Critico_Migrazione', 'Dipendenze')
        print(f"  - Dipendenze totali: {len(dependency_map)}")
        
        # Trova nuovi oggetti
        print("\nConfrontando dipendenze con tabelle...")
        classified_objects = find_new_objects_with_context(tables, dependency_map)
        new_tables = classified_objects['tables']
        new_sp_functions = classified_objects['sp_functions']
        
        # Conta dipendenze critiche
        critical_tables = [t for t in new_tables if t['is_critical_dependency'] == 'SÌ']
        critical_sp = [s for s in new_sp_functions if s['is_critical_dependency'] == 'SÌ']
        
        print(f"  - Nuove TABELLE da analizzare: {len(new_tables)} (di cui {len(critical_tables)} critiche)")
        print(f"  - Nuove SP/FUNCTIONS da analizzare: {len(new_sp_functions)} (di cui {len(critical_sp)} critiche)\n")
        
        if new_tables or new_sp_functions:
            # Esporta risultati
            input_path = Path(INPUT_FILE)
            output_path = input_path.parent / f"{input_path.stem}{OUTPUT_SUFFIX}.xlsx"
            
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Sheet 1: Nuove Tabelle
                if new_tables:
                    tables_df = pd.DataFrame([{
                        'Nuova_Tabella': t['name'],
                        'Dipendenza_Critica': t['is_critical_dependency'],
                        'N_Chiamanti_Totali': t['total_callers'],
                        'N_Chiamanti_Critici': t['critical_callers'],
                        'Tipi_Chiamanti': t['caller_types'],
                        'Tipi_Chiamanti_Critici': t['critical_caller_types'],
                        'Chiamata_Da': t['called_by'],
                        'Chiamata_Da_Critici': t['called_by_critical'],
                        'Azione': 'Aggiungere all\'estrazione'
                    } for t in new_tables])
                    # Ordina per criticità e numero chiamanti critici
                    tables_df = tables_df.sort_values(['Dipendenza_Critica', 'N_Chiamanti_Critici'], ascending=[False, False])
                    tables_df.to_excel(writer, sheet_name='Nuove Tabelle', index=False)
                
                # Sheet 2: Nuove SP/Functions
                if new_sp_functions:
                    sp_df = pd.DataFrame([{
                        'Nuovo_Oggetto': s['name'],
                        'Dipendenza_Critica': s['is_critical_dependency'],
                        'N_Chiamanti_Totali': s['total_callers'],
                        'N_Chiamanti_Critici': s['critical_callers'],
                        'Tipi_Chiamanti': s['caller_types'],
                        'Tipi_Chiamanti_Critici': s['critical_caller_types'],
                        'Chiamata_Da': s['called_by'],
                        'Chiamata_Da_Critici': s['called_by_critical'],
                        'Azione': 'Analizzare per migrazione'
                    } for s in new_sp_functions])
                    # Ordina per criticità e numero chiamanti critici
                    sp_df = sp_df.sort_values(['Dipendenza_Critica', 'N_Chiamanti_Critici'], ascending=[False, False])
                    sp_df.to_excel(writer, sheet_name='Nuove SP-Functions', index=False)
                
                # Sheet 3: Solo Dipendenze Critiche (filtro)
                critical_deps = []
                for t in new_tables:
                    if t['is_critical_dependency'] == 'SÌ':
                        critical_deps.append({
                            'Tipo': 'Tabella',
                            'Nome': t['name'],
                            'N_Chiamanti_Critici': t['critical_callers'],
                            'Tipi_Chiamanti_Critici': t['critical_caller_types'],
                            'Chiamata_Da_Critici': t['called_by_critical']
                        })
                for s in new_sp_functions:
                    if s['is_critical_dependency'] == 'SÌ':
                        critical_deps.append({
                            'Tipo': 'SP/Function',
                            'Nome': s['name'],
                            'N_Chiamanti_Critici': s['critical_callers'],
                            'Tipi_Chiamanti_Critici': s['critical_caller_types'],
                            'Chiamata_Da_Critici': s['called_by_critical']
                        })
                
                if critical_deps:
                    critical_df = pd.DataFrame(critical_deps)
                    critical_df = critical_df.sort_values('N_Chiamanti_Critici', ascending=False)
                    critical_df.to_excel(writer, sheet_name='Solo Critiche', index=False)
                
                # Sheet 4: Statistiche
                stats_df = pd.DataFrame({
                    'Metrica': [
                        'Tabelle Analizzate', 
                        'Dipendenze Totali', 
                        'Nuove Tabelle',
                        'Nuove Tabelle Critiche',
                        'Nuove SP/Functions',
                        'Nuove SP/Functions Critiche',
                        '% Critiche su Totali'
                    ],
                    'Valore': [
                        len(tables), 
                        len(dependency_map), 
                        len(new_tables),
                        len(critical_tables),
                        len(new_sp_functions),
                        len(critical_sp),
                        f"{(len(critical_tables)+len(critical_sp))/(len(new_tables)+len(new_sp_functions))*100:.1f}%" if (new_tables or new_sp_functions) else "0%"
                    ]
                })
                stats_df.to_excel(writer, sheet_name='Statistiche', index=False)
            
            print(f"Risultati esportati in: {output_path}\n")
            
            # Mostra risultati
            if new_tables:
                print(f"Prime 5 NUOVE TABELLE CRITICHE:")
                critical_first = sorted(new_tables, key=lambda x: (x['is_critical_dependency'] != 'SÌ', -x['critical_callers']))
                for i, obj in enumerate(critical_first[:5], 1):
                    crit_flag = "⚠️ CRITICA" if obj['is_critical_dependency'] == 'SÌ' else ""
                    print(f"  {i}. {obj['name']} {crit_flag}")
                    print(f"     Chiamata da {obj['critical_callers']} oggetti critici")
                print()
            
            if new_sp_functions:
                print(f"Prime 5 NUOVE SP/FUNCTIONS CRITICHE:")
                critical_first = sorted(new_sp_functions, key=lambda x: (x['is_critical_dependency'] != 'SÌ', -x['critical_callers']))
                for i, obj in enumerate(critical_first[:5], 1):
                    crit_flag = "⚠️ CRITICA" if obj['is_critical_dependency'] == 'SÌ' else ""
                    print(f"  {i}. {obj['name']} {crit_flag}")
                    print(f"     Chiamata da {obj['critical_callers']} oggetti critici")
                print()
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
