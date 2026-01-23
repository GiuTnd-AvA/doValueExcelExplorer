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
    """Classifica un oggetto con tipo specifico SQL basandosi sul nome."""
    obj_lower = obj_name.lower()
    
    # Pattern per Stored Procedures
    sp_patterns = ['sp_', 'usp_', 'asp_', 'proc_', '_sp_', '[sp_', 'p_']
    
    # Pattern per Scalar Functions
    scalar_fn_patterns = ['fn_', 'udf_', 'f_', '_fn_', '_udf_', '[fn_', '[udf_']
    
    # Pattern per Table-Valued Functions (inline o multi-statement)
    tvf_patterns = ['tf_', 'if_', 'tvf_', '_tf_', '_tvf_', '[tf_', 'fn_get', 'udf_get']
    
    # Prima controlla Table-Valued Functions (più specifiche)
    for pattern in tvf_patterns:
        if pattern in obj_lower:
            # Alcuni pattern come fn_get potrebbero restituire tabelle
            if any(p in obj_lower for p in ['fn_get', 'udf_get', 'tf_', 'tvf_', 'if_']):
                return 'SQL_TABLE_VALUED_FUNCTION'
    
    # Controlla Scalar Functions
    for pattern in scalar_fn_patterns:
        if pattern in obj_lower:
            return 'SQL_SCALAR_FUNCTION'
    
    # Controlla Stored Procedures
    for pattern in sp_patterns:
        if pattern in obj_lower:
            return 'SQL_STORED_PROCEDURE'
    
    # Trigger patterns
    if 'trigger' in obj_lower or 'tr_' in obj_lower or '_tr_' in obj_lower:
        return 'SQL_TRIGGER'
    
    # Se ha parentesi quadre e inizia con schema, verifica pattern semantici
    if 'dbo.' in obj_lower or '[dbo].' in obj_lower:
        # Pattern che suggeriscono functions
        if any(p in obj_lower for p in ['calc', 'compute', 'convert', 'format', 'parse', 'validate']):
            return 'SQL_SCALAR_FUNCTION'
        # Pattern che suggeriscono SP
        if any(p in obj_lower for p in ['exec', 'run', 'process', 'update', 'insert', 'delete', 'manage']):
            return 'SQL_STORED_PROCEDURE'
    
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
        
        # Estrai nomi oggetti critici chiamanti
        critical_caller_names = [c['object_name'] for c in critical_callers] if critical_callers else []
        
        obj_info = {
            'name': dep_name,
            'object_type': obj_type,
            'total_callers': len(callers),
            'critical_callers': len(critical_callers),
            'caller_types': '; '.join(sorted(caller_types)),
            'critical_caller_types': '; '.join(sorted(critical_caller_types)) if critical_caller_types else 'Nessuno',
            'called_by': '; '.join([c['object_name'] for c in callers[:5]]),  # Primi 5
            'called_by_critical': '; '.join([c['object_name'] for c in critical_callers[:5]]) if critical_callers else 'Nessuno',
            'critical_caller_names': '; '.join(critical_caller_names) if critical_caller_names else 'Nessuno',
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
        all_new_tables = classified_objects['tables']
        all_new_sp_functions = classified_objects['sp_functions']
        
        # Filtra SOLO dipendenze critiche (usate da oggetti critici)
        new_tables = [t for t in all_new_tables if t['is_critical_dependency'] == 'SÌ']
        new_sp_functions = [s for s in all_new_sp_functions if s['is_critical_dependency'] == 'SÌ']
        
        print(f"  - Nuove TABELLE CRITICHE da analizzare: {len(new_tables)} (totali trovate: {len(all_new_tables)})")
        print(f"  - Nuove SP/FUNCTIONS CRITICHE da analizzare: {len(new_sp_functions)} (totali trovate: {len(all_new_sp_functions)})\n")
        
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
                        'Oggetti_Critici_Partenza': t['critical_caller_names'],
                        'Azione': 'Aggiungere all\'estrazione'
                    } for t in new_tables])
                    # Ordina per criticità e numero chiamanti critici
                    tables_df = tables_df.sort_values(['Dipendenza_Critica', 'N_Chiamanti_Critici'], ascending=[False, False])
                    tables_df.to_excel(writer, sheet_name='Nuove Tabelle', index=False)
                
                # Sheet 2: Nuove SP/Functions
                if new_sp_functions:
                    sp_df = pd.DataFrame([{
                        'Nuovo_Oggetto': s['name'],
                        'ObjectType': s['object_type'],
                        'Dipendenza_Critica': s['is_critical_dependency'],
                        'N_Chiamanti_Totali': s['total_callers'],
                        'N_Chiamanti_Critici': s['critical_callers'],
                        'Tipi_Chiamanti': s['caller_types'],
                        'Tipi_Chiamanti_Critici': s['critical_caller_types'],
                        'Chiamata_Da': s['called_by'],
                        'Chiamata_Da_Critici': s['called_by_critical'],
                        'Oggetti_Critici_Partenza': s['critical_caller_names'],
                        'Azione': 'Analizzare per migrazione'
                    } for s in new_sp_functions])
                    # Ordina per criticità e numero chiamanti critici
                    sp_df = sp_df.sort_values(['Dipendenza_Critica', 'N_Chiamanti_Critici'], ascending=[False, False])
                    sp_df.to_excel(writer, sheet_name='Nuove SP-Functions', index=False)
                
                # Sheet 3: Solo Dipendenze Critiche (ora uguale agli altri sheet, tutti sono critici)
                critical_deps = []
                for t in new_tables:
                    critical_deps.append({
                        'Tipo': 'Tabella',
                        'Nome': t['name'],
                        'N_Chiamanti_Critici': t['critical_callers'],
                        'Tipi_Chiamanti_Critici': t['critical_caller_types'],
                        'Chiamata_Da_Critici': t['called_by_critical']
                    })
                for s in new_sp_functions:
                    critical_deps.append({
                        'Tipo': s['object_type'],
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
                        'Dipendenze Totali Trovate', 
                        'Nuove Dipendenze Totali',
                        'Nuove Tabelle CRITICHE (esportate)',
                        'Nuove SP/Functions CRITICHE (esportate)',
                        'Tabelle Totali (incluse non critiche)',
                        'SP/Functions Totali (incluse non critiche)'
                    ],
                    'Valore': [
                        len(tables), 
                        len(dependency_map), 
                        len(all_new_tables) + len(all_new_sp_functions),
                        len(new_tables),
                        len(new_sp_functions),
                        len(all_new_tables),
                        len(all_new_sp_functions)
                    ]
                })
                stats_df.to_excel(writer, sheet_name='Statistiche', index=False)
            
            print(f"Risultati esportati in: {output_path}\n")
            
            # Mostra risultati
            if new_tables:
                print(f"Prime 5 NUOVE TABELLE CRITICHE:")
                sorted_tables = sorted(new_tables, key=lambda x: -x['critical_callers'])
                for i, obj in enumerate(sorted_tables[:5], 1):
                    print(f"  {i}. {obj['name']}")
                    print(f"     Chiamata da {obj['critical_callers']} oggetti critici")
                print()
            
            if new_sp_functions:
                print(f"Prime 5 NUOVE SP/FUNCTIONS CRITICHE:")
                sorted_sp = sorted(new_sp_functions, key=lambda x: -x['critical_callers'])
                for i, obj in enumerate(sorted_sp[:5], 1):
                    print(f"  {i}. {obj['name']} ({obj['object_type']})")
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
