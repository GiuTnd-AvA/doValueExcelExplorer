# =========================
# IMPORT
# =========================
from config.config import EXCEL_OUTPUT_PATH
import pandas as pd
import re
import os
from pathlib import Path

# =========================
# CONFIG
# =========================
INPUT_PATH = EXCEL_OUTPUT_PATH  # Percorso dei file di estrazione
OUTPUT_SUFFIX = "_analyzed"

# =========================
# FUNZIONI DI ANALISI
# =========================

def count_lines(sql_def):
    """Conta le righe non vuote di codice SQL."""
    if not sql_def:
        return 0
    return len([line for line in sql_def.split('\n') if line.strip()])

def analyze_patterns(sql_def):
    """Identifica pattern T-SQL nella definizione."""
    if not sql_def:
        return set()
    
    sql_lower = sql_def.lower()
    patterns = set()
    
    # Cursori
    if re.search(r'\bdeclare\s+\w+\s+cursor\b', sql_lower):
        patterns.add('CURSOR')
    
    # Dynamic SQL
    if re.search(r'\bexec\s*\(\s*@', sql_lower) or re.search(r'\bsp_executesql\b', sql_lower):
        patterns.add('DYNAMIC_SQL')
    
    # Transazioni esplicite
    if re.search(r'\bbegin\s+tran(saction)?\b', sql_lower):
        patterns.add('TRANSACTION')
    
    # Temp tables
    if re.search(r'#\w+', sql_def):
        patterns.add('TEMP_TABLE')
    
    # Table variables
    if re.search(r'\bdeclare\s+@\w+\s+table\b', sql_lower):
        patterns.add('TABLE_VARIABLE')
    
    # TRY-CATCH
    if re.search(r'\bbegin\s+try\b', sql_lower):
        patterns.add('ERROR_HANDLING')
    
    # WHILE loops
    if re.search(r'\bwhile\b', sql_lower):
        patterns.add('LOOP')
    
    # CTE
    if re.search(r'\bwith\s+\w+\s+as\s*\(', sql_lower):
        patterns.add('CTE')
    
    # PIVOT/UNPIVOT
    if re.search(r'\b(pivot|unpivot)\b', sql_lower):
        patterns.add('PIVOT')
    
    # XML operations
    if re.search(r'\b(for\s+xml|openxml|\.query\(|\.value\()', sql_lower):
        patterns.add('XML')
    
    # Funzioni window
    if re.search(r'\b(row_number|rank|dense_rank|partition\s+by)\b', sql_lower):
        patterns.add('WINDOW_FUNCTION')
    
    return patterns

def count_dml_operations(sql_def, clause_type):
    """Conta operazioni DML critiche."""
    if not sql_def:
        return 0
    
    sql_lower = sql_def.lower()
    count = 0
    
    # Conta INSERT
    count += len(re.findall(r'\binsert\s+into\b', sql_lower))
    
    # Conta UPDATE
    count += len(re.findall(r'\bupdate\b(?!\s+statistics)', sql_lower))
    
    # Conta DELETE
    count += len(re.findall(r'\bdelete\s+from\b', sql_lower))
    
    # Conta MERGE
    count += len(re.findall(r'\bmerge\s+into\b', sql_lower))
    
    return count

def count_joins(sql_def):
    """Conta il numero di JOIN."""
    if not sql_def:
        return 0
    return len(re.findall(r'\b(inner\s+join|left\s+join|right\s+join|full\s+join|cross\s+join|join)\b', sql_def.lower()))

def extract_tables_from_sql(sql_def):
    """Estrae tabelle referenziate da FROM/JOIN."""
    if not sql_def:
        return set()
    
    tables = set()
    
    # Pattern per FROM/JOIN
    table_pattern = r'(?:FROM|JOIN)\s+(?:\[?[\w]+\]?\.)?\[?([\w]+)\]?'
    for match in re.finditer(table_pattern, sql_def, re.IGNORECASE):
        table_name = match.group(1).lower()
        # Escludi keyword SQL e tabelle temporanee
        if table_name not in ['select', 'deleted', 'inserted', 'dual', 'information_schema']:
            if not table_name.startswith('#'):
                tables.add(table_name)
    
    return tables

def extract_objects_from_sql(sql_def):
    """Estrae SP/Functions/Trigger chiamati."""
    if not sql_def:
        return set()
    
    objects = set()
    
    # EXEC sp_name o EXECUTE sp_name
    exec_pattern = r'\bexec(?:ute)?\s+(\[?\w+\]?\.\[?\w+\]?\.?\[?\w+\]?)'
    for match in re.finditer(exec_pattern, sql_def.lower()):
        dep = match.group(1).strip()
        if dep not in ['sp_executesql', 'xp_cmdshell', 'sp_who', 'sp_help']:
            objects.add(dep)
    
    # Funzioni: dbo.fn_name( o [dbo].[fn_name](
    func_pattern = r'(\[?\w+\]?\.\[?[a-z_]\w+\]?)\s*\('
    for match in re.finditer(func_pattern, sql_def.lower()):
        dep = match.group(1).strip()
        # Escludi funzioni di sistema comuni
        if not dep.startswith(('cast', 'convert', 'isnull', 'coalesce', 'len', 'substring', 'getdate', 'count', 'sum', 'max', 'min', 'avg')):
            objects.add(dep)
    
    return objects

def calculate_complexity_score(sql_def, patterns, dml_count, join_count, total_dependencies):
    """Calcola uno score di complessità 0-100."""
    if not sql_def:
        return 0
    
    score = 0
    
    # Linee di codice (max 30 punti)
    lines = count_lines(sql_def)
    score += min(30, lines // 10)
    
    # Pattern complessi (max 30 punti)
    complex_patterns = {'CURSOR': 10, 'DYNAMIC_SQL': 8, 'LOOP': 6, 'XML': 5, 'PIVOT': 4}
    for pattern, points in complex_patterns.items():
        if pattern in patterns:
            score += points
    
    # DML operations (max 20 punti)
    score += min(20, dml_count * 3)
    
    # JOIN complexity (max 10 punti)
    score += min(10, join_count * 2)
    
    # Dipendenze (max 10 punti)
    score += min(10, total_dependencies * 2)
    
    return min(100, score)

def classify_criticality(score, dml_count, patterns):
    """Classifica la criticità per la migrazione."""
    if score >= 70 or 'DYNAMIC_SQL' in patterns or 'CURSOR' in patterns:
        return 'ALTA'
    elif score >= 40 or dml_count >= 3:
        return 'MEDIA'
    else:
        return 'BASSA'

def generate_description(sql_def, patterns, dml_count, join_count, total_tables, total_objects, clause_type):
    """Genera una descrizione testuale del comportamento."""
    if not sql_def:
        return "Definizione SQL non disponibile"
    
    parts = []
    
    # Tipo di operazione principale
    if clause_type and isinstance(clause_type, str):
        clause_types = set(clause_type.split('; '))
        if any(op in clause_types for op in ['INSERT INTO', 'UPDATE', 'DELETE FROM', 'MERGE INTO']):
            parts.append("Modifica dati")
        elif 'ALTER TABLE' in clause_types or 'CREATE TABLE' in clause_types:
            parts.append("Gestione struttura")
        else:
            parts.append("Lettura dati")
    
    # DML operations
    if dml_count > 0:
        parts.append(f"{dml_count} operazioni DML")
    
    # JOIN complexity
    if join_count > 5:
        parts.append(f"{join_count} JOIN complessi")
    elif join_count > 0:
        parts.append(f"{join_count} JOIN")
    
    # Pattern specifici
    if 'CURSOR' in patterns:
        parts.append("usa cursori")
    if 'DYNAMIC_SQL' in patterns:
        parts.append("SQL dinamico")
    if 'TRANSACTION' in patterns:
        parts.append("gestione transazioni")
    if 'TEMP_TABLE' in patterns or 'TABLE_VARIABLE' in patterns:
        parts.append("tabelle temporanee")
    if 'ERROR_HANDLING' in patterns:
        parts.append("gestione errori")
    if 'LOOP' in patterns:
        parts.append("cicli iterativi")
    if 'CTE' in patterns:
        parts.append("CTE")
    if 'XML' in patterns:
        parts.append("operazioni XML")
    if 'WINDOW_FUNCTION' in patterns:
        parts.append("funzioni window")
    
    # Dipendenze
    if total_tables > 3 or total_objects > 3:
        parts.append(f"usa {total_tables} tabelle e {total_objects} oggetti SQL")
    elif total_tables > 0:
        parts.append(f"usa {total_tables} tabelle")
    elif total_objects > 0:
        parts.append(f"chiama {total_objects} oggetti SQL")
    
    # Complessità generale
    lines = count_lines(sql_def)
    if lines > 200:
        parts.append(f"molto esteso ({lines} righe)")
    elif lines > 100:
        parts.append(f"esteso ({lines} righe)")
    
    if not parts:
        return "Operazione semplice"
    
    return "; ".join(parts).capitalize()

def is_critical_for_migration(clause_type):
    """Determina se l'oggetto è critico per la migrazione basandosi su CLAUSE_TYPE."""
    if not clause_type or not isinstance(clause_type, str):
        return 'NO'
    
    clause_type_upper = clause_type.upper()
    critical_operations = ['INSERT INTO', 'UPDATE', 'DELETE FROM', 'MERGE INTO', 'CREATE TABLE', 'ALTER TABLE']
    
    for op in critical_operations:
        if op in clause_type_upper:
            return 'SÌ'
    
    return 'NO'

def analyze_sql_object(row):
    """Analizza un singolo oggetto SQL."""
    sql_def = row.get('SQLDefinition', '')
    clause_type = row.get('CLAUSE_TYPE', '')
    
    # Analisi pattern
    patterns = analyze_patterns(sql_def)
    dml_count = count_dml_operations(sql_def, clause_type)
    join_count = count_joins(sql_def)
    
    # Estrai dipendenze separate
    tables = extract_tables_from_sql(sql_def)
    objects = extract_objects_from_sql(sql_def)
    
    # IMPORTANTE: Rimuovi tabelle dagli oggetti
    # Le tabelle possono apparire in objects con schema tipo [dbo].[tabella]
    # Confronta solo il nome della tabella (lowercase)
    tables_lowercase = {t.lower() for t in tables}
    objects_filtered = set()
    
    # Pattern tipici per identificare tabelle (naming conventions)
    table_patterns = [
        r'^tab_',           # Prefisso tab_
        r'_tab$',           # Suffisso _tab
        r'_t$',             # Suffisso _t (tabella)
        r'^tbl_',           # Prefisso tbl_
        r'_table$',         # Suffisso _table
        r'^t_',             # Prefisso t_
    ]
    
    for obj in objects:
        # Estrai solo il nome dell'oggetto (rimuovi schema e parentesi)
        obj_name = obj.split('.')[-1].strip('[]').lower()
        
        # Filtro 1: Confronta con tabelle estratte
        if obj_name in tables_lowercase:
            continue
        
        # Filtro 2: Escludi se il nome matcha pattern tipici delle tabelle
        is_likely_table = False
        for pattern in table_patterns:
            if re.search(pattern, obj_name, re.IGNORECASE):
                is_likely_table = True
                break
        
        if not is_likely_table:
            objects_filtered.add(obj)
    
    total_dependencies = len(tables) + len(objects_filtered)
    
    # Calcoli
    complexity_score = calculate_complexity_score(sql_def, patterns, dml_count, join_count, total_dependencies)
    criticality = classify_criticality(complexity_score, dml_count, patterns)
    description = generate_description(sql_def, patterns, dml_count, join_count, len(tables), len(objects_filtered), clause_type)
    critical_migration = is_critical_for_migration(clause_type)
    
    return {
        'Critico_Migrazione': critical_migration,
        'Descrizione_Comportamento': description,
        'Complessità_Score': complexity_score,
        'Criticità_Tecnica': criticality,
        'Pattern_Identificati': '; '.join(sorted(patterns)) if patterns else 'Nessuno',
        'Dipendenze_Count': total_dependencies,
        'Dipendenze_Tabelle': '; '.join(sorted(tables)) if tables else 'Nessuna',
        'Dipendenze_Tabelle_Count': len(tables),
        'Dipendenze_Oggetti': '; '.join(sorted(objects_filtered)) if objects_filtered else 'Nessuna',
        'Dipendenze_Oggetti_Count': len(objects_filtered),
        'DML_Count': dml_count,
        'JOIN_Count': join_count,
        'Righe_Codice': count_lines(sql_def)
    }

# =========================
# MAIN
# =========================

def main():
    print("=== Analisi Complessità SQL Objects ===\n")
    
    # Trova tutti i file di estrazione
    base_path = Path(INPUT_PATH)
    parent_dir = base_path.parent
    base_name_full = base_path.name  # Include .xlsx
    
    # Pattern: {base_name}.xlsx_parziale_*.xlsx (es. estrazione_dipendenze_sql.xlsx_parziale_oggetti_1_50.xlsx)
    files = list(parent_dir.glob(f"{base_name_full}_parziale_*.xlsx"))
    
    if not files:
        print(f"ERRORE: Nessun file trovato con pattern '{base_name_full}_parziale_*.xlsx' in {parent_dir}")
        return
    
    print(f"Trovati {len(files)} file da analizzare:\n")
    
    for file_path in sorted(files):
        print(f"Analisi di: {file_path.name}")
        
        try:
            # Leggi file Excel
            df = pd.read_excel(file_path)
            print(f"  - Righe lette: {len(df)}")
            
            # Analizza ogni riga
            analysis_results = []
            for idx, row in df.iterrows():
                result = analyze_sql_object(row)
                analysis_results.append(result)
            
            # Crea DataFrame con risultati
            analysis_df = pd.DataFrame(analysis_results)
            
            # Combina con dati originali
            result_df = pd.concat([df, analysis_df], axis=1)
            
            # Crea sheet "Oggetti_Tabelle_Esploso" - una riga per ogni oggetto→tabella
            obj_table_rows = []
            for idx, row in result_df.iterrows():
                obj_name = row.get('ObjectName', '')
                obj_type = row.get('ObjectType', '')
                obj_server = row.get('Server', '')
                obj_db = row.get('Database', '')
                critico_migr = row.get('Critico_Migrazione', '')
                criticita_tec = row.get('Criticità_Tecnica', '')
                
                # Esplode tabelle
                tables_str = row.get('Dipendenze_Tabelle', '')
                if pd.notna(tables_str) and tables_str not in ['Nessuna', '']:
                    tables = [t.strip() for t in str(tables_str).split(';') if t.strip()]
                    for table in tables:
                        obj_table_rows.append({
                            'ObjectName': obj_name,
                            'ObjectType': obj_type,
                            'Server': obj_server,
                            'Database': obj_db,
                            'Critico_Migrazione': critico_migr,
                            'Criticità_Tecnica': criticita_tec,
                            'Tabella_Dipendente': table,
                            'Tipo_Relazione': 'DIPENDE_DA_TABELLA'
                        })
            
            df_obj_tables = pd.DataFrame(obj_table_rows)
            
            # Crea sheet "Oggetti_Oggetti_Esploso" - una riga per ogni oggetto→oggetto
            obj_obj_rows = []
            for idx, row in result_df.iterrows():
                obj_name = row.get('ObjectName', '')
                obj_type = row.get('ObjectType', '')
                obj_server = row.get('Server', '')
                obj_db = row.get('Database', '')
                critico_migr = row.get('Critico_Migrazione', '')
                criticita_tec = row.get('Criticità_Tecnica', '')
                
                # Esplode oggetti
                objects_str = row.get('Dipendenze_Oggetti', '')
                if pd.notna(objects_str) and objects_str not in ['Nessuna', '']:
                    objects = [o.strip() for o in str(objects_str).split(';') if o.strip()]
                    for obj_dep in objects:
                        obj_obj_rows.append({
                            'ObjectName': obj_name,
                            'ObjectType': obj_type,
                            'Server': obj_server,
                            'Database': obj_db,
                            'Critico_Migrazione': critico_migr,
                            'Criticità_Tecnica': criticita_tec,
                            'Oggetto_Dipendente': obj_dep,
                            'Tipo_Relazione': 'DIPENDE_DA_OGGETTO'
                        })
            
            df_obj_objects = pd.DataFrame(obj_obj_rows)
            
            # Esporta risultati con multi-sheet
            output_path = file_path.parent / f"{file_path.stem}{OUTPUT_SUFFIX}.xlsx"
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                result_df.to_excel(writer, sheet_name='Oggetti', index=False)
                if len(df_obj_tables) > 0:
                    df_obj_tables.to_excel(writer, sheet_name='Oggetti_Tabelle_Esploso', index=False)
                if len(df_obj_objects) > 0:
                    df_obj_objects.to_excel(writer, sheet_name='Oggetti_Oggetti_Esploso', index=False)
            
            print(f"  - Esportato: {output_path.name}")
            print(f"  - Sheet Oggetti: {len(result_df)} righe")
            print(f"  - Sheet Oggetti_Tabelle_Esploso: {len(df_obj_tables)} relazioni")
            print(f"  - Sheet Oggetti_Oggetti_Esploso: {len(df_obj_objects)} relazioni")
            
            # Statistiche
            criticita_counts = analysis_df['Criticità_Tecnica'].value_counts()
            critici_migr = len(analysis_df[analysis_df['Critico_Migrazione'] == 'SÌ'])
            print(f"  - Criticità Tecnica: ALTA={criticita_counts.get('ALTA', 0)}, MEDIA={criticita_counts.get('MEDIA', 0)}, BASSA={criticita_counts.get('BASSA', 0)}")
            print(f"  - Critici per migrazione (DML/DDL): {critici_migr}")
            print(f"  - Complessità media: {analysis_df['Complessità_Score'].mean():.1f}\n")
            
        except Exception as e:
            print(f"  ERRORE durante l'analisi di {file_path.name}: {e}\n")
    
    print("\n=== Analisi completata ===")

if __name__ == "__main__":
    main()
