# =========================
# IMPORT
# =========================
import pandas as pd
import pyodbc
from pathlib import Path
import re
import sys

# Importa funzioni da analyze_sql_complexity
sys.path.append(str(Path(__file__).parent))

# =========================
# CONFIG
# =========================
# File input
NEW_DEPS_FILE = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\analisi_sqldefinition_criticità_nuove_dipendenze.xlsx'
ANALYZED_FILE = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\analisi_oggetti_critici.xlsx'

# Output
OUTPUT_DIR = Path(r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\Dipendenze Ricorsive')
OUTPUT_DIR.mkdir(exist_ok=True)

# SQL Server connection
SQL_SERVER = 'EPCP3'

# Parametri ricorsione
MAX_LEVELS = 5  # Massimo 5 livelli di profondità
BATCH_SIZE = 50  # Estrai 50 oggetti alla volta

# =========================
# FUNZIONI SQL
# =========================

def get_sql_connection(database=None):
    """Crea connessione SQL Server con autenticazione Windows."""
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
    )
    if database:
        connection_string += f"DATABASE={database};"
    connection_string += "Trusted_Connection=yes;"
    
    return pyodbc.connect(connection_string)

def get_available_databases(conn):
    """Estrae lista database disponibili sul server."""
    query = """
    SELECT name 
    FROM sys.databases 
    WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
    AND state_desc = 'ONLINE'
    ORDER BY name
    """
    df = pd.read_sql(query, conn)
    return df['name'].tolist()

def extract_sql_definition_multi_db(databases, object_name, preferred_db=None):
    """Estrae SQLDefinition cercando prima in preferred_db, poi negli altri."""
    search_order = []
    
    # Prima cerca nel DB preferito
    if preferred_db and preferred_db in databases:
        search_order.append(preferred_db)
    
    # Poi cerca negli altri
    for db in databases:
        if db != preferred_db:
            search_order.append(db)
    
    # Cerca in ordine
    for db in search_order:
        conn = None
        try:
            conn = get_sql_connection(db)
            
            # Query case-insensitive con LOWER()
            query = """
            SELECT 
                o.name AS ObjectName,
                o.type_desc AS ObjectType,
                m.definition AS SQLDefinition,
                SCHEMA_NAME(o.schema_id) AS SchemaName,
                DB_NAME() AS Database
            FROM sys.sql_modules m
            INNER JOIN sys.objects o ON m.object_id = o.object_id
            WHERE LOWER(o.name) = LOWER(?)
            """
            
            cursor = conn.cursor()
            cursor.execute(query, object_name)
            
            # Fetch risultati
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            
            if rows:
                # Converti prima riga in dict
                result = dict(zip(columns, rows[0]))
                return result
                
        except Exception as e:
            if conn:
                try:
                    conn.close()
                except:
                    pass
            # Non stampare errore, prova prossimo DB silenziosamente
            continue
    
    return None  # Non trovato in nessun DB

# =========================
# FUNZIONI ANALISI
# =========================

def classify_object_type(obj_name):
    """Classifica tipo oggetto da pattern nome."""
    obj_lower = obj_name.lower()
    
    if 'trigger' in obj_lower or 'tr_' in obj_lower or '_tr_' in obj_lower:
        return 'SQL_TRIGGER'
    
    sp_patterns = ['sp_', 'usp_', 'asp_', 'proc_', '_sp_', '[sp_']
    if any(p in obj_lower for p in sp_patterns):
        return 'SQL_STORED_PROCEDURE'
    
    fn_patterns = ['fn_', 'udf_', 'f_', '_fn_', '_udf_']
    if any(p in obj_lower for p in fn_patterns):
        return 'SQL_SCALAR_FUNCTION'
    
    tvf_patterns = ['tf_', 'if_', 'tvf_', '_tf_']
    if any(p in obj_lower for p in tvf_patterns):
        return 'SQL_TABLE_VALUED_FUNCTION'
    
    return 'Tabella'

def extract_dependencies_from_sql(sql_definition):
    """Estrae dipendenze da SQLDefinition usando regex."""
    if not sql_definition or not isinstance(sql_definition, str):
        return []
    
    dependencies = set()
    
    # Pattern per tabelle: FROM/JOIN [schema].[table] o [table]
    table_pattern = r'(?:FROM|JOIN)\s+(?:\[?[\w]+\]?\.)?\[?([\w]+)\]?'
    matches = re.finditer(table_pattern, sql_definition, re.IGNORECASE)
    for match in matches:
        table_name = match.group(1).lower()
        # Filtra parole chiave SQL comuni
        if table_name not in ['select', 'deleted', 'inserted', 'dual']:
            dependencies.add(table_name)
    
    # Pattern per SP: EXEC/EXECUTE [schema].[sp_name] o sp_name
    sp_pattern = r'(?:EXEC(?:UTE)?)\s+(?:\[?[\w]+\]?\.)?\[?([\w]+)\]?'
    matches = re.finditer(sp_pattern, sql_definition, re.IGNORECASE)
    for match in matches:
        sp_name = match.group(1).lower()
        dependencies.add(sp_name)
    
    # Pattern per functions in SELECT o WHERE: dbo.fn_Name() o fn_Name()
    fn_pattern = r'(?:\[?[\w]+\]?\.)?\[?(fn_[\w]+|udf_[\w]+|tf_[\w]+)\]?\s*\('
    matches = re.finditer(fn_pattern, sql_definition, re.IGNORECASE)
    for match in matches:
        fn_name = match.group(1).lower()
        dependencies.add(fn_name)
    
    return list(dependencies)

def analyze_object(row):
    """Analizza un oggetto e restituisce dizionario con info."""
    sql_def = row.get('SQLDefinition', '')
    
    # Estrai dipendenze
    deps = extract_dependencies_from_sql(sql_def)
    deps_str = '; '.join(sorted(deps)) if deps else 'Nessuna'
    
    # Calcola metriche base
    lines = len(sql_def.split('\n')) if isinstance(sql_def, str) else 0
    
    return {
        'ObjectName': row.get('ObjectName', ''),
        'ObjectType': row.get('ObjectType', ''),
        'SchemaName': row.get('SchemaName', 'dbo'),
        'Dipendenze': deps_str,
        'N_Dipendenze': len(deps),
        'Righe_Codice': lines,
        'SQLDefinition': sql_def
    }

# =========================
# FUNZIONI RICORSIONE
# =========================

def load_known_objects():
    """Carica tutti gli oggetti già analizzati con info database."""
    known = {}  # {object_name: database}
    
    # Carica oggetti critici
    try:
        df_analyzed = pd.read_excel(ANALYZED_FILE)
        for idx, row in df_analyzed.iterrows():
            obj_name = str(row['ObjectName']).lower()
            db = row.get('Database', 'Unknown')
            known[obj_name] = db
        print(f"Oggetti critici caricati: {len(known)}")
    except Exception as e:
        print(f"ATTENZIONE: Non posso caricare {ANALYZED_FILE}: {e}")
    
    return known

def load_new_objects_to_analyze():
    """Carica nuove SP/Functions da file dipendenze."""
    try:
        df_new_sp = pd.read_excel(NEW_DEPS_FILE, sheet_name='Nuove SP-Functions')
        
        # Filtra solo critiche
        df_critical = df_new_sp[df_new_sp['Dipendenza_Critica'] == 'SÌ'].copy()
        
        # Estrai nomi oggetti
        objects = []
        for idx, row in df_critical.iterrows():
            obj_name = str(row['Nuovo_Oggetto'])
            
            # Pulisci nome: rimuovi database, schema, parentesi quadre
            # Esempi: [DB].[dbo].[sp_test] → sp_test
            #         dbo.sp_test → sp_test
            #         sp_test → sp_test
            parts = obj_name.replace('[', '').replace(']', '').split('.')
            clean_name = parts[-1].strip()  # Prendi solo ultima parte
            
            # Debug: mostra primi 5 per verifica
            if len(objects) < 5:
                print(f"  Debug: '{obj_name}' → '{clean_name}'")
            
            objects.append({
                'name': clean_name,
                'full_name': obj_name,
                'type': row.get('ObjectType', 'Unknown')
            })
        
        print(f"Nuove SP/Functions critiche da analizzare: {len(objects)}")
        return objects
        
    except Exception as e:
        print(f"ERRORE caricamento nuove SP/Functions: {e}")
        import traceback
        traceback.print_exc()
        return []

def recursive_extraction(databases, known_objects, level=2):
    """Estrazione ricorsiva dipendenze."""
    print(f"\n{'='*60}")
    print(f"LIVELLO {level} - Estrazione dipendenze")
    print(f"{'='*60}")
    
    if level > MAX_LEVELS + 1:  # MAX_LEVELS=5, quindi max livello 6
        print(f"Raggiunto massimo livello di ricorsione (livello {MAX_LEVELS + 1})")
        return pd.DataFrame()
    
    # Carica oggetti da analizzare
    if level == 2:
        new_objects = load_new_objects_to_analyze()
    else:
        # Leggi dal file del livello precedente
        prev_file = OUTPUT_DIR / f"livello_{level-1}_nuove_dipendenze.xlsx"
        if not prev_file.exists():
            print(f"File livello precedente non trovato: {prev_file}")
            return pd.DataFrame()
        
        df_prev = pd.read_excel(prev_file)
        new_objects = [{'name': row['Nuovo_Oggetto'], 
                       'full_name': row['Nuovo_Oggetto'], 
                       'type': row.get('ObjectType', 'Unknown'),
                       'database': row.get('Database_Preferito', None)} 
                      for idx, row in df_prev.iterrows()]
    
    if not new_objects:
        print("Nessun oggetto da analizzare a questo livello")
        return pd.DataFrame()
    
    # Filtra oggetti già noti
    to_extract = [obj for obj in new_objects 
                  if obj['name'].lower() not in known_objects]
    
    print(f"Oggetti totali: {len(new_objects)}")
    print(f"Oggetti da estrarre (non ancora analizzati): {len(to_extract)}")
    
    if not to_extract:
        print("Tutti gli oggetti sono già stati analizzati!")
        return pd.DataFrame()
    
    # Estrai oggetti
    all_analyzed = []
    not_found = []
    for i, obj in enumerate(to_extract):
        obj_name = obj['name']
        preferred_db = obj.get('database')
        
        if (i+1) % 10 == 0:
            print(f"Progresso: {i+1}/{len(to_extract)} oggetti...")
        
        try:
            # Cerca in database (prima preferito, poi altri)
            result = extract_sql_definition_multi_db(databases, obj_name, preferred_db)
            
            if result:
                analyzed = analyze_object(result)
                analyzed['Livello'] = level
                analyzed['Database'] = result['Database']
                all_analyzed.append(analyzed)
                
                # Aggiungi a known_objects
                known_objects[obj_name.lower()] = result['Database']
                
                if (i+1) <= 10:  # Mostra primi 10 trovati
                    print(f"  ✓ {obj_name} trovato in {result['Database']}")
            else:
                not_found.append(obj_name)
                if len(not_found) <= 10:  # Mostra primi 10 non trovati
                    print(f"  ✗ {obj_name} NON trovato in nessun database")
            
        except Exception as e:
            print(f"  ERRORE {obj_name}: {e}")
    
    if not_found:
        print(f"\n⚠ Oggetti non trovati: {len(not_found)}/{len(to_extract)}")
        print("Possibili cause:")
        print("  - Oggetti sono tabelle/viste (non hanno SQLDefinition)")
        print("  - Nomi nel file includono prefissi non riconosciuti")
        print("  - Oggetti non esistono nei database disponibili")
    
    if not all_analyzed:
        print("Nessun oggetto estratto a questo livello")
        return pd.DataFrame()
    
    # Crea DataFrame
    df_level = pd.DataFrame(all_analyzed)
    
    # Salva risultati livello
    output_file = OUTPUT_DIR / f"livello_{level}_analizzati.xlsx"
    df_level.to_excel(output_file, index=False)
    print(f"\nSalvato: {output_file}")
    
    # Trova nuove dipendenze per questo livello
    all_deps = {}  # {dep_name: [list of caller_dbs]}
    
    for idx, row in df_level.iterrows():
        caller_db = row['Database']
        deps_str = row['Dipendenze']
        
        if isinstance(deps_str, str) and deps_str != 'Nessuna':
            deps = [d.strip().lower() for d in deps_str.split(';')]
            for dep in deps:
                if dep not in all_deps:
                    all_deps[dep] = []
                all_deps[dep].append(caller_db)
    
    # Filtra dipendenze già note
    new_deps = []
    for dep_name, caller_dbs in all_deps.items():
        if dep_name not in known_objects:
            dep_type = classify_object_type(dep_name)
            
            # Database preferito: quello più comune tra i chiamanti
            preferred_db = max(set(caller_dbs), key=caller_dbs.count)
            
            new_deps.append({
                'Nuovo_Oggetto': dep_name,
                'ObjectType': dep_type,
                'Livello_Trovato': level,
                'Database_Preferito': preferred_db,
                'N_Chiamanti': len(caller_dbs)
            })
    
    if new_deps:
        df_new_deps = pd.DataFrame(new_deps)
        
        # Filtra solo SP/Functions/Triggers (non tabelle)
        df_new_deps = df_new_deps[df_new_deps['ObjectType'] != 'Tabella']
        
        new_deps_file = OUTPUT_DIR / f"livello_{level}_nuove_dipendenze.xlsx"
        df_new_deps.to_excel(new_deps_file, index=False)
        print(f"Nuove dipendenze trovate: {len(df_new_deps)}")
        print(f"Salvato: {new_deps_file}")
        
        # Ricorsione sul prossimo livello
        return pd.concat([df_level, recursive_extraction(databases, known_objects, level+1)])
    else:
        print(f"\nNessuna nuova dipendenza trovata al livello {level}")
        print("Estrazione ricorsiva completata!")
        return df_level

# =========================
# MAIN
# =========================

def main():
    print("="*60)
    print("ESTRAZIONE RICORSIVA DIPENDENZE")
    print("="*60)
    
    # Carica oggetti già noti
    print("\n1. Caricamento oggetti già analizzati...")
    known_objects = load_known_objects()
    
    # Connessione SQL per lista database
    print(f"\n2. Connessione a SQL Server {SQL_SERVER}...")
    try:
        conn = get_sql_connection()
        print(f"Connesso a: {SQL_SERVER}")
        
        # Ottieni lista database
        print("\n3. Recupero lista database disponibili...")
        databases = get_available_databases(conn)
        print(f"Database trovati: {len(databases)}")
        for db in databases:
            print(f"  - {db}")
        
        conn.close()
        
    except Exception as e:
        print(f"ERRORE connessione: {e}")
        print(f"\nVERIFICA: Server = {SQL_SERVER}")
        return
    
    # Estrazione ricorsiva
    print("\n4. Avvio estrazione ricorsiva...")
    try:
        df_all = recursive_extraction(databases, known_objects, level=2)
        
        if not df_all.empty:
            # Salva file consolidato
            consolidated_file = OUTPUT_DIR / "dipendenze_ricorsive_consolidate.xlsx"
            df_all.to_excel(consolidated_file, index=False)
            print(f"\n{'='*60}")
            print(f"COMPLETATO!")
            print(f"{'='*60}")
            print(f"Totale oggetti analizzati: {len(df_all)}")
            
            # Statistiche per database
            db_counts = df_all['Database'].value_counts()
            print(f"\nDistribuzione per database:")
            for db, count in db_counts.items():
                print(f"  - {db}: {count} oggetti")
            
            print(f"\nFile consolidato: {consolidated_file}")
        else:
            print("\nNessun oggetto estratto")
            
    except Exception as e:
        print(f"\nERRORE durante estrazione: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
