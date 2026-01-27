# =========================
# IMPORT
# =========================
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

# =========================
# CONFIG
# =========================
# Path al file Excel con le tabelle da validare
INPUT_EXCEL = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\Copia di SUMMARY_REPORT_HYBRID.xlsx'

# Output
OUTPUT_EXCEL = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\Tabelle_Dipendenze_VALIDATED.xlsx'

# Driver SQL Server
DRIVER = 'ODBC Driver 18 for SQL Server'
try:
    if not any('ODBC Driver 18 for SQL Server' in d for d in pyodbc.drivers()):
        DRIVER = 'ODBC Driver 17 for SQL Server'
except:
    DRIVER = 'ODBC Driver 17 for SQL Server'

# =========================
# FUNZIONI
# =========================

def get_engine(server, database, driver):
    """Crea engine SQLAlchemy per connessione SQL Server."""
    conn_str = f"mssql+pyodbc://@{server}/{database}?driver={driver}&trusted_connection=yes"
    return create_engine(conn_str, pool_pre_ping=True, pool_size=10, max_overflow=20)

def get_object_type(server, database, table_name, engine_cache):
    """
    Interroga SQL Server per ottenere il tipo dell'oggetto.
    Ritorna: (object_type, schema_name) o (None, None) se non trovato.
    """
    try:
        # Crea o recupera engine
        key = (server, database)
        if key not in engine_cache:
            engine_cache[key] = get_engine(server, database, DRIVER)
        
        engine = engine_cache[key]
        
        # Query per ottenere tipo oggetto
        # Cerca in USER_TABLE e VIEW (più comuni)
        query = text("""
            SELECT 
                o.type_desc AS ObjectType,
                SCHEMA_NAME(o.schema_id) AS SchemaName
            FROM sys.objects o
            WHERE o.name = :table_name
              AND o.type IN ('U', 'V', 'S', 'IT', 'TF', 'IF')  -- U=TABLE, V=VIEW, S=SYSTEM_TABLE, etc.
            ORDER BY 
                CASE o.type
                    WHEN 'U' THEN 1  -- Preferisci USER_TABLE
                    WHEN 'V' THEN 2  -- Poi VIEW
                    ELSE 3
                END
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"table_name": table_name}).fetchone()
            
            if result:
                return result[0], result[1]
            else:
                return None, None
    
    except Exception as e:
        print(f"✗ Errore su {server}.{database}.{table_name}: {e}")
        return 'ERROR', None

def validate_tables(input_path, output_path):
    """
    Valida le tabelle nel file Excel interrogando SQL Server.
    Aggiunge colonne: Schema, ObjectType, Status
    """
    print("\n" + "="*80)
    print("VALIDAZIONE TIPI OGGETTI SQL")
    print("="*80)
    print(f"\nInput:  {input_path}")
    print(f"Output: {output_path}\n")
    
    # Leggi Excel (sheet 21 = indice 20)
    try:
        df = pd.read_excel(input_path, sheet_name=20)  # 21esimo sheet (indice 0-based)
        print(f"✓ Lette {len(df)} righe dal sheet 21 del file Excel")
        print(f"  Colonne presenti: {list(df.columns)}")
    except Exception as e:
        print(f"✗ Errore lettura Excel: {e}")
        return
    
    # Verifica colonne richieste
    required_cols = ['SERVER', 'DATABASE', 'TABLE']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"✗ Colonne mancanti: {missing_cols}")
        print(f"  Colonne disponibili: {list(df.columns)}")
        return
    
    # Prepara liste per nuove colonne
    schemas = []
    object_types = []
    statuses = []
    
    # Cache engine
    engine_cache = {}
    
    # Statistiche
    stats = defaultdict(int)
    errors = []
    
    # Elabora ogni riga
    print("\nInterrogazione SQL Server in corso...\n")
    
    total = len(df)
    row_num = 0
    for i, row in df.iterrows():
        row_num += 1
        server = str(row.get('SERVER', ''))
        database = str(row.get('DATABASE', ''))
        table = str(row.get('TABLE', ''))
        
        # Progress
        if row_num % 50 == 0:
            print(f"  Processate {row_num}/{total} righe...")
        
        # Valida parametri
        if not server or not database or not table or pd.isna(row.get('SERVER')) or pd.isna(row.get('DATABASE')) or pd.isna(row.get('TABLE')):
            schemas.append(None)
            object_types.append(None)
            statuses.append('MISSING_DATA')
            stats['MISSING_DATA'] += 1
            continue
        
        # Normalizza nomi
        server = server.strip()
        database = database.strip()
        table = table.strip()
        
        # Interroga SQL Server
        obj_type, schema = get_object_type(server, database, table, engine_cache)
        
        # Aggiorna liste
        if obj_type is not None and obj_type:
            schemas.append(schema if schema else 'dbo')
            object_types.append(obj_type)
            
            if obj_type == 'ERROR':
                statuses.append('ERROR')
                stats['ERROR'] += 1
                errors.append(f"{server}.{database}.{table}: Query error")
            else:
                statuses.append('FOUND')
                stats['FOUND'] += 1
                # Conta anche per tipo specifico
                stats[f'Type_{obj_type}'] += 1
        else:
            schemas.append(None)
            object_types.append(None)
            statuses.append('NOT_FOUND')
            stats['NOT_FOUND'] += 1
            errors.append(f"{server}.{database}.{table}: Not found in database")
    
    # Aggiungi colonne al DataFrame
    df['Schema'] = schemas
    df['ObjectType'] = object_types
    df['Status'] = statuses
    
    # Chiudi connessioni
    for engine in engine_cache.values():
        engine.dispose()
    
    # Stampa statistiche
    print(f"\n{'='*80}")
    print("STATISTICHE VALIDAZIONE")
    print(f"{'='*80}\n")
    
    print(f"Totale oggetti processati:  {total}\n")
    
    print("Per tipo oggetto:")
    # Estrai tipi specifici
    type_stats = {k.replace('Type_', ''): v for k, v in stats.items() if k.startswith('Type_')}
    for obj_type in sorted(type_stats.keys()):
        count = type_stats[obj_type]
        pct = count / total * 100
        print(f"  • {obj_type:<30}: {count:>4} ({pct:>5.1f}%)")
    
    print(f"\n{'Status':<30}{'Count':<10}{'%'}")
    print("-" * 50)
    print(f"{'FOUND (oggetti validi)':<30}{stats.get('FOUND', 0):<10}{stats.get('FOUND', 0)/total*100:>5.1f}%")
    print(f"{'NOT_FOUND':<30}{stats.get('NOT_FOUND', 0):<10}{stats.get('NOT_FOUND', 0)/total*100:>5.1f}%")
    print(f"{'ERROR':<30}{stats.get('ERROR', 0):<10}{stats.get('ERROR', 0)/total*100:>5.1f}%")
    print(f"{'MISSING_DATA':<30}{stats.get('MISSING_DATA', 0):<10}{stats.get('MISSING_DATA', 0)/total*100:>5.1f}%")
    
    # Mostra primi 10 errori
    if errors and len(errors) > 0:
        print(f"\n⚠️  Primi 10 oggetti non trovati/errori:")
        for err in errors[:10]:
            print(f"    {err}")
        if len(errors) > 10:
            print(f"    ... e altri {len(errors) - 10} errori")
    
    # Salva Excel
    print(f"\n💾 Salvataggio risultati...")
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Sheet 1: Tutti i dati validati
            df.to_excel(writer, sheet_name='Validated', index=False)
            
            # Sheet 2: Summary statistics
            summary_data = []
            
            # Aggiungi tipi specifici
            type_stats = {k.replace('Type_', ''): v for k, v in stats.items() if k.startswith('Type_')}
            for obj_type in sorted(type_stats.keys()):
                summary_data.append({
                    'Type': obj_type,
                    'Count': type_stats[obj_type],
                    'Percentage': f"{type_stats[obj_type]/total*100:.1f}%"
                })
            
            # Aggiungi status
            summary_data.append({'Type': '--- STATUS ---', 'Count': '', 'Percentage': ''})
            for status in ['FOUND', 'NOT_FOUND', 'ERROR', 'MISSING_DATA']:
                if status in stats:
                    summary_data.append({
                        'Type': status,
                        'Count': stats[status],
                        'Percentage': f"{stats[status]/total*100:.1f}%"
                    })
            
            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name='Summary', index=False)
            
            # Sheet 3: Solo oggetti trovati (USER_TABLE e VIEW)
            df_found = df[df['ObjectType'].isin(['USER_TABLE', 'VIEW'])].copy()
            if len(df_found) > 0:
                df_found.to_excel(writer, sheet_name='Tables_Views_Only', index=False)
            
            # Sheet 4: Oggetti problematici (NOT_FOUND, ERROR)
            df_issues = df[df['Status'].isin(['NOT_FOUND', 'ERROR', 'MISSING_DATA'])].copy()
            if len(df_issues) > 0:
                df_issues.to_excel(writer, sheet_name='Issues', index=False)
        
        print(f"✅ File salvato: {output_path}")
        
    except Exception as e:
        print(f"✗ Errore salvataggio Excel: {e}")
        return
    
    print("\n" + "="*80)
    print("✅ VALIDAZIONE COMPLETATA")
    print("="*80)
    print(f"\nFile Excel con 4 sheets:")
    print(f"  1. Validated: Tutti i dati con Schema, ObjectType, Status")
    print(f"  2. Summary: Statistiche per tipo")
    print(f"  3. Tables_Views_Only: Solo USER_TABLE e VIEW valide")
    print(f"  4. Issues: Oggetti non trovati o con errori\n")

# =========================
# MAIN
# =========================

def main():
    validate_tables(INPUT_EXCEL, OUTPUT_EXCEL)

if __name__ == "__main__":
    main()
