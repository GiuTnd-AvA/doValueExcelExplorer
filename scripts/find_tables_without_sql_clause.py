# =========================
# IMPORT
# =========================
from config.config import EXCEL_OUTPUT_PATH
import pandas as pd
from pathlib import Path

# =========================
# MAIN
# =========================

def main():
    print("=== Ricerca Tabelle senza SQL_CLAUSE ===\n")
    
    # Trova tutti i file di estrazione
    base_path = Path(EXCEL_OUTPUT_PATH)
    parent_dir = base_path.parent
    base_name_full = base_path.name  # Include .xlsx
    
    # Pattern: {base_name}.xlsx_parziale_*.xlsx
    files = list(parent_dir.glob(f"{base_name_full}_parziale_*.xlsx"))
    
    if not files:
        print(f"ERRORE: Nessun file trovato con pattern '{base_name_full}_parziale_*.xlsx' in {parent_dir}")
        return
    
    print(f"Trovati {len(files)} file da analizzare\n")
    
    # Lista per raccogliere tutte le tabelle senza SQL_CLAUSE
    tables_without_clause = []
    total_rows = 0
    total_without_clause = 0
    
    for file_path in sorted(files):
        print(f"Analisi: {file_path.name}")
        
        try:
            # Leggi file Excel
            df = pd.read_excel(file_path)
            total_rows += len(df)
            
            # Verifica che la colonna SQL_CLAUSE esista
            if 'SQL_CLAUSE' not in df.columns:
                print(f"  - ATTENZIONE: Colonna SQL_CLAUSE non trovata, skip file")
                continue
            
            # Filtra righe senza SQL_CLAUSE (vuote o NaN)
            df_no_clause = df[df['SQL_CLAUSE'].isna() | (df['SQL_CLAUSE'] == '')]
            
            count_no_clause = len(df_no_clause)
            total_without_clause += count_no_clause
            
            print(f"  - Righe totali: {len(df)}")
            print(f"  - Senza SQL_CLAUSE: {count_no_clause}")
            
            if count_no_clause > 0:
                # Estrai colonne rilevanti
                for idx, row in df_no_clause.iterrows():
                    tables_without_clause.append({
                        'File': file_path.name,
                        'ObjectName': row.get('ObjectName', ''),
                        'ObjectType': row.get('ObjectType', ''),
                        'Server': row.get('Server', ''),
                        'Database': row.get('Database', ''),
                        'Table': row.get('Table', ''),
                        'Schema': row.get('Schema', ''),
                        'SQL_CLAUSE': row.get('SQL_CLAUSE', ''),
                        'CLAUSE_TYPE': row.get('CLAUSE_TYPE', '')
                    })
        
        except Exception as e:
            print(f"  ERRORE: {e}")
    
    print(f"\n{'='*60}")
    print(f"RIEPILOGO:")
    print(f"  - Righe totali processate: {total_rows}")
    print(f"  - Tabelle senza SQL_CLAUSE: {total_without_clause}")
    print(f"  - Percentuale: {(total_without_clause/total_rows*100):.1f}%")
    print(f"{'='*60}\n")
    
    if tables_without_clause:
        # Crea DataFrame
        df_result = pd.DataFrame(tables_without_clause)
        
        # Esporta risultati
        output_path = parent_dir / "tabelle_senza_sql_clause.xlsx"
        df_result.to_excel(output_path, index=False)
        print(f"✓ Esportato: {output_path}")
        print(f"  - {len(df_result)} righe salvate\n")
        
        # Mostra prime 20 tabelle
        print("Prime 20 tabelle senza SQL_CLAUSE:")
        print("-" * 80)
        for i, (idx, row) in enumerate(df_result.head(20).iterrows(), start=1):
            schema = row.get('Schema', '')
            table = row.get('Table', '')
            table_name = f"{schema}.{table}" if schema and pd.notna(schema) and schema != '' else table
            print(f"{i}. {table_name} (DB: {row['Database']}, Oggetto: {row['ObjectName']})")
        
        if len(df_result) > 20:
            print(f"\n... e altre {len(df_result) - 20} tabelle (vedi file Excel)")
        
        # Statistiche per database
        print(f"\n{'='*60}")
        print("Distribuzione per Database:")
        db_counts = df_result['Database'].value_counts()
        for db, count in db_counts.items():
            print(f"  - {db}: {count} tabelle")
        
    else:
        print("✓ Tutte le tabelle hanno SQL_CLAUSE popolato!")
    
    print(f"\n{'='*60}")
    print("=== Analisi completata ===")

if __name__ == "__main__":
    main()
