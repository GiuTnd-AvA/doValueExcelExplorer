"""
Script per generare report .txt di validazione tabelle SQL
Input: Tabelle_Dipendenze_VALIDATED.xlsx (output di validate_table_types.py)
Output: TABLES_VALIDATION_REPORT.txt
"""

import pandas as pd
from datetime import datetime
from collections import defaultdict
from pathlib import Path

# Configurazione percorsi
BASE_PATH = Path(r"\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL")
INPUT_EXCEL = BASE_PATH / "Tabelle_Dipendenze_VALIDATED.xlsx"
OUTPUT_TXT = BASE_PATH / "TABLES_VALIDATION_REPORT.txt"


def generate_header():
    """Genera intestazione del report"""
    now = datetime.now()
    lines = []
    lines.append("=" * 80)
    lines.append("REPORT VALIDAZIONE TABELLE E DIPENDENZE SQL".center(80))
    lines.append("=" * 80)
    lines.append("")
    lines.append(f"Data generazione: {now.strftime('%d/%m/%Y %H:%M:%S')}")
    lines.append(f"Input file: {INPUT_EXCEL.name}")
    lines.append("")
    return lines


def generate_summary(df, stats):
    """Genera sezione Summary Esecutivo"""
    lines = []
    lines.append("")
    lines.append("─" * 80)
    lines.append("SUMMARY ESECUTIVO")
    lines.append("─" * 80)
    lines.append("")
    
    total = len(df)
    found = stats['Status_FOUND']
    not_found = stats['Status_NOT_FOUND']
    error = stats['Status_ERROR']
    missing = stats['Status_MISSING_DATA']
    
    lines.append(f"Totale oggetti analizzati: {total}")
    lines.append("")
    lines.append("STATO VALIDAZIONE:")
    lines.append(f"  • FOUND (Trovati):        {found:>6} ({found/total*100:>5.1f}%)")
    lines.append(f"  • NOT_FOUND (Non trovati):{not_found:>6} ({not_found/total*100:>5.1f}%)")
    lines.append(f"  • ERROR (Errori query):   {error:>6} ({error/total*100:>5.1f}%)")
    lines.append(f"  • MISSING_DATA (Dati mancanti): {missing:>6} ({missing/total*100:>5.1f}%)")
    lines.append("")
    
    # Breakdown per tipo oggetto
    if found > 0:
        lines.append("TIPOLOGIE OGGETTI TROVATI:")
        type_stats = {k.replace('Type_', ''): v for k, v in stats.items() if k.startswith('Type_')}
        for obj_type in sorted(type_stats.keys(), key=lambda x: type_stats[x], reverse=True):
            count = type_stats[obj_type]
            lines.append(f"  • {obj_type:<30} {count:>6} ({count/found*100:>5.1f}%)")
        lines.append("")
    
    # Breakdown per server
    servers = df[df['Status'] == 'FOUND']['SERVER'].value_counts()
    if len(servers) > 0:
        lines.append("DISTRIBUZIONE PER SERVER:")
        for server, count in servers.items():
            lines.append(f"  • {server:<30} {count:>6} oggetti")
        lines.append("")
    
    return lines


def generate_detail_by_type(df):
    """Genera sezione dettaglio per tipo di oggetto"""
    lines = []
    lines.append("")
    lines.append("─" * 80)
    lines.append("DETTAGLIO PER TIPO DI OGGETTO")
    lines.append("─" * 80)
    
    # Filtra solo oggetti trovati
    found_df = df[df['Status'] == 'FOUND'].copy()
    
    if len(found_df) == 0:
        lines.append("")
        lines.append("Nessun oggetto trovato.")
        return lines
    
    # Raggruppa per tipo
    for obj_type in sorted(found_df['ObjectType'].unique()):
        type_df = found_df[found_df['ObjectType'] == obj_type]
        lines.append("")
        lines.append(f"┌─ {obj_type} ({len(type_df)} oggetti)")
        lines.append("│")
        
        # Raggruppa per database
        for db in sorted(type_df['DATABASE'].unique()):
            db_objects = type_df[type_df['DATABASE'] == db]
            lines.append(f"│  Database: [{db}] ({len(db_objects)} oggetti)")
            
            # Lista oggetti (max 50 per database per non esagerare)
            for idx, row in db_objects.head(50).iterrows():
                server = row.get('SERVER', 'N/A')
                schema = row.get('Schema', 'dbo')
                table = row.get('TABLE', 'N/A')
                lines.append(f"│    • [{server}].[{db}].[{schema}].[{table}]")
            
            if len(db_objects) > 50:
                lines.append(f"│    ... e altri {len(db_objects) - 50} oggetti")
            lines.append("│")
        
        lines.append("└" + "─" * 79)
    
    return lines


def generate_detail_by_database(df):
    """Genera sezione dettaglio per database"""
    lines = []
    lines.append("")
    lines.append("─" * 80)
    lines.append("DETTAGLIO PER DATABASE")
    lines.append("─" * 80)
    
    found_df = df[df['Status'] == 'FOUND'].copy()
    
    if len(found_df) == 0:
        lines.append("")
        lines.append("Nessun oggetto trovato.")
        return lines
    
    lines.append("")
    
    # Raggruppa per database
    for db in sorted(found_df['DATABASE'].unique()):
        db_df = found_df[found_df['DATABASE'] == db]
        lines.append(f"Database: [{db}]")
        lines.append(f"  Totale oggetti: {len(db_df)}")
        
        # Breakdown per tipo
        type_counts = db_df['ObjectType'].value_counts()
        lines.append("  Breakdown per tipo:")
        for obj_type, count in type_counts.items():
            lines.append(f"    • {obj_type:<25} {count:>4}")
        
        lines.append("")
    
    return lines


def generate_issues_section(df):
    """Genera sezione problematiche"""
    lines = []
    lines.append("")
    lines.append("─" * 80)
    lines.append("PROBLEMATICHE E ANOMALIE")
    lines.append("─" * 80)
    
    # NOT_FOUND
    not_found_df = df[df['Status'] == 'NOT_FOUND']
    if len(not_found_df) > 0:
        lines.append("")
        lines.append(f"OGGETTI NON TROVATI ({len(not_found_df)}):")
        lines.append("Possibili cause: oggetto eliminato, nome errato, database non accessibile")
        lines.append("")
        for idx, row in not_found_df.head(100).iterrows():
            server = row.get('SERVER', 'N/A')
            db = row.get('DATABASE', 'N/A')
            table = row.get('TABLE', 'N/A')
            lines.append(f"  • [{server}].[{db}].[{table}]")
        
        if len(not_found_df) > 100:
            lines.append(f"  ... e altri {len(not_found_df) - 100} oggetti")
    
    # ERROR
    error_df = df[df['Status'] == 'ERROR']
    if len(error_df) > 0:
        lines.append("")
        lines.append(f"ERRORI DI QUERY ({len(error_df)}):")
        lines.append("Possibili cause: problemi di connessione, permessi insufficienti")
        lines.append("")
        for idx, row in error_df.head(50).iterrows():
            server = row.get('SERVER', 'N/A')
            db = row.get('DATABASE', 'N/A')
            table = row.get('TABLE', 'N/A')
            lines.append(f"  • [{server}].[{db}].[{table}]")
        
        if len(error_df) > 50:
            lines.append(f"  ... e altri {len(error_df) - 50} oggetti")
    
    # MISSING_DATA
    missing_df = df[df['Status'] == 'MISSING_DATA']
    if len(missing_df) > 0:
        lines.append("")
        lines.append(f"DATI MANCANTI ({len(missing_df)}):")
        lines.append("Righe con SERVER, DATABASE o TABLE vuoti")
        lines.append("")
        for idx, row in missing_df.head(50).iterrows():
            server = row.get('SERVER', 'N/A') if pd.notna(row.get('SERVER')) else 'N/A'
            db = row.get('DATABASE', 'N/A') if pd.notna(row.get('DATABASE')) else 'N/A'
            table = row.get('TABLE', 'N/A') if pd.notna(row.get('TABLE')) else 'N/A'
            lines.append(f"  • [{server}].[{db}].[{table}]")
        
        if len(missing_df) > 50:
            lines.append(f"  ... e altri {len(missing_df) - 50} oggetti")
    
    if len(not_found_df) == 0 and len(error_df) == 0 and len(missing_df) == 0:
        lines.append("")
        lines.append("✓ Nessuna problematica rilevata - tutti gli oggetti validati con successo!")
    
    lines.append("")
    
    return lines


def generate_complete_list(df):
    """Genera lista completa oggetti"""
    lines = []
    lines.append("")
    lines.append("─" * 80)
    lines.append("LISTA COMPLETA OGGETTI VALIDATI")
    lines.append("─" * 80)
    lines.append("")
    
    found_df = df[df['Status'] == 'FOUND'].copy()
    
    if len(found_df) == 0:
        lines.append("Nessun oggetto trovato.")
        return lines
    
    # Ordina per server, database, schema, table
    found_df = found_df.sort_values(['SERVER', 'DATABASE', 'Schema', 'TABLE'])
    
    current_server = None
    current_db = None
    
    for idx, row in found_df.iterrows():
        server = row.get('SERVER', 'N/A')
        db = row.get('DATABASE', 'N/A')
        schema = row.get('Schema', 'dbo')
        table = row.get('TABLE', 'N/A')
        obj_type = row.get('ObjectType', 'UNKNOWN')
        
        # Intestazione server
        if server != current_server:
            lines.append("")
            lines.append(f"SERVER: {server}")
            lines.append("─" * 80)
            current_server = server
            current_db = None
        
        # Intestazione database
        if db != current_db:
            lines.append(f"  Database: {db}")
            current_db = db
        
        # Oggetto
        lines.append(f"    • [{schema}].[{table}] | {obj_type}")
    
    lines.append("")
    
    return lines


def generate_footer(stats):
    """Genera footer del report"""
    lines = []
    lines.append("")
    lines.append("─" * 80)
    lines.append("STATISTICHE FINALI")
    lines.append("─" * 80)
    lines.append("")
    
    for key in sorted(stats.keys()):
        lines.append(f"  {key:<40} {stats[key]:>6}")
    
    lines.append("")
    lines.append("=" * 80)
    lines.append("FINE REPORT")
    lines.append("=" * 80)
    
    return lines


def main():
    """Funzione principale"""
    print("\n" + "="*80)
    print("GENERAZIONE REPORT VALIDAZIONE TABELLE")
    print("="*80)
    print(f"\nInput:  {INPUT_EXCEL}")
    print(f"Output: {OUTPUT_TXT}\n")
    
    # Leggi Excel
    try:
        df = pd.read_excel(INPUT_EXCEL, sheet_name='Validated')
        print(f"✓ Lette {len(df)} righe dal file Excel")
    except Exception as e:
        print(f"✗ Errore lettura Excel: {e}")
        return
    
    # Calcola statistiche
    stats = defaultdict(int)
    stats['Total'] = len(df)
    
    # Conta per status
    for status in ['FOUND', 'NOT_FOUND', 'ERROR', 'MISSING_DATA']:
        stats[f'Status_{status}'] = len(df[df['Status'] == status])
    
    # Conta per tipo oggetto (solo FOUND)
    found_df = df[df['Status'] == 'FOUND']
    if len(found_df) > 0:
        for obj_type in found_df['ObjectType'].unique():
            if pd.notna(obj_type):
                stats[f'Type_{obj_type}'] = len(found_df[found_df['ObjectType'] == obj_type])
    
    print("✓ Statistiche calcolate")
    
    # Genera report
    report_lines = []
    
    report_lines.extend(generate_header())
    report_lines.extend(generate_summary(df, stats))
    report_lines.extend(generate_detail_by_type(df))
    report_lines.extend(generate_detail_by_database(df))
    report_lines.extend(generate_issues_section(df))
    report_lines.extend(generate_complete_list(df))
    report_lines.extend(generate_footer(stats))
    
    # Scrivi file
    try:
        with open(OUTPUT_TXT, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        print(f"✓ Report generato: {OUTPUT_TXT}")
        print(f"  Totale righe: {len(report_lines)}")
    except Exception as e:
        print(f"✗ Errore scrittura report: {e}")
        return
    
    # Summary finale
    print("\n" + "─"*80)
    print("SUMMARY:")
    print(f"  Totale oggetti:    {stats['Total']}")
    print(f"  Trovati (FOUND):   {stats['Status_FOUND']} ({stats['Status_FOUND']/stats['Total']*100:.1f}%)")
    print(f"  Non trovati:       {stats['Status_NOT_FOUND']} ({stats['Status_NOT_FOUND']/stats['Total']*100:.1f}%)")
    print(f"  Errori:            {stats['Status_ERROR']} ({stats['Status_ERROR']/stats['Total']*100:.1f}%)")
    print(f"  Dati mancanti:     {stats['Status_MISSING_DATA']} ({stats['Status_MISSING_DATA']/stats['Total']*100:.1f}%)")
    print("─"*80 + "\n")


if __name__ == "__main__":
    main()
