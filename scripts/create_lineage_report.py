# =========================
# IMPORT
# =========================
import pandas as pd
from pathlib import Path
from collections import defaultdict

# =========================
# CONFIG
# =========================
SUMMARY_PATH = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\SUMMARY_REPORT.xlsx'
OUTPUT_PATH = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\LINEAGE_REPORT.txt'
OUTPUT_MD_PATH = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\LINEAGE_REPORT.md'

# =========================
# FUNZIONI
# =========================

def load_summary_data(summary_path):
    """Carica tutti i livelli dal SUMMARY_REPORT."""
    print(f"Caricamento dati da: {summary_path}")
    
    data = {}
    sheet_names = ['L1', 'L2', 'L3', 'L4', 'Conteggi']
    exploded_sheets = [
        'Tabelle_Esplose_L1', 'Oggetti_Esplosi_L1',
        'Tabelle_Esplose_L2', 'Oggetti_Esplosi_L2',
        'Tabelle_Esplose_L3', 'Oggetti_Esplosi_L3',
        'Tabelle_Esplose_L4', 'Oggetti_Esplosi_L4'
    ]
    
    # Carica sheet principali
    for sheet in sheet_names:
        try:
            df = pd.read_excel(summary_path, sheet_name=sheet)
            # Normalizza nomi database a uppercase
            if 'Database' in df.columns:
                df['Database'] = df['Database'].str.upper()
            data[sheet] = df
            print(f"  ✓ {sheet}: {len(data[sheet])} righe")
        except Exception as e:
            print(f"  ✗ {sheet}: {e}")
            data[sheet] = pd.DataFrame()
    
    # Carica exploded sheets
    for sheet in exploded_sheets:
        try:
            df = pd.read_excel(summary_path, sheet_name=sheet)
            # Normalizza nomi database a uppercase
            if 'Database' in df.columns:
                df['Database'] = df['Database'].str.upper()
            data[sheet] = df
            print(f"  ✓ {sheet}: {len(data[sheet])} righe")
        except Exception as e:
            print(f"  ✗ {sheet}: {e}")
            data[sheet] = pd.DataFrame()
    
    return data

def analyze_level(df, level_name):
    """Analizza un livello e restituisce statistiche."""
    if df.empty:
        return {
            'total': 0,
            'by_type': {},
            'critical': 0,
            'criticality_tech': {},
            'databases': set()
        }
    
    stats = {
        'total': len(df),
        'by_type': df['ObjectType'].value_counts().to_dict() if 'ObjectType' in df.columns else {},
        'critical': len(df[df['Critico_Migrazione'] == 'SÌ']) if 'Critico_Migrazione' in df.columns else 0,
        'criticality_tech': df['Criticità_Tecnica'].value_counts().to_dict() if 'Criticità_Tecnica' in df.columns else {},
        'databases': set(df['Database'].unique()) if 'Database' in df.columns else set()
    }
    
    return stats

def analyze_dependencies(df_exploded_tables, df_exploded_objects):
    """Analizza dipendenze da exploded sheets."""
    deps = {
        'total_table_deps': len(df_exploded_tables) if not df_exploded_tables.empty else 0,
        'unique_tables': df_exploded_tables['Tabella_Dipendente'].nunique() if not df_exploded_tables.empty and 'Tabella_Dipendente' in df_exploded_tables.columns else 0,
        'total_object_deps': len(df_exploded_objects) if not df_exploded_objects.empty else 0,
        'unique_objects': df_exploded_objects['Oggetto_Dipendente'].nunique() if not df_exploded_objects.empty and 'Oggetto_Dipendente' in df_exploded_objects.columns else 0
    }
    
    return deps

def generate_text_report(data):
    """Genera report testuale con lineage completo."""
    lines = []
    lines.append("="*80)
    lines.append("LINEAGE REPORT - SQL OBJECTS MIGRATION")
    lines.append("="*80)
    lines.append("")
    
    # L1: Oggetti Critici (punto di partenza)
    lines.append("█ LIVELLO 1 - OGGETTI CRITICI DI PARTENZA")
    lines.append("-"*80)
    stats_l1 = analyze_level(data['L1'], 'L1')
    lines.append(f"Totale oggetti: {stats_l1['total']}")
    lines.append(f"Critici per migrazione (DML/DDL): {stats_l1['critical']}")
    lines.append("")
    lines.append("Distribuzione per tipo oggetto:")
    for obj_type, count in sorted(stats_l1['by_type'].items(), key=lambda x: -x[1]):
        lines.append(f"  • {obj_type:<30} {count:>5} oggetti")
    lines.append("")
    lines.append("Criticità Tecnica:")
    for crit, count in sorted(stats_l1['criticality_tech'].items()):
        lines.append(f"  • {crit:<10} {count:>5} oggetti")
    lines.append("")
    lines.append(f"Database coinvolti: {len(stats_l1['databases'])}")
    for db in sorted(stats_l1['databases']):
        lines.append(f"  • {db}")
    lines.append("")
    
    # Dipendenze L1
    deps_l1 = analyze_dependencies(data['Tabelle_Esplose_L1'], data['Oggetti_Esplosi_L1'])
    lines.append("Dipendenze identificate:")
    lines.append(f"  • Relazioni con tabelle: {deps_l1['total_table_deps']} (tabelle uniche: {deps_l1['unique_tables']})")
    lines.append(f"  • Relazioni con oggetti: {deps_l1['total_object_deps']} (oggetti unici: {deps_l1['unique_objects']})")
    lines.append("")
    lines.append("")
    
    # L2: Dipendenze di primo livello
    lines.append("█ LIVELLO 2 - OGGETTI CHE DIPENDONO DA L1")
    lines.append("-"*80)
    stats_l2 = analyze_level(data['L2'], 'L2')
    lines.append(f"Totale oggetti: {stats_l2['total']}")
    lines.append(f"Critici per migrazione: {stats_l2['critical']}")
    lines.append("")
    lines.append("Distribuzione per tipo oggetto:")
    for obj_type, count in sorted(stats_l2['by_type'].items(), key=lambda x: -x[1]):
        lines.append(f"  • {obj_type:<30} {count:>5} oggetti")
    lines.append("")
    
    deps_l2 = analyze_dependencies(data['Tabelle_Esplose_L2'], data['Oggetti_Esplosi_L2'])
    lines.append("Dipendenze L2:")
    lines.append(f"  • Relazioni con tabelle: {deps_l2['total_table_deps']} (tabelle uniche: {deps_l2['unique_tables']})")
    lines.append(f"  • Relazioni con oggetti: {deps_l2['total_object_deps']} (oggetti unici: {deps_l2['unique_objects']})")
    lines.append("")
    lines.append("")
    
    # L3: Dipendenze di secondo livello
    lines.append("█ LIVELLO 3 - OGGETTI CHE DIPENDONO DA L2")
    lines.append("-"*80)
    stats_l3 = analyze_level(data['L3'], 'L3')
    lines.append(f"Totale oggetti: {stats_l3['total']}")
    lines.append(f"Critici per migrazione: {stats_l3['critical']}")
    lines.append("")
    lines.append("Distribuzione per tipo oggetto:")
    for obj_type, count in sorted(stats_l3['by_type'].items(), key=lambda x: -x[1]):
        lines.append(f"  • {obj_type:<30} {count:>5} oggetti")
    lines.append("")
    
    deps_l3 = analyze_dependencies(data['Tabelle_Esplose_L3'], data['Oggetti_Esplosi_L3'])
    lines.append("Dipendenze L3:")
    lines.append(f"  • Relazioni con tabelle: {deps_l3['total_table_deps']} (tabelle uniche: {deps_l3['unique_tables']})")
    lines.append(f"  • Relazioni con oggetti: {deps_l3['total_object_deps']} (oggetti unici: {deps_l3['unique_objects']})")
    lines.append("")
    lines.append("")
    
    # L4: Dipendenze di terzo livello
    lines.append("█ LIVELLO 4 - OGGETTI CHE DIPENDONO DA L3")
    lines.append("-"*80)
    stats_l4 = analyze_level(data['L4'], 'L4')
    lines.append(f"Totale oggetti: {stats_l4['total']}")
    lines.append(f"Critici per migrazione: {stats_l4['critical']}")
    lines.append("")
    lines.append("Distribuzione per tipo oggetto:")
    for obj_type, count in sorted(stats_l4['by_type'].items(), key=lambda x: -x[1]):
        lines.append(f"  • {obj_type:<30} {count:>5} oggetti")
    lines.append("")
    
    deps_l4 = analyze_dependencies(data['Tabelle_Esplose_L4'], data['Oggetti_Esplosi_L4'])
    lines.append("Dipendenze L4:")
    lines.append(f"  • Relazioni con tabelle: {deps_l4['total_table_deps']} (tabelle uniche: {deps_l4['unique_tables']})")
    lines.append(f"  • Relazioni con oggetti: {deps_l4['total_object_deps']} (oggetti unici: {deps_l4['unique_objects']})")
    lines.append("")
    lines.append("")
    
    # RIEPILOGO FINALE
    lines.append("="*80)
    lines.append("RIEPILOGO MIGRAZIONE")
    lines.append("="*80)
    lines.append("")
    
    total_objects = stats_l1['total'] + stats_l2['total'] + stats_l3['total'] + stats_l4['total']
    total_critical = stats_l1['critical'] + stats_l2['critical'] + stats_l3['critical'] + stats_l4['critical']
    
    lines.append(f"Totale oggetti nel lineage: {total_objects}")
    lines.append(f"  • L1 (Critici partenza):  {stats_l1['total']:>6}")
    lines.append(f"  • L2 (Dipendenti da L1):  {stats_l2['total']:>6}")
    lines.append(f"  • L3 (Dipendenti da L2):  {stats_l3['total']:>6}")
    lines.append(f"  • L4 (Dipendenti da L3):  {stats_l4['total']:>6}")
    lines.append("")
    lines.append(f"Oggetti critici totali (DML/DDL): {total_critical}")
    lines.append("")
    
    # Tabelle uniche referenziate
    all_tables = set()
    for sheet in ['Tabelle_Esplose_L1', 'Tabelle_Esplose_L2', 'Tabelle_Esplose_L3', 'Tabelle_Esplose_L4']:
        if not data[sheet].empty and 'Tabella_Dipendente' in data[sheet].columns:
            all_tables.update(data[sheet]['Tabella_Dipendente'].unique())
    
    lines.append(f"Tabelle uniche referenziate: {len(all_tables)}")
    lines.append("")
    
    # Aggregazione per tipo oggetto
    lines.append("Distribuzione complessiva per tipo oggetto:")
    all_types = defaultdict(int)
    for stats in [stats_l1, stats_l2, stats_l3, stats_l4]:
        for obj_type, count in stats['by_type'].items():
            all_types[obj_type] += count
    
    for obj_type, count in sorted(all_types.items(), key=lambda x: -x[1]):
        lines.append(f"  • {obj_type:<30} {count:>6} oggetti")
    lines.append("")
    
    lines.append("="*80)
    lines.append("RACCOMANDAZIONI MIGRAZIONE")
    lines.append("="*80)
    lines.append("")
    lines.append("1. OGGETTI DA MIGRARE:")
    lines.append(f"   - Totale: {total_objects} oggetti SQL")
    lines.append(f"   - Priorità ALTA: {stats_l1['criticality_tech'].get('ALTA', 0)} oggetti con criticità tecnica ALTA")
    lines.append("")
    lines.append("2. TABELLE DA MIGRARE:")
    lines.append(f"   - Totale tabelle referenziate: {len(all_tables)}")
    lines.append("   - Consultare sheets Tabelle_Esplose_L* per elenco completo")
    lines.append("")
    lines.append("3. ORDINE DI MIGRAZIONE SUGGERITO:")
    lines.append("   1) Migrare tabelle (dipendenze dati)")
    lines.append("   2) Migrare oggetti L1 (critici)")
    lines.append("   3) Migrare oggetti L2 (dipendenti da L1)")
    lines.append("   4) Migrare oggetti L3 (dipendenti da L2)")
    lines.append("   5) Migrare oggetti L4 (dipendenti da L3)")
    lines.append("")
    lines.append("="*80)
    
    return "\n".join(lines)

def generate_markdown_report(data):
    """Genera report in formato Markdown."""
    lines = []
    lines.append("# LINEAGE REPORT - SQL OBJECTS MIGRATION")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # L1
    lines.append("## 📊 LIVELLO 1 - OGGETTI CRITICI DI PARTENZA")
    lines.append("")
    stats_l1 = analyze_level(data['L1'], 'L1')
    lines.append(f"**Totale oggetti**: {stats_l1['total']}  ")
    lines.append(f"**Critici per migrazione (DML/DDL)**: {stats_l1['critical']}  ")
    lines.append("")
    lines.append("### Distribuzione per tipo oggetto")
    lines.append("")
    lines.append("| Tipo Oggetto | Conteggio |")
    lines.append("|--------------|-----------|")
    for obj_type, count in sorted(stats_l1['by_type'].items(), key=lambda x: -x[1]):
        lines.append(f"| {obj_type} | {count} |")
    lines.append("")
    
    deps_l1 = analyze_dependencies(data['Tabelle_Esplose_L1'], data['Oggetti_Esplosi_L1'])
    lines.append("### Dipendenze")
    lines.append(f"- **Relazioni con tabelle**: {deps_l1['total_table_deps']} (tabelle uniche: {deps_l1['unique_tables']})")
    lines.append(f"- **Relazioni con oggetti**: {deps_l1['total_object_deps']} (oggetti unici: {deps_l1['unique_objects']})")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # L2
    lines.append("## 📊 LIVELLO 2 - OGGETTI CHE DIPENDONO DA L1")
    lines.append("")
    stats_l2 = analyze_level(data['L2'], 'L2')
    lines.append(f"**Totale oggetti**: {stats_l2['total']}  ")
    lines.append(f"**Critici per migrazione**: {stats_l2['critical']}  ")
    lines.append("")
    lines.append("### Distribuzione per tipo oggetto")
    lines.append("")
    lines.append("| Tipo Oggetto | Conteggio |")
    lines.append("|--------------|-----------|")
    for obj_type, count in sorted(stats_l2['by_type'].items(), key=lambda x: -x[1]):
        lines.append(f"| {obj_type} | {count} |")
    lines.append("")
    
    deps_l2 = analyze_dependencies(data['Tabelle_Esplose_L2'], data['Oggetti_Esplosi_L2'])
    lines.append("### Dipendenze")
    lines.append(f"- **Relazioni con tabelle**: {deps_l2['total_table_deps']} (tabelle uniche: {deps_l2['unique_tables']})")
    lines.append(f"- **Relazioni con oggetti**: {deps_l2['total_object_deps']} (oggetti unici: {deps_l2['unique_objects']})")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # L3
    lines.append("## 📊 LIVELLO 3 - OGGETTI CHE DIPENDONO DA L2")
    lines.append("")
    stats_l3 = analyze_level(data['L3'], 'L3')
    lines.append(f"**Totale oggetti**: {stats_l3['total']}  ")
    lines.append("")
    lines.append("### Distribuzione per tipo oggetto")
    lines.append("")
    lines.append("| Tipo Oggetto | Conteggio |")
    lines.append("|--------------|-----------|")
    for obj_type, count in sorted(stats_l3['by_type'].items(), key=lambda x: -x[1]):
        lines.append(f"| {obj_type} | {count} |")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # L4
    lines.append("## 📊 LIVELLO 4 - OGGETTI CHE DIPENDONO DA L3")
    lines.append("")
    stats_l4 = analyze_level(data['L4'], 'L4')
    lines.append(f"**Totale oggetti**: {stats_l4['total']}  ")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # RIEPILOGO
    lines.append("## 🎯 RIEPILOGO MIGRAZIONE")
    lines.append("")
    total_objects = stats_l1['total'] + stats_l2['total'] + stats_l3['total'] + stats_l4['total']
    lines.append(f"**Totale oggetti nel lineage**: {total_objects}")
    lines.append("")
    lines.append("| Livello | Oggetti |")
    lines.append("|---------|---------|")
    lines.append(f"| L1 (Critici partenza) | {stats_l1['total']} |")
    lines.append(f"| L2 (Dipendenti da L1) | {stats_l2['total']} |")
    lines.append(f"| L3 (Dipendenti da L2) | {stats_l3['total']} |")
    lines.append(f"| L4 (Dipendenti da L3) | {stats_l4['total']} |")
    lines.append("")
    
    lines.append("### 📋 Raccomandazioni")
    lines.append("")
    lines.append("1. **Ordine di migrazione suggerito**:")
    lines.append("   - Migrare tabelle (dipendenze dati)")
    lines.append("   - Migrare oggetti L1 → L2 → L3 → L4")
    lines.append("")
    lines.append("2. **File di riferimento**: `SUMMARY_REPORT.xlsx`")
    lines.append("   - Sheet `L1`, `L2`, `L3`, `L4` per dettagli oggetti")
    lines.append("   - Sheet `Tabelle_Esplose_L*` per elenco tabelle")
    lines.append("   - Sheet `Conteggi` per statistiche aggregate")
    lines.append("")
    
    return "\n".join(lines)

# =========================
# MAIN
# =========================

def main():
    print("="*80)
    print("LINEAGE REPORT GENERATOR")
    print("="*80)
    print("")
    
    # Carica dati
    data = load_summary_data(SUMMARY_PATH)
    
    print("\n" + "="*80)
    print("Generazione report...")
    print("="*80 + "\n")
    
    # Genera report testuale
    text_report = generate_text_report(data)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(text_report)
    print(f"✓ Report testuale salvato: {OUTPUT_PATH}")
    
    # Genera report markdown
    md_report = generate_markdown_report(data)
    with open(OUTPUT_MD_PATH, 'w', encoding='utf-8') as f:
        f.write(md_report)
    print(f"✓ Report markdown salvato: {OUTPUT_MD_PATH}")
    
    print("\n" + "="*80)
    print("Report generati con successo!")
    print("="*80)
    
    # Stampa preview
    print("\n" + "="*80)
    print("PREVIEW REPORT")
    print("="*80 + "\n")
    print(text_report)

if __name__ == "__main__":
    main()
