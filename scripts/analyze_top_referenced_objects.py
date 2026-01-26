# =========================
# IMPORT
# =========================
import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# =========================
# CONFIG
# =========================
VALIDATION_REPORT = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\VALIDATION_REPORT.xlsx'
OUTPUT_ANALYSIS = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\TOP_REFERENCED_ANALYSIS.xlsx'
OUTPUT_TXT = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\TOP_REFERENCED_ANALYSIS.txt'

# Soglie per classificazione
HIGH_REFERENCE_THRESHOLD = 50  # Oggetti con 50+ riferimenti = CRITICI per dipendenze
MEDIUM_REFERENCE_THRESHOLD = 10  # 10-49 riferimenti = ALTA priorità
LOW_REFERENCE_THRESHOLD = 5     # 5-9 riferimenti = MEDIA priorità

# =========================
# FUNZIONI
# =========================

def load_validation_data(validation_path):
    """Carica tutti i sheet dal VALIDATION_REPORT."""
    print("="*80)
    print("CARICAMENTO DATI VALIDAZIONE")
    print("="*80 + "\n")
    
    sheets = {}
    
    try:
        # Carica tutti gli sheet
        xl_file = pd.ExcelFile(validation_path)
        
        for sheet_name in xl_file.sheet_names:
            sheets[sheet_name] = pd.read_excel(validation_path, sheet_name=sheet_name)
            print(f"✓ {sheet_name}: {len(sheets[sheet_name])} righe")
        
        print(f"\n✓ Caricati {len(sheets)} sheet")
        return sheets
        
    except Exception as e:
        print(f"✗ Errore caricamento: {e}")
        return {}

def analyze_top_non_critici(df_top_non_critici):
    """Analisi approfondita degli oggetti Top Referenced NON nel lineage."""
    print("\n" + "="*80)
    print("ANALISI TOP_NON_CRITICI - Oggetti Mancanti dal Lineage")
    print("="*80 + "\n")
    
    if df_top_non_critici.empty:
        print("✗ Nessun dato da analizzare")
        return {}
    
    total = len(df_top_non_critici)
    print(f"Totale oggetti: {total}")
    print("")
    
    # 1. Distribuzione per Database
    print("1️⃣ DISTRIBUZIONE PER DATABASE")
    print("-" * 60)
    db_dist = df_top_non_critici['Database'].value_counts().sort_values(ascending=False)
    for db, count in db_dist.items():
        pct = count/total*100
        print(f"  {db:20s}: {count:4d} oggetti ({pct:5.1f}%)")
    print("")
    
    # 2. Distribuzione per Tipo
    print("2️⃣ DISTRIBUZIONE PER TIPO OGGETTO")
    print("-" * 60)
    type_dist = df_top_non_critici['ObjectType'].value_counts().sort_values(ascending=False)
    for obj_type, count in type_dist.items():
        pct = count/total*100
        print(f"  {obj_type:30s}: {count:4d} ({pct:5.1f}%)")
    print("")
    
    # 3. Statistiche ReferenceCount
    print("3️⃣ STATISTICHE REFERENCE COUNT")
    print("-" * 60)
    ref_stats = df_top_non_critici['ReferenceCount'].describe()
    print(f"  Min:       {ref_stats['min']:.0f}")
    print(f"  25%:       {ref_stats['25%']:.0f}")
    print(f"  Mediana:   {ref_stats['50%']:.0f}")
    print(f"  75%:       {ref_stats['75%']:.0f}")
    print(f"  Max:       {ref_stats['max']:.0f}")
    print(f"  Media:     {ref_stats['mean']:.1f}")
    print("")
    
    # 4. Classificazione per Criticità Dipendenze
    print("4️⃣ CLASSIFICAZIONE PER CRITICITÀ DIPENDENZE")
    print("-" * 60)
    
    df_top_non_critici['Criticità_Dipendenze'] = pd.cut(
        df_top_non_critici['ReferenceCount'],
        bins=[0, LOW_REFERENCE_THRESHOLD, MEDIUM_REFERENCE_THRESHOLD, HIGH_REFERENCE_THRESHOLD, float('inf')],
        labels=['BASSA (1-4)', f'MEDIA ({LOW_REFERENCE_THRESHOLD}-{MEDIUM_REFERENCE_THRESHOLD-1})', 
                f'ALTA ({MEDIUM_REFERENCE_THRESHOLD}-{HIGH_REFERENCE_THRESHOLD-1})', f'CRITICA ({HIGH_REFERENCE_THRESHOLD}+)']
    )
    
    crit_dist = df_top_non_critici['Criticità_Dipendenze'].value_counts().sort_index(ascending=False)
    for crit_level, count in crit_dist.items():
        pct = count/total*100
        print(f"  {crit_level:20s}: {count:4d} oggetti ({pct:5.1f}%)")
    print("")
    
    # 5. Top 20 oggetti per ReferenceCount
    print("5️⃣ TOP 20 OGGETTI PIÙ REFERENZIATI (Mancanti dal Lineage)")
    print("-" * 60)
    top_20 = df_top_non_critici.nlargest(20, 'ReferenceCount')
    for i, (idx, row) in enumerate(top_20.iterrows(), start=1):
        obj_full = f"[{row['Database']}].[{row['Schema']}].[{row['ObjectName']}]"
        print(f"  {i:2d}. {obj_full:60s} | {row['ObjectType']:15s} | {row['ReferenceCount']:3.0f} refs")
    print("")
    
    # 6. Oggetti CRITICI per dipendenze (50+ refs) - DA AGGIUNGERE AL LINEAGE
    critical_deps = df_top_non_critici[df_top_non_critici['ReferenceCount'] >= HIGH_REFERENCE_THRESHOLD]
    print(f"6️⃣ OGGETTI CRITICI PER DIPENDENZE ({HIGH_REFERENCE_THRESHOLD}+ refs) - DA AGGIUNGERE")
    print("-" * 60)
    print(f"  Totale: {len(critical_deps)} oggetti")
    
    if len(critical_deps) > 0:
        print(f"\n  Per Database:")
        for db, count in critical_deps['Database'].value_counts().sort_values(ascending=False).items():
            print(f"    • {db}: {count} oggetti")
        
        print(f"\n  Per Tipo:")
        for obj_type, count in critical_deps['ObjectType'].value_counts().sort_values(ascending=False).items():
            print(f"    • {obj_type}: {count} oggetti")
    print("")
    
    # 7. Crosstab Database x ObjectType per criticità
    print("7️⃣ MATRICE DATABASE x TIPO per CRITICI (50+ refs)")
    print("-" * 60)
    if len(critical_deps) > 0:
        crosstab = pd.crosstab(
            critical_deps['Database'],
            critical_deps['ObjectType'],
            margins=True,
            margins_name='TOTALE'
        )
        print(crosstab)
    else:
        print("  Nessun oggetto critico trovato")
    print("")
    
    return {
        'total': total,
        'db_distribution': db_dist,
        'type_distribution': type_dist,
        'reference_stats': ref_stats,
        'criticality_distribution': crit_dist,
        'top_20': top_20,
        'critical_dependencies': critical_deps,
        'df_enriched': df_top_non_critici
    }

def analyze_critici_non_top(df_critici_non_top):
    """Analisi oggetti Critici ma NON top referenced."""
    print("\n" + "="*80)
    print("ANALISI CRITICI_NON_TOP - Critici ma Poco Referenziati")
    print("="*80 + "\n")
    
    if df_critici_non_top.empty:
        print("✗ Nessun dato da analizzare")
        return {}
    
    total = len(df_critici_non_top)
    print(f"Totale oggetti: {total}")
    print("")
    
    # Distribuzione per Database
    print("DISTRIBUZIONE PER DATABASE")
    print("-" * 60)
    for db, count in df_critici_non_top['Database'].value_counts().sort_values(ascending=False).items():
        pct = count/total*100
        print(f"  {db:20s}: {count:4d} oggetti ({pct:5.1f}%)")
    print("")
    
    # Distribuzione per Tipo
    print("DISTRIBUZIONE PER TIPO")
    print("-" * 60)
    for obj_type, count in df_critici_non_top['ObjectType'].value_counts().sort_values(ascending=False).items():
        pct = count/total*100
        print(f"  {obj_type:30s}: {count:4d} ({pct:5.1f}%)")
    print("")
    
    # DML_Count stats
    if 'DML_Count' in df_critici_non_top.columns:
        print("STATISTICHE DML/DDL COUNT")
        print("-" * 60)
        dml_stats = df_critici_non_top['DML_Count'].describe()
        print(f"  Media DML/DDL: {dml_stats['mean']:.1f}")
        print(f"  Max DML/DDL:   {dml_stats['max']:.0f}")
        print("")
    
    return {
        'total': total,
        'db_distribution': df_critici_non_top['Database'].value_counts(),
        'type_distribution': df_critici_non_top['ObjectType'].value_counts()
    }

def generate_recommendations(analysis_top, analysis_critici):
    """Genera raccomandazioni basate sulle analisi."""
    print("\n" + "="*80)
    print("RACCOMANDAZIONI STRATEGICHE")
    print("="*80 + "\n")
    
    recommendations = []
    
    # Raccomandazione 1: Oggetti critici per dipendenze
    if 'critical_dependencies' in analysis_top and len(analysis_top['critical_dependencies']) > 0:
        critical_count = len(analysis_top['critical_dependencies'])
        recommendations.append({
            'Priorità': 'MASSIMA',
            'Categoria': 'Dipendenze Critiche',
            'Count': critical_count,
            'Azione': f'Aggiungere al lineage {critical_count} oggetti con {HIGH_REFERENCE_THRESHOLD}+ riferimenti',
            'Impatto': 'Breaking dependencies multiple se non migrati'
        })
        print(f"1️⃣ PRIORITÀ MASSIMA - Dipendenze Critiche")
        print(f"   • {critical_count} oggetti con {HIGH_REFERENCE_THRESHOLD}+ riferimenti")
        print(f"   • Rischio: Breaking changes su decine/centinaia di oggetti dipendenti")
        print(f"   • Azione: AGGIUNGERE IMMEDIATAMENTE al lineage")
        print("")
    
    # Raccomandazione 2: Oggetti alta priorità
    if 'df_enriched' in analysis_top:
        df_enriched = analysis_top['df_enriched']
        high_priority = df_enriched[
            (df_enriched['ReferenceCount'] >= MEDIUM_REFERENCE_THRESHOLD) & 
            (df_enriched['ReferenceCount'] < HIGH_REFERENCE_THRESHOLD)
        ]
        
        if len(high_priority) > 0:
            recommendations.append({
                'Priorità': 'ALTA',
                'Categoria': 'Dipendenze Multiple',
                'Count': len(high_priority),
                'Azione': f'Valutare inclusione di {len(high_priority)} oggetti con {MEDIUM_REFERENCE_THRESHOLD}-{HIGH_REFERENCE_THRESHOLD-1} riferimenti',
                'Impatto': 'Rischio moderato di breaking changes'
            })
            print(f"2️⃣ PRIORITÀ ALTA - Dipendenze Multiple")
            print(f"   • {len(high_priority)} oggetti con {MEDIUM_REFERENCE_THRESHOLD}-{HIGH_REFERENCE_THRESHOLD-1} riferimenti")
            print(f"   • Rischio: Breaking changes su oggetti multipli")
            print(f"   • Azione: Valutare inclusione nel lineage")
            print("")
    
    # Raccomandazione 3: Critici poco referenziati
    if 'total' in analysis_critici and analysis_critici['total'] > 0:
        recommendations.append({
            'Priorità': 'MEDIA',
            'Categoria': 'DML/DDL Isolati',
            'Count': analysis_critici['total'],
            'Azione': f'Verificare {analysis_critici["total"]} oggetti con DML ma pochi riferimenti',
            'Impatto': 'Modificano dati ma usati raramente - verificare se necessari'
        })
        print(f"3️⃣ PRIORITÀ MEDIA - DML/DDL Isolati")
        print(f"   • {analysis_critici['total']} oggetti con DML ma poco referenziati")
        print(f"   • Rischio: Modificano dati ma usati raramente")
        print(f"   • Azione: Verificare se realmente necessari o deprecabili")
        print("")
    
    # Raccomandazione 4: Strategia ibrida
    print(f"4️⃣ STRATEGIA IBRIDA CONSIGLIATA")
    print(f"   • Criterio nuovo: Critico_Migrazione = SÌ se:")
    print(f"     - Ha operazioni DML/DDL (INSERT/UPDATE/DELETE/CREATE/ALTER)")
    print(f"     - OPPURE")
    print(f"     - Ha ReferenceCount >= {HIGH_REFERENCE_THRESHOLD} (dipendenze critiche)")
    print(f"   • Beneficio: Cattura sia rischi operativi che architetturali")
    print("")
    
    return recommendations

def export_analysis_report(sheets, analysis_top, analysis_critici, recommendations):
    """Esporta report di analisi in Excel e TXT."""
    print("\n" + "="*80)
    print("EXPORT REPORT ANALISI")
    print("="*80 + "\n")
    
    # Excel Report
    with pd.ExcelWriter(OUTPUT_ANALYSIS, engine='openpyxl') as writer:
        
        # Sheet 1: Raccomandazioni
        df_recommendations = pd.DataFrame(recommendations)
        if not df_recommendations.empty:
            df_recommendations.to_excel(writer, sheet_name='Raccomandazioni', index=False)
            print(f"✓ Sheet: Raccomandazioni ({len(recommendations)} items)")
        
        # Sheet 2: Top_Non_Critici Enriched (con classificazione)
        if 'df_enriched' in analysis_top:
            df_enriched = analysis_top['df_enriched'].copy()
            df_enriched = df_enriched.sort_values('ReferenceCount', ascending=False)
            df_enriched.to_excel(writer, sheet_name='Top_Non_Critici_Analysis', index=False)
            print(f"✓ Sheet: Top_Non_Critici_Analysis ({len(df_enriched)} oggetti)")
        
        # Sheet 3: CRITICI per Dipendenze (50+ refs) - DA AGGIUNGERE
        if 'critical_dependencies' in analysis_top and len(analysis_top['critical_dependencies']) > 0:
            df_critical_deps = analysis_top['critical_dependencies'].copy()
            df_critical_deps = df_critical_deps.sort_values('ReferenceCount', ascending=False)
            df_critical_deps.to_excel(writer, sheet_name='DA_AGGIUNGERE_Critici_Deps', index=False)
            print(f"✓ Sheet: DA_AGGIUNGERE_Critici_Deps ({len(df_critical_deps)} oggetti)")
        
        # Sheet 4: ALTA Priorità (10-49 refs)
        if 'df_enriched' in analysis_top:
            df_high_priority = analysis_top['df_enriched'][
                (analysis_top['df_enriched']['ReferenceCount'] >= MEDIUM_REFERENCE_THRESHOLD) &
                (analysis_top['df_enriched']['ReferenceCount'] < HIGH_REFERENCE_THRESHOLD)
            ].copy()
            df_high_priority = df_high_priority.sort_values('ReferenceCount', ascending=False)
            if len(df_high_priority) > 0:
                df_high_priority.to_excel(writer, sheet_name='DA_VALUTARE_Alta_Priorità', index=False)
                print(f"✓ Sheet: DA_VALUTARE_Alta_Priorità ({len(df_high_priority)} oggetti)")
        
        # Sheet 5: Summary Statistics
        summary_data = []
        summary_data.append(['CATEGORIA', 'METRICA', 'VALORE'])
        summary_data.append(['', '', ''])
        summary_data.append(['TOP NON CRITICI', 'Totale oggetti', analysis_top.get('total', 0)])
        
        if 'critical_dependencies' in analysis_top:
            summary_data.append(['', f'Critici ({HIGH_REFERENCE_THRESHOLD}+ refs)', len(analysis_top['critical_dependencies'])])
        
        if 'reference_stats' in analysis_top:
            stats = analysis_top['reference_stats']
            summary_data.append(['', 'ReferenceCount medio', f"{stats['mean']:.1f}"])
            summary_data.append(['', 'ReferenceCount max', f"{stats['max']:.0f}"])
        
        summary_data.append(['', '', ''])
        summary_data.append(['CRITICI NON TOP', 'Totale oggetti', analysis_critici.get('total', 0)])
        
        df_summary = pd.DataFrame(summary_data)
        df_summary.to_excel(writer, sheet_name='Summary_Stats', index=False, header=False)
        print(f"✓ Sheet: Summary_Stats")
    
    print(f"\n✓ Excel salvato: {OUTPUT_ANALYSIS}")
    
    # TXT Report (leggibile)
    with open(OUTPUT_TXT, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("ANALISI TOP REFERENCED OBJECTS - Oggetti Mancanti dal Lineage\n")
        f.write("="*80 + "\n\n")
        
        f.write(f"Data analisi: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Summary
        f.write("SUMMARY\n")
        f.write("-"*80 + "\n")
        f.write(f"Oggetti Top Referenced NON nel lineage: {analysis_top.get('total', 0)}\n")
        if 'critical_dependencies' in analysis_top:
            f.write(f"  • CRITICI ({HIGH_REFERENCE_THRESHOLD}+ refs):  {len(analysis_top['critical_dependencies'])}\n")
        f.write(f"Oggetti Critici poco referenziati:      {analysis_critici.get('total', 0)}\n\n")
        
        # Raccomandazioni
        f.write("RACCOMANDAZIONI\n")
        f.write("-"*80 + "\n")
        for i, rec in enumerate(recommendations, start=1):
            f.write(f"{i}. [{rec['Priorità']}] {rec['Categoria']}\n")
            f.write(f"   • Count: {rec['Count']}\n")
            f.write(f"   • Azione: {rec['Azione']}\n")
            f.write(f"   • Impatto: {rec['Impatto']}\n\n")
        
        # Top 20
        if 'top_20' in analysis_top:
            f.write("\nTOP 20 OGGETTI PIÙ REFERENZIATI (Mancanti)\n")
            f.write("-"*80 + "\n")
            for i, (idx, row) in enumerate(analysis_top['top_20'].iterrows(), start=1):
                obj_full = f"[{row['Database']}].[{row['Schema']}].[{row['ObjectName']}]"
                f.write(f"{i:2d}. {obj_full:60s} | {row['ObjectType']:15s} | {row['ReferenceCount']:3.0f} refs\n")
    
    print(f"✓ TXT salvato:   {OUTPUT_TXT}")

# =========================
# MAIN
# =========================

def main():
    print("\n")
    print("="*80)
    print("ANALISI APPROFONDITA TOP REFERENCED OBJECTS")
    print("="*80)
    print("")
    print(f"Source: {VALIDATION_REPORT}")
    print(f"Output Excel: {OUTPUT_ANALYSIS}")
    print(f"Output TXT:   {OUTPUT_TXT}")
    print("")
    
    # Carica dati
    sheets = load_validation_data(VALIDATION_REPORT)
    
    if not sheets:
        print("\n✗ Impossibile caricare dati. Terminazione.")
        return
    
    # Analizza Top_Non_Critici
    if 'Top_Non_Critici' in sheets:
        analysis_top = analyze_top_non_critici(sheets['Top_Non_Critici'])
    else:
        print("✗ Sheet 'Top_Non_Critici' non trovato")
        analysis_top = {}
    
    # Analizza Critici_Non_Top
    if 'Critici_Non_Top' in sheets:
        analysis_critici = analyze_critici_non_top(sheets['Critici_Non_Top'])
    else:
        print("✗ Sheet 'Critici_Non_Top' non trovato")
        analysis_critici = {}
    
    # Genera raccomandazioni
    recommendations = generate_recommendations(analysis_top, analysis_critici)
    
    # Export report
    export_analysis_report(sheets, analysis_top, analysis_critici, recommendations)
    
    print("\n" + "="*80)
    print("ANALISI COMPLETATA")
    print("="*80)
    print("")
    print("📊 Report generati:")
    print(f"   • Excel: {OUTPUT_ANALYSIS}")
    print(f"   • TXT:   {OUTPUT_TXT}")
    print("")
    print("📋 Prossimi passi:")
    print("   1. Aprire TOP_REFERENCED_ANALYSIS.xlsx")
    print("   2. Verificare sheet 'DA_AGGIUNGERE_Critici_Deps'")
    print("   3. Eseguire hybrid_criticality.py per integrare logica ibrida")
    print("")

if __name__ == "__main__":
    main()
