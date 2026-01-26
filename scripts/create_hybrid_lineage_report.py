# =========================
# IMPORT
# =========================
import pandas as pd
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# =========================
# CONFIG
# =========================
HYBRID_SUMMARY_PATH = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\SUMMARY_REPORT_HYBRID.xlsx'
OUTPUT_TXT = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\LINEAGE_HYBRID_REPORT.txt'
OUTPUT_MD = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\LINEAGE_HYBRID_REPORT.md'

# =========================
# FUNZIONI
# =========================

def load_hybrid_data(summary_path):
    """Carica dati dal SUMMARY_REPORT_HYBRID."""
    print("="*80)
    print("CARICAMENTO SUMMARY_REPORT_HYBRID")
    print("="*80 + "\n")
    
    sheets = {}
    
    try:
        xl_file = pd.ExcelFile(summary_path)
        
        for sheet_name in xl_file.sheet_names:
            sheets[sheet_name] = pd.read_excel(summary_path, sheet_name=sheet_name)
            print(f"✓ {sheet_name}: {len(sheets[sheet_name])} righe")
        
        print(f"\n✓ Caricati {len(sheets)} sheet")
        return sheets
        
    except Exception as e:
        print(f"✗ Errore caricamento: {e}")
        return {}

def analyze_level_hybrid(df_level, level_name):
    """Analizza un livello con logica ibrida."""
    total = len(df_level)
    
    # Critici totali (ibrido)
    critici_totali = df_level[df_level['Critico_Migrazione'] == 'SÌ']
    num_critici = len(critici_totali)
    
    # Distingui per motivo criticità
    if 'Motivo_Criticità' in df_level.columns:
        # Solo DML/DDL (no dipendenze)
        critici_solo_dml = df_level[
            (df_level['Critico_Migrazione'] == 'SÌ') & 
            (df_level['Motivo_Criticità'] == 'DML/DDL')
        ]
        
        # Solo Dipendenze (no DML/DDL)
        critici_solo_deps = df_level[
            (df_level['Critico_Migrazione'] == 'SÌ') & 
            (df_level['Motivo_Criticità'].str.contains('Dipendenze \\(', na=False, regex=True)) &
            (~df_level['Motivo_Criticità'].str.contains('DML/DDL', na=False))
        ]
        
        # ENTRAMBI (DML/DDL + Dipendenze)
        critici_entrambi = df_level[
            (df_level['Critico_Migrazione'] == 'SÌ') & 
            (df_level['Motivo_Criticità'] == 'DML/DDL + Dipendenze')
        ]
        
        # Totali con DML (solo_dml + entrambi)
        critici_dml = df_level[
            (df_level['Critico_Migrazione'] == 'SÌ') & 
            (df_level['Motivo_Criticità'].isin(['DML/DDL', 'DML/DDL + Dipendenze']))
        ]
        
        # Totali con Dipendenze (solo_deps + entrambi)
        critici_deps = df_level[
            (df_level['Critico_Migrazione'] == 'SÌ') & 
            (df_level['Motivo_Criticità'].str.contains('Dipendenze', na=False))
        ]
    else:
        critici_solo_dml = critici_totali
        critici_solo_deps = pd.DataFrame()
        critici_entrambi = pd.DataFrame()
        critici_dml = critici_totali
        critici_deps = pd.DataFrame()
    
    # Distribuzione per tipo
    type_dist = critici_totali['ObjectType'].value_counts() if num_critici > 0 else pd.Series()
    
    # Distribuzione per database
    db_dist = critici_totali['Database'].value_counts() if num_critici > 0 else pd.Series()
    
    # Distribuzione per criticità tecnica
    if num_critici > 0 and 'Criticità_Tecnica' in critici_totali.columns:
        crit_tech_dist = critici_totali['Criticità_Tecnica'].value_counts()
    else:
        crit_tech_dist = pd.Series()
    
    # Top oggetti per ReferenceCount
    if num_critici > 0 and 'ReferenceCount' in critici_totali.columns:
        # Verifica che tutte le colonne necessarie esistano
        required_cols = ['Database', 'Schema', 'ObjectName', 'ObjectType', 'ReferenceCount', 'Motivo_Criticità']
        available_cols = [col for col in required_cols if col in critici_totali.columns]
        
        if len(available_cols) >= 5:  # Almeno le colonne base
            top_refs = critici_totali.nlargest(10, 'ReferenceCount')[available_cols]
        else:
            top_refs = pd.DataFrame()
    else:
        top_refs = pd.DataFrame()
    
    return {
        'level': level_name,
        'total': total,
        'critici_totali': num_critici,
        'critici_solo_dml': len(critici_solo_dml),
        'critici_solo_deps': len(critici_solo_deps),
        'critici_entrambi': len(critici_entrambi),
        'critici_dml': len(critici_dml),
        'critici_deps': len(critici_deps),
        'type_distribution': type_dist,
        'db_distribution': db_dist,
        'crit_tech_distribution': crit_tech_dist,
        'top_references': top_refs,
        'df_critici': critici_totali
    }

def generate_txt_report(sheets, analyses):
    """Genera report TXT leggibile."""
    print("\n" + "="*80)
    print("GENERAZIONE LINEAGE_HYBRID_REPORT.txt")
    print("="*80 + "\n")
    
    lines = []
    
    # Header
    lines.append("="*100)
    lines.append("LINEAGE HYBRID REPORT - Oggetti da Migrare")
    lines.append("="*100)
    lines.append("")
    lines.append(f"Data generazione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Source: SUMMARY_REPORT_HYBRID.xlsx")
    lines.append("")
    lines.append("CRITERIO CRITICITÀ IBRIDO:")
    lines.append("  ✓ Oggetti con operazioni DML/DDL (INSERT/UPDATE/DELETE/CREATE/ALTER)")
    lines.append("  ✓ OPPURE Oggetti con ReferenceCount >= 50 (dipendenze critiche)")
    lines.append("")
    lines.append("="*100)
    lines.append("")
    
    # =====================
    # SEZIONE 1: SUMMARY ESECUTIVO
    # =====================
    lines.append("1. SUMMARY ESECUTIVO")
    lines.append("="*100)
    lines.append("")
    
    total_objects = sum(a['total'] for a in analyses.values())
    total_critici = sum(a['critici_totali'] for a in analyses.values())
    total_critici_solo_dml = sum(a['critici_solo_dml'] for a in analyses.values())
    total_critici_solo_deps = sum(a['critici_solo_deps'] for a in analyses.values())
    total_critici_entrambi = sum(a['critici_entrambi'] for a in analyses.values())
    
    lines.append(f"Oggetti totali analizzati (L1-L4):           {total_objects}")
    lines.append(f"Oggetti CRITICI da migrare (IBRIDO):         {total_critici} ({total_critici/total_objects*100:.1f}%)")
    lines.append("")
    lines.append("Breakdown per motivo criticità:")
    lines.append(f"  • Critici SOLO per DML/DDL:                {total_critici_solo_dml}")
    lines.append(f"  • Critici SOLO per Dipendenze (50+ refs):  {total_critici_solo_deps}")
    lines.append(f"  • Critici per ENTRAMBI i motivi:           {total_critici_entrambi}")
    lines.append(f"  • TOTALE (verifica):                       {total_critici_solo_dml + total_critici_solo_deps + total_critici_entrambi}")
    lines.append("")
    
    # Per livello
    lines.append("Distribuzione per livello:")
    for level in ['L1', 'L2', 'L3', 'L4']:
        if level in analyses:
            a = analyses[level]
            lines.append(f"  {level}: {a['critici_totali']:3d} critici / {a['total']:4d} totali ({a['critici_totali']/a['total']*100:5.1f}%)")
            lines.append(f"       Solo DML: {a['critici_solo_dml']} | Solo Deps: {a['critici_solo_deps']} | Entrambi: {a['critici_entrambi']}")
    lines.append("")
    
    # =====================
    # SEZIONE 2: ANALISI PER LIVELLO
    # =====================
    lines.append("")
    lines.append("2. DETTAGLIO PER LIVELLO - Oggetti Critici da Migrare")
    lines.append("="*100)
    lines.append("")
    
    for level in ['L1', 'L2', 'L3', 'L4']:
        if level not in analyses:
            continue
        
        a = analyses[level]
        
        lines.append(f"{'─'*100}")
        lines.append(f"LIVELLO {level}")
        lines.append(f"{'─'*100}")
        lines.append("")
        
        lines.append(f"Oggetti totali:                    {a['total']}")
        lines.append(f"Oggetti CRITICI da migrare:        {a['critici_totali']} ({a['critici_totali']/a['total']*100:.1f}%)")
        lines.append("")
        lines.append(f"Breakdown per motivo:")
        lines.append(f"  • SOLO DML/DDL (no dipendenze):  {a['critici_solo_dml']}")
        lines.append(f"  • SOLO Dipendenze (no DML/DDL):  {a['critici_solo_deps']}")
        lines.append(f"  • ENTRAMBI (DML/DDL + Deps):     {a['critici_entrambi']}")
        lines.append(f"  • Totale con DML/DDL:            {a['critici_dml']}")
        lines.append(f"  • Totale con Dipendenze:         {a['critici_deps']}")
        lines.append("")
        
        # Distribuzione per tipo
        if not a['type_distribution'].empty:
            lines.append("Per tipo oggetto:")
            for obj_type, count in a['type_distribution'].items():
                pct = count / a['critici_totali'] * 100
                lines.append(f"  • {obj_type:35s}: {count:3d} ({pct:5.1f}%)")
            lines.append("")
        
        # Distribuzione per database
        if not a['db_distribution'].empty:
            lines.append("Per database:")
            for db, count in a['db_distribution'].sort_values(ascending=False).items():
                pct = count / a['critici_totali'] * 100
                lines.append(f"  • {db:20s}: {count:3d} ({pct:5.1f}%)")
            lines.append("")
        
        # Distribuzione per criticità tecnica
        if not a['crit_tech_distribution'].empty:
            lines.append("Per criticità tecnica:")
            for crit, count in a['crit_tech_distribution'].sort_values(ascending=False).items():
                pct = count / a['critici_totali'] * 100
                lines.append(f"  • {crit:35s}: {count:3d} ({pct:5.1f}%)")
            lines.append("")
        
        # Top 10 per ReferenceCount
        if not a['top_references'].empty:
            lines.append("TOP 10 per dipendenze (ReferenceCount):")
            for i, (idx, row) in enumerate(a['top_references'].iterrows(), start=1):
                obj_full = f"[{row['Database']}].[{row['Schema']}].[{row['ObjectName']}]"
                motivo = row.get('Motivo_Criticità', 'N/A')
                ref_count = row.get('ReferenceCount', 0)
                lines.append(f"  {i:2d}. {obj_full:55s} | {row['ObjectType']:15s} | {ref_count:3.0f} refs | {motivo}")
            lines.append("")
        
        lines.append("")
    
    # =====================
    # SEZIONE 3: LISTA COMPLETA OGGETTI CRITICI
    # =====================
    lines.append("")
    lines.append("3. LISTA COMPLETA OGGETTI CRITICI DA MIGRARE")
    lines.append("="*100)
    lines.append("")
    
    for level in ['L1', 'L2', 'L3', 'L4']:
        if level not in analyses:
            continue
        
        a = analyses[level]
        
        if a['critici_totali'] == 0:
            continue
        
        lines.append(f"{'─'*100}")
        lines.append(f"{level} - {a['critici_totali']} oggetti critici")
        lines.append(f"{'─'*100}")
        lines.append("")
        
        df_critici = a['df_critici'].copy()
        
        # Ordina per ReferenceCount (se disponibile) poi per Database/ObjectName
        if 'ReferenceCount' in df_critici.columns:
            df_critici = df_critici.sort_values(['ReferenceCount', 'Database', 'ObjectName'], ascending=[False, True, True])
        else:
            df_critici = df_critici.sort_values(['Database', 'ObjectName'])
        
        for idx, row in df_critici.iterrows():
            obj_full = f"[{row['Database']}].[{row.get('Schema', 'dbo')}].[{row['ObjectName']}]"
            obj_type = row['ObjectType']
            motivo = row.get('Motivo_Criticità', 'N/A')
            ref_count = row.get('ReferenceCount', 0)
            crit_tech = row.get('Criticità_Tecnica', 'N/A')
            
            lines.append(f"  • {obj_full:60s} | {obj_type:15s}")
            lines.append(f"    Motivo:           {motivo}")
            lines.append(f"    ReferenceCount:   {ref_count}")
            lines.append(f"    Criticità Tecnica: {crit_tech}")
            lines.append("")
        
        lines.append("")
    
    # =====================
    # SEZIONE 4: RACCOMANDAZIONI
    # =====================
    lines.append("")
    lines.append("4. RACCOMANDAZIONI MIGRAZIONE")
    lines.append("="*100)
    lines.append("")
    
    lines.append("PRIORITÀ MIGRAZIONE:")
    lines.append("")
    lines.append("1️⃣  MASSIMA - Critici per DML/DDL + Dipendenze")
    lines.append(f"   • Oggetti che modificano dati E sono molto referenziati")
    lines.append(f"   • Rischio: Operativo + Architetturale")
    lines.append(f"   • Azione: Migrare IMMEDIATAMENTE")
    lines.append("")
    
    lines.append("2️⃣  ALTA - Critici solo per Dipendenze (50+ refs)")
    lines.append(f"   • Oggetti con {total_critici_solo_deps} dipendenze critiche senza DML/DDL")
    lines.append(f"   • Rischio: Breaking changes su molti oggetti dipendenti")
    lines.append(f"   • Azione: Migrare prima degli oggetti che li referenziano")
    lines.append("")
    
    lines.append("3️⃣  MEDIA - Critici solo per DML/DDL (poche dipendenze)")
    lines.append(f"   • Oggetti che modificano dati ma poco referenziati")
    lines.append(f"   • Rischio: Operativo (perdita/corruzione dati)")
    lines.append(f"   • Azione: Migrare con attenzione a business logic")
    lines.append("")
    
    lines.append("STRATEGIA CONSIGLIATA:")
    lines.append("")
    lines.append("  Fase 1: Migrare oggetti L1 critici (fondazioni)")
    lines.append("  Fase 2: Migrare oggetti L2 critici (dipendono da L1)")
    lines.append("  Fase 3: Migrare oggetti L3 critici (dipendono da L2)")
    lines.append("  Fase 4: Migrare oggetti L4 critici (top layer)")
    lines.append("")
    lines.append("  All'interno di ogni fase, prioritizzare:")
    lines.append("    1. DML/DDL + Dipendenze (massimo rischio)")
    lines.append("    2. Solo Dipendenze (rischio architetturale)")
    lines.append("    3. Solo DML/DDL (rischio operativo)")
    lines.append("")
    
    # =====================
    # SEZIONE 5: DATABASES COINVOLTI
    # =====================
    lines.append("")
    lines.append("5. DATABASE COINVOLTI")
    lines.append("="*100)
    lines.append("")
    
    # Aggrega per database
    all_critici = []
    for level in ['L1', 'L2', 'L3', 'L4']:
        if level in analyses:
            all_critici.append(analyses[level]['df_critici'])
    
    if all_critici:
        df_all_critici = pd.concat(all_critici, ignore_index=True)
        db_summary = df_all_critici.groupby('Database').agg({
            'ObjectName': 'count',
            'ReferenceCount': 'sum' if 'ReferenceCount' in df_all_critici.columns else 'count'
        }).sort_values('ObjectName', ascending=False)
        
        lines.append(f"{'Database':<20s} | {'Oggetti Critici':>20s} | {'ReferenceCount Totale':>25s}")
        lines.append(f"{'-'*20:} | {'-'*20:} | {'-'*25:}")
        
        for db in db_summary.index:
            obj_count = db_summary.loc[db, 'ObjectName']
            ref_total = db_summary.loc[db, 'ReferenceCount'] if 'ReferenceCount' in df_all_critici.columns else 0
            lines.append(f"{db:<20s} | {obj_count:>20d} | {ref_total:>25.0f}")
        
        lines.append("")
        lines.append(f"TOTALE: {len(df_all_critici)} oggetti critici da migrare")
        lines.append("")
    
    # Footer
    lines.append("")
    lines.append("="*100)
    lines.append("FINE REPORT")
    lines.append("="*100)
    
    # Scrivi file
    with open(OUTPUT_TXT, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    print(f"✓ Report salvato: {OUTPUT_TXT}")
    return lines

def generate_md_report(sheets, analyses):
    """Genera report Markdown."""
    print(f"Generazione {OUTPUT_MD}...")
    
    lines = []
    
    # Header
    lines.append("# LINEAGE HYBRID REPORT - Oggetti da Migrare")
    lines.append("")
    lines.append(f"**Data generazione:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("## Criterio Criticità Ibrido")
    lines.append("")
    lines.append("Un oggetto è **CRITICO** e da migrare se:")
    lines.append("- ✅ Ha operazioni **DML/DDL** (INSERT/UPDATE/DELETE/CREATE/ALTER)")
    lines.append("- ✅ **OPPURE** ha **ReferenceCount ≥ 50** (dipendenze critiche)")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # Summary
    lines.append("## 1. Summary Esecutivo")
    lines.append("")
    
    total_objects = sum(a['total'] for a in analyses.values())
    total_critici = sum(a['critici_totali'] for a in analyses.values())
    total_critici_solo_dml = sum(a['critici_solo_dml'] for a in analyses.values())
    total_critici_solo_deps = sum(a['critici_solo_deps'] for a in analyses.values())
    total_critici_entrambi = sum(a['critici_entrambi'] for a in analyses.values())
    
    lines.append(f"- **Oggetti totali analizzati (L1-L4):** {total_objects}")
    lines.append(f"- **Oggetti CRITICI da migrare:** {total_critici} ({total_critici/total_objects*100:.1f}%)")
    lines.append("")
    lines.append("### Breakdown per motivo:")
    lines.append("")
    lines.append(f"| Motivo | Count |")
    lines.append(f"|--------|------:|")
    lines.append(f"| SOLO DML/DDL | {total_critici_solo_dml} |")
    lines.append(f"| SOLO Dipendenze (50+ refs) | {total_critici_solo_deps} |")
    lines.append(f"| ENTRAMBI (DML/DDL + Dipendenze) | {total_critici_entrambi} |")
    lines.append(f"| **TOTALE** | **{total_critici}** |")
    lines.append("")
    
    lines.append("### Per livello:")
    lines.append("")
    lines.append("| Livello | Critici | Totali | % | Solo DML | Solo Deps | Entrambi |")
    lines.append("|---------|--------:|-------:|--:|---------:|----------:|---------:|")
    for level in ['L1', 'L2', 'L3', 'L4']:
        if level in analyses:
            a = analyses[level]
            lines.append(f"| {level} | {a['critici_totali']} | {a['total']} | {a['critici_totali']/a['total']*100:.1f}% | {a['critici_solo_dml']} | {a['critici_solo_deps']} | {a['critici_entrambi']} |")
    lines.append("")
    
    # Dettaglio per livello
    lines.append("---")
    lines.append("")
    lines.append("## 2. Dettaglio per Livello")
    lines.append("")
    
    for level in ['L1', 'L2', 'L3', 'L4']:
        if level not in analyses:
            continue
        
        a = analyses[level]
        
        lines.append(f"### Livello {level}")
        lines.append("")
        lines.append(f"- **Totali:** {a['total']}")
        lines.append(f"- **Critici:** {a['critici_totali']} ({a['critici_totali']/a['total']*100:.1f}%)")
        lines.append(f"  - SOLO DML/DDL: {a['critici_solo_dml']}")
        lines.append(f"  - SOLO Dipendenze: {a['critici_solo_deps']}")
        lines.append(f"  - ENTRAMBI: {a['critici_entrambi']}")
        lines.append("")
        
        # Per tipo
        if not a['type_distribution'].empty:
            lines.append("#### Per tipo oggetto:")
            lines.append("")
            lines.append("| Tipo | Count | % |")
            lines.append("|------|------:|--:|")
            for obj_type, count in a['type_distribution'].items():
                pct = count / a['critici_totali'] * 100
                lines.append(f"| {obj_type} | {count} | {pct:.1f}% |")
            lines.append("")
        
        # Per database
        if not a['db_distribution'].empty:
            lines.append("#### Per database:")
            lines.append("")
            lines.append("| Database | Count | % |")
            lines.append("|----------|------:|--:|")
            for db, count in a['db_distribution'].sort_values(ascending=False).head(10).items():
                pct = count / a['critici_totali'] * 100
                lines.append(f"| {db} | {count} | {pct:.1f}% |")
            lines.append("")
        
        lines.append("")
    
    # Raccomandazioni
    lines.append("---")
    lines.append("")
    lines.append("## 3. Raccomandazioni Migrazione")
    lines.append("")
    lines.append("### Priorità:")
    lines.append("")
    lines.append("1. **MASSIMA** - Critici per DML/DDL + Dipendenze")
    lines.append("   - Rischio: Operativo + Architetturale")
    lines.append("   - Azione: Migrare IMMEDIATAMENTE")
    lines.append("")
    lines.append("2. **ALTA** - Critici solo per Dipendenze (50+ refs)")
    lines.append(f"   - Count: {total_critici_solo_deps}")
    lines.append("   - Rischio: Breaking changes multipli")
    lines.append("   - Azione: Migrare prima degli oggetti dipendenti")
    lines.append("")
    lines.append("3. **MEDIA** - Critici solo per DML/DDL")
    lines.append(f"   - Count: {total_critici_solo_dml}")
    lines.append("   - Rischio: Operativo (dati)")
    lines.append("   - Azione: Verificare business logic")
    lines.append("")
    lines.append("### Strategia:")
    lines.append("")
    lines.append("```")
    lines.append("Fase 1: L1 critici (fondazioni)")
    lines.append("Fase 2: L2 critici (dipendono da L1)")
    lines.append("Fase 3: L3 critici (dipendono da L2)")
    lines.append("Fase 4: L4 critici (top layer)")
    lines.append("")
    lines.append("Priorità interna:")
    lines.append("  1. DML/DDL + Dipendenze")
    lines.append("  2. Solo Dipendenze")
    lines.append("  3. Solo DML/DDL")
    lines.append("```")
    lines.append("")
    
    # Database summary
    lines.append("---")
    lines.append("")
    lines.append("## 4. Database Coinvolti")
    lines.append("")
    
    all_critici = []
    for level in ['L1', 'L2', 'L3', 'L4']:
        if level in analyses:
            all_critici.append(analyses[level]['df_critici'])
    
    if all_critici:
        df_all_critici = pd.concat(all_critici, ignore_index=True)
        db_summary = df_all_critici.groupby('Database').agg({
            'ObjectName': 'count'
        }).sort_values('ObjectName', ascending=False)
        
        lines.append("| Database | Oggetti Critici |")
        lines.append("|----------|----------------:|")
        
        for db in db_summary.index:
            obj_count = db_summary.loc[db, 'ObjectName']
            lines.append(f"| {db} | {obj_count} |")
        
        lines.append("")
        lines.append(f"**TOTALE:** {len(df_all_critici)} oggetti critici da migrare")
        lines.append("")
    
    # Footer
    lines.append("---")
    lines.append("")
    lines.append("*Fine Report*")
    
    # Scrivi file
    with open(OUTPUT_MD, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    print(f"✓ Report salvato: {OUTPUT_MD}")

# =========================
# MAIN
# =========================

def main():
    print("\n")
    print("="*80)
    print("GENERAZIONE LINEAGE HYBRID REPORT")
    print("="*80)
    print("")
    print(f"Source: {HYBRID_SUMMARY_PATH}")
    print(f"Output TXT: {OUTPUT_TXT}")
    print(f"Output MD:  {OUTPUT_MD}")
    print("")
    
    # Carica dati
    sheets = load_hybrid_data(HYBRID_SUMMARY_PATH)
    
    if not sheets:
        print("\n✗ Impossibile caricare dati. Terminazione.")
        return
    
    # Analizza ogni livello
    print("\n" + "="*80)
    print("ANALISI LIVELLI CON LOGICA IBRIDA")
    print("="*80 + "\n")
    
    analyses = {}
    
    for level in ['L1', 'L2', 'L3', 'L4']:
        if level in sheets:
            print(f"Analisi {level}...")
            analyses[level] = analyze_level_hybrid(sheets[level], level)
            a = analyses[level]
            print(f"  ✓ {a['critici_totali']} critici / {a['total']} totali")
            print(f"    Solo DML: {a['critici_solo_dml']} | Solo Deps: {a['critici_solo_deps']} | Entrambi: {a['critici_entrambi']}")
            print("")
    
    # Genera report TXT
    generate_txt_report(sheets, analyses)
    
    # Genera report MD
    generate_md_report(sheets, analyses)
    
    print("\n" + "="*80)
    print("GENERAZIONE COMPLETATA")
    print("="*80)
    print("")
    print("📊 File generati:")
    print(f"   • TXT:  {OUTPUT_TXT}")
    print(f"   • MD:   {OUTPUT_MD}")
    print("")
    print("📋 Contenuto:")
    print("   1. Summary esecutivo con breakdown per motivo criticità")
    print("   2. Dettaglio per livello (L1-L4)")
    print("   3. Lista completa oggetti critici da migrare")
    print("   4. Raccomandazioni priorità migrazione")
    print("   5. Database coinvolti")
    print("")
    print("✅ Report pronto per condivisione con il team!")
    print("")

if __name__ == "__main__":
    main()
