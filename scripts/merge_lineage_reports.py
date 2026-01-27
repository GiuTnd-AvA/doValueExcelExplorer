# =========================
# IMPORT
# =========================
import re
import pandas as pd
from collections import defaultdict
from datetime import datetime

# =========================
# CONFIG
# =========================
# Path ai due report da unire
REPORT_1_PATH = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\LINEAGE_HYBRID_REPORT_1.txt'
REPORT_2_PATH = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\LINEAGE_HYBRID_REPORT_2.txt'

# Output
OUTPUT_PATH = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\LINEAGE_HYBRID_REPORT_MERGED.txt'
OUTPUT_EXCEL_PATH = r'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\LINEAGE_HYBRID_REPORT_MERGED.xlsx'

# =========================
# FUNZIONI
# =========================

def normalize_object_key(database, schema, object_name):
    """Crea chiave normalizzata per identificare duplicati."""
    db = str(database).upper().strip() if pd.notna(database) else ''
    sch = str(schema).upper().strip() if pd.notna(schema) else 'DBO'
    obj = str(object_name).upper().strip() if pd.notna(object_name) else ''
    return f"{db}.{sch}.{obj}"

def parse_lineage_report(file_path):
    """
    Parsa un report di lineage e estrae tutti gli oggetti con dettagli completi.
    Ritorna: dict con chiave = livello, valore = lista di oggetti
    """
    print(f"\n📖 Parsing: {file_path}")
    
    objects_by_level = {
        'L1': [],
        'L2': [],
        'L3': [],
        'L4': []
    }
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Pattern per identificare sezioni livello
        level_patterns = [
            re.compile(r'LIVELLO\s+L(\d+)', re.IGNORECASE),
            re.compile(r'─+\s*LIVELLO\s+L(\d+)', re.IGNORECASE),
            re.compile(r'^L(\d+)\s*-\s*\d+\s*oggetti', re.MULTILINE | re.IGNORECASE),  # Match "L4 - 794 oggetti critici"
            re.compile(r'─+\s*L(\d+)\s*─+', re.IGNORECASE),  # Match "────L1────"
        ]
        
        # Pattern per oggetti bullet (inizio blocco oggetto)
        # Formato: "  • [Database].[Schema].[ObjectName]           | ObjectType"
        # Con spazi variabili tra componenti
        object_bullet_pattern = re.compile(
            r'^\s*[•\*\-]\s+\[([^\]]+)\]\.\[([^\]]+)\]\.\[([^\]]+)\]\s*\|\s*([^\n]+)',
            re.MULTILINE
        )
        
        current_level = None
        i = 0
        
        while i < len(lines):
            line = lines[i]
            
            # Identifica cambio livello
            for pattern in level_patterns:
                level_match = pattern.search(line)
                if level_match:
                    level_num = level_match.group(1)
                    current_level = f'L{level_num}'
                    break
            
            # Estrae oggetti con dettagli multi-riga
            if current_level:
                obj_match = object_bullet_pattern.search(line)
                if obj_match:
                    db = obj_match.group(1).strip()
                    schema = obj_match.group(2).strip()
                    obj_name = obj_match.group(3).strip()
                    obj_type = obj_match.group(4).strip()
                    
                    # Estrae ReferenceCount dalla prima riga
                    ref_count = None
                    ref_match = re.search(r'(\d+)\s*refs?\b', line, re.IGNORECASE)
                    if ref_match:
                        ref_count = int(ref_match.group(1))
                    
                    # Inizializza campi aggiuntivi
                    motivo = None
                    criticita_tecnica = None
                    
                    # Leggi le righe successive per dettagli (Motivo, ReferenceCount, Criticità Tecnica)
                    j = i + 1
                    while j < len(lines) and j < i + 5:  # Max 5 righe dopo
                        next_line = lines[j].strip()
                        
                        # Stop se incontriamo un nuovo oggetto o sezione
                        if next_line.startswith('•') or next_line.startswith('*') or '────' in next_line:
                            break
                        
                        # Estrae Motivo
                        if next_line.startswith('Motivo:'):
                            motivo_match = re.search(r'Motivo:\s*(.+)', next_line)
                            if motivo_match:
                                motivo = motivo_match.group(1).strip()
                        
                        # Estrae ReferenceCount (se non già trovato)
                        if next_line.startswith('ReferenceCount:') and ref_count is None:
                            rc_match = re.search(r'ReferenceCount:\s*(\d+)', next_line)
                            if rc_match:
                                ref_count = int(rc_match.group(1))
                        
                        # Estrae Criticità Tecnica
                        if next_line.startswith('Criticità Tecnica:'):
                            ct_match = re.search(r'Criticità Tecnica:\s*(.+)', next_line)
                            if ct_match:
                                criticita_tecnica = ct_match.group(1).strip()
                        
                        j += 1
                    
                    # Determina se critico - oggetti da lineage report sono SEMPRE critici
                    critical = 'SÌ'  # Default: tutti gli oggetti nei report sono critici
                    
                    objects_by_level[current_level].append({
                        'Database': db,
                        'Schema': schema,
                        'ObjectName': obj_name,
                        'ObjectType': obj_type,
                        'Critico_Migrazione': critical,
                        'Motivo': motivo,
                        'ReferenceCount': ref_count,
                        'Criticità_Tecnica': criticita_tecnica,
                        'Livello': current_level,
                        'Key': normalize_object_key(db, schema, obj_name)
                    })
            
            i += 1
        
        # Stampa statistiche parsing
        for level in ['L1', 'L2', 'L3', 'L4']:
            count = len(objects_by_level[level])
            if count > 0:
                print(f"  ✓ {level}: {count} oggetti")
        
        return objects_by_level
        
    except Exception as e:
        print(f"✗ Errore nel parsing: {e}")
        import traceback
        traceback.print_exc()
        return objects_by_level

def merge_objects(objects1, objects2):
    """
    Unisce due dizionari di oggetti eliminando duplicati.
    In caso di duplicati, mantiene quello con più informazioni.
    """
    print("\n🔄 Merge oggetti...")
    
    merged = {
        'L1': [],
        'L2': [],
        'L3': [],
        'L4': []
    }
    
    stats = {
        'total_1': 0,
        'total_2': 0,
        'duplicates': 0,
        'unique': 0
    }
    
    for level in ['L1', 'L2', 'L3', 'L4']:
        seen_keys = {}
        
        stats['total_1'] += len(objects1[level])
        stats['total_2'] += len(objects2[level])
        
        # Processa oggetti dal report 1
        for obj in objects1[level]:
            key = obj['Key']
            seen_keys[key] = obj
        
        # Processa oggetti dal report 2
        for obj in objects2[level]:
            key = obj['Key']
            
            if key in seen_keys:
                # Duplicato: merge intelligente
                existing = seen_keys[key]
                
                # Prendi il ReferenceCount più alto
                if obj.get('ReferenceCount') and existing.get('ReferenceCount'):
                    obj['ReferenceCount'] = max(obj['ReferenceCount'], existing['ReferenceCount'])
                elif existing.get('ReferenceCount'):
                    obj['ReferenceCount'] = existing['ReferenceCount']
                
                # Prendi dati più completi
                if not obj.get('ObjectType') and existing.get('ObjectType'):
                    obj['ObjectType'] = existing['ObjectType']
                
                if not obj.get('Motivo') and existing.get('Motivo'):
                    obj['Motivo'] = existing['Motivo']
                
                if not obj.get('Criticità_Tecnica') and existing.get('Criticità_Tecnica'):
                    obj['Criticità_Tecnica'] = existing['Criticità_Tecnica']
                
                # Aggiorna
                seen_keys[key] = obj
                stats['duplicates'] += 1
            else:
                # Nuovo oggetto
                seen_keys[key] = obj
        
        # Aggiungi al merged
        merged[level] = list(seen_keys.values())
        stats['unique'] += len(merged[level])
        
        print(f"  • {level}: {len(merged[level])} oggetti unici")
    
    print(f"\n📊 Statistiche Merge:")
    print(f"  • Report 1: {stats['total_1']} oggetti")
    print(f"  • Report 2: {stats['total_2']} oggetti")
    print(f"  • Duplicati rimossi: {stats['duplicates']}")
    print(f"  • Totale unici: {stats['unique']}")
    
    return merged, stats

def generate_merged_report(merged_objects, stats, output_path):
    """Genera il report unificato in formato .txt seguendo struttura LINEAGE_HYBRID_REPORT."""
    print(f"\n📝 Generazione report unificato...")
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            # Header
            f.write("="*100 + "\n")
            f.write("LINEAGE HYBRID REPORT - MERGED (Oggetti da Migrare)\n")
            f.write("="*100 + "\n\n")
            f.write(f"Data generazione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Source: Merge di 2 report lineage\n\n")
            f.write("CRITERIO CRITICITÀ IBRIDO:\n")
            f.write("  ✓ Oggetti con operazioni DML/DDL (INSERT/UPDATE/DELETE/CREATE/ALTER)\n")
            f.write("  ✓ OPPURE Oggetti con ReferenceCount >= 50 (dipendenze critiche)\n\n")
            f.write("="*100 + "\n\n")
            
            # Raccogli tutti gli oggetti
            all_objects = []
            for level in ['L1', 'L2', 'L3', 'L4']:
                all_objects.extend(merged_objects[level])
            
            # Calcola breakdown per motivo
            solo_dml = sum(1 for obj in all_objects 
                          if obj.get('Motivo') and 'DML/DDL' in obj['Motivo'].upper() 
                          and 'DIPENDENZE' not in obj['Motivo'].upper() and '+' not in obj['Motivo'])
            solo_deps = sum(1 for obj in all_objects 
                           if obj.get('Motivo') and ('DIPENDENZE' in obj['Motivo'].upper() or 'Bottom-Up' in obj['Motivo'])
                           and 'DML/DDL' not in obj['Motivo'].upper())
            entrambi = sum(1 for obj in all_objects 
                          if obj.get('Motivo') and 'DML/DDL' in obj['Motivo'].upper() 
                          and ('DIPENDENZE' in obj['Motivo'].upper() or '+' in obj['Motivo']))
            
            # Summary esecutivo (con controllo divisione per zero)
            f.write("1. SUMMARY ESECUTIVO\n")
            f.write("="*100 + "\n\n")
            f.write(f"Oggetti totali analizzati (L1-L4):           {stats['unique']}\n")
            critici_count = sum(1 for obj in all_objects if obj.get('Critico_Migrazione') == 'SÌ')
            
            if stats['unique'] > 0:
                pct_crit = critici_count/stats['unique']*100
            else:
                pct_crit = 0.0
            
            f.write(f"Oggetti CRITICI da migrare (IBRIDO):         {critici_count} ({pct_crit:.1f}%)\n\n")
            
            f.write("Breakdown per motivo criticità:\n")
            f.write(f"  • Critici SOLO per DML/DDL:                {solo_dml}\n")
            f.write(f"  • Critici SOLO per Dipendenze (50+ refs):  {solo_deps}\n")
            f.write(f"  • Critici per ENTRAMBI i motivi:           {entrambi}\n")
            f.write(f"  • TOTALE (verifica):                       {solo_dml + solo_deps + entrambi}\n\n")
            
            # Distribuzione per livello
            f.write("Distribuzione per livello:\n")
            for level in ['L1', 'L2', 'L3', 'L4']:
                level_objs = merged_objects[level]
                total_level = len(level_objs)
                critici_level = sum(1 for obj in level_objs if obj.get('Critico_Migrazione') == 'SÌ')
                
                if total_level > 0:
                    pct = critici_level/total_level*100 if total_level > 0 else 0
                    f.write(f"  {level}: {critici_level:>3} critici / {total_level:>4} totali ({pct:>5.1f}%)\n")
                    
                    # Breakdown motivo per livello
                    solo_dml_l = sum(1 for obj in level_objs 
                                    if obj.get('Motivo') and 'DML/DDL' in obj['Motivo'].upper() 
                                    and 'DIPENDENZE' not in obj['Motivo'].upper() and '+' not in obj['Motivo'])
                    solo_deps_l = sum(1 for obj in level_objs 
                                     if obj.get('Motivo') and ('DIPENDENZE' in obj['Motivo'].upper() or 'Bottom-Up' in obj['Motivo'])
                                     and 'DML/DDL' not in obj['Motivo'].upper())
                    entrambi_l = sum(1 for obj in level_objs 
                                    if obj.get('Motivo') and 'DML/DDL' in obj['Motivo'].upper() 
                                    and ('DIPENDENZE' in obj['Motivo'].upper() or '+' in obj['Motivo']))
                    
                    f.write(f"       Solo DML: {solo_dml_l} | Solo Deps: {solo_deps_l} | Entrambi: {entrambi_l}\n")
            f.write("\n\n")
            
            # 2. DETTAGLIO PER LIVELLO
            f.write("2. DETTAGLIO PER LIVELLO - Oggetti Critici da Migrare\n")
            f.write("="*100 + "\n\n")
            
            for level in ['L1', 'L2', 'L3', 'L4']:
                level_objs = merged_objects[level]
                if len(level_objs) == 0:
                    continue
                
                f.write("─"*100 + "\n")
                f.write(f"LIVELLO {level}\n")
                f.write("─"*100 + "\n\n")
                
                total_level = len(level_objs)
                critici_level = sum(1 for obj in level_objs if obj.get('Critico_Migrazione') == 'SÌ')
                
                f.write(f"Oggetti totali:                    {total_level}\n")
                f.write(f"Oggetti CRITICI da migrare:        {critici_level} ({critici_level/total_level*100:.1f}%)\n\n")
                
                # Breakdown per motivo
                solo_dml_l = sum(1 for obj in level_objs 
                                if obj.get('Motivo') and 'DML/DDL' in obj['Motivo'].upper() 
                                and 'DIPENDENZE' not in obj['Motivo'].upper() and '+' not in obj['Motivo'])
                solo_deps_l = sum(1 for obj in level_objs 
                                 if obj.get('Motivo') and ('DIPENDENZE' in obj['Motivo'].upper() or 'Bottom-Up' in obj['Motivo'])
                                 and 'DML/DDL' not in obj['Motivo'].upper())
                entrambi_l = sum(1 for obj in level_objs 
                                if obj.get('Motivo') and 'DML/DDL' in obj['Motivo'].upper() 
                                and ('DIPENDENZE' in obj['Motivo'].upper() or '+' in obj['Motivo']))
                
                f.write("Breakdown per motivo:\n")
                f.write(f"  • SOLO DML/DDL (no dipendenze):  {solo_dml_l}\n")
                f.write(f"  • SOLO Dipendenze (no DML/DDL):  {solo_deps_l}\n")
                f.write(f"  • ENTRAMBI (DML/DDL + Deps):     {entrambi_l}\n")
                f.write(f"  • Totale con DML/DDL:            {solo_dml_l + entrambi_l}\n")
                f.write(f"  • Totale con Dipendenze:         {solo_deps_l + entrambi_l}\n\n")
                
                # Per tipo oggetto
                type_counts = defaultdict(int)
                for obj in level_objs:
                    obj_type = obj.get('ObjectType') or 'UNKNOWN'
                    type_counts[obj_type] += 1
                
                f.write("Per tipo oggetto:\n")
                for obj_type in sorted(type_counts.keys(), key=lambda x: type_counts[x], reverse=True):
                    pct = type_counts[obj_type] / total_level * 100
                    f.write(f"  • {obj_type:<40}: {type_counts[obj_type]:>3} ({pct:>5.1f}%)\n")
                f.write("\n")
                
                # Per database
                db_counts = defaultdict(int)
                for obj in level_objs:
                    db_counts[obj['Database']] += 1
                
                f.write("Per database:\n")
                for db in sorted(db_counts.keys(), key=lambda x: db_counts[x], reverse=True):
                    pct = db_counts[db] / total_level * 100
                    f.write(f"  • {db:<20}: {db_counts[db]:>3} ({pct:>5.1f}%)\n")
                f.write("\n")
                
                # Per criticità tecnica
                crit_counts = defaultdict(int)
                for obj in level_objs:
                    crit = obj.get('Criticità_Tecnica') or 'NON_SPECIFICATA'
                    crit_counts[crit] += 1
                
                f.write("Per criticità tecnica:\n")
                for crit in sorted(crit_counts.keys(), key=lambda x: crit_counts[x], reverse=True):
                    pct = crit_counts[crit] / total_level * 100
                    f.write(f"  • {crit:<40}: {crit_counts[crit]:>3} ({pct:>5.1f}%)\n")
                f.write("\n")
                
                # TOP 10 per dipendenze
                top_refs = sorted(level_objs, key=lambda x: x.get('ReferenceCount', 0), reverse=True)[:10]
                f.write("TOP 10 per dipendenze (ReferenceCount):\n")
                for i, obj in enumerate(top_refs, 1):
                    refs = obj.get('ReferenceCount', 0)
                    motivo = obj.get('Motivo', 'N/A')
                    f.write(f"  {i:>2}. [{obj['Database']}].[{obj['Schema']}].[{obj['ObjectName']:<50}] | {obj.get('ObjectType', 'UNKNOWN'):<25} | {refs:>3} refs | {motivo}\n")
                f.write("\n\n")
            
            # 3. LISTA COMPLETA OGGETTI
            f.write("3. LISTA COMPLETA OGGETTI CRITICI DA MIGRARE\n")
            f.write("="*100 + "\n\n")
            
            for level in ['L1', 'L2', 'L3', 'L4']:
                level_objs = merged_objects[level]
                if len(level_objs) == 0:
                    continue
                
                f.write("─"*100 + "\n")
                f.write(f"{level} - {len(level_objs)} oggetti critici\n")
                f.write("─"*100 + "\n\n")
                
                # Ordina per database, schema, nome
                sorted_objs = sorted(level_objs, key=lambda x: (x['Database'], x['Schema'], x['ObjectName']))
                
                for obj in sorted_objs:
                    f.write(f"  • [{obj['Database']}].[{obj['Schema']}].[{obj['ObjectName']}] | {obj.get('ObjectType', 'UNKNOWN')}\n")
                    
                    if obj.get('Motivo'):
                        f.write(f"    Motivo:           {obj['Motivo']}\n")
                    
                    ref_val = obj.get('ReferenceCount', 0)
                    f.write(f"    ReferenceCount:   {ref_val}\n")
                    
                    if obj.get('Criticità_Tecnica'):
                        f.write(f"    Criticità Tecnica: {obj['Criticità_Tecnica']}\n")
                    
                    f.write("\n")
                
                f.write("\n")
            
            # 5. DATABASE COINVOLTI (come da struttura originale)
            f.write("\n5. DATABASE COINVOLTI\n")
            f.write("="*100 + "\n\n")
            f.write(f"{'Database':<20} | {'Oggetti Critici':<20} | {'ReferenceCount Totale':<25}\n")
            f.write("-" * 70 + "\n")
            
            db_stats = defaultdict(lambda: {'count': 0, 'refs': 0})
            for obj in all_objects:
                db_stats[obj['Database']]['count'] += 1
                if obj.get('ReferenceCount'):
                    db_stats[obj['Database']]['refs'] += obj['ReferenceCount']
            
            for db in sorted(db_stats.keys(), key=lambda x: db_stats[x]['count'], reverse=True):
                f.write(f"{db:<20} | {db_stats[db]['count']:<20} | {db_stats[db]['refs']:<25}\n")
            
            f.write(f"\nTOTALE: {stats['unique']} oggetti critici da migrare\n")
        
        print(f"✅ Report generato: {output_path}")
        return True
        
    except Exception as e:
        print(f"✗ Errore nella generazione: {e}")
        import traceback
        traceback.print_exc()
        return False
    """Genera il report unificato in formato .txt con dettagli completi."""
    print(f"\n📝 Generazione report unificato...")
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            # Header
            f.write("="*100 + "\n")
            f.write("LINEAGE HYBRID REPORT - MERGED\n")
            f.write("="*100 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"\nTotal Unique Objects: {stats['unique']}\n")
            f.write(f"Duplicates Removed: {stats['duplicates']}\n")
            f.write("="*100 + "\n\n")
            
            # Raccogli tutti gli oggetti per statistiche globali
            all_objects = []
            for level in ['L1', 'L2', 'L3', 'L4']:
                all_objects.extend(merged_objects[level])
            
            # Calcola breakdown per motivo
            solo_dml = sum(1 for obj in all_objects if obj.get('Motivo') and 'DML/DDL' in obj['Motivo'].upper() and 'DIPENDENZE' not in obj['Motivo'].upper())
            solo_deps = sum(1 for obj in all_objects if obj.get('Motivo') and 'DIPENDENZE' in obj['Motivo'].upper() and 'DML/DDL' not in obj['Motivo'].upper())
            entrambi = sum(1 for obj in all_objects if obj.get('Motivo') and 'DML/DDL' in obj['Motivo'].upper() and 'DIPENDENZE' in obj['Motivo'].upper())
            
            # Summary esecutivo
            f.write("1. SUMMARY ESECUTIVO\n")
            f.write("="*100 + "\n\n")
            f.write(f"Oggetti totali analizzati (L1-L4):           {stats['unique']}\n")
            critici_count = sum(1 for obj in all_objects if obj.get('Critico_Migrazione') == 'SÌ')
            f.write(f"Oggetti CRITICI da migrare:                  {critici_count} ({critici_count/stats['unique']*100:.1f}%)\n\n")
            
            f.write("Breakdown per motivo:\n")
            f.write(f"  • Critici SOLO per DML/DDL:                {solo_dml}\n")
            f.write(f"  • Critici SOLO per Dipendenze (50+ refs):  {solo_deps}\n")
            f.write(f"  • Critici per ENTRAMBI i motivi:           {entrambi}\n")
            f.write(f"  • TOTALE (verifica):                       {solo_dml + solo_deps + entrambi}\n\n")
            
            # Distribuzione per livello
            f.write("Distribuzione per livello:\n")
            for level in ['L1', 'L2', 'L3', 'L4']:
                total_level = len(merged_objects[level])
                critici_level = sum(1 for obj in merged_objects[level] if obj.get('Critico_Migrazione') == 'SÌ')
                if total_level > 0:
                    f.write(f"  {level}: {critici_level} critici / {total_level} totali ({critici_level/total_level*100:.1f}%)\n")
            f.write("\n")
            
            # Summary per database
            f.write("5. DATABASE COINVOLTI\n")
            f.write("="*100 + "\n\n")
            f.write(f"{'Database':<20} | {'Oggetti Critici':<20} | {'ReferenceCount Totale':<25}\n")
            f.write("-" * 70 + "\n")
            
            db_stats = defaultdict(lambda: {'count': 0, 'refs': 0})
            for obj in all_objects:
                if obj.get('Critico_Migrazione') == 'SÌ':
                    db_stats[obj['Database']]['count'] += 1
                    if obj.get('ReferenceCount'):
                        db_stats[obj['Database']]['refs'] += obj['ReferenceCount']
            
            for db in sorted(db_stats.keys(), key=lambda x: db_stats[x]['count'], reverse=True):
                f.write(f"{db:<20} | {db_stats[db]['count']:<20} | {db_stats[db]['refs']:<25}\n")
            f.write(f"\nTOTALE: {stats['unique']} oggetti critici da migrare\n\n")
            
            # Summary per tipo
            f.write("Per tipo oggetto:\n")
            type_counts = defaultdict(int)
            for obj in all_objects:
                obj_type = obj.get('ObjectType') or 'UNKNOWN'
                type_counts[obj_type] += 1
            
            for obj_type in sorted(type_counts.keys(), key=lambda x: type_counts[x], reverse=True):
                pct = type_counts[obj_type] / stats['unique'] * 100
                f.write(f"  • {obj_type:<40}: {type_counts[obj_type]:>3} ({pct:>5.1f}%)\n")
            f.write("\n")
            
            # Breakdown dettagliato per tipo con distribuzione database
            f.write("6. BREAKDOWN PER TIPO OGGETTO\n")
            f.write("="*100 + "\n\n")
            
            # Raggruppa per tipo
            objects_by_type = defaultdict(list)
            for obj in all_objects:
                obj_type = obj.get('ObjectType') or 'UNKNOWN'
                objects_by_type[obj_type].append(obj)
            
            for obj_type in sorted(objects_by_type.keys(), key=lambda x: len(objects_by_type[x]), reverse=True):
                objs = objects_by_type[obj_type]
                f.write(f"\n{obj_type}\n")
                f.write("-" * 100 + "\n")
                f.write(f"Totale: {len(objs)} oggetti ({len(objs)/stats['unique']*100:.1f}%)\n\n")
                
                # Distribuzione per livello
                level_dist = defaultdict(int)
                for obj in objs:
                    level_dist[obj['Livello']] += 1
                
                f.write("Per livello:\n")
                for level in ['L1', 'L2', 'L3', 'L4']:
                    if level in level_dist:
                        f.write(f"  • {level}: {level_dist[level]} ({level_dist[level]/len(objs)*100:.1f}%)\n")
                
                # Distribuzione per database
                db_dist = defaultdict(int)
                for obj in objs:
                    db_dist[obj['Database']] += 1
                
                f.write("\nPer database:\n")
                for db in sorted(db_dist.keys(), key=lambda x: db_dist[x], reverse=True)[:5]:
                    f.write(f"  • {db}: {db_dist[db]} ({db_dist[db]/len(objs)*100:.1f}%)\n")
                
                # Criticità
                critici = sum(1 for obj in objs if obj.get('Critico_Migrazione') == 'SÌ')
                f.write(f"\nCritici: {critici} ({critici/len(objs)*100:.1f}%)\n")
            
            f.write("\n")
            
            # Dettaglio per livello
            for level in ['L1', 'L2', 'L3', 'L4']:
                if len(merged_objects[level]) == 0:
                    continue
                
                f.write("\n" + "─"*100 + "\n")
                f.write(f"LIVELLO {level}\n")
                f.write("─"*100 + "\n\n")
                
                f.write(f"Oggetti totali:                    {len(merged_objects[level])}\n")
                critici_level = sum(1 for obj in merged_objects[level] if obj.get('Critico_Migrazione') == 'SÌ')
                f.write(f"Oggetti CRITICI da migrare:        {critici_level} ({critici_level/len(merged_objects[level])*100:.1f}%)\n\n")
                
                # Statistiche per tipo
                level_type_counts = defaultdict(int)
                for obj in merged_objects[level]:
                    obj_type = obj.get('ObjectType') or 'UNKNOWN'
                    level_type_counts[obj_type] += 1
                
                f.write("Per tipo oggetto:\n")
                for obj_type in sorted(level_type_counts.keys(), key=lambda x: level_type_counts[x], reverse=True):
                    pct = level_type_counts[obj_type] / len(merged_objects[level]) * 100
                    f.write(f"  • {obj_type:<40}: {level_type_counts[obj_type]:>3} ({pct:>5.1f}%)\n")
                f.write("\n")
                
                # Oggetti ordinati per database
                sorted_objects = sorted(
                    merged_objects[level],
                    key=lambda x: (x['Database'], x['Schema'], x['ObjectName'])
                )
                
                current_db = None
                for obj in sorted_objects:
                    # Separatore per database
                    if obj['Database'] != current_db:
                        if current_db is not None:
                            f.write("\n")
                        current_db = obj['Database']
                    
                    # Oggetto
                    f.write(f"• [{obj['Database']}].[{obj['Schema']}].[{obj['ObjectName']}]")
                    f.write(f" | {obj.get('ObjectType', 'UNKNOWN')}\n")
                    
                    if obj.get('Motivo'):
                        f.write(f"  Motivo:           {obj['Motivo']}\n")
                    
                    ref_val = obj.get('ReferenceCount', 0)
                    f.write(f"  ReferenceCount:   {ref_val}\n")
                    
                    if obj.get('Criticità_Tecnica'):
                        f.write(f"  Criticità Tecnica: {obj['Criticità_Tecnica']}\n")
                    
                    f.write("\n")
        
        print(f"✅ Report generato: {output_path}")
        return True
        
    except Exception as e:
        print(f"✗ Errore nella generazione: {e}")
        import traceback
        traceback.print_exc()
        return False

def generate_excel_report(merged_objects, stats, output_path):
    """Genera il report unificato in formato Excel con dettagli completi."""
    print(f"\n📊 Generazione Excel...")
    
    try:
        # Prepara DataFrame con tutti gli oggetti e dettagli
        all_objects = []
        for level in ['L1', 'L2', 'L3', 'L4']:
            for obj in merged_objects[level]:
                all_objects.append({
                    'Livello': obj.get('Livello', level),  # Usa get con fallback
                    'Database': obj.get('Database', ''),
                    'Schema': obj.get('Schema', ''),
                    'ObjectName': obj.get('ObjectName', ''),
                    'ObjectType': obj.get('ObjectType', ''),
                    'Critico_Migrazione': obj.get('Critico_Migrazione', ''),
                    'Motivo': obj.get('Motivo', ''),
                    'ReferenceCount': obj.get('ReferenceCount', 0),
                    'Criticità_Tecnica': obj.get('Criticità_Tecnica', '')
                })
        
        if len(all_objects) == 0:
            print("⚠️ Nessun oggetto da esportare in Excel")
            return False
        
        df = pd.DataFrame(all_objects)
        
        # Crea Excel con più sheets
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Sheet 1: Tutti gli oggetti
            df.to_excel(writer, sheet_name='All Objects', index=False)
            
            # Sheet 2-5: Un sheet per livello
            for level in ['L1', 'L2', 'L3', 'L4']:
                df_level = df[df['Livello'] == level].copy()
                if len(df_level) > 0:
                    df_level.to_excel(writer, sheet_name=level, index=False)
            
            # Sheet 6: Summary statistics
            summary_data = []
            summary_data.append(['Metric', 'Value'])
            summary_data.append(['Total Objects', stats['unique']])
            summary_data.append(['Duplicates Removed', stats['duplicates']])
            summary_data.append(['Report 1 Objects', stats['total_1']])
            summary_data.append(['Report 2 Objects', stats['total_2']])
            summary_data.append(['', ''])
            
            for level in ['L1', 'L2', 'L3', 'L4']:
                count = len(merged_objects[level])
                critici = sum(1 for obj in merged_objects[level] if obj.get('Critico_Migrazione') == 'SÌ')
                summary_data.append([f'{level} Total', count])
                summary_data.append([f'{level} Critical', critici])
            
            # Breakdown per motivo
            summary_data.append(['', ''])
            summary_data.append(['Breakdown per Motivo', ''])
            solo_dml = sum(1 for obj in all_objects if obj.get('Motivo') and 'DML/DDL' in str(obj['Motivo']).upper() and 'DIPENDENZE' not in str(obj['Motivo']).upper())
            solo_deps = sum(1 for obj in all_objects if obj.get('Motivo') and 'DIPENDENZE' in str(obj['Motivo']).upper() and 'DML/DDL' not in str(obj['Motivo']).upper())
            entrambi = sum(1 for obj in all_objects if obj.get('Motivo') and 'DML/DDL' in str(obj['Motivo']).upper() and 'DIPENDENZE' in str(obj['Motivo']).upper())
            summary_data.append(['Solo DML/DDL', solo_dml])
            summary_data.append(['Solo Dipendenze', solo_deps])
            summary_data.append(['Entrambi', entrambi])
            
            df_summary = pd.DataFrame(summary_data[1:], columns=summary_data[0])
            df_summary.to_excel(writer, sheet_name='Summary', index=False)
            
            # Sheet 7: Per Database (con ReferenceCount totale)
            db_stats = defaultdict(lambda: {'Count': 0, 'TotalRefs': 0})
            for _, row in df.iterrows():
                db = row['Database']
                db_stats[db]['Count'] += 1
                if pd.notna(row.get('ReferenceCount')):
                    db_stats[db]['TotalRefs'] += row['ReferenceCount']
            
            db_summary = pd.DataFrame([
                {'Database': db, 'Count': stats['Count'], 'TotalReferenceCount': stats['TotalRefs']}
                for db, stats in db_stats.items()
            ])
            db_summary = db_summary.sort_values('Count', ascending=False)
            db_summary.to_excel(writer, sheet_name='By Database', index=False)
            
            # Sheet 8: Per Tipo (dettagliato)
            type_detail_data = []
            for obj_type in sorted(df['ObjectType'].unique()):
                if pd.isna(obj_type):
                    obj_type = 'UNKNOWN'
                df_type = df[df['ObjectType'] == obj_type]
                
                # Per livello
                for level in ['L1', 'L2', 'L3', 'L4']:
                    df_level = df_type[df_type['Livello'] == level]
                    if len(df_level) > 0:
                        critici = sum(df_level['Critico_Migrazione'] == 'SÌ')
                        type_detail_data.append({
                            'ObjectType': obj_type,
                            'Livello': level,
                            'Count': len(df_level),
                            'Critici': critici,
                            'Top_Database': df_level['Database'].value_counts().index[0] if len(df_level) > 0 else None
                        })
            
            type_detail_df = pd.DataFrame(type_detail_data)
            type_detail_df.to_excel(writer, sheet_name='By Type Detail', index=False)
            
            # Sheet 9: Per Tipo - Summary
            type_summary = df.groupby('ObjectType').agg({
                'ObjectName': 'count',
                'ReferenceCount': 'sum'
            }).rename(columns={'ObjectName': 'Count', 'ReferenceCount': 'TotalRefs'})
            type_summary = type_summary.reset_index().sort_values('Count', ascending=False)
            type_summary.to_excel(writer, sheet_name='By Type', index=False)
            
            # Sheet 10-N: Un sheet per ogni tipo oggetto
            type_sheet_names = {
                'SQL_STORED_PROCEDURE': 'StoredProcedures',
                'SQL_TRIGGER': 'Triggers',
                'VIEW': 'Views',
                'SQL_SCALAR_FUNCTION': 'ScalarFunctions',
                'SQL_INLINE_TABLE_VALUED_FUNCTION': 'InlineFunctions',
                'SQL_TABLE_VALUED_FUNCTION': 'TableFunctions'
            }
            
            for obj_type, sheet_name in type_sheet_names.items():
                df_type = df[df['ObjectType'] == obj_type].copy()
                if len(df_type) > 0:
                    df_type.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Sheet finale: Per Criticità Tecnica
            crit_summary = df.groupby('Criticità_Tecnica').size().reset_index(name='Count')
            crit_summary = crit_summary.sort_values('Count', ascending=False)
            crit_summary.to_excel(writer, sheet_name='By Criticità', index=False)
            
            # Sheet finale+1: Per Motivo
            motivo_summary = df.groupby('Motivo').size().reset_index(name='Count')
            motivo_summary = motivo_summary.sort_values('Count', ascending=False)
            motivo_summary.to_excel(writer, sheet_name='By Motivo', index=False)
        
        print(f"✅ Excel generato: {output_path}")
        return True
        
    except Exception as e:
        print(f"✗ Errore nella generazione Excel: {e}")
        import traceback
        traceback.print_exc()
        return False

# =========================
# MAIN
# =========================

def main():
    print("\n" + "="*80)
    print("MERGE LINEAGE REPORTS")
    print("="*80)
    print(f"\nReport 1: {REPORT_1_PATH}")
    print(f"Report 2: {REPORT_2_PATH}")
    print(f"Output TXT:   {OUTPUT_PATH}")
    print(f"Output EXCEL: {OUTPUT_EXCEL_PATH}")
    
    # 1. Parse entrambi i report
    objects1 = parse_lineage_report(REPORT_1_PATH)
    objects2 = parse_lineage_report(REPORT_2_PATH)
    
    # 2. Merge eliminando duplicati
    merged, stats = merge_objects(objects1, objects2)
    
    # 3. Genera report unificato TXT
    success_txt = generate_merged_report(merged, stats, OUTPUT_PATH)
    
    # 4. Genera report Excel
    success_excel = generate_excel_report(merged, stats, OUTPUT_EXCEL_PATH)
    
    if success_txt and success_excel:
        print("\n" + "="*80)
        print("✅ MERGE COMPLETATO CON SUCCESSO")
        print("="*80)
        print(f"\nOggetti totali: {stats['unique']}")
        print(f"Duplicati rimossi: {stats['duplicates']}")
        print(f"\nReport disponibili in:")
        print(f"  TXT:   {OUTPUT_PATH}")
        print(f"  EXCEL: {OUTPUT_EXCEL_PATH}")
        print("")
    else:
        print("\n✗ Merge parzialmente fallito")
        if success_txt:
            print("✓ File TXT generato")
        if success_excel:
            print("✓ File Excel generato")

if __name__ == "__main__":
    main()
