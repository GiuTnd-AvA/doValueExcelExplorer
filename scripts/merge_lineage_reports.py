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
    Parsa un report di lineage e estrae tutti gli oggetti.
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
            content = f.read()
        
        # Pattern per identificare sezioni livello (vari formati)
        level_patterns = [
            re.compile(r'LIVELLO\s+L(\d+)', re.IGNORECASE),
            re.compile(r'─+\s*LIVELLO\s+L(\d+)', re.IGNORECASE),
            re.compile(r'LIVELLO\s+(\d+)', re.IGNORECASE),
        ]
        
        # Pattern per oggetti nel formato:
        # [Database].[Schema].[ObjectName] | ObjectType | refs | ...
        # Oppure:
        #   1. [Database].[Schema].[ObjectName] | ObjectType | ...
        object_pattern = re.compile(
            r'(?:\d+\.\s+)?\[([^\]]+)\]\.\[([^\]]+)\]\.\[([^\]]+)\]\s*\|\s*([^\|]+?)(?:\s*\|\s*(\d+)\s*refs?)?',
            re.IGNORECASE
        )
        
        current_level = None
        
        for line in content.split('\n'):
            # Identifica cambio livello
            for pattern in level_patterns:
                level_match = pattern.search(line)
                if level_match:
                    level_num = level_match.group(1)
                    current_level = f'L{level_num}'
                    break
            
            # Estrae oggetti
            if current_level:
                obj_match = object_pattern.search(line)
                if obj_match:
                    db = obj_match.group(1).strip()
                    schema = obj_match.group(2).strip()
                    obj = obj_match.group(3).strip()
                    obj_type = obj_match.group(4).strip() if obj_match.group(4) else None
                    ref_count = int(obj_match.group(5)) if obj_match.group(5) else None
                    
                    # Estrae criticità
                    critical = 'SÌ' if 'DML/DDL' in line or 'CRITICO' in line.upper() else 'NO'
                    
                    objects_by_level[current_level].append({
                        'Database': db,
                        'Schema': schema,
                        'ObjectName': obj,
                        'ObjectType': obj_type,
                        'Critico_Migrazione': critical,
                        'ReferenceCount': ref_count,
                        'Livello': current_level,
                        'Key': normalize_object_key(db, schema, obj)
                    })
        
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
                # Duplicato: mantieni quello con più info
                existing = seen_keys[key]
                
                # Prendi il ReferenceCount più alto
                if obj['ReferenceCount'] and existing['ReferenceCount']:
                    obj['ReferenceCount'] = max(obj['ReferenceCount'], existing['ReferenceCount'])
                elif obj['ReferenceCount']:
                    pass  # Usa quello nuovo
                else:
                    obj['ReferenceCount'] = existing['ReferenceCount']
                
                # Prendi ObjectType se mancante
                if not obj['ObjectType'] and existing['ObjectType']:
                    obj['ObjectType'] = existing['ObjectType']
                
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
    """Genera il report unificato in formato .txt."""
    print(f"\n📝 Generazione report unificato...")
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            # Header
            f.write("="*80 + "\n")
            f.write("LINEAGE HYBRID REPORT - MERGED\n")
            f.write("="*80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"\nTotal Unique Objects: {stats['unique']}\n")
            f.write(f"Duplicates Removed: {stats['duplicates']}\n")
            f.write("="*80 + "\n\n")
            
            # Summary per livello
            f.write("SUMMARY BY LEVEL\n")
            f.write("-"*80 + "\n")
            for level in ['L1', 'L2', 'L3', 'L4']:
                count = len(merged_objects[level])
                critici = sum(1 for obj in merged_objects[level] if obj['Critico_Migrazione'] == 'SÌ')
                f.write(f"{level}: {count} objects ({critici} critical)\n")
            f.write("\n")
            
            # Summary per database
            f.write("SUMMARY BY DATABASE\n")
            f.write("-"*80 + "\n")
            all_objects = []
            for level in ['L1', 'L2', 'L3', 'L4']:
                all_objects.extend(merged_objects[level])
            
            db_counts = defaultdict(int)
            for obj in all_objects:
                db_counts[obj['Database']] += 1
            
            for db in sorted(db_counts.keys(), key=lambda x: db_counts[x], reverse=True):
                f.write(f"  • {db}: {db_counts[db]} objects\n")
            f.write("\n")
            
            # Summary per tipo
            f.write("SUMMARY BY TYPE\n")
            f.write("-"*80 + "\n")
            type_counts = defaultdict(int)
            for obj in all_objects:
                obj_type = obj['ObjectType'] or 'UNKNOWN'
                type_counts[obj_type] += 1
            
            for obj_type in sorted(type_counts.keys(), key=lambda x: type_counts[x], reverse=True):
                f.write(f"  • {obj_type}: {type_counts[obj_type]} objects\n")
            f.write("\n")
            
            # Dettaglio per livello
            for level in ['L1', 'L2', 'L3', 'L4']:
                if len(merged_objects[level]) == 0:
                    continue
                
                f.write("\n" + "="*80 + "\n")
                f.write(f"LIVELLO {level[1]} - {len(merged_objects[level])} OBJECTS\n")
                f.write("="*80 + "\n\n")
                
                # Ordina per database, poi schema, poi nome
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
                        f.write(f"--- {obj['Database']} ---\n")
                        current_db = obj['Database']
                    
                    # Oggetto
                    obj_line = f"  • [{obj['Database']}].[{obj['Schema']}].[{obj['ObjectName']}]"
                    
                    if obj['ObjectType']:
                        obj_line += f" - {obj['ObjectType']}"
                    
                    if obj['Critico_Migrazione'] == 'SÌ':
                        obj_line += " [CRITICO]"
                    
                    if obj['ReferenceCount']:
                        obj_line += f" ({obj['ReferenceCount']} refs)"
                    
                    f.write(obj_line + "\n")
                
                f.write("\n")
        
        print(f"✅ Report generato: {output_path}")
        return True
        
    except Exception as e:
        print(f"✗ Errore nella generazione: {e}")
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
    print(f"Output:   {OUTPUT_PATH}")
    
    # 1. Parse entrambi i report
    objects1 = parse_lineage_report(REPORT_1_PATH)
    objects2 = parse_lineage_report(REPORT_2_PATH)
    
    # 2. Merge eliminando duplicati
    merged, stats = merge_objects(objects1, objects2)
    
    # 3. Genera report unificato
    success = generate_merged_report(merged, stats, OUTPUT_PATH)
    
    if success:
        print("\n" + "="*80)
        print("✅ MERGE COMPLETATO CON SUCCESSO")
        print("="*80)
        print(f"\nOggetti totali: {stats['unique']}")
        print(f"Duplicati rimossi: {stats['duplicates']}")
        print(f"\nReport disponibile in:")
        print(f"  {OUTPUT_PATH}")
        print("")
    else:
        print("\n✗ Merge fallito")

if __name__ == "__main__":
    main()
