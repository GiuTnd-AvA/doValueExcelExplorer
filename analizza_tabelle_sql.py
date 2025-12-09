import pandas as pd
import re

# Percorso input/output
input_path = r'C:\Users\giuseppe.tanda\Desktop\doValue\Risultati_SQL.xlsx'
output_path = r'C:\Users\giuseppe.tanda\Desktop\doValue\Analisi_Tabelle_SQL.xlsx'

def estrai_tabelle(sql):
    if not isinstance(sql, str):
        return []
    # Cerca tabelle in FROM e JOIN
    pattern = r'(?:FROM|JOIN)\s+([\w\[\]\.]+)'  # Prende schema.tabella, tabella, [schema].[tabella]
    matches = re.findall(pattern, sql, re.IGNORECASE)
    tabelle = []
    for m in matches:
        parts = m.split('.')
        if len(parts) == 2:
            schema, table = parts
            schema = schema.replace('[','').replace(']','')
            table = table.replace('[','').replace(']','')
            tabelle.append(f'[{schema}].[{table}]')
        else:
            # Solo tabella, senza schema
            table = m.replace('[','').replace(']','')
            tabelle.append(f'[default].[{table}]')  # Se vuoi gestire senza schema
    return list(dict.fromkeys(tabelle))  # Solo unici, ordine preservato

def main():
    df = pd.read_excel(input_path)
    risultati = []
    occorrenze = {}
    # Prima passata: sheet1 come prima
    seen = set()
    for idx, row in df.iterrows():
        object_name = row.get('ObjectName')
        sql_def = row.get('SQLDefinition')
        server = row.get('Server', '')
        db = row.get('Database', '')
        tabelle = estrai_tabelle(sql_def)
        key = (object_name, '; '.join(tabelle))
        if key in seen:
            continue
        seen.add(key)
        risultati.append({
            'ObjectName': object_name,
            'NumTabelle': len(tabelle),
            'Tabelle': '; '.join(tabelle)
        })
        # Seconda passata: conta schema.object_name in SQLDefinition SOLO UNA VOLTA per query
        pattern = r'(?:FROM|JOIN)\s+([\w\[\]]+)\.([\w\[\]]+)'  # schema.object
        chiavi_trovate = set()
        for match in re.findall(pattern, str(sql_def), re.IGNORECASE):
            schema, obj = match
            schema = schema.replace('[','').replace(']','')
            obj = obj.replace('[','').replace(']','')
            key = f"{server}.{db}.{schema}.{obj}"
            chiavi_trovate.add(key)
        for key in chiavi_trovate:
            occorrenze[key] = occorrenze.get(key, 0) + 1
    # Scrivi su due sheet
    with pd.ExcelWriter(output_path) as writer:
        pd.DataFrame(risultati).to_excel(writer, index=False, sheet_name='Analisi')
        pd.DataFrame(list(occorrenze.items()), columns=['Elemento', 'Conteggio']).to_excel(writer, index=False, sheet_name='Occorrenze')
    print(f"Analisi completata. Output in: {output_path}")

if __name__ == "__main__":
    main()
