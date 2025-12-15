import os
import glob
import openpyxl
import pandas as pd
import sys

user_folder = os.path.expanduser("~")
EXCEL_ROOT_PATH = rf'{user_folder}\Desktop\doValue'
EXPORT_MCODE_PATH = rf'{user_folder}\Desktop\Export M Code'

import xml.etree.ElementTree as ET

# Importa il percorso dalla config
sys.path.append(os.path.dirname(__file__))

def find_excel_files(root_dir):
    excel_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for f in filenames:
            if f.lower().endswith(('.xlsx', '.xlsm')):
                excel_files.append(os.path.join(dirpath, f))
    return excel_files

def count_workbook_connections(excel_path):
    # Cerca la cartella _xmls o _xml accanto al file excel
    base = os.path.splitext(excel_path)[0]
    xml_dirs = [base + '_xmls', base + '_xml']
    for xml_dir in xml_dirs:
        connections_path = os.path.join(xml_dir, 'connections.xml')
        if os.path.exists(connections_path):
            try:
                tree = ET.parse(connections_path)
                root = tree.getroot()
                count = 0
                for conn in root.iter():
                    if conn.tag.endswith('connection'):
                        if conn.attrib.get('dsn', '') == '$Workbook$' or conn.attrib.get('name', '') == '$Workbook$':
                            count += 1
                        # Alcuni connections.xml usano attributi diversi, quindi controlla anche DataSource
                        if conn.attrib.get('dataSource', '') == '$Workbook$':
                            count += 1
                return count
            except Exception:
                return 'Errore XML'
    return 0

def count_txt_files(txt_root_dir, excel_path):
    base = os.path.splitext(os.path.basename(excel_path))[0]
    txt_pattern = os.path.join(txt_root_dir, f"{base}*.txt")
    return len(glob.glob(txt_pattern))

def main():
    # Usa i percorsi configurati
    root_dir = EXCEL_ROOT_PATH
    txt_root_dir = EXPORT_MCODE_PATH
    excel_files = find_excel_files(root_dir)
    data = []
    for excel in excel_files:
        n_conn = count_workbook_connections(excel)
        n_txt = count_txt_files(txt_root_dir, excel)
        data.append({
            'File Excel': os.path.relpath(excel, root_dir),
            'N° Connessioni $Workbook$': n_conn,
            'N° TXT': n_txt
        })
    df = pd.DataFrame(data)
    output_path = os.path.join(root_dir, 'verifica_connessioni.xlsx')
    df.to_excel(output_path, index=False)
    print(f"Risultato salvato in {output_path}")

if __name__ == "__main__":
    main()