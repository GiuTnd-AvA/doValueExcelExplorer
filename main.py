from Config.config import POWERSHELL_SCRIPT_PATH, EXCEL_ROOT_PATH, EXPORT_MCODE_PATH, EXCEL_OUTPUT_PATH, CHUNK_SIZE
from PowerShellScripts.Excecute_Power_Shell_Script import ExecPsCode as ps
from BusinessLogic.Business_Logic import BusinessLogic as bl
from Report.Excel_Writer import ExcelWriter as ew
from datetime import datetime

print(f'start at: {datetime.now()}')
# run_ps = ps(POWERSHELL_SCRIPT_PATH, EXCEL_ROOT_PATH, EXPORT_MCODE_PATH)
# return_code, output, error = run_ps.run()

# if return_code == 0:
#     print("PowerShell script executed successfully.")
#     print("Output:")
#     print(output)
# else:
#     print("PowerShell script execution failed.")
#     print("Error:")
#     print(error)

bl_obj = bl(EXCEL_ROOT_PATH, EXPORT_MCODE_PATH)
excel_file_paths = bl_obj.get_excel_file_paths()
excel_files_list = bl_obj.split_excel_root_path()

# Chunking: size configured via Config/config.py (CHUNK_SIZE)

def _file_name_only(path_pair):
    # path_pair is [folder, filename]
    return path_pair[1] if isinstance(path_pair, list) and len(path_pair) == 2 else str(path_pair)

def _chunk_ranges(n, size):
    ranges = []
    start = 0
    while start < n:
        end = min(start + size - 1, n - 1)
        ranges.append((start, end))
        start = end + 1
    return ranges

file_names_order = [_file_name_only(p) for p in excel_files_list]
name_to_index = {name: idx for idx, name in enumerate(file_names_order)}

# Helper to filter rows by file name index belonging to a range
def _filter_by_range(rows, get_name_fn, rng):
    start, end = rng
    return [r for r in rows if get_name_fn(r) in name_to_index and start <= name_to_index[get_name_fn(r)] <= end]

columns_file_list = ['Percorsi', 'File']
columns_connection_no_power_query = ['File_Name','Server','Database','Schema','Table']
columns_connessioni = ['File_Name',
                       'Creatore_file',
                       'Ultimo_modificatore_file',
                       'Data_creazione_file',
                       'Data_ultima_modifica_file',
                       'Collegamento_esterno',
                       'Source',
                       'Server',
                       'Database',
                       'Schema',
                       'Table',
                       'Type']

ranges = _chunk_ranges(len(excel_files_list), CHUNK_SIZE)

bl_obj = bl(EXCEL_ROOT_PATH, EXPORT_MCODE_PATH)
xml = bl_obj.connessioni_xml()

for i in xml:
    print(i) 
# for start, end in ranges:
#     suffix = f"{start}-{end}"
#     out_name = f"Report_Connessioni_{suffix}.xlsx"
#     writer = ew(EXCEL_OUTPUT_PATH, out_name)
#     # Lista file per range
#     files_chunk = excel_files_list[start:end+1]
#     writer.write_excel(columns_file_list, files_chunk, sheet_name='Lista file')
#     # Paths chunk to analyze and export only this batch
#     paths_chunk = excel_file_paths[start:end+1]
#     aggregated_info_chunk = bl_obj.get_aggregated_info_for_files(paths_chunk)
#     writer.write_excel(columns_connessioni, aggregated_info_chunk, sheet_name='Connessioni')
#     #connection_list_No_Power_Query_chunk = bl_obj.get_excel_connections_without_txt_for_files(paths_chunk)
#     connection_list_No_Power_Query_chunk = bl_obj.connessioni_xml()
#     writer.write_excel(columns_connection_no_power_query, connection_list_No_Power_Query_chunk, sheet_name='Connessioni_Senza_Power_Query')
#     print(f"Creato: {out_name} per range {suffix}")

print(f'end at: {datetime.now()}')