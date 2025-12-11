from Config.config import POWERSHELL_SCRIPT_PATH, EXCEL_ROOT_PATH, EXPORT_MCODE_PATH, EXCEL_OUTPUT_PATH
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
aggregated_info = bl_obj.get_aggregated_info()
excel_files_list = bl_obj.split_excel_root_path()
connection_list_No_Power_Query = bl_obj.get_excel_connections_without_txt()

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

stampa_report_connessioni = ew(EXCEL_OUTPUT_PATH,'Report_Connessioni.xlsx')
stampa_report_connessioni.write_excel(columns_file_list, excel_files_list, sheet_name = 'Lista file')
stampa_report_connessioni.write_excel(columns_connessioni, aggregated_info, sheet_name = 'Connessioni')
stampa_report_connessioni.write_excel(columns_connection_no_power_query, connection_list_No_Power_Query, sheet_name = 'Connessioni_Senza_Power_Query')
print("Report connessioni creato correttamente.")

print(f'end at: {datetime.now()}')