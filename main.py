from Config.config import POWERSHELL_SCRIPT_PATH, EXCEL_ROOT_PATH, EXPORT_MCODE_PATH
from PowerShellScripts.Excecute_Power_Shell_Script import ExecPsCode as ps
from BusinessLogic.Business_Logic import BusinessLogic as bl
from Report.Excel_Writer import ExcelWriter as ew

run_ps = ps(POWERSHELL_SCRIPT_PATH, EXCEL_ROOT_PATH, EXPORT_MCODE_PATH)
return_code, output, error = run_ps.run()

if return_code == 0:
    print("PowerShell script executed successfully.")
    print("Output:")
    print(output)
else:
    print("PowerShell script execution failed.")
    print("Error:")
    print(error)

bl_obj = bl(EXCEL_ROOT_PATH, EXPORT_MCODE_PATH)


aggregated_info = bl_obj.get_aggregated_info()

columns = ['File_Name',
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

stampa_report_connessioni = ew(r'C:\Users\ciro.andreano\Desktop','Report_Connessioni.xlsx')
stampa_report_connessioni.write_excel(columns, aggregated_info)
print("Report connessioni creato correttamente.")