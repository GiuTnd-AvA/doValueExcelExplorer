from Config.config import POWERSHELL_SCRIPT_PATH, EXCEL_ROOT_PATH, EXPORT_MCODE_PATH
from PowerShellScripts.Excecute_Power_Shell_Script import ExecPsCode as ps
from PowerShellScripts.Excecute_Power_Shell_Script import ExecPsCode as ps
from BusinessLogic.Business_Logic import BusinessLogic as bl
from Report.Excel_Writer import ExcelWriter as ew
import os 

<<<<<<< HEAD
user_folder = os.path.expanduser("~")
run_ps = ps(rf'{user_folder}\Desktop\doValue\doValueExcelExplorer\ExportMCode.ps1')

=======
run_ps = ps(POWERSHELL_SCRIPT_PATH)
>>>>>>> ciro.mod
return_code, output, error = run_ps.run()

if return_code == 0:
    print("PowerShell script executed successfully.")
    print("Output:")
    print(output)
else:
    print("PowerShell script execution failed.")
    print("Error:")
    print(error)
<<<<<<< HEAD

bl_obj = bl(rf'{user_folder}\Desktop\doValue', rf'{user_folder}\Desktop\doValue\Export M Code')
=======
    
run_ps = ps(POWERSHELL_SCRIPT_PATH)
bl_obj = bl(EXCEL_ROOT_PATH, EXPORT_MCODE_PATH)
>>>>>>> ciro.mod

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
           'Table']

<<<<<<< HEAD
stampa_report_connessioni = ew(rf'{user_folder}\Desktop','Report_Connessioni.xlsx')
=======
stampa_report_connessioni = ew(r'C:\Users\ciro.andreano\Desktop','Report_Connessioni.xlsx')
>>>>>>> ciro.mod
stampa_report_connessioni.write_excel(columns, aggregated_info)
print("Report connessioni creato correttamente.")