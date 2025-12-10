# config.py
import os

user_folder = os.path.expanduser("~")
# Main.py paths
POWERSHELL_SCRIPT_PATH = rf'{user_folder}\Desktop\doValueExcelExplorer\PowerShellScripts\ExportMCode.ps1'
EXCEL_ROOT_PATH = rf'{user_folder}\Desktop\doValue'
EXPORT_MCODE_PATH = rf'{user_folder}\Desktop\Export M Code'

# PowerShell script paths
FOLDER = rf'{user_folder}\Desktop\doValue'  # Cartella radice

# extract_db_from_excel.py paths
EXCEL_INPUT_PATH = rf'{user_folder}\Desktop\Connessioni Trovate.xlsx'
EXCEL_OUTPUT_PATH = rf'{user_folder}\Desktop'
