# config.py
import os

user_folder = os.path.expanduser("~")
# Main.py paths
POWERSHELL_SCRIPT_PATH = rf'{user_folder}\Desktop\doValueExcelExplorer\mcode_extraction\extraction\export_mcode.ps1'
EXCEL_ROOT_PATH = rf'{user_folder}\Desktop\doValue'
EXPORT_MCODE_PATH = rf'{user_folder}\Desktop\Export M Code'

# extract_db_from_excel.py paths
EXCEL_INPUT_PATH = rf'{user_folder}\Desktop\Report_Connessioni.xlsx'
EXCEL_OUTPUT_PATH = rf'{user_folder}\Desktop\Report_Estratto_DB.xlsx'
