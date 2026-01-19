# config.py
import os

user_folder = os.path.expanduser("~")
# Main.py paths
POWERSHELL_SCRIPT_PATH = rf'{user_folder}\Desktop\doValueExcelExplorer\mcode_extraction\extraction\export_mcode.ps1'
EXCEL_ROOT_PATH = rf'{user_folder}\Desktop\doValue'
EXPORT_MCODE_PATH = rf'{user_folder}\Desktop\Export M Code'

# extract_db_from_excel.py paths
EXCEL_INPUT_PATH = rf'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Connessioni verificate.xlsx'
EXCEL_OUTPUT_PATH = rf'\\dobank\progetti\S1\2025_pj_Unified_Data_Analytics_Tool\Esportazione Oggetti SQL\Estrazioni parziali\estrazione_dipendenze_sql.xlsx'
