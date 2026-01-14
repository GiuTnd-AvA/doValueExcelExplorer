"""
Main entry point for doValue Excel Explorer.
Orchestrates Power Query M code extraction and analysis workflow.
"""
from config.config import (
    POWERSHELL_SCRIPT_PATH,
    EXCEL_ROOT_PATH,
    EXPORT_MCODE_PATH,
    EXCEL_OUTPUT_PATH
)
from mcode_extraction.extraction.powershell_runner import ExecPsCode
from core.excel_analyzer import ExcelAnalyzer
from core.report_generator import ReportGenerator
from io.excel_exporter import ExcelExporter


# Expected connection counts (provided by user)
USER_EXPECTED_CONNECTIONS = '''
DB_Contatti_v8_x_KPI.txt\t95
Delibere_Master_Report_Team_V12.txt\t55
Report Delibere_KPI_2025_split_noGenn_v38_SG.txt\t17
Report Contatti_KPI_2025_split_noGenn_v22_SG.txt\t17
Semiannual Report ICCREA 3_20250630_v1.txt\t12
'''  # Truncated for brevity - add full list as needed


def main():
    """Main execution workflow."""
    print("=== doValue Excel Explorer ===\n")
    
    # Step 1: Extract M code from Excel files using PowerShell
    print("Step 1: Extracting M code from Excel files...")
    ps_runner = ExecPsCode(POWERSHELL_SCRIPT_PATH, EXCEL_ROOT_PATH, EXPORT_MCODE_PATH)
    return_code, output, error = ps_runner.run()
    
    if return_code != 0:
        print(f"PowerShell extraction failed:\n{error}")
        return
    
    print("M code extraction completed successfully.\n")
    
    # Step 2: Analyze extracted M code
    print("Step 2: Analyzing M code connections...")
    analyzer = ExcelAnalyzer(EXCEL_ROOT_PATH, EXPORT_MCODE_PATH)
    
    excel_files_list = analyzer.get_excel_file_list()
    connection_info = analyzer.get_connection_info(include_file_counts=True)
    
    print(f"Found {len(excel_files_list)} Excel files")
    print(f"Extracted {len(connection_info)} connections\n")
    
    # Step 3: Generate reports
    print("Step 3: Generating Excel reports...")
    exporter = ExcelExporter(r'C:\Users\ciro.andreano\Desktop', 'Report_Connessioni.xlsx')
    
    # Report 1: File list
    exporter.write_excel(
        columns=['Percorsi', 'File'],
        data=excel_files_list,
        sheet_name='Lista file'
    )
    
    # Report 2: Connections
    columns_conn = [
        'File_Name', 'Source', 'Server', 'Database',
        'Schema', 'Table', 'Join', 'Type', 'N_Connessioni_PQ'
    ]
    exporter.write_excel(
        columns=columns_conn,
        data=connection_info,
        sheet_name='Connessioni'
    )
    
    # Report 3: Comparison (Expected vs Extracted)
    report_gen = ReportGenerator()
    report_gen.load_expected_connections(USER_EXPECTED_CONNECTIONS)
    comparison = report_gen.generate_comparison_report(connection_info)
    
    exporter.write_excel(
        columns=['Radice', 'Attese', 'Estratte', 'OK/KO'],
        data=comparison,
        sheet_name='File gestiti parzialmente'
    )
    
    print("Reports generated successfully!")
    print(f"Output: {exporter.folder_path}\\{exporter.file_name}")


if __name__ == "__main__":
    main()
