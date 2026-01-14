"""
Helper functions for parsing and processing M code files.
"""


def parse_source_line(file_path: str) -> str | None:
    """
    Reads a text file and extracts the first line containing a Power Query source.
    
    Searches for common Power Query source patterns:
    - Sql.Database
    - SharePoint.Files/Contents/Tables
    - Excel.Workbook
    
    Args:
        file_path: Path to the M code text file
        
    Returns:
        The source line if found, None otherwise
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line_stripped = line.strip()
                if any(keyword in line_stripped for keyword in [
                    'Sql.Database',
                    'SharePoint.Files',
                    'SharePoint.Contents',
                    'SharePoint.Tables',
                    'Excel.Workbook'
                ]):
                    return line_stripped
    except Exception as e:
        return f"Errore: {e}"
    
    return None


def get_connection_type_from_class(class_name: str) -> str:
    """
    Maps connection class names to readable connection types.
    
    Args:
        class_name: Name of the connection class
        
    Returns:
        Human-readable connection type
    """
    type_mapping = {
        'GetSqlConnection': 'Sql',
        'GetSharePointConnection': 'SharePoint',
        'GetExcelConnection': 'Excel',
        'EmptyConnection': 'Unknown'
    }
    return type_mapping.get(class_name, 'Unknown')
