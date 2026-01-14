"""
File scanning utilities for Excel and text files.
Simplified file finder without abstract base classes.
"""
import os
from typing import List


class FileScanner:
    """Scans directory trees for files with specific extensions."""
    
    def __init__(self, root_path: str):
        self.root_path = root_path
    
    def find_files(self, extensions: List[str], exclude_temp: bool = True) -> List[str]:
        """
        Recursively finds files with specified extensions.
        
        Args:
            extensions: List of file extensions (e.g., ['.xlsx', '.xls'])
            exclude_temp: Whether to exclude temporary files starting with ~$
            
        Returns:
            List of absolute file paths
        """
        found_files = []
        
        try:
            for root, dirs, files in os.walk(self.root_path):
                for filename in files:
                    # Check extension
                    if not any(filename.lower().endswith(ext) for ext in extensions):
                        continue
                    
                    # Check temp file exclusion
                    if exclude_temp and filename.startswith("~$"):
                        continue
                    
                    full_path = os.path.join(root, filename)
                    found_files.append(full_path)
        except Exception as e:
            print(f"Error scanning directory {self.root_path}: {e}")
        
        return found_files
    
    def find_excel_files(self) -> List[str]:
        """Finds all Excel files (.xls, .xlsx, .xlsm, .xlsb)."""
        return self.find_files(['.xls', '.xlsx', '.xlsm', '.xlsb'])
    
    def find_txt_files(self) -> List[str]:
        """Finds all text files (.txt)."""
        return self.find_files(['.txt'])
