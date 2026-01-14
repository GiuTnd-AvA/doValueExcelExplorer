"""
Report generation logic for Excel Power Query analysis.
Handles comparison between expected and extracted connections.
"""
from typing import Dict, List, Tuple
import os


class ReportGenerator:
    """Generates comparison reports and analysis for Power Query connections."""
    
    def __init__(self):
        self.expected_connections: Dict[str, int] = {}
    
    def load_expected_connections(self, user_data: str) -> None:
        """
        Parses expected connection counts from tab-separated string.
        
        Args:
            user_data: Multi-line string with format "filename.txt\\tcount"
        """
        lines = user_data.strip().splitlines()
        self.expected_connections = {
            line.split('\t')[0].strip(): int(line.split('\t')[1])
            for line in lines
            if '\t' in line and line.split('\t')[1].strip().isdigit()
        }
    
    def generate_comparison_report(self, extracted_info: List[List]) -> List[List]:
        """
        Compares expected vs extracted connection counts.
        
        Args:
            extracted_info: List of extracted connection data
            
        Returns:
            List of comparison rows: [prefix, expected, extracted, status]
        """
        # Count extracted connections by file prefix
        prefix_found = {k.replace('.txt', ''): 0 for k in self.expected_connections}
        seen_per_prefix = {prefix: set() for prefix in prefix_found}
        
        for row in extracted_info:
            if not row:
                continue
            fname = row[0] if isinstance(row, (list, tuple)) else str(row)
            
            for prefix in prefix_found:
                if fname.startswith(prefix) and fname not in seen_per_prefix[prefix]:
                    prefix_found[prefix] += 1
                    seen_per_prefix[prefix].add(fname)
                    break
        
        # Generate comparison rows
        summary_rows = []
        for filename, expected_count in self.expected_connections.items():
            prefix = filename.replace('.txt', '')
            extracted_count = prefix_found.get(prefix, 0)
            status = 'OK' if extracted_count == expected_count else 'KO'
            summary_rows.append([prefix, expected_count, extracted_count, status])
        
        return summary_rows
    
    def format_aggregated_info(self, aggregated_info: List[List]) -> List[List]:
        """
        Converts .txt file references to .xlsx in aggregated info.
        
        Args:
            aggregated_info: List of connection data rows
            
        Returns:
            Formatted list with .xlsx extensions
        """
        formatted_records = []
        
        for row in aggregated_info:
            if isinstance(row, (list, tuple)) and row:
                file_txt = row[0]
                if file_txt.lower().endswith('.txt'):
                    file_xlsx = file_txt[:-4] + '.xlsx'
                    new_row = [file_xlsx] + list(row[1:])
                    formatted_records.append(new_row)
                else:
                    formatted_records.append(list(row))
            else:
                formatted_records.append(row)
        
        return formatted_records
