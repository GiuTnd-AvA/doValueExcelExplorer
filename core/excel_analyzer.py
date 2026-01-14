"""
Excel Power Query Analyzer - Main orchestrator for analyzing M code connections.
"""
from typing import List, Tuple
import os
from io.file_scanner import FileScanner
from mcode_extraction.parsers.connection_base import IConnection, EmptyConnection
from mcode_extraction.parsers.sql_parser import SqlParser
from core.helpers import parse_source_line, get_connection_type_from_class


class ExcelAnalyzer:
    """
    Analyzes Excel Power Query files and extracts connection information.
    Main orchestrator for the analysis workflow.
    """

    def __init__(self, excel_root_path: str, txt_root_path: str):
        self.excel_scanner = FileScanner(excel_root_path)
        self.txt_scanner = FileScanner(txt_root_path)

    def get_excel_file_list(self) -> List[Tuple[str, str]]:
        """
        Returns list of Excel files as [directory, filename] pairs.
        Filters to .xls and .xlsm files only.
        """
        all_files = self.excel_scanner.find_excel_files()
        filtered = [f for f in all_files if f.lower().endswith(('.xls', '.xlsm'))]
        
        excel_file_list = []
        for file_path in filtered:
            path_parts = file_path.split('\\')
            directory = '\\'.join(path_parts[:-1])
            filename = path_parts[-1]
            excel_file_list.append([directory, filename])
        
        return excel_file_list

    def get_connection_info(self, include_file_counts: bool = True) -> List[List]:
        """
        Extracts connection information from all M code text files.
        
        Args:
            include_file_counts: Whether to include connection count per file
            
        Returns:
            List of connection details: [filename, source, server, database, 
                                        schema, table, joins, type, count]
        """
        connection_objects = self._parse_all_connections()
        
        # Count connections per file if needed
        conn_count_map = {}
        if include_file_counts:
            conn_count_map = self._count_connections_per_file(connection_objects)
        
        # Format output
        output = []
        for conn in connection_objects:
            row = self._format_connection_row(conn, conn_count_map)
            output.append(row)
        
        return output

    def _parse_all_connections(self) -> List[IConnection]:
        """Parses all text files and extracts connection objects."""
        txt_files = self.txt_scanner.find_txt_files()
        results = []
        
        for file_path in txt_files:
            source_line = parse_source_line(file_path)
            
            if source_line is None:
                conn = EmptyConnection(file_path, error='Sorgente non trovata')
                results.append(conn)
                continue
            
            if 'Sql.Database' in source_line:
                conn = SqlParser(file_path)
                conn.get_connection()
                results.append(conn)
            else:
                conn = EmptyConnection(
                    file_path,
                    error=f"Parser non implementato per: {source_line[:50]}"
                )
                results.append(conn)
        
        return results

    def _count_connections_per_file(self, connections: List[IConnection]) -> dict:
        """Counts number of connections per base filename."""
        conn_count_map = {}
        
        for conn in connections:
            if hasattr(conn, 'txt_file'):
                base = os.path.basename(conn.txt_file)
                base = base.replace('.txt', '').replace('.xlsx', '')
                conn_count_map[base] = conn_count_map.get(base, 0) + 1
        
        return conn_count_map

    def _format_connection_row(self, conn: IConnection, count_map: dict) -> List:
        """Formats a connection object into a data row."""
        # Get connection count
        if hasattr(conn, 'txt_file') and count_map:
            base = os.path.basename(conn.txt_file)
            base = base.replace('.txt', '').replace('.xlsx', '')
            n_connessioni = count_map.get(base, 0)
        else:
            n_connessioni = 0
        
        # Get connection type
        conn_type = getattr(conn, 'type', None)
        if not conn_type:
            conn_type = get_connection_type_from_class(conn.__class__.__name__)
        
        # Format JOIN tables
        join_tables = getattr(conn, 'join_tables', [])
        join_tables_str = ', '.join(join_tables) if join_tables else ''
        
        return [
            os.path.basename(getattr(conn, 'txt_file', '')),
            getattr(conn, 'source', None),
            getattr(conn, 'server', None),
            getattr(conn, 'database', None),
            getattr(conn, 'schema', None),
            getattr(conn, 'table', None),
            join_tables_str,
            conn_type,
            n_connessioni
        ]
