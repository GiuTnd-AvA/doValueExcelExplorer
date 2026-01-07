from .Txt_Source_Lines import TxtSplitLines
from FileFinder.Excel_Finder import ExcelFinder
from FileFinder.TXT_Finder import TxtFinder
from Codice_M.Estrazione_Connessione_SQL.IConnection import IConnection, EmptyConnection
from Codice_M.Estrazione_Connessione_SQL.Get_SQL_Connection import GetSqlConnection
from typing import List
import os

class BusinessLogic:

    def get_txt_only_connection_info(self) -> list:
        """
        Estrae tutte le connessioni PowerQuery dai file .txt, senza richiedere la presenza dei file Excel.
        Restituisce una lista di liste con tutte le colonne di dettaglio, Join e N_Connessioni_PQ (conteggio per file txt).
        """
        connection_info = self._get_connection_info()
        # Conta le connessioni per ogni file txt
        conn_count_map = {}
        for conn in connection_info:
            if hasattr(conn, 'txt_file'):
                base = os.path.basename(conn.txt_file)
                base = base.replace('.txt', '').replace('.xlsx', '')
                conn_count_map[base] = conn_count_map.get(base, 0) + 1
        output = []
        for conn in connection_info:
            if hasattr(conn, 'txt_file'):
                base = os.path.basename(conn.txt_file)
                base = base.replace('.txt', '').replace('.xlsx', '')
                n_connessioni = conn_count_map.get(base, 0)
            else:
                n_connessioni = 0
            conn_type = getattr(conn, 'type', None)
            if not conn_type:
                if conn.__class__.__name__ == 'GetSqlConnection':
                    conn_type = 'Sql'
                elif conn.__class__.__name__ == 'GetSharePointConnection':
                    conn_type = 'SharePoint'
                elif conn.__class__.__name__ == 'GetExcelConnection':
                    conn_type = 'Excel'
                else:
                    conn_type = 'Unknown'
            join_tables = getattr(conn, 'join_tables', [])
            join_tables_str = ', '.join(join_tables) if join_tables else ''
            output.append([
                os.path.basename(getattr(conn, 'txt_file', '')),
                getattr(conn, 'source', None),
                getattr(conn, 'server', None),
                getattr(conn, 'database', None),
                getattr(conn, 'schema', None),
                getattr(conn, 'table', None),
                join_tables_str,
                conn_type,
                n_connessioni
            ])
        return output

    def __init__(self, root_path_excel: str, root_path_txt: str):
        self.excel_finder = ExcelFinder(root_path_excel)
        self.txt_finder = TxtFinder(root_path_txt)

    def _excel_file_list(self) -> list[str]:
        # Restituisce solo file .xls e .xlsm (e .xlsx se vuoi includerli)
        all_files = self.excel_finder.file_finder()
        filtered = [f for f in all_files if f.lower().endswith('.xls') or f.lower().endswith('.xlsm')]
        return filtered

    def split_excel_root_path(self) -> List[str]:
        excel_list = self._excel_file_list()
        
        excel_file_list = []

        for file in excel_list:
            path = file.split('\\')
            excel_file_list.append(['\\'.join(path[0:len(path)-1]),path[-1]])
        return excel_file_list   

    def _txt_file_list(self) -> list[str]:
        return self.txt_finder.file_finder()
    
    
    
    def _get_connection_info(self) -> List[IConnection]:
        txt_files = self._txt_file_list()
        results: list[IConnection] = []
        for file_path in txt_files:
            txt_splitter = TxtSplitLines(file_path)
            txt_splitter.get_txt_contents()
            source = txt_splitter.source
            if source is None:
                # Create an EmptyConnection object with error info
                conn = EmptyConnection(file_path, error='Sorgente non trovata')
                results.append(conn)
                continue
            if 'Sql.Database' in source:
                conn = GetSqlConnection(file_path)
                conn.get_connection()
                results.append(conn)
            else:
                conn = EmptyConnection(file_path, error=f"Get connection non ancora implementata per la sorgente: {source}")
                results.append(conn)
        return results
    
    def get_aggregated_info(self) -> List[list]:
        connection_info = self._get_connection_info()
        conn_count_map = {}
        for conn in connection_info:
            if hasattr(conn, 'txt_file'):
                base = os.path.basename(conn.txt_file)
                base = base.replace('.txt', '').replace('.xlsx', '')
                conn_count_map[base] = conn_count_map.get(base, 0) + 1
        output = []
        for conn in connection_info:
            if hasattr(conn, 'txt_file'):
                base = os.path.basename(conn.txt_file)
                base = base.replace('.txt', '').replace('.xlsx', '')
                n_connessioni = conn_count_map.get(base, 0)
            else:
                n_connessioni = 0
            conn_type = getattr(conn, 'type', None)
            if not conn_type:
                if conn.__class__.__name__ == 'GetSqlConnection':
                    conn_type = 'Sql'
                else:
                    conn_type = 'Unknown'
            join_tables = getattr(conn, 'join_tables', [])
            join_tables_str = ', '.join(join_tables) if join_tables else ''
            output.append([
                os.path.basename(getattr(conn, 'txt_file', '')),
                getattr(conn, 'source', None),
                getattr(conn, 'server', None),
                getattr(conn, 'database', None),
                getattr(conn, 'schema', None),
                getattr(conn, 'table', None),
                join_tables_str,
                conn_type,
                n_connessioni
            ])
        return output
