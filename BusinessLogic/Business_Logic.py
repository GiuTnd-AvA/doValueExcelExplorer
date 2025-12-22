from Finder.Excel_Finder import ExcelFinder
from Finder.TXT_Finder import TxtFinder
from .Excel_Metadata_Extractor import ExcelMetadataExtractor
from .Txt_Source_Lines import TxtSplitLines
from Connection.IConnection import IConnection
from Connection.Get_SQL_Connection import GetSqlConnection
from Connection.Get_SharePoint_Connection import GetSharePointConnection
from Connection.Get_Excel_Connection import GetExcelConnection
from Connection.IConnection import EmptyConnection
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
    
    def _excel_metadata(self) -> list[ExcelMetadataExtractor]:
        excel_files = self._excel_file_list()
        metadata_list = []
        for file_path in excel_files:
            extractor = ExcelMetadataExtractor(file_path)
            extractor.get_metadata(file_path)
            metadata_list.append(extractor)
        return metadata_list
    
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
            elif any(s in source for s in ['SharePoint.Files', 'SharePoint.Contents', 'SharePoint.Tables']):
                conn = GetSharePointConnection(file_path)
                conn.get_connection()
                results.append(conn)
            elif 'Excel.Workbook' in source:
                print("Trovata sorgente Excel")
                conn = GetExcelConnection(file_path)  # Da implementare
                conn.get_connection()
                results.append(conn)
            else:
                conn = EmptyConnection(file_path, error=f"Get connection non ancora implementata per la sorgente: {source}")
                results.append(conn)
        return results
    
    def get_aggregated_info(self) -> List[list]:
        metadata = self._excel_metadata()
        connection_info = self._get_connection_info()
        print_string = []
        # Mappa file excel base name (senza estensione) -> numero connessioni PQ
        conn_count_map = {}
        for conn in connection_info:
            if hasattr(conn, 'txt_file'):
                base = os.path.basename(conn.txt_file)
                base = base.replace('.txt', '').replace('.xlsx', '')
                conn_count_map[base] = conn_count_map.get(base, 0) + 1

        for conn in connection_info:
            # Trova la corrispondenza con il file Excel
            excel_file_name = None
            for data in metadata:
                # Match su .xls, .xlsm, .xlsx, .xlsb senza estensione
                if hasattr(data, 'file_path') and data.nome_file:
                    base_excel = os.path.splitext(data.nome_file)[0]
                    if base_excel in conn.txt_file:
                        excel_file_name = data.file_path
                        creatore_file = data.creatore_file
                        ultimo_modificatore = data.ultimo_modificatore
                        data_creazione = data.data_creazione
                        data_ultima_modifica = data.data_ultima_modifica
                        collegamento_esterno = data.collegamento_esterno
                        break
            else:
                excel_file_name = os.path.basename(getattr(conn, 'txt_file', ''))
                creatore_file = None
                ultimo_modificatore = None
                data_creazione = None
                data_ultima_modifica = None
                collegamento_esterno = None

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

            # Tabella principale (FROM)
            main_table = getattr(conn, 'table', None)
            # Tutte le tabelle di JOIN
            join_tables = getattr(conn, 'join_tables', [])
            join_tables_str = ', '.join(join_tables) if join_tables else ''

            file_path_full = getattr(conn, 'txt_file', '')
            file_name_only = os.path.basename(file_path_full)
            # Trova il file xlsx di origine associato al txt
            xlsx_file = None
            for data in metadata:
                if hasattr(data, 'file_path') and data.nome_file:
                    base_excel = os.path.splitext(data.nome_file)[0]
                    if base_excel in file_name_only:
                        xlsx_file = data.file_path
                        break
            print_string.append([
                file_path_full,
                xlsx_file,
                creatore_file,
                ultimo_modificatore,
                data_creazione,
                data_ultima_modifica,
                collegamento_esterno,
                getattr(conn, 'source', None),
                getattr(conn, 'server', None),
                getattr(conn, 'database', None),
                getattr(conn, 'schema', None),
                main_table,
                join_tables_str,
                conn_type,
                conn_count_map.get(file_name_only.replace('.txt', '').replace('.xlsx', ''), 0)
            ])
        return print_string
