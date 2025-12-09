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

class BusinessLogic:

    def __init__(self, root_path_excel: str, root_path_txt: str):
        self.excel_finder = ExcelFinder(root_path_excel)
        self.txt_finder = TxtFinder(root_path_txt)

    def _excel_file_list(self) -> list[str]:
        return self.excel_finder.file_finder()

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
        for data in metadata:
            if data.nome_file:
                name_wo_extension = data.nome_file.replace('.xlsx', '')
            else:
                name_wo_extension = ''
            matched = False
            for conn in connection_info:
                if name_wo_extension in conn.txt_file:
                    # Usa l'attributo type se presente, altrimenti deduci dal nome classe
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
                    print_string.append([
                        data.nome_file,
                        data.creatore_file,
                        data.ultimo_modificatore,
                        data.data_creazione,
                        data.data_ultima_modifica,
                        data.collegamento_esterno,
                        getattr(conn, 'source', None),
                        getattr(conn, 'server', None),
                        getattr(conn, 'database', None),
                        getattr(conn, 'schema', None),
                        getattr(conn, 'table', None),
                        conn_type
                    ])
                    matched = True
            if not matched:
                print_string.append([
                    data.nome_file,
                    data.creatore_file,
                    data.ultimo_modificatore,
                    data.data_creazione,
                    data.data_ultima_modifica,
                    data.collegamento_esterno,
                    None,
                    None,
                    None,
                    None,
                    None,
                    'Unknown'  # Type sempre valorizzato
                ])
        return print_string
