from Finder.Excel_Finder import ExcelFinder
from Finder.TXT_Finder import TxtFinder
from .Excel_Metadata_Extractor import ExcelMetadataExtractor
from .Txt_Source_Lines import TxtSplitLines
from Connection.IConnection import IConnection
from Connection.Get_SQL_Connection import GetSqlConnection
from Connection.Get_SharePoint_Connection import GetSharePointConnection
from Connection.Get_Excel_Connection import GetExcelConnection
from Connection.IConnection import EmptyConnection
from Connection.Connessione_Senza_Txt import ConnessioniSenzaTxt
from Connection.Get_Xml_Connection import GetXmlConnection
from typing import List
import os

class BusinessLogic:

    def __init__(self, root_path_excel: str, root_path_txt: str):
        self.excel_finder = ExcelFinder(root_path_excel)
        self.txt_finder = TxtFinder(root_path_txt)

    def _excel_file_list(self) -> list[str]:
        return self.excel_finder.file_finder()

    def get_excel_file_paths(self) -> list[str]:
        # Accessor pubblico per i percorsi completi dei file Excel
        return self._excel_file_list()

    def split_excel_root_path(self) -> List[str]:
        excel_list = self._excel_file_list()
        
        excel_file_list = []

        for file in excel_list:
            path = file.split('\\')
            excel_file_list.append(['\\'.join(path[0:len(path)-1]),path[-1]])
        return excel_file_list   

    def _txt_file_list(self) -> list[str]:
        return self.txt_finder.file_finder()
    
    def _excel_metadata_for_files(self, excel_files: list[str]) -> list[ExcelMetadataExtractor]:
        total = len(excel_files)
        metadata_list = []
        for idx, file_path in enumerate(excel_files, start=1):
            print(f"[Excel] Elaborazione file {idx}/{total}: {file_path}")
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
    
    def get_excel_connections_without_txt_for_files(self, excel_files: list[str]) -> List[list]:
        connections = []
        total = len(excel_files)
        for idx, file_path in enumerate(excel_files, start=1):
            # Pre-check: salta file senza connections.xml
            meta = ExcelMetadataExtractor(file_path)
            meta.get_metadata(file_path)
            if meta.collegamento_esterno != 'Si':
                # opzionale: log leggero di skip
                # print(f"[Connessioni] Skip {idx}/{total}: nessuna connections.xml -> {file_path}")
                continue
            print(f"[Connessioni] Elaborazione file {idx}/{total}: {file_path}")
            conn = ConnessioniSenzaTxt(file_path)
            conn_list = conn.estrai_connessioni()
            for info in conn_list:
                connections.append([
                    conn.file_name,
                    info.get('Server'),
                    info.get('Database'),
                    info.get('Schema'),
                    info.get('Tabella')
                ])
        return connections


    def get_aggregated_info_for_files(self, excel_files: list[str]) -> List[list]:
        metadata = self._excel_metadata_for_files(excel_files)
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
    
    def connessioni_xml(self, excel_files: list[str]) -> List[list]:
        metadata_list = self._excel_metadata_for_files(excel_files)
        connessioni_xml = []
        total = len(metadata_list)
        for idx, meta in enumerate(metadata_list, start=1):
            if meta.collegamento_esterno != 'Si':
                continue
            xml = GetXmlConnection(meta.file_path)
            infos = xml.extract_connection_info()
            if not infos:
                print(f"[Connessioni] Nessuna connessione rilevata: {meta.file_path}")
            for info in infos:
                server = info.get('Server')
                database = info.get('Database')
                schema = info.get('Schema')
                table = info.get('Tabella')
                # Skip completely empty/placeholder entries
                if not any([server, database, schema, table]):
                    continue
                row = [xml.file_name, server, database, schema, table]
                print("\n"+str(row)+"\n")
                connessioni_xml.append(row)
            print(f"[Connessioni] Elaborazione file {idx}/{total}: {meta.file_path}")
        return connessioni_xml
