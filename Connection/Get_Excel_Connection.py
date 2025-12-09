from Connection.IConnection import IConnection


class GetExcelConnection(IConnection):
    
    def __init__(self, txt_file):
        super().__init__(txt_file)


    def get_connection(self):
        # Implementazione specifica per l'estrazione delle informazioni di connessione da file Excel
        pass