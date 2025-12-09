from abc import ABC, abstractmethod

class IConnection(ABC):
    
    def __init__(self, txt_file):
        self.txt_file = txt_file
        self.source = None
        self.server = None
        self.database = None
        self.schema = None
        self.table = None

    @abstractmethod
    def get_connection(self):
        """
        Deve essere implementato dalle sottoclassi per restituire una connessione basata sul file di testo.
        """
        pass
