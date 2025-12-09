
from abc import ABC, abstractmethod
import pyodbc

class IConnection(ABC):
    DRIVER: str  # Contratto: ogni sottoclasse deve specificare il driver

    def __init__(self, server, database, schema=None, table=None):
        self.server = server
        self.database = database
        self.schema = schema
        self.table = table

    def _connect(self):
        conn_str = f"DRIVER={self.DRIVER};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes;"
        self.connection = pyodbc.connect(conn_str)
        return self.connection


    
