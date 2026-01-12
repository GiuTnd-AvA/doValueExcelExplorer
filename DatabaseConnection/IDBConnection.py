
from abc import ABC, abstractmethod
import pyodbc
from typing import List

# Opzioni di cifratura/Trust. Regola se necessario.
ODBC_ENCRYPT_OPTS: str = "Encrypt=no;TrustServerCertificate=yes;"

class IDBConnection(ABC):
    DRIVER: str  # Contratto: ogni sottoclasse può specificare un driver preferito

    def __init__(self, server, database, schema=None, table=None):
        self.server = server
        self.database = database
        self.schema = schema
        self.table = table

    def _candidate_drivers(self) -> List[str]:
        try:
            installed = [d for d in pyodbc.drivers() if "sql server" in d.lower()]
        except Exception:
            installed = []
        preferred = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server",
            "SQL Server Native Client 11.0",
            "ODBC Driver 13 for SQL Server",
            "ODBC Driver 11 for SQL Server",
            self.DRIVER if getattr(self, "DRIVER", None) else "",
        ]
        preferred = [d for d in preferred if d]
        ordered = [d for d in preferred if d in installed]
        ordered += [d for d in installed if d not in ordered]
        if not ordered:
            ordered = preferred or installed
        return ordered

    def _connect(self):
        last_error = None
        for drv in self._candidate_drivers():
            try:
                enc_opts = ODBC_ENCRYPT_OPTS
                if drv.lower().strip() == "sql server":
                    enc_opts = ""
                # Valida il driver contro master per evitare 4060 sul DB target
                test_conn_str = f"DRIVER={{{drv}}};SERVER={self.server};DATABASE=master;Trusted_Connection=yes;" + enc_opts
                tconn = pyodbc.connect(test_conn_str, timeout=3)
                tconn.close()
                # Connessione sul DB target
                conn_str = f"DRIVER={{{drv}}};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes;" + enc_opts
                self.connection = pyodbc.connect(conn_str)
                return self.connection
            except Exception as e:
                last_error = e
                continue
        raise RuntimeError(f"Connessione a {self.server}/{self.database} fallita. Ultimo errore: {last_error}")


    
