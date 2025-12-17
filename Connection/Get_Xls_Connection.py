from Connection.IConnection import IConnection
# Import sicuro di olefile a livello di modulo
try:
    import olefile
except Exception:
    olefile = None

class GetXlsConnection(IConnection):

    def __init__(self, txt_file: str):
        super().__init__(txt_file)
        # Per coerenza con gli altri parser
        self.type = 'Excel'

    def get_connection(self):
        """
        Estrae le informazioni di connessione da un file .xls utilizzando il modulo olefile.
        Non solleva eccezioni: in caso di errore stampa un messaggio in console e restituisce self.
        Imposta i campi: source, server, database, schema, table se rilevati.
        """
        if olefile is None:
            print(f"[Excel XLS] Modulo 'olefile' non disponibile: impossibile analizzare {self.txt_file}")
            return self
        
        ole = None
        try:
            # Verifica che il file sia un Compound File Binary (OLE2)
            if not olefile.isOleFile(self.txt_file):
                print(f"[Excel XLS] File non OLE2/Compound: {self.txt_file}")
                return self

            ole = olefile.OleFileIO(self.txt_file)
            # Elenco dei flussi; alcune versioni di olefile accettano parametri, gestiamo entrambe
            try:
                streams = ole.listdir()
            except Exception:
                streams = []

            # Ricerca euristica di connection string nei vari stream
            import re
            patterns = [
                re.compile(rb"(Data\s*Source|Server)\s*=\s*([^;\r\n]+)", re.IGNORECASE),
                re.compile(rb"(Initial\s*Catalog|Database)\s*=\s*([^;\r\n]+)", re.IGNORECASE),
                re.compile(rb"DSN\s*=\s*([^;\r\n]+)", re.IGNORECASE),
                re.compile(rb"Schema\s*=\s*([^;\r\n]+)", re.IGNORECASE),
                re.compile(rb"(Table|TABLENAME)\s*=\s*([^;\r\n]+)", re.IGNORECASE),
            ]

            found_any = False
            for path in streams:
                # path è una tupla di componenti (es. ('Workbook',))
                try:
                    data = ole.openstream(path).read()
                except Exception:
                    continue

                server = database = schema = table = None

                # Cerca pattern di connessione in raw bytes
                for pat in patterns:
                    for m in pat.finditer(data):
                        # Alcuni pattern hanno due gruppi (chiave, valore), altri solo valore
                        try:
                            if m.lastindex and m.lastindex >= 2:
                                key = m.group(1).decode(errors='ignore').lower()
                                val = m.group(2).decode(errors='ignore').strip()
                            else:
                                key = 'dsn'
                                val = m.group(1).decode(errors='ignore').strip()
                        except Exception:
                            continue

                        if 'server' in key or 'data source' in key:
                            server = val
                        elif 'database' in key or 'initial catalog' in key:
                            database = val
                        elif 'schema' in key:
                            schema = val
                        elif 'table' in key:
                            table = val
                        elif key == 'dsn' and not server:
                            # DSN presente: lo salviamo come server/alias
                            server = val

                if any([server, database, schema, table]):
                    self.source = 'Excel.Workbook'
                    self.server = server
                    self.database = database
                    self.schema = schema
                    self.table = table
                    found_any = True
                    break

            if not found_any:
                print(f"[Excel XLS] Nessuna connessione rilevata in: {self.txt_file}")

            return self

        except Exception as e:
            print(f"[Excel XLS] Errore durante l'analisi di {self.txt_file}: {e}")
            return self
        finally:
            try:
                if ole is not None:
                    ole.close()
            except Exception:
                pass


class GetExcelConnection(IConnection):
    """
    Classe placeholder per compatibilità con BusinessLogic._get_connection_info.
    La logica di estrazione per .xlsx è gestita altrove (XML/ConnessioniSenzaTxt).
    """
    def __init__(self, txt_file: str):
        super().__init__(txt_file)
        self.type = 'Excel'

    def get_connection(self):
        return self
