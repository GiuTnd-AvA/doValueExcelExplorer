from Connection.IConnection import IConnection
import re

class GetSharePointConnection(IConnection):
    def __init__(self, txt_file):
        super().__init__(txt_file)

    def get_connection(self):
        try:
            with open(self.txt_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except Exception as e:
            raise Exception(f"Errore apertura file: {e}")

        # Cerca la riga Source
        for l in lines:
            l_strip = l.strip()
            if l_strip.lower().startswith('source ='):
                self.source = l_strip
                # Estrai URL SharePoint
                m = re.search(r'SharePoint\.Files\("([^"]+)"', l_strip)
                if m:
                    url = m.group(1)
                    # Server: prendi il nome del sito SharePoint
                    server_match = re.search(r'https://[^/]+/sites/([^/]+)/', url)
                    if server_match:
                        self.server = server_match.group(1)
                break

        # Cerca la riga con Table.SelectRows per il Database (nome file)
        for l in lines:
            l_strip = l.strip()
            db_match = re.search(r'Table\.SelectRows\(.*\[Name\]\s*=\s*"([^"]+)"', l_strip)
            if db_match:
                self.database = db_match.group(1)
                break

        # Cerca la riga con Kind e Item per Schema e Table
        for l in lines:
            l_strip = l.strip()
            kind_match = re.search(r'Kind\s*=\s*"([^"]+)"', l_strip)
            item_match = re.search(r'Item\s*=\s*"([^"]+)"', l_strip)
            if kind_match:
                self.schema = kind_match.group(1)
            if item_match:
                self.table = item_match.group(1)
            if kind_match or item_match:
                break
        # Imposta il tipo
        self.type = 'SharePoint'