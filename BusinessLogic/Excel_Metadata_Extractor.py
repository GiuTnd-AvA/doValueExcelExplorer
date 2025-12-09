import os
import zipfile
from openpyxl import load_workbook

class ExcelMetadataExtractor():

    def __init__(self, file_path):
        self.file_path = file_path
        self.nome_file = None
        self.creatore_file = None
        self.ultimo_modificatore = None
        self.data_creazione = None
        self.data_ultima_modifica = None
        self.collegamento_esterno = None

    def get_metadata(self, percorso):
        nome_file = os.path.basename(percorso)
        creator = last_modified_by = created = modified = connessione_esterna = None
        try:
            wb = load_workbook(percorso, read_only=True, data_only=True)
            props = wb.properties
            creator = props.creator
            last_modified_by = props.lastModifiedBy
            created = props.created
            modified = props.modified
        except Exception as e:
            creator = last_modified_by = created = modified = f"Errore: {e}"
        try:
            with zipfile.ZipFile(percorso) as zF:
                connessione_esterna = 'Si' if "xl/connections.xml" in zF.namelist() else 'No'
        except Exception as e:
            connessione_esterna = f"Errore: {e}"
        # Popola gli attributi della classe
        self.nome_file = nome_file
        self.creatore_file = creator
        self.ultimo_modificatore = last_modified_by
        self.data_creazione = created
        self.data_ultima_modifica = modified
        self.collegamento_esterno = connessione_esterna