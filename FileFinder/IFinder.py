from abc import ABC, abstractmethod
import os

class IFinder(ABC):
    EXTENSION: str #Contratto: ogni sottoclasse deve specificare un'estensione di file

    def __init__(self, root_path: str):
        self.root_path = root_path

    def file_finder(self) -> list[str]:
        """Ritorna una lista di percorsi completi per l'estensione definita dalla sottoclasse."""
        if not hasattr(self, "EXTENSION"):
            raise NotImplementedError("La sottoclasse deve definire EXTENSION.")
        found_files: list[str] = []
        try:
            for root, dirs, files in os.walk(self.root_path):
                for f in files:
                    if f.endswith(self.EXTENSION) and not f.startswith("~$"):
                        full_path = os.path.join(root, f)
                        found_files.append(full_path)
        except Exception as e:
            print(f"Errore nella ricerca dei file {self.EXTENSION}: {e}")
        return found_files
