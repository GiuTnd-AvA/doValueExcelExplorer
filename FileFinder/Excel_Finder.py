import os
from FileFinder.IFinder import IFinder

class ExcelFinder(IFinder):
    EXTENSIONS = ['.xls', '.xlsm', '.xlsx', '.xlsb']

    def file_finder(self) -> list[str]:
        found_files: list[str] = []
        try:
            for root, dirs, files in os.walk(self.root_path):
                for f in files:
                    if any(f.endswith(ext) for ext in self.EXTENSIONS) and not f.startswith("~$"):
                        full_path = os.path.join(root, f)
                        found_files.append(full_path)
        except Exception as e:
            print(f"Errore nella ricerca dei file Excel: {e}")
        return found_files
