import pandas as pd
import os
from openpyxl import load_workbook

class ExcelWriter:
    
    def __init__(self, folder_path, file_name):
        self.folder_path = folder_path
        self.file_name = file_name
        # Tracks whether the file has been initialized in the current run
        self._initialized = False

    def write_excel(self, columns, data, sheet_name='Sheet1'):
        # Ensure output directory exists
        if self.folder_path and not os.path.isdir(self.folder_path):
            os.makedirs(self.folder_path, exist_ok=True)

        output_path = os.path.join(self.folder_path, self.file_name)
        df = pd.DataFrame(data, columns=columns)

        # Overwrite file on first write of this instance; append thereafter
        if not self._initialized:
            # First write in this run: start fresh (overwrite file)
            mode = 'w'
            # If a previous file exists, starting with 'w' will overwrite it
            self._initialized = True
        else:
            # Subsequent writes in the same run: append sheets
            mode = 'a'

        writer_kwargs = dict(engine='openpyxl', mode=mode)
        if mode == 'a':
            writer_kwargs['if_sheet_exists'] = 'replace'

        with pd.ExcelWriter(output_path, **writer_kwargs) as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)

        

    