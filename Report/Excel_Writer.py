import pandas as pd
import os
from openpyxl import load_workbook
import re

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

        # Sanitize DataFrame to remove characters illegal for openpyxl
        # Remove ASCII control chars except tab(\x09), newline(\x0A), carriage return(\x0D)
        illegal_chars = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")

        def _clean(val):
            if isinstance(val, str):
                return illegal_chars.sub("", val)
            return val

        # Pandas deprecates DataFrame.applymap; prefer DataFrame.map for element-wise ops
        df = df.map(_clean)

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

        # Also ensure sheet_name is clean and within Excel limits
        clean_sheet_name = illegal_chars.sub("", sheet_name)[:31] or "Sheet1"

        with pd.ExcelWriter(output_path, **writer_kwargs) as writer:
            df.to_excel(writer, index=False, sheet_name=clean_sheet_name)