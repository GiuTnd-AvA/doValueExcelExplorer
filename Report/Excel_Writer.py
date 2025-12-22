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
        # Resolved output path to keep sheets in the same file across writes
        self._resolved_output_path = None

    def write_excel(self, columns, data, sheet_name='Sheet1'):
        # Ensure output directory exists
        if self.folder_path and not os.path.isdir(self.folder_path):
            os.makedirs(self.folder_path, exist_ok=True)

        base_output_path = os.path.join(self.folder_path, self.file_name)
        if self._resolved_output_path is None:
            self._resolved_output_path = base_output_path
        output_path = self._resolved_output_path
        df = pd.DataFrame(data, columns=columns)

        # Sanitize DataFrame to remove characters illegal for openpyxl
        # Remove ASCII control chars except tab(\x09), newline(\x0A), carriage return(\x0D)
        illegal_chars = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")

        def _clean(val):
            if isinstance(val, str):
                return illegal_chars.sub("", val)
            return val

        # Clean each cell value (applymap is supported for DataFrame element-wise ops)
        df = df.applymap(_clean)

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

        try:
            with pd.ExcelWriter(output_path, **writer_kwargs) as writer:
                df.to_excel(writer, index=False, sheet_name=clean_sheet_name)
        except PermissionError:
            # If the target file is locked (e.g., opened in Excel), fall back to a timestamped filename
            import datetime
            ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            alt_name = os.path.splitext(self.file_name)[0] + f"_{ts}" + os.path.splitext(self.file_name)[1]
            self._resolved_output_path = os.path.join(self.folder_path, alt_name)
            output_path = self._resolved_output_path
            # On first write, we still want 'w'; for subsequent writes, keep 'a'
            # Always start a new file in write mode for the fallback path
            writer_kwargs = dict(engine='openpyxl', mode='w')
            with pd.ExcelWriter(output_path, **writer_kwargs) as writer:
                df.to_excel(writer, index=False, sheet_name=clean_sheet_name)
            # Mark as initialized so subsequent writes append to the same file
            self._initialized = True