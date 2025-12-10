import pandas as pd
import os

class ExcelWriter:
    
    def __init__(self, folder_path, file_name):
        self.folder_path = folder_path
        self.file_name = file_name

    def write_excel(self, columns, data, sheet_name='Sheet1'):
        output_path = os.path.join(self.folder_path, self.file_name)
        df = pd.DataFrame(data, columns=columns)
        df.to_excel(output_path, index=False, sheet_name=sheet_name)

        

    