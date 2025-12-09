class TxtSplitLines:

    
    def __init__(self, txt_file):
        self.txt_file = txt_file
        self.source = None

    def get_txt_contents(self):
        source_line = None
        try:
            with open(self.txt_file, 'r', encoding='utf-8') as file:
                for l in file:
                    l_strip = l.strip()
                    if (
                        'Sql.Database' in l_strip or
                        'SharePoint.Files' in l_strip or
                        'SharePoint.Contents' in l_strip or
                        'SharePoint.Tables' in l_strip or
                        'Excel.Workbook' in l_strip
                    ):
                        source_line = l_strip
                        self.source = source_line
                        break
        except Exception as e:
            source_line = f"Errore: {e}"
            self.source = source_line