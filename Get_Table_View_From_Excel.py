"""
Compat wrapper: exposes class TableViewExtractor and script name
aligned with user's request, delegating to Get_Table_Views_From_Excel.
"""

from Get_Table_Views_From_Excel import (
    TableViewsExtractor as TableViewExtractor,
    INPUT_EXCEL_PATH,
    OUTPUT_EXCEL_PATH,
)


if __name__ == "__main__":
    if not INPUT_EXCEL_PATH:
        raise SystemExit("Imposta INPUT_EXCEL_PATH a inizio file.")
    extractor = TableViewExtractor(INPUT_EXCEL_PATH, OUTPUT_EXCEL_PATH or "")
    out_path = extractor.run()
    print(f"Viste scritte in: {out_path}")
