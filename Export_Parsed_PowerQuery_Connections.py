import argparse
import os
from typing import List, Tuple
from openpyxl import load_workbook, Workbook
from openpyxl.utils import get_column_letter
from BusinessLogic.PowerQuerySourceConnectionParser import PowerQuerySourceConnectionParser


def read_sources_from_excel(input_path: str) -> List[Tuple[str, str, str]]:
    """
    Returns a list of tuples (path, file, source_line) from the report produced by Export_PowerQuery_Sources.py
    Expected headers: Path | File | Source
    """
    wb = load_workbook(input_path)
    ws = wb.active

    headers = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
    header_index = {h: i for i, h in enumerate(headers)}
    required = ["Path", "File", "Source"]
    for r in required:
        if r not in header_index:
            raise ValueError(f"Input Excel missing required column '{r}'. Found: {headers}")

    rows: List[Tuple[str, str, str]] = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        path = row[header_index["Path"]]
        file = row[header_index["File"]]
        source = row[header_index["Source"]]
        if source:
            rows.append((path or "", file or "", source))
    return rows


def write_parsed_excel(entries: List[Tuple[str, str, str, str, str]], output_path: str) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "ParsedConnections"

    headers = ["File", "Server", "Database", "Schema", "Table"]
    ws.append(headers)

    for file, server, database, schema, table in entries:
        ws.append([file, server, database, schema, table])

    # Simple sizing
    widths = [40, 20, 20, 16, 48]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    wb.save(output_path)


def main():
    ap = argparse.ArgumentParser(description="Parse PowerQuery Source lines into connection parts")
    ap.add_argument("--in", dest="input", default=os.path.join("Report", "PowerQuery_Sources_Report.xlsx"),
                    help="Path to the input Excel with Source lines (default: Report/PowerQuery_Sources_Report.xlsx)")
    ap.add_argument("--out", dest="output", default=os.path.join("Report", "PowerQuery_Parsed_Connections.xlsx"),
                    help="Output path for the parsed connections Excel")
    args = ap.parse_args()

    rows = read_sources_from_excel(args.input)
    parser = PowerQuerySourceConnectionParser()

    parsed: List[Tuple[str, str, str, str, str]] = []
    for _, file, source in rows:
        info = parser.parse(source)
        parsed.append((file, info.get("server"), info.get("database"), info.get("schema"), info.get("table")))

    write_parsed_excel(parsed, args.output)
    print(f"Parsed {len(parsed)} entries. Report written: {os.path.abspath(args.output)}")


if __name__ == "__main__":
    main()
