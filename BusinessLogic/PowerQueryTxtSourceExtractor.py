import os
import re
from typing import List, Optional, Tuple

try:
    from openpyxl import Workbook
    from openpyxl.styles import Alignment
except ImportError:  # pragma: no cover
    Workbook = None  # type: ignore
    Alignment = None  # type: ignore


class PowerQueryTxtSourceExtractor:
    """
    Scans a root directory recursively, finds .txt files that contain exported
    Power Query M code, extracts the first complete 'Source =' line, and can
    write results to an Excel report.
    """

    # Match any line containing 'Source =', even if preceded by 'let' or whitespace
    SOURCE_PATTERN = re.compile(r"^.*\bSource\s*=.*$", re.MULTILINE)

    def __init__(self, root_dir: str) -> None:
        self.root_dir = root_dir
        self.rows: List[Tuple[str, str]] = []  # (file_name, source_line)

    @staticmethod
    def _read_text_best_effort(path: str) -> Optional[str]:
        """Read text with common encodings, returning None on failure."""
        for enc in ("utf-8", "utf-16", "latin-1", "cp1252"):
            try:
                with open(path, "r", encoding=enc, errors="strict") as f:
                    return f.read()
            except Exception:
                continue
        return None

    @classmethod
    def extract_source_line(cls, content: str) -> Optional[str]:
        """Return the first line containing 'Source =' from content, if any."""
        m = cls.SOURCE_PATTERN.search(content)
        if not m:
            return None
        line = m.group(0).strip()
        return line

    def scan(self, verbose: bool = True) -> List[Tuple[str, str]]:
        """Populate self.rows with (file_name, source_line) for each .txt file.
        Returns the rows list.
        """
        self.rows.clear()

        # First pass: count total .txt files
        total_txt = 0
        for dirpath, _, filenames in os.walk(self.root_dir):
            total_txt += sum(1 for fn in filenames if fn.lower().endswith(".txt"))
        if verbose:
            print(f"Total .txt files to process: {total_txt}")

        processed = 0
        found = 0
        for dirpath, _, filenames in os.walk(self.root_dir):
            if verbose:
                print(f"Scanning folder: {dirpath}")
            for fname in filenames:
                if not fname.lower().endswith(".txt"):
                    continue
                processed += 1
                full_path = os.path.join(dirpath, fname)
                if verbose:
                    print(f"[{processed}/{total_txt}] Processing: {fname}")

                content = self._read_text_best_effort(full_path)
                if content is None:
                    if verbose:
                        print(f"WARN: Could not read file: {full_path}")
                    continue

                src_line = self.extract_source_line(content)
                if src_line:
                    self.rows.append((fname, src_line))
                    found += 1

        if verbose:
            print(f"Scan complete: processed {processed}/{total_txt} .txt files, found {found} with 'Source ='.")
        return self.rows

    def write_report(self, output_path: str) -> str:
        if Workbook is None:
            raise RuntimeError("openpyxl not installed; cannot write Excel report.")

        wb = Workbook()
        ws = wb.active
        ws.title = "PowerQuery Sources"
        ws.cell(row=1, column=1, value="File")
        ws.cell(row=1, column=2, value="Source")

        wrap = Alignment(wrap_text=True, vertical="top")
        max_cell_len = 32767

        for idx, (fname, source) in enumerate(self.rows, start=2):
            val = source if len(source) <= max_cell_len else (source[:max_cell_len - 15] + "... [TRUNCATED]")
            ws.cell(row=idx, column=1, value=fname)
            c2 = ws.cell(row=idx, column=2, value=val)
            c2.alignment = wrap

        # Set widths
        ws.column_dimensions['A'].width = 50
        ws.column_dimensions['B'].width = 120

        # Ensure directory exists
        out_dir = os.path.dirname(output_path)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)

        wb.save(output_path)
        return os.path.abspath(output_path)
