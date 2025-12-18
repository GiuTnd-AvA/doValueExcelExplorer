import argparse
import os
import sys
import time

from BusinessLogic.PowerQueryTxtSourceExtractor import PowerQueryTxtSourceExtractor

DEFAULT_ROOT = r"C:\\Users\\giuseppe.tanda\\Desktop\\doValue\\Report excel"
DEFAULT_OUT = "Report/PowerQuery_Sources_Report.xlsx"


def main():
    parser = argparse.ArgumentParser(description="Scan .txt files for Power Query 'Source =' lines and export to Excel.")
    parser.add_argument("root", nargs="?", default=DEFAULT_ROOT, help="Root folder to scan")
    parser.add_argument("--out", default=DEFAULT_OUT, help="Output Excel file path")
    parser.add_argument("--quiet", action="store_true", help="Reduce logging output")
    args = parser.parse_args()

    root = args.root
    out = args.out

    if not os.path.exists(root):
        print(f"Root path does not exist: {root}")
        sys.exit(2)

    extractor = PowerQueryTxtSourceExtractor(root)
    print(f"Scanning for .txt under: {root}")
    start = time.time()
    rows = extractor.scan(verbose=not args.quiet)
    elapsed = time.time() - start
    print(f"Found {len(rows)} files containing 'Source =' (elapsed {elapsed:.1f}s)")

    out_abs = extractor.write_report(out)
    print(f"Report written: {out_abs}")


if __name__ == "__main__":
    main()
