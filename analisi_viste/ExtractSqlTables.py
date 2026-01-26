# -------------------------------------------------------------------
# Scopo: prende un file .sql e lo legge per estrarre tutte le tabelle 
# e la clausola con cui viene chiamata
# -------------------------------------------------------------------

import argparse
import os
import re
import csv
import datetime
from typing import List, Dict, Tuple

try:
    import openpyxl
except ImportError:  # Fallback if not installed
    openpyxl = None

# User-configurable default input path. If you don't pass the positional
# argument, the script will use this path.
# Update this value to your .sql file path.
INPUT_SQL = r"c:/Users/giuseppe.tanda/Desktop/doValue/OneDrive_1_12-30-2025/Append SQL script.sql"

# Identifier: [name] or "name" or `name` or simple names, including temp tables #/##
IDENTIFIER = r'(?:\[[^\]]+\]|"[^"]+"|`[^`]+`|[#]{1,2}[A-Za-z_][A-Za-z0-9_$]*|[A-Za-z_][A-Za-z0-9_$]*)'
# Qualified names support:
# - table
# - schema.table
# - db.schema.table
# - db..table   (missing schema -> default to dbo)
# The regex below allows 1 to 4-part names, with support for an empty schema
# via a double dot (e.g., db..table or server.db..table). We achieve this by
# allowing up to three repeatable dot-segments where each segment is either an
# identifier or an empty placeholder detected via lookahead for the next dot.
QUALIFIED = rf'{IDENTIFIER}(?:\s*\.\s*(?:{IDENTIFIER}|(?=\s*\.))){{0,3}}'

# Detect patterns with missing schema to normalize to dbo
EMPTY_SCHEMA_3 = re.compile(rf'^(?P<db>{IDENTIFIER})\s*\.\s*\.\s*(?P<table>{IDENTIFIER})$', re.IGNORECASE)
EMPTY_SCHEMA_4 = re.compile(rf'^(?P<server>{IDENTIFIER})\s*\.\s*(?P<db>{IDENTIFIER})\s*\.\s*\.\s*(?P<table>{IDENTIFIER})$', re.IGNORECASE)

# CTE detection: capture names defined via WITH ... AS (...), including comma-separated CTEs
CTE_FIRST_PATTERN = re.compile(rf"\bWITH\s+(?P<name>{IDENTIFIER})\s*(?:\([^)]*\))?\s+AS\b", re.IGNORECASE|re.DOTALL)
CTE_NEXT_PATTERN = re.compile(rf",\s*(?P<name>{IDENTIFIER})\s*(?:\([^)]*\))?\s+AS\b", re.IGNORECASE|re.DOTALL)

def _strip_delimiters(s: str) -> str:
    s = s.strip()
    if s.startswith('[') and s.endswith(']'):
        s = s[1:-1]
    elif s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    elif s.startswith('`') and s.endswith('`'):
        s = s[1:-1]
    return s

def _last_segment(qualified: str) -> str:
    parts = [p.strip() for p in qualified.split('.')]
    if not parts:
        return _strip_delimiters(qualified)
    return _strip_delimiters(parts[-1])

def _is_temp_table(qualified: str) -> bool:
    last = _last_segment(qualified)
    # Treat temporary tables (#, ##) and table variables (@) as non-persistent
    # objects to ignore in extraction results.
    return last.startswith('#') or last.startswith('@')

def _extract_cte_names(text: str) -> set:
    names = set()
    for m in CTE_FIRST_PATTERN.finditer(text):
        names.add(_strip_delimiters(m.group('name')))
        # Scan forward after a WITH for subsequent comma-separated CTE names
        tail = text[m.end():]
        for n in CTE_NEXT_PATTERN.finditer(tail):
            names.add(_strip_delimiters(n.group('name')))
    return {n.lower() for n in names}

# Alias detection: FROM/JOIN <table> [AS] <alias>
ALIAS_PATTERN = re.compile(
    rf"(?:(?:\bFROM\b)|(?:\bJOIN\b))\s+(?:\(\s*(?!SELECT\b|WITH\b|VALUES\b))?(?P<table>{QUALIFIED})\s+(?:AS\s+)?(?P<alias>{IDENTIFIER})\b",
    re.IGNORECASE|re.DOTALL,
)

def _extract_alias_map(text: str) -> Dict[str, str]:
    alias_map: Dict[str, str] = {}
    for m in ALIAS_PATTERN.finditer(text):
        base = m.group('table').strip()
        # Normalize missing schema
        em3 = EMPTY_SCHEMA_3.match(base)
        em4 = EMPTY_SCHEMA_4.match(base)
        if em4:
            base = f"{em4.group('server')}.{em4.group('db')}.dbo.{em4.group('table')}"
        elif em3:
            base = f"{em3.group('db')}.dbo.{em3.group('table')}"
        alias = _strip_delimiters(m.group('alias')).lower()
        # Ignore temp/variable aliases (rare) and CTE aliases
        if alias.startswith('#') or alias.startswith('@'):
            continue
        alias_map[alias] = base
    return alias_map

def _strip_sql_comments(text: str) -> str:
    """Remove T-SQL style comments: line comments (--) and block comments (/* ... */).
    Keeps content otherwise unchanged. This is a best-effort stripper and does not
    handle comment markers inside quoted strings.
    """
    # Remove block comments
    text = re.sub(r"/\*[\s\S]*?\*/", " ", text)
    # Remove line comments (from -- to end of line)
    text = re.sub(r"(?m)--.*$", "", text)
    return text

CLAUSE_PATTERNS: List[Tuple[str, re.Pattern]] = [
    ("DROP TABLE",     re.compile(rf"\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?P<table>{QUALIFIED})", re.IGNORECASE|re.DOTALL)),
    ("TRUNCATE TABLE", re.compile(rf"\bTRUNCATE\s+TABLE\s+(?P<table>{QUALIFIED})", re.IGNORECASE|re.DOTALL)),
    ("CREATE TABLE",   re.compile(rf"\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<table>{QUALIFIED})", re.IGNORECASE|re.DOTALL)),
    ("ALTER TABLE",    re.compile(rf"\bALTER\s+TABLE\s+(?P<table>{QUALIFIED})", re.IGNORECASE|re.DOTALL)),
    ("INSERT INTO",    re.compile(rf"\bINSERT\s+INTO\s+(?P<table>{QUALIFIED})", re.IGNORECASE|re.DOTALL)),
    ("SELECT INTO",    re.compile(rf"\bSELECT\b[\s\S]*?\bINTO\s+(?P<table>{QUALIFIED})", re.IGNORECASE|re.DOTALL)),
    ("UPDATE",         re.compile(rf"\bUPDATE\s+(?P<table>{QUALIFIED})", re.IGNORECASE|re.DOTALL)),
    ("DELETE FROM",    re.compile(rf"\bDELETE\s+FROM\s+(?P<table>{QUALIFIED})", re.IGNORECASE|re.DOTALL)),
    ("MERGE INTO",     re.compile(rf"\bMERGE\s+INTO\s+(?P<table>{QUALIFIED})", re.IGNORECASE|re.DOTALL)),
    # FROM: allow optional parenthesis when not starting a subquery (SELECT/WITH)
    ("FROM",           re.compile(rf"\bFROM\s+(?:\(\s*(?!SELECT\b|WITH\b|VALUES\b))?(?P<table>{QUALIFIED})", re.IGNORECASE|re.DOTALL)),
    # JOIN: similarly allow optional parenthesis when not a subquery
    ("JOIN",           re.compile(rf"(?:(?P<type>INNER|LEFT|RIGHT|FULL(?:\s+OUTER)?|CROSS)\s+)?JOIN\s+(?:\(\s*(?!SELECT\b|WITH\b|VALUES\b))?(?P<table>{QUALIFIED})", re.IGNORECASE|re.DOTALL)),
]

MARKER_REGEX = re.compile(r"(?m)^\s*--\s*(?P<num>\d+)\s+(?P<path>.+?\.sql)\s*$")

# Detect context like "FETCH NEXT FROM <cursor>" to avoid misclassifying
# cursor names as tables when matching FROM clauses.
CURSOR_FETCH_BEFORE = re.compile(r"\bFETCH\s+(?:NEXT|PRIOR|FIRST|LAST)?\s*$", re.IGNORECASE)


def extract_matches(text: str) -> List[Dict[str, str]]:
    # Collect all matches with the position of the TABLE token,
    # then sort by that position to reflect exact encounter order.
    collected: List[Dict[str, str]] = []
    cte_names = _extract_cte_names(text)
    alias_map = _extract_alias_map(text)
    for clause, pattern in CLAUSE_PATTERNS:
        for m in pattern.finditer(text):
            c = clause
            t = m.group('table').strip()
            # Skip FROM when it's part of a cursor FETCH statement
            try:
                from_start = m.start()  # our pattern starts at the SQL keyword (e.g., FROM)
                context = text[max(0, from_start-80):from_start]
                if CURSOR_FETCH_BEFORE.search(context):
                    continue
            except Exception:
                pass
            # Skip dynamic SQL: detect if table name is part of string concatenation
            # Pattern: "dbo.CARTESIO_' + @variable" or "dbo.' + @variable" or similar
            # Check if the matched table is followed by ' (incomplete in dynamic SQL)
            # or by .' (incomplete identifier), or by + @ (concatenation)
            try:
                match_end = m.end('table') if 'table' in m.groupdict() else m.end()
                following = text[match_end:match_end+20]
                following_stripped = following.lstrip()
                # If followed by ' (single quote, string literal end), or .' or + @, skip
                if (following_stripped.startswith("'") or 
                    following_stripped.startswith(".'") or 
                    following_stripped.startswith("+ @")):
                    continue
            except Exception:
                pass
            # Resolve alias for UPDATE/others: if single identifier equals alias, map to base table
            t_stripped = _strip_delimiters(t)
            if '.' not in t_stripped and t_stripped.lower() in alias_map:
                t = alias_map[t_stripped.lower()]
            elif c.upper() == 'UPDATE' and '.' not in t_stripped:
                # If UPDATE targets a single-name alias we don't know, skip to avoid false positives
                # (prefer correctness over completeness)
                continue
            # Normalize missing schema: db..table or server.db..table
            em3 = EMPTY_SCHEMA_3.match(t)
            em4 = EMPTY_SCHEMA_4.match(t)
            if em4:
                t = f"{em4.group('server')}.{em4.group('db')}.dbo.{em4.group('table')}"
            elif em3:
                t = f"{em3.group('db')}.dbo.{em3.group('table')}"
            # Skip temp tables (#, ##) and CTEs
            if _is_temp_table(t):
                continue
            if _strip_delimiters(_last_segment(t)).lower() in cte_names:
                continue
            join_type = m.groupdict().get('type')
            if join_type:
                c = f"{join_type.upper()} JOIN"
            # Prefer the position where the table token appears
            try:
                pos = m.start('table')
            except Exception:
                pos = m.start()
            collected.append({'Clause': c, 'Table': t, '_pos': pos})
    collected.sort(key=lambda x: x['_pos'])
    # Drop position before returning
    return [{'Clause': x['Clause'], 'Table': x['Table']} for x in collected]


def parse_blocks(content: str, input_path: str, verbose: bool = True) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    markers = list(MARKER_REGEX.finditer(content))
    if not markers:
        # Single block case
        if verbose:
            print("Nessun marker trovato; elaboro il file come singolo blocco…")
        file_name = os.path.basename(input_path)
        content_nc = _strip_sql_comments(content)
        matches = extract_matches(content_nc)
        if verbose:
            print(f"  Riferimenti estratti: {len(matches)}")
        for m in matches:
            rows.append({'Path': input_path, 'File': file_name, 'Clause': m['Clause'], 'Table': m['Table']})
        return rows

    # Multiple blocks
    if verbose:
        print(f"Trovati {len(markers)} marker di file; elaboro i blocchi…")
    for i, marker in enumerate(markers):
        path = marker.group('path').strip()
        file_name = os.path.basename(path)
        start = marker.end()
        end = markers[i+1].start() if i < len(markers)-1 else len(content)
        block = content[start:end]
        block_nc = _strip_sql_comments(block)
        matches = extract_matches(block_nc)
        if verbose:
            print(f"  Blocco {i+1}/{len(markers)}: {file_name}")
            print(f"    Riferimenti estratti: {len(matches)}")
        for m in matches:
            rows.append({'Path': path, 'File': file_name, 'Clause': m['Clause'], 'Table': m['Table']})
    return rows


def write_csv(rows: List[Dict[str, str]], output_path: str) -> None:
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['Path', 'File', 'Clause', 'Table'])
        for r in rows:
            w.writerow([r['Path'], r['File'], r['Clause'], r['Table']])


def write_xlsx(rows: List[Dict[str, str]], output_path: str) -> None:
    if openpyxl is None:
        raise RuntimeError('openpyxl is not installed')

    # Prepare headers and rows
    headers = ['Path', 'File', 'Clause', 'Table']
    tuple_rows: List[Tuple[str, str, str, str]] = [
        (r['Path'], r['File'], r['Clause'], r['Table']) for r in rows
    ]

    # Compute approximate widths based on content
    def _safe_len(s: object) -> int:
        return len(str(s)) if s is not None else 0

    col_max = [len(h) for h in headers]
    for r in tuple_rows:
        col_max = [max(col_max[i], _safe_len(r[i])) for i in range(4)]
    widths = [min(m + 2, 80) for m in col_max]

    try:
        from Report.Excel_Writer import write_rows_split_across_files
    except Exception:
        write_rows_split_across_files = None  # type: ignore

    if write_rows_split_across_files is not None:
        write_rows_split_across_files(headers, tuple_rows, output_path, sheet_name='Tables', column_widths=widths)
    else:
        # Fallback to previous single-file implementation
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = 'Tables'
        ws.append(headers)
        for r in tuple_rows:
            ws.append(list(r))
        for i, w in enumerate(widths, start=1):
            ws.column_dimensions[openpyxl.utils.get_column_letter(i)].width = w
        try:
            wb.save(output_path)
        except PermissionError:
            ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            base, ext = os.path.splitext(output_path)
            alt_path = f"{base}_{ts}{ext}"
            print(f"File Excel destinazione bloccato: salvo come {alt_path}")
            wb.save(alt_path)


def main():
    ap = argparse.ArgumentParser(description='Extract table references from appended SQL into Excel/CSV.')
    ap.add_argument('input', nargs='?', help='Path to appended SQL file (optional if INPUT_SQL is set)')
    ap.add_argument('-o', '--output', help='Output file path (.xlsx or .csv)')
    ap.add_argument('-f', '--format', choices=['xlsx', 'csv'], default='xlsx', help='Output format')
    args = ap.parse_args()

    # Resolve input path: CLI arg takes precedence; otherwise use INPUT_SQL
    input_path = args.input if args.input else INPUT_SQL
    if not input_path:
        raise FileNotFoundError('Percorso input non fornito: passa l\'argomento "input" oppure imposta INPUT_SQL all\'inizio dello script.')
    if not os.path.exists(input_path):
        raise FileNotFoundError(f'Input file not found: {input_path}')

    # Default output path
    if not args.output:
        ext = 'xlsx' if args.format == 'xlsx' else 'csv'
        args.output = os.path.join(os.path.dirname(input_path), f'SQL_Table_References.{ext}')

    print('Avvio estrazione…')
    print(f'Input SQL: {input_path}')
    with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()

    rows = parse_blocks(content, input_path, verbose=True)

    # Preserve original encounter order (no sorting)
    print(f'Totale riferimenti trovati: {len(rows)}')

    if args.format == 'csv':
        print(f'Scrittura CSV in {args.output}…')
        write_csv(rows, args.output)
        print(f'CSV written to {args.output}')
    else:
        print(f'Scrittura Excel in {args.output}…')
        write_xlsx(rows, args.output)
        print(f'Excel written to {args.output}')


if __name__ == '__main__':
    main()
