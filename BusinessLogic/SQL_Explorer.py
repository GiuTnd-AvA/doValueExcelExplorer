
import os
import re

class SqlExplorer:
    
    def __init__(self, sql_file_path: str):
        self.sql_file_path = sql_file_path
        self.file_name = os.path.basename(sql_file_path)
        self.into = None  # Placeholder for INTO clause info
        self.from_tables = None  # Placeholder for FROM clause info
        self.joins = []  # Placeholder for JOIN clause info

    def _read_sql(self) -> str:
        if not os.path.exists(self.sql_file_path):
            return ""
        try:
            with open(self.sql_file_path, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
        except Exception:
            try:
                with open(self.sql_file_path, "r", encoding="latin-1", errors="ignore") as f:
                    return f.read()
            except Exception:
                return ""

    def _strip_comments(self, sql: str) -> str:
        sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
        sql = re.sub(r"--.*?$", " ", sql, flags=re.M)
        # Remove batch separators like GO on their own line
        sql = re.sub(r"^\s*go\s*$", " ", sql, flags=re.I | re.M)
        return sql

    def _clean_name(self, name: str) -> str:
        name = name.strip()
        # Remove brackets and quotes
        name = re.sub(r"^\[|\]$", "", name)
        name = name.replace("[", "").replace("]", "")
        name = name.replace('"', "")
        # Collapse whitespace around dot
        name = re.sub(r"\s*\.\s*", ".", name)
        return name

    def _extract_from_segment(self, block: str) -> str:
        m = re.search(r"\bfrom\b\s+(.*?)(?=\bwhere\b|\bgroup\b|\border\b|\bhaving\b|\bunion\b|;|$)", block, flags=re.I | re.S)
        return m.group(1) if m else ""

    def _extract_into_table(self, block: str) -> str | None:
        m = re.search(r"\binto\b\s+(?P<into>(?:\[[^\]]+\]|\"[^\"]+\"|[\w\.]+)(?:\s*\.\s*(?:\[[^\]]+\]|\"[^\"]+\"|[\w\.]+))?)", block, flags=re.I)
        return self._clean_name(m.group("into")) if m else None

    def _extract_join_tables(self, seg: str) -> list:
        joins = []
        for jm in re.finditer(r"(?:\binner\b|\bleft\b(?:\s+outer)?|\bright\b(?:\s+outer)?|\bfull\b(?:\s+outer)?|\bcross\b)?\s+join\s+(?P<t>(?:\[[^\]]+\]|\"[^\"]+\"|\w+)(?:\s*\.\s*(?:\[[^\]]+\]|\"[^\"]+\"|\w+))?)", seg, flags=re.I):
            t = self._clean_name(jm.group("t"))
            joins.append(t)
        return joins

    def _extract_from_tables(self, seg: str) -> list:
        # Consider only part before first JOIN to capture base FROM tables
        jpos = re.search(r"\bjoin\b", seg, flags=re.I)
        head = seg[:jpos.start()] if jpos else seg
        head = head.strip()
        if head.startswith("("):  # derived table
            return []
        # Split by commas outside parentheses (simple approach)
        parts = []
        buf = []
        depth = 0
        for ch in head:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            if ch == ',' and depth == 0:
                parts.append(''.join(buf))
                buf = []
            else:
                buf.append(ch)
        if buf:
            parts.append(''.join(buf))
        tables = []
        for p in parts:
            p = p.strip()
            if not p:
                continue
            m = re.match(r"(?P<t>(?:\[[^\]]+\]|\"[^\"]+\"|\w+)(?:\s*\.\s*(?:\[[^\]]+\]|\"[^\"]+\"|\w+))?)", p)
            if m:
                tables.append(self._clean_name(m.group("t")))
        return tables

    def _split_select_blocks(self, sql: str) -> list:
        blocks = []
        selects = list(re.finditer(r"\bselect\b", sql, flags=re.I))
        for i, m in enumerate(selects):
            start = m.start()
            next_select_pos = selects[i + 1].start() if i + 1 < len(selects) else None
            # Prefer terminating at the first semicolon after start, if it appears before the next SELECT
            semi_m = re.search(r";", sql[start:])
            if semi_m:
                semi_pos = start + semi_m.start() + 1
            else:
                semi_pos = None
            if semi_pos and (next_select_pos is None or semi_pos <= next_select_pos):
                end = semi_pos
            else:
                end = next_select_pos if next_select_pos is not None else len(sql)
            blocks.append(sql[start:end])
        return blocks

    def _split_insert_blocks(self, sql: str) -> list:
        blocks = []
        for m in re.finditer(r"\binsert\b\s+\binto\b", sql, flags=re.I):
            start = m.start()
            # End at next semicolon or end of string
            sm = re.search(r";", sql[start:], flags=re.S)
            end = (start + sm.start() + 1) if sm else len(sql)
            blocks.append(sql[start:end])
        return blocks

    def sql_clause(self) -> list:
        sql = self._read_sql()
        if not sql:
            return []
        sql = self._strip_comments(sql)
        rows = []
        for block in self._split_select_blocks(sql):
            into_table = self._extract_into_table(block)
            from_seg = self._extract_from_segment(block)
            if not into_table or not from_seg:
                continue  # Only consider SELECT with INTO and FROM
            from_tables = self._extract_from_tables(from_seg)
            join_tables = self._extract_join_tables(from_seg)
            rows.append([
                self.file_name,
                into_table,
                "; ".join(from_tables) if from_tables else "",
                "; ".join(join_tables) if join_tables else "",
            ])
        # Also parse INSERT INTO statements (e.g., INSERT INTO dbo.T ... VALUES ... or SELECT ... FROM ...)
        for iblock in self._split_insert_blocks(sql):
            m_into = re.search(r"\binsert\b\s+\binto\b\s+(?P<t>(?:\[[^\]]+\]|\"[^\"]+\"|\w+)(?:\s*\.\s*(?:\[[^\]]+\]|\"[^\"]+\"|\w+))?)", iblock, flags=re.I)
            into_table = self._clean_name(m_into.group("t")) if m_into else None
            from_seg = self._extract_from_segment(iblock)
            from_tables = self._extract_from_tables(from_seg) if from_seg else []
            join_tables = self._extract_join_tables(from_seg) if from_seg else []
            if into_table:
                rows.append([
                    self.file_name,
                    into_table,
                    "; ".join(from_tables) if from_tables else "",
                    "; ".join(join_tables) if join_tables else "",
                ])
        return rows
    
        
