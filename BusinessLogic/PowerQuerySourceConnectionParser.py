import re
from typing import Dict, Optional, Tuple

LF_TOKENS = [
    "#(lf)", "#(cr,lf)", "#(cr)",
    "#{lf}", "#{cr,lf}", "#{cr}",
    "#(tab)", "#{tab}",
]


def _unescape_m_linebreaks(text: str) -> str:
    if not text:
        return text
    out = text
    for tok in LF_TOKENS:
        out = out.replace(tok, " ")
    return out


def _strip_brackets(name: str) -> str:
    if not name:
        return name
    name = name.strip()
    if name.startswith("[") and name.endswith("]"):
        return name[1:-1]
    return name


class PowerQuerySourceConnectionParser:
    """
    Parses a Power Query M `Source = Sql.Database(...)` line to extract
    server, database, schema and table. Database is resolved from the
    SQL query's fully-qualified object when present (e.g. FROM DB.Schema.Table),
    otherwise falls back to the second Sql.Database parameter.
    """

    # Capture server and database params: Sql.Database("server", "db", ...)
    _SQL_DATABASE_RE = re.compile(
        r"Sql\s*\.\s*Database\s*\(\s*['\"](?P<server>[^'\"]+)['\"]\s*,\s*['\"](?P<db>[^'\"]+)['\"]",
        re.IGNORECASE,
    )

    # Capture query value inside [Query="..."] or [Query='...']
    _QUERY_RE = re.compile(
        r"\bQuery\s*=\s*(['\"])(?P<query>.*?)(\1)",
        re.IGNORECASE | re.DOTALL,
    )

    # Capture 3-part or 2-part names after FROM/JOIN
    # Examples handled:
    #   FROM dbo.Table
    #   FROM [dbo].[Table]
    #   FROM DB.dbo.Table
    #   FROM [DB].[dbo].[Table]
    #   FROM DB..Table  (schema missing)
    _FROM_OR_JOIN_OBJECT_RE = re.compile(
        r"\b(?:FROM|JOIN)\s+"  # FROM or JOIN keyword
        r"(?!\()"  # not a subquery starting with '('
        r"(?:(?P<db>\[[^\]]+\]|[A-Za-z0-9_]+)\.)?"  # optional DB.
        r"(?:(?P<schema>\[[^\]]+\]|[A-Za-z0-9_]+)\.)?"  # optional schema.
        r"(?P<table>\[[^\]]+\]|[A-Za-z0-9_]+)"  # table
        r"(?:\s+(?:AS\s+)?[A-Za-z0-9_]+)?",  # optional alias
        re.IGNORECASE,
    )

    # Temporary table: FROM #Temp or FROM ##Temp
    _FROM_OR_JOIN_TEMP_RE = re.compile(
        r"\b(?:FROM|JOIN)\s+#{1,2}(?P<temp>[A-Za-z0-9_]+)\b",
        re.IGNORECASE,
    )

    def parse_all(self, source_line: str):
        """Return a list of dicts for all tables found in FROM/JOIN clauses.
        Each dict contains server, database, schema, table.
        Deduplicates by (database, schema, table).
        """
        results = []
        seen = set()

        if not source_line:
            return results

        # Base params
        server = None
        param_db = None
        m_db = self._SQL_DATABASE_RE.search(source_line)
        if m_db:
            server = m_db.group("server")
            param_db = m_db.group("db")

        m_q = self._QUERY_RE.search(source_line)
        if not m_q:
            return results

        query_raw = m_q.group("query")
        query = _unescape_m_linebreaks(query_raw)
        query = re.sub(r"\s+", " ", query)

        # Temp tables
        for mt in self._FROM_OR_JOIN_TEMP_RE.finditer(query):
            table = mt.group("temp")
            db = None or param_db  # temp tables don't specify DB; keep param as context
            schema = "temp"
            key = (db or "", schema or "", table or "")
            if key not in seen:
                seen.add(key)
                results.append({
                    "server": server,
                    "database": db or param_db,
                    "schema": schema,
                    "table": table,
                })

        # FROM/JOIN regular objects
        for mf in self._FROM_OR_JOIN_OBJECT_RE.finditer(query):
            g_db = mf.group("db")
            g_schema = mf.group("schema")
            g_table = mf.group("table")

            db = None
            schema = None
            table = None

            if g_db and not g_schema and g_table:
                # two-part name (schema.table)
                db = None
                schema = _strip_brackets(g_db)
                table = _strip_brackets(g_table)
            else:
                db = _strip_brackets(g_db or "") or None
                schema = _strip_brackets(g_schema or "") or None
                table = _strip_brackets(g_table or "") or None

            final_db = db or param_db
            key = (final_db or "", schema or "", table or "")
            if table and key not in seen:
                seen.add(key)
                results.append({
                    "server": server,
                    "database": final_db,
                    "schema": schema,
                    "table": table,
                })

        return results

    def parse(self, source_line: str) -> Dict[str, Optional[str]]:
        server = None
        param_db = None
        db = None
        schema = None
        table = None

        if not source_line:
            return {"server": None, "database": None, "schema": None, "table": None}

        # Extract server and database from Sql.Database(...)
        m_db = self._SQL_DATABASE_RE.search(source_line)
        if m_db:
            server = m_db.group("server")
            param_db = m_db.group("db")

        # Extract query and parse FROM clause
        m_q = self._QUERY_RE.search(source_line)
        if m_q:
            query_raw = m_q.group("query")
            query = _unescape_m_linebreaks(query_raw)
            # Normalize whitespace for easier regex
            query = re.sub(r"\s+", " ", query)
            # First, try to capture temporary tables (e.g., ##MyTemp)
            m_temp = self._FROM_OR_JOIN_TEMP_RE.search(query)
            if m_temp:
                db = None
                schema = "temp"
                table = m_temp.group("temp")
            else:
                m_from = self._FROM_OR_JOIN_OBJECT_RE.search(query)
                if m_from:
                    g_db = m_from.group("db")
                    g_schema = m_from.group("schema")
                    g_table = m_from.group("table")

                    # If we only have two-part name (schema.table), the regex will put
                    # the first part in g_db and leave g_schema empty. Re-map accordingly.
                    if g_db and not g_schema and g_table:
                        db = None
                        schema = _strip_brackets(g_db)
                        table = _strip_brackets(g_table)
                    else:
                        db = _strip_brackets(g_db or "") or None
                        schema = _strip_brackets(g_schema or "") or None
                        table = _strip_brackets(g_table or "") or None

        # Decide database: prefer one found in FROM; otherwise fallback to param
        if not db:
            db = param_db

        return {
            "server": server,
            "database": db,
            "schema": schema,
            "table": table,
        }
