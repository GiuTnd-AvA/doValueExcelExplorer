import re
from typing import Dict, Optional, Tuple

LF_TOKENS = ["#(lf)", "#(cr,lf)", "#(cr)", "#{lf}", "#{cr,lf}", "#{cr}"]


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

    # Capture 3-part or 2-part names after FROM
    # Examples handled:
    #   FROM dbo.Table
    #   FROM [dbo].[Table]
    #   FROM DB.dbo.Table
    #   FROM [DB].[dbo].[Table]
    #   FROM DB..Table  (schema missing)
    _FROM_OBJECT_RE = re.compile(
        r"\bFROM\s+"  # FROM keyword
        r"(?:(?P<db>\[[^\]]+\]|[A-Za-z0-9_]+)\.)?"  # optional DB.
        r"(?:(?P<schema>\[[^\]]+\]|[A-Za-z0-9_]+)\.)?"  # optional schema.
        r"(?P<table>\[[^\]]+\]|[A-Za-z0-9_]+)",  # table
        re.IGNORECASE,
    )

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
            m_from = self._FROM_OBJECT_RE.search(query)
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
