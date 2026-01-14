"""
SQL Parser for Power Query M code.
Parses SQL connections and extracts database metadata from M code text files.
"""
from .connection_base import IConnection
import re


class SqlParser(IConnection):
    """
    Parses SQL database connections from Power Query M code.
    Extracts server, database, schema, table, query, and JOIN information.
    """

    def __init__(self, txt_file):
        super().__init__(txt_file)
        self.join_tables = []

    def get_connection(self):
        """Extracts SQL connection information from the M code file."""
        try:
            with open(self.txt_file, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            with open(self.txt_file, 'r', encoding='latin-1') as f:
                content = f.read()

        # Extract Source line
        self._extract_source(content)
        
        # Extract server, database, and query
        if self.source:
            self._extract_server_database()
            self._extract_query_and_tables()
        
        # Extract schema and table from Data access pattern
        self._extract_schema_table_from_data(content)
        
        print(f"File: {self.txt_file} | Table: {self.table}")

    def _extract_source(self, content: str) -> None:
        """Extracts the Sql.Database/Databases call from content."""
        sql_call = re.search(r'Sql\.(Databases|Database)\s*\(', content, re.IGNORECASE)
        if not sql_call:
            self.source = None
            return
        
        # Find matching closing parenthesis
        start = sql_call.start()
        par_count = 0
        end = start
        found = False
        
        while end < len(content):
            if content[end] == '(':
                par_count += 1
            elif content[end] == ')':
                par_count -= 1
            if par_count == 0 and content[end] == ')':
                found = True
                break
            end += 1
        
        self.source = content[start:end+1].strip() if found else None

    def _extract_server_database(self) -> None:
        """Extracts server and database from the source string."""
        if not self.source:
            return
        
        # Try Sql.Databases pattern
        dbs_match = re.match(r'Sql\.Databases\(["\']([^"\']+)["\']\)', self.source)
        if dbs_match:
            self.server = dbs_match.group(1)
            # Look for database in Source{[Name="..."]}[Data] pattern
            db_line = re.search(r'(Source|Origine)\{\[Name="([^"]+)"\]\}\[Data\]', self.source)
            if db_line:
                self.database = db_line.group(2)
            return
        
        # Try Sql.Database pattern
        db_match = re.match(
            r'Sql\.Database\(["\']([^"\']+)["\']\s*,\s*["\']([^"\']+)["\']',
            self.source,
            re.DOTALL
        )
        if db_match:
            self.server = db_match.group(1)
            self.database = db_match.group(2)

    def _extract_query_and_tables(self) -> None:
        """Extracts SQL query and parses tables/joins from it."""
        if not self.source:
            return
        
        # Extract content between first [ and last ]
        bracket_match = re.search(r'\[(.*)\]', self.source, re.DOTALL)
        if not bracket_match:
            return
        
        third_arg = bracket_match.group(1)
        query_match = re.search(r'Query\s*=\s*"([\s\S]*?)"', third_arg)
        if not query_match:
            return
        
        self.query = query_match.group(1)
        cleaned_query = self._normalize_query(self.query)
        
        # Extract JOIN tables
        self._extract_join_tables(cleaned_query)
        
        # Extract main table and schema
        self._extract_main_table(cleaned_query)

    def _normalize_query(self, query: str) -> str:
        """Normalizes SQL query by removing special characters and comments."""
        cleaned = query
        
        # Replace Power Query special characters
        cleaned = re.sub(r'#\((lf|cr|tab)\)', ' ', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'[\r\n\t]+', ' ', cleaned)
        cleaned = re.sub(r'#\([a-zA-Z0-9]+\)', '', cleaned)
        
        # Remove SQL comments
        cleaned = re.sub(r'--.*?\n', ' ', cleaned)
        cleaned = re.sub(r'/\*.*?\*/', ' ', cleaned, flags=re.DOTALL)
        
        # Normalize whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        return cleaned

    def _extract_join_tables(self, query: str) -> None:
        """Extracts all tables involved in JOINs."""
        join_matches = re.findall(
            r'JOIN\s+((?:[\w\[\]]+\.){0,2}\[?[\w]+\]?)(?:\s+AS\s+\w+|\s+\w+)?',
            query,
            re.IGNORECASE
        )
        
        self.join_tables = []
        for jm in join_matches:
            table_full = jm.strip('[]"`()')
            parts = [p.strip('[]') for p in re.split(r'\.', table_full) if p.strip('[]')]
            
            if len(parts) >= 2:
                schema, table = parts[-2], parts[-1]
            elif len(parts) == 1:
                schema, table = '', parts[0]
            else:
                schema, table = '', table_full
            
            self.join_tables.append(f"{schema}.{table}" if schema else table)

    def _extract_main_table(self, query: str) -> None:
        """Extracts main table and schema from FROM clause."""
        # Try schema..[table] pattern
        table_matches = re.findall(
            r'(?:FROM|JOIN)[^a-zA-Z]+([\w]+)\.\.[\[]?([\w]+)\]?',
            query,
            re.IGNORECASE
        )
        
        if table_matches:
            self.schema, self.table = table_matches[0]
            return
        
        # Try standard schema.table pattern
        table_matches = re.findall(
            r'(?:FROM|JOIN)\s+((?:[\w\[\]]+\.){0,2}\[?[\w]+\]?)(?:\s+AS\s+\w+|\s+\w+)?',
            query,
            re.IGNORECASE
        )
        
        if table_matches:
            table_full = table_matches[0].strip('[]"`()')
            parts = [p.strip('[]') for p in re.split(r'\.', table_full) if p.strip('[]')]
            
            if len(parts) >= 2:
                self.schema, self.table = parts[-2], parts[-1]
            elif len(parts) == 1:
                self.schema, self.table = '', parts[0]
            else:
                self.schema, self.table = '', table_full
            return
        
        # Try temp table pattern (# or ##)
        temp_table_match = re.search(
            r'(?:FROM|JOIN)\s+(#\#?[\w_]+)',
            query,
            re.IGNORECASE
        )
        if temp_table_match:
            self.schema = ''
            self.table = temp_table_match.group(1)

    def _extract_schema_table_from_data(self, content: str) -> None:
        """Extracts schema and table from {[Schema="...", Item="..."]}[Data] pattern."""
        table_match = re.search(
            r'\{\[Schema="([^"]+)",\s*Item="([^"]+)"\]\}\[Data\]',
            content
        )
        if table_match:
            self.schema = table_match.group(1)
            self.table = table_match.group(2)
