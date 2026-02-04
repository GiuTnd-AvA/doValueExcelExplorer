# -----------------------------------------------------------------------------
# Scopo: verifica se la lista di tabelle passate da un file excel esistono sul 
# database e se non esistono da un messaggio sul fatto che non sono state trovate
# -----------------------------------------------------------------------------
import os
from typing import Dict, List, Optional, Set, Tuple

try:
    import pyodbc
except Exception:
    pyodbc = None  # type: ignore

try:
    import pandas as pd
except Exception:
    pd = None  # type: ignore

# -----------------------------------------------------------------------------
# Configurazione: inserisci qui i percorsi degli Excel e i parametri di connessione
# -----------------------------------------------------------------------------
INPUT_EXCEL_PATH: Optional[str] = None  # es: r"C:\\path\\ListaTabelle.xlsx"
OUTPUT_EXCEL_PATH: Optional[str] = None  # es: r"C:\\path\\TabelleEsistenti.xlsx"

# Parametri connessione
DEFAULT_SERVER: str = "EPCP3"
TRUSTED_CONNECTION: bool = True
SQL_USERNAME: Optional[str] = None  # usato se TRUSTED_CONNECTION=False
SQL_PASSWORD: Optional[str] = None  # usato se TRUSTED_CONNECTION=False

ODBC_DRIVERS: List[str] = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "SQL Server",
    # legacy/common names
    "SQL Server Native Client 11.0",
    "ODBC Driver 13 for SQL Server",
    "ODBC Driver 11 for SQL Server",
]

ODBC_ENCRYPT_OPTS: str = "Encrypt=no;TrustServerCertificate=yes;"
CONNECTION_TEST_TIMEOUT: int = 3
QUERY_TIMEOUT: int = 60
# Includi anche le viste nella ricerca
INCLUDE_VIEWS: bool = True
# Includi anche i sinonimi (sys.synonyms) nella ricerca
INCLUDE_SYNONYMS: bool = True
# Includi stored procedure, funzioni (scalar/TVF), e trigger
INCLUDE_PROCS: bool = True
INCLUDE_FUNCTIONS: bool = True  # include FN, IF, TF
INCLUDE_TRIGGERS: bool = True


class TableExistenceChecker:
    """Legge un Excel con una lista di tabelle e verifica se esistono
    su SQL Server (server=EPCP3 per default). Per ogni tabella trovata,
    scrive in un Excel: Server, DB, Schema, Table.

    Formati input supportati (prima riga come intestazione):
    - Colonne separate: [schema] (opzionale), table (obbligatoria), [db] (opzionale)
    - Oppure una colonna unica 'table' che può contenere anche 'schema.table'

    Se "db" non è fornito, la ricerca avviene su tutti i DB utente del server.
    Se "schema" non è fornito, la ricerca avviene per nome tabella a prescindere dallo schema.
    """

    def __init__(self, input_excel: str, output_excel: str, server: str = DEFAULT_SERVER):
        if pd is None:
            raise RuntimeError("pandas non installato. Installa 'pip install pandas openpyxl'.")
        if pyodbc is None:
            raise RuntimeError("pyodbc non installato. Installa 'pip install pyodbc'.")
        if not input_excel:
            raise ValueError("Percorso Excel di input non valorizzato.")
        self.input_excel = input_excel
        self.output_excel = output_excel or os.path.join(os.getcwd(), "TabelleEsistenti.xlsx")
        self.server = server or DEFAULT_SERVER

    # ------------------------------ Utilità Excel ------------------------------
    def _read_targets(self) -> List[Tuple[Optional[str], Optional[str], str]]:
        """Ritorna lista di triple (db, schema, table).
        - db può essere None => cerca in tutti i DB utente
        - schema può essere None => cerca per nome tabella su tutti gli schemi
        - table è obbligatoria
        """
        print(f"[CHECK] Lettura input da: {self.input_excel}")
        df = pd.read_excel(self.input_excel)
        if df.empty:
            return []
        df.columns = [str(c).strip().lower() for c in df.columns]

        # Colonne rilevanti, flessibili
        db_col = "db" if "db" in df.columns else ("database" if "database" in df.columns else None)
        schema_col = "schema" if "schema" in df.columns else None
        table_col = "table" if "table" in df.columns else None

        targets: List[Tuple[Optional[str], Optional[str], str]] = []
        for _, row in df.iterrows():
            db = str(row[db_col]).strip() if db_col and pd.notna(row[db_col]) else None
            schema = str(row[schema_col]).strip() if schema_col and pd.notna(row[schema_col]) else None
            if table_col and pd.notna(row.get(table_col)):
                raw_table = str(row[table_col]).strip()
            else:
                # fallback: cerca prima colonna significativa
                non_na = [str(v).strip() for v in row.values if pd.notna(v)]
                raw_table = non_na[0] if non_na else ""
            if not raw_table:
                continue

            # Permetti formato "schema.table" nella colonna table
            if not schema and "." in raw_table:
                parts = raw_table.split(".", 1)
                schema = parts[0].strip()
                table = parts[1].strip()
            else:
                table = raw_table

            targets.append((db or None, schema or None, table))
        print(f"[CHECK] Target letti: {len(targets)}")
        return targets

    # ------------------------------ Utilità SQL -------------------------------
    def _build_conn_str(self, database: Optional[str]) -> str:
        last_error: Optional[Exception] = None
        for drv in ODBC_DRIVERS:
            try:
                conn_str = f"DRIVER={{{drv}}};SERVER={self.server};"
                if database:
                    conn_str += f"DATABASE={database};"
                if TRUSTED_CONNECTION:
                    conn_str += "Trusted_Connection=yes;"
                else:
                    if not SQL_USERNAME or not SQL_PASSWORD:
                        raise RuntimeError("Imposta SQL_USERNAME e SQL_PASSWORD oppure usa Trusted_Connection.")
                    conn_str += f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
                conn_str += ODBC_ENCRYPT_OPTS
                # Test rapido
                tconn = pyodbc.connect(conn_str, timeout=CONNECTION_TEST_TIMEOUT)
                tconn.close()
                return conn_str
            except Exception as e:
                last_error = e
                continue
        raise RuntimeError(f"Nessun driver ODBC valido trovato. Ultimo errore: {last_error}")

    def _list_user_databases(self) -> List[str]:
        print(f"[CHECK] Connessione a {self.server} per elencare i database utente...")
        conn = pyodbc.connect(self._build_conn_str(None), timeout=QUERY_TIMEOUT)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT name
                FROM sys.databases
                WHERE name NOT IN ('master','tempdb','model','msdb')
                  AND state = 0 -- ONLINE
                ORDER BY name;
                """
            )
            dbs = [str(r[0]) for r in cur.fetchall()]
            print(f"[CHECK] Database utente trovati: {len(dbs)}")
            return dbs
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _fetch_tables_in_db(self, db: str) -> Set[Tuple[str, str, str]]:
        """Ritorna set di (schema, object_name, object_type_label) esistenti nel DB.
        Include, in base ai flag: tabelle, viste, sinonimi, stored procedure, funzioni, trigger.
        `object_type_label` è allineato a `sys.objects.type_desc` o literal 'SYNONYM'.
        """
        parts = [
            # Tabelle
            (
                """
                SELECT s.name AS schema_name, t.name AS object_name, 'USER_TABLE' AS object_type
                FROM sys.tables AS t
                JOIN sys.schemas AS s ON s.schema_id = t.schema_id
                """
            )
        ]
        if INCLUDE_VIEWS:
            parts.append(
                """
                SELECT s.name AS schema_name, v.name AS object_name, 'VIEW' AS object_type
                FROM sys.views AS v
                JOIN sys.schemas AS s ON s.schema_id = v.schema_id
                """
            )
        if INCLUDE_SYNONYMS:
            parts.append(
                """
                SELECT s.name AS schema_name, sy.name AS object_name, 'SYNONYM' AS object_type
                FROM sys.synonyms AS sy
                JOIN sys.schemas AS s ON s.schema_id = sy.schema_id
                """
            )

        type_codes: List[str] = []
        if INCLUDE_PROCS:
            type_codes.append('P')
        if INCLUDE_FUNCTIONS:
            type_codes.extend(['FN', 'IF', 'TF'])
        if INCLUDE_TRIGGERS:
            type_codes.append('TR')
        if type_codes:
            in_list = ", ".join([f"'{c}'" for c in type_codes])
            parts.append(
                f"""
                SELECT s.name AS schema_name, o.name AS object_name, o.type_desc AS object_type
                FROM sys.objects AS o
                JOIN sys.schemas AS s ON s.schema_id = o.schema_id
                WHERE o.type IN ({in_list})
                """
            )

        sql = "\nUNION ALL\n".join(parts)
        print(f"[CHECK] Carico elenco oggetti (flags: views={INCLUDE_VIEWS}, synonyms={INCLUDE_SYNONYMS}, procs={INCLUDE_PROCS}, functions={INCLUDE_FUNCTIONS}, triggers={INCLUDE_TRIGGERS}) per DB: {db}")
        conn = pyodbc.connect(self._build_conn_str(db), timeout=QUERY_TIMEOUT)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            fetched = {(str(r[0]), str(r[1]), str(r[2])) for r in cur.fetchall()}
            print(f"[CHECK] Oggetti in {db}: {len(fetched)}")
            return fetched
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _fetch_table_ddl(self, db: str, schema: str, table: str) -> str:
        """Genera una definizione CREATE TABLE per una tabella usando metadata di sistema."""
        conn = pyodbc.connect(self._build_conn_str(db), timeout=QUERY_TIMEOUT)
        try:
            tsql_stringagg = r"""
DECLARE @schema_table nvarchar(512) = QUOTENAME(?) + N'.' + QUOTENAME(?);
DECLARE @obj_id int = OBJECT_ID(@schema_table);
IF @obj_id IS NULL
    SELECT CAST(N'ERROR: tabella non trovata: ' + @schema_table AS nvarchar(max)) AS ddl;
ELSE
BEGIN
    DECLARE @cols nvarchar(max) =
    (
        SELECT STRING_AGG(
            N'[' + c.name + N'] ' +
            UPPER(t.name) +
            CASE 
                WHEN t.name IN (N'char',N'nchar',N'varchar',N'nvarchar',N'binary',N'varbinary') 
                     THEN N'(' + CASE 
                                   WHEN t.name IN (N'nchar',N'nvarchar') 
                                        THEN CASE WHEN c.max_length = -1 THEN N'MAX' ELSE CAST(c.max_length/2 AS nvarchar(10)) END
                                   ELSE CASE WHEN c.max_length = -1 THEN N'MAX' ELSE CAST(c.max_length AS nvarchar(10)) END
                                 END + N')'
                WHEN t.name IN (N'decimal',N'numeric') 
                     THEN N'(' + CAST(c.precision AS nvarchar(10)) + N',' + CAST(c.scale AS nvarchar(10)) + N')'
                WHEN t.name IN (N'datetime2',N'time',N'datetimeoffset')
                     THEN N'(' + CAST(c.scale AS nvarchar(10)) + N')'
                ELSE N''
            END +
            CASE WHEN ic.is_identity = 1 
                 THEN N' IDENTITY(' + CAST(ic.seed_value AS nvarchar(50)) + N',' + CAST(ic.increment_value AS nvarchar(50)) + N')' 
                 ELSE N'' 
            END +
            CASE WHEN c.is_nullable = 0 THEN N' NOT NULL' ELSE N' NULL' END +
            COALESCE(N' DEFAULT ' + dc.definition, N'')
            , N',' + CHAR(13) + CHAR(10)
        ) WITHIN GROUP (ORDER BY c.column_id)
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        LEFT JOIN sys.default_constraints dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
        LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE c.object_id = @obj_id
    );

    DECLARE @pk nvarchar(max) =
    (
        SELECT N'CONSTRAINT [' + kc.name + N'] PRIMARY KEY (' +
               STRING_AGG(N'[' + c.name + N']' + CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N'' END, N', ')
               WITHIN GROUP (ORDER BY ic.key_ordinal) + N')'
        FROM sys.key_constraints kc
        JOIN sys.indexes i ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE kc.type = 'PK' AND kc.parent_object_id = @obj_id
    );

    SELECT N'CREATE TABLE ' + @schema_table + CHAR(13) + CHAR(10) +
           N'(' + ISNULL(@cols, N'') + N')' +
           ISNULL(CHAR(13) + CHAR(10) + @pk, N'') AS ddl;
END
"""
            tsql_xmlpath = r"""
DECLARE @schema_table nvarchar(512) = QUOTENAME(?) + N'.' + QUOTENAME(?);
DECLARE @obj_id int = OBJECT_ID(@schema_table);
IF @obj_id IS NULL
    SELECT CAST(N'ERROR: tabella non trovata: ' + @schema_table AS nvarchar(max)) AS ddl;
ELSE
BEGIN
    DECLARE @cols nvarchar(max) = N'';
    SELECT @cols = STUFF((
        SELECT N',' + CHAR(13) + CHAR(10) +
               N'[' + c.name + N'] ' +
               UPPER(t.name) +
               CASE 
                   WHEN t.name IN (N'char',N'nchar',N'varchar',N'nvarchar',N'binary',N'varbinary') 
                        THEN N'(' + CASE 
                                      WHEN t.name IN (N'nchar',N'nvarchar') 
                                           THEN CASE WHEN c.max_length = -1 THEN N'MAX' ELSE CAST(c.max_length/2 AS nvarchar(10)) END
                                      ELSE CASE WHEN c.max_length = -1 THEN N'MAX' ELSE CAST(c.max_length AS nvarchar(10)) END
                                    END + N')'
                   WHEN t.name IN (N'decimal',N'numeric') 
                        THEN N'(' + CAST(c.precision AS nvarchar(10)) + N',' + CAST(c.scale AS nvarchar(10)) + N')'
                   WHEN t.name IN (N'datetime2',N'time',N'datetimeoffset')
                        THEN N'(' + CAST(c.scale AS nvarchar(10)) + N')'
                   ELSE N''
               END +
               CASE WHEN ic.is_identity = 1 
                    THEN N' IDENTITY(' + CAST(ic.seed_value AS nvarchar(50)) + N',' + CAST(ic.increment_value AS nvarchar(50)) + N')' 
                    ELSE N'' 
               END +
               CASE WHEN c.is_nullable = 0 THEN N' NOT NULL' ELSE N' NULL' END +
               COALESCE(N' DEFAULT ' + dc.definition, N'')
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        LEFT JOIN sys.default_constraints dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
        LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE c.object_id = @obj_id
        ORDER BY c.column_id
        FOR XML PATH(''), TYPE
    ).value('.', 'nvarchar(max)'), 1, 1, N'');

    DECLARE @pk nvarchar(max) = NULL;
    SELECT @pk = N'CONSTRAINT [' + kc.name + N'] PRIMARY KEY (' +
                 STUFF((
                    SELECT N', ' + N'[' + c.name + N']' + CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N'' END
                    FROM sys.index_columns ic
                    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                    ORDER BY ic.key_ordinal
                    FOR XML PATH(''), TYPE
                 ).value('.', 'nvarchar(max)'), 1, 2, N'') + N')'
    FROM sys.key_constraints kc
    JOIN sys.indexes i ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id
    WHERE kc.type = 'PK' AND kc.parent_object_id = @obj_id;

    SELECT N'CREATE TABLE ' + @schema_table + CHAR(13) + CHAR(10) +
           N'(' + ISNULL(@cols, N'') + N')' +
           ISNULL(CHAR(13) + CHAR(10) + @pk, N'') AS ddl;
END
"""
            cur = conn.cursor()
            try:
                cur.execute(tsql_stringagg, (schema, table))
            except Exception:
                cur.execute(tsql_xmlpath, (schema, table))
            row = cur.fetchone()
            return str(row[0]) if row and row[0] is not None else ""
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _fetch_view_definition(self, db: str, schema: str, view: str) -> str:
        """Ritorna la definizione testuale della vista dal DB."""
        conn = pyodbc.connect(self._build_conn_str(db), timeout=QUERY_TIMEOUT)
        try:
            sql = (
                """
                SELECT sm.definition
                FROM sys.sql_modules AS sm
                WHERE sm.object_id = OBJECT_ID(QUOTENAME(?) + N'.' + QUOTENAME(?));
                """
            )
            cur = conn.cursor()
            try:
                cur.execute(sql, (schema, view))
                r = cur.fetchone()
                if r and r[0]:
                    return str(r[0])
                cur.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(QUOTENAME(?) + N'.' + QUOTENAME(?)))", (schema, view))
                r2 = cur.fetchone()
                if r2 and r2[0]:
                    return str(r2[0])
                return "ERROR: definizione non disponibile (possibile oggetto crittografato)"
            except Exception as e:
                return f"ERROR: lettura definizione vista fallita: {e}"
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _fetch_module_definition(self, db: str, schema: str, name: str) -> str:
        """Ritorna definizione testuale per oggetti con modulo SQL (proc, func, trigger, view)."""
        conn = pyodbc.connect(self._build_conn_str(db), timeout=QUERY_TIMEOUT)
        try:
            sql = (
                """
                SELECT sm.definition
                FROM sys.sql_modules AS sm
                WHERE sm.object_id = OBJECT_ID(QUOTENAME(?) + N'.' + QUOTENAME(?));
                """
            )
            cur = conn.cursor()
            try:
                cur.execute(sql, (schema, name))
                r = cur.fetchone()
                if r and r[0]:
                    return str(r[0])
                cur.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(QUOTENAME(?) + N'.' + QUOTENAME(?)))", (schema, name))
                r2 = cur.fetchone()
                if r2 and r2[0]:
                    return str(r2[0])
                return "ERROR: definizione non disponibile (possibile oggetto crittografato)"
            except Exception as e:
                return f"ERROR: lettura definizione oggetto fallita: {e}"
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _fetch_synonym_ddl(self, db: str, schema: str, synonym: str) -> str:
        """Costruisce CREATE SYNONYM basandosi su sys.synonyms.base_object_name."""
        conn = pyodbc.connect(self._build_conn_str(db), timeout=QUERY_TIMEOUT)
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT base_object_name FROM sys.synonyms WHERE schema_id = SCHEMA_ID(?) AND name = ?",
                    (schema, synonym),
                )
                r = cur.fetchone()
                base = str(r[0]) if r and r[0] is not None else None
                if not base:
                    return f"ERROR: sinonimo non trovato: [{schema}].[{synonym}]"
                return f"CREATE SYNONYM [{schema}].[{synonym}] FOR {base};"
            except Exception as e:
                return f"ERROR: lettura sinonimo fallita: {e}"
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _get_ddl(self, db: str, schema: str, name: str, obj_type: str) -> str:
        t = (obj_type or "").upper()
        if t == "VIEW":
            return self._fetch_view_definition(db, schema, name)
        if t == "SYNONYM":
            return self._fetch_synonym_ddl(db, schema, name)
        if ("PROCEDURE" in t) or ("FUNCTION" in t) or ("TRIGGER" in t):
            return self._fetch_module_definition(db, schema, name)
        # default: table
        return self._fetch_table_ddl(db, schema, name)

    # ------------------------------ Run principale ----------------------------
    def run(self) -> str:
        targets = self._read_targets()
        if not targets:
            print("[CHECK] Nessuna tabella in input.")
            return self.output_excel

        # Prepara lista DB da ispezionare
        # Se nell'input ci sono DB specificati, usa quelli; altrimenti lista completa user DB
        dbs_in_input = sorted({db for (db, _schema, _table) in targets if db})
        if dbs_in_input:
            databases = dbs_in_input
            print(f"[CHECK] DB specificati in input: {', '.join(databases)}")
        else:
            databases = self._list_user_databases()
            print(f"[CHECK] DB enumerati dal server: {', '.join(databases)}")

        print(f"[CHECK] DB da verificare: {len(databases)}")

        # Normalizza input per confronto case-insensitive
        # Manteniamo comunque le tuple originali per eventuale debug
        norm_targets: List[Tuple[Optional[str], Optional[str], str]] = []
        for (db, schema, table) in targets:
            norm_targets.append(
                (db.lower() if db else None, schema.lower() if schema else None, table.lower())
            )

        results: List[List[str]] = []
        total_matches = 0
        # Tracciamo quali target trovano almeno una corrispondenza
        found_flags: List[bool] = [False] * len(targets)
        # Mappa di errori per DB (se un DB non è stato analizzato)
        db_errors: Dict[str, str] = {}

        # Per efficienza, per ogni DB carichiamo tutte le tabelle e poi confrontiamo in memoria
        for db in databases:
            print(f"[CHECK] Scansione tabelle in DB: {db}")
            try:
                existing = self._fetch_tables_in_db(db)
            except Exception as e:
                msg = f"Errore lettura tabelle: {e}"
                print(f"[CHECK] {msg}")
                # Riga di errore per il DB corrente
                results.append([self.server, db, "", "", msg])
                db_errors[db.lower()] = msg
                continue
            # Costruisci indici
            by_table: Dict[str, List[Tuple[str, str, str]]] = {}
            for sch, tbl, typ in existing:
                key = tbl.lower()
                by_table.setdefault(key, []).append((sch, tbl, typ))

            # Valuta target che chiedono proprio questo DB (o tutti i DB)
            matches_in_db = 0
            for idx, (tdb, tschema, ttable) in enumerate(norm_targets):
                if tdb is not None and tdb != db.lower():
                    continue
                matches: List[Tuple[str, str, str]] = []
                if tschema:
                    # match preciso schema.table
                    candidates = by_table.get(ttable, [])
                    matches = [(s, t, ty) for (s, t, ty) in candidates if s.lower() == tschema]
                else:
                    # qualsiasi schema con quel table name
                    matches = by_table.get(ttable, [])

                for (sch, tbl, typ) in matches:
                    ddl = self._get_ddl(db, sch, tbl, typ)
                    results.append([self.server, db, sch, tbl, typ, ddl, ""])  # nessun errore
                    matches_in_db += 1
                    total_matches += 1
                    found_flags[idx] = True
            print(f"[CHECK] Corrispondenze trovate in {db}: {matches_in_db}")

        # Aggiungi righe per i target non trovati
        if targets:
            # Elenco DB con errori (per nota informativa)
            errored_dbs = [d for d in databases if d.lower() in db_errors]
            for i, (orig_db, orig_schema, orig_table) in enumerate(targets):
                if found_flags[i]:
                    continue
                if orig_db and orig_db.lower() in db_errors:
                    err = f"Ricerca non eseguita su DB '{orig_db}': {db_errors[orig_db.lower()]}"
                    results.append([self.server, orig_db, orig_schema or "", orig_table, "", "", err])
                else:
                    if orig_db:
                        err = "Cercata ma non trovata"
                        results.append([self.server, orig_db, orig_schema or "", orig_table, "", "", err])
                    else:
                        note = (
                            f" Non analizzati: {', '.join(errored_dbs)}" if errored_dbs else ""
                        )
                        err = f"Cercata ma non trovata in nessun DB utente.{note}"
                        results.append([self.server, "", orig_schema or "", orig_table, "", "", err])

        # Scrivi risultati
        out_dir = os.path.dirname(self.output_excel)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        df_out = pd.DataFrame(results, columns=["Server", "DB", "Schema", "Table", "ObjectType", "DDL", "Error"])
        # Normalizza la colonna Error per evitare NaN
        if "Error" in df_out.columns:
            df_out["Error"] = df_out["Error"].fillna("")
        try:
            from Report.Excel_Writer import write_dataframe_split_across_files
        except Exception:
            write_dataframe_split_across_files = None  # type: ignore

        if write_dataframe_split_across_files is not None:
            written = write_dataframe_split_across_files(df_out, self.output_excel, sheet_name="Tabelle")
            out_display = ", ".join(written)
        else:
            with pd.ExcelWriter(self.output_excel, engine="openpyxl", mode="w") as writer:
                df_out.to_excel(writer, index=False, sheet_name="Tabelle")
            out_display = self.output_excel
        print(f"[CHECK] Totale corrispondenze: {total_matches}")
        print(f"[CHECK] Output scritto in: {out_display} (righe: {len(results)})")
        return self.output_excel


if __name__ == "__main__":
    # Esempio d'uso rapido: imposta i percorsi all'inizio del file
    if not INPUT_EXCEL_PATH:
        raise SystemExit("Imposta INPUT_EXCEL_PATH a inizio file (o modifica lo script).")
    checker = TableExistenceChecker(INPUT_EXCEL_PATH, OUTPUT_EXCEL_PATH or "")
    out = checker.run()
    print(out)
