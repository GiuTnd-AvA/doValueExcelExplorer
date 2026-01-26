# -----------------------------------------------------------------------------
# Scopo: legge un file Excel con colonne:
# server, database, schema, Table, Nome Oggetto, Tipo Oggetto, Script Creazione
# Per i record che rappresentano stored procedure (Tipo Oggetto), appende la DDL
# in un file di testo unico aggiungendo un commento iniziale nel formato:
# -- <n> server\database\schema\<nome_oggetto>.sql
# e subito sotto la definizione della stored procedure (Script Creazione / DDL).
# Produce output .txt e una copia .sql opzionale, identico per formato
# a quanto fa Append_Views_From_Excel per le viste.
# -----------------------------------------------------------------------------

import os
import re
from typing import Dict, List, Optional

try:
    from openpyxl import load_workbook
except Exception:
    load_workbook = None


# Config di default (puoi sovrascrivere da CLI se necessario)
DEFAULT_EXCEL_PATH: Optional[str] = None
DEFAULT_SHEET_NAME: Optional[str] = None  # usa il primo foglio se None
DEFAULT_OUTPUT_PATH: Optional[str] = None  # se None, crea output vicino all'Excel


class SPDDLAppender:
    """Legge un Excel e appende le DDL delle sole stored procedure in un file unico.

    Richiede intestazioni (case-insensitive, spazi tollerati):
    - server
    - database (alias: db)
    - schema
    - table
    - nome oggetto (alias: object name)
    - tipo oggetto (alias: object type)
    - script creazione (alias: DDL)
    """

    REQUIRED_HEADERS = [
        "server",
        "database",
        "schema",
        "table",
        "nome oggetto",
        "tipo oggetto",
        "script creazione",
    ]
    HEADER_ALIASES = {
        "database": ["db"],
        "tipo oggetto": ["object type", "tipooggetto", "objecttype"],
        "script creazione": ["ddl", "scriptcreazione"],
        "nome oggetto": ["nome", "object name", "oggetto", "nomeoggetto", "objectname"],
    }

    def __init__(
        self,
        excel_path: str,
        output_txt: Optional[str] = None,
        sheet_name: Optional[str] = DEFAULT_SHEET_NAME,
        create_sql_copy: bool = True,
    ):
        if load_workbook is None:
            raise RuntimeError("openpyxl non installato. Installa 'pip install openpyxl'.")
        if not excel_path:
            raise ValueError("Percorso Excel non valorizzato.")
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excel non trovato: {excel_path}")
        self.excel_path = excel_path
        self.sheet_name = sheet_name
        if output_txt:
            self.output_txt = output_txt
        else:
            base_dir = os.path.dirname(excel_path) or os.getcwd()
            self.output_txt = os.path.join(base_dir, "SP_Append.txt")
        self.create_sql_copy = create_sql_copy

    @staticmethod
    def _norm_header(h: Optional[str]) -> str:
        return (str(h).strip().lower() if h is not None else "")

    @staticmethod
    def _is_stored_procedure(tipo: str) -> bool:
        t = (tipo or "").strip().lower()
        return (
            t == "stored procedure"
            or t == "storedprocedure"
            or t == "procedure"
            or t == "procedura"
            or t == "sp"
            or ("proc" in t)
        )

    @staticmethod
    def _extract_proc_from_ddl(ddl: str) -> Dict[str, Optional[str]]:
        if not ddl:
            return {"schema": None, "name": None}
        text = str(ddl)
        m = re.search(
            r"\bcreate\s+(?:or\s+alter\s+)?procedure\s+\[([^\]]+)\]\s*\.\s*\[([^\]]+)\]",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if m:
            return {"schema": m.group(1), "name": m.group(2)}
        m = re.search(
            r"\bcreate\s+(?:or\s+alter\s+)?procedure\s+\[([^\]]+)\]",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if m:
            return {"schema": None, "name": m.group(1)}
        m = re.search(
            r"\bcreate\s+(?:or\s+alter\s+)?procedure\s+([a-zA-Z0-9_]+)\s*\.\s*([a-zA-Z0-9_]+)\b",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            return {"schema": m.group(1), "name": m.group(2)}
        m = re.search(
            r"\bcreate\s+(?:or\s+alter\s+)?procedure\s+([a-zA-Z0-9_]+)\b",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            return {"schema": None, "name": m.group(1)}
        return {"schema": None, "name": None}

    def _read_rows(self) -> List[Dict[str, str]]:
        wb = load_workbook(self.excel_path, read_only=True, data_only=True)
        try:
            ws = (
                wb[self.sheet_name]
                if self.sheet_name and self.sheet_name in wb.sheetnames
                else wb.worksheets[0]
            )
            rows = list(ws.iter_rows(values_only=True))
            if not rows:
                return []
            headers = [self._norm_header(h) for h in (rows[0] or [])]
            idx_map: Dict[str, int] = {}
            for needed in self.REQUIRED_HEADERS:
                if needed in headers:
                    idx_map[needed] = headers.index(needed)
                    continue
                needed_compact = needed.replace(" ", "")
                found_idx = None
                for i, h in enumerate(headers):
                    if h.replace(" ", "") == needed_compact:
                        found_idx = i
                        break
                if found_idx is None:
                    for alias in self.HEADER_ALIASES.get(needed, []):
                        alias_norm = self._norm_header(alias)
                        alias_compact = alias_norm.replace(" ", "")
                        for i, h in enumerate(headers):
                            if h == alias_norm or h.replace(" ", "") == alias_compact:
                                found_idx = i
                                break
                        if found_idx is not None:
                            break
                if found_idx is not None:
                    idx_map[needed] = found_idx
                else:
                    raise RuntimeError(f"Colonna richiesta non trovata: '{needed}'")

            data_rows = rows[1:]
            out: List[Dict[str, str]] = []
            for r in data_rows:
                if not r:
                    continue
                def get(col: str) -> str:
                    i = idx_map[col]
                    return str(r[i]).strip() if i < len(r) and r[i] is not None else ""

                tipo = get("tipo oggetto")
                if not self._is_stored_procedure(tipo):
                    continue

                server = get("server")
                db = get("database")
                schema = get("schema")
                table = get("table")
                nome_oggetto = get("nome oggetto")
                ddl = get("script creazione")

                if (not nome_oggetto) and ddl:
                    parsed = self._extract_proc_from_ddl(ddl)
                    if parsed["name"]:
                        nome_oggetto = parsed["name"]
                    if (not schema) and parsed["schema"]:
                        schema = parsed["schema"]

                if not nome_oggetto:
                    nome_oggetto = "UNKNOWN_PROC"

                out.append(
                    {
                        "server": server,
                        "db": db,
                        "schema": schema,
                        "object_name": nome_oggetto,
                        "ddl": ddl,
                    }
                )
            return out
        finally:
            wb.close()

    def run(self) -> str:
        rows = self._read_rows()
        if not rows:
            raise RuntimeError("Nessuna stored procedure trovata nell'Excel.")

        out_dir = os.path.dirname(self.output_txt)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)

        with open(self.output_txt, "w", encoding="utf-8", errors="ignore") as f:
            for idx, r in enumerate(rows, start=1):
                header_path = os.path.join(
                    r["server"], r["db"], r["schema"], f"{r['object_name']}.sql"
                )
                header_path_win = header_path.replace("/", "\\")
                f.write(f"-- {idx} {header_path_win}\n")
                if r["ddl"]:
                    f.write(str(r["ddl"]))
                f.write("\n")

        if self.create_sql_copy:
            sql_copy = os.path.splitext(self.output_txt)[0] + ".sql"
            try:
                with open(self.output_txt, "rb") as src, open(sql_copy, "wb") as dst:
                    dst.write(src.read())
            except Exception:
                pass

        return self.output_txt


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description=(
            "Append DDL delle stored procedure da Excel in output .txt/.sql "
            "(input colonne: server, database, schema, Table, Nome Oggetto, Tipo Oggetto, Script Creazione)"
        )
    )
    ap.add_argument("excel", nargs="?", help="Percorso al file Excel di input")
    ap.add_argument("-o", "--output", help="Percorso file di output .txt")
    ap.add_argument("-s", "--sheet", help="Nome foglio Excel da usare (default: primo)")
    ap.add_argument("--no-sql-copy", action="store_true", help="Non creare la copia .sql")
    args = ap.parse_args()

    create_sql_copy = not args.no_sql_copy
    excel_path = args.excel or DEFAULT_EXCEL_PATH
    if not excel_path:
        raise SystemExit(
            "Errore: specifica il percorso Excel (argomento 'excel') oppure imposta DEFAULT_EXCEL_PATH all'inizio del file."
        )

    app = SPDDLAppender(
        excel_path,
        args.output or DEFAULT_OUTPUT_PATH,
        args.sheet or DEFAULT_SHEET_NAME,
        create_sql_copy=create_sql_copy,
    )
    out = app.run()
    print(f"Output creato: {out}")
    if create_sql_copy:
        print(f"Copia .sql: {os.path.splitext(out)[0] + '.sql'}")
