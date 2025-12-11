import win32com.client

class ConnessioniSenzaTxt:
    def __init__(self, percorso_excel):
        self.percorso_excel = percorso_excel
        self.file_name = percorso_excel.split('\\')[-1]
        self.server = None
        self.database = None
        self.schema = None
        self.table = None

    def estrai_connessioni(self):
        self.connessioni = []
        # Avvia una nuova istanza di Excel per evitare conflitti con istanze aperte
        excel = win32com.client.DispatchEx("Excel.Application")
        # Sopprimi UI e pop-up: non mostrare Excel, niente aggiornamenti schermo, niente avvisi
        try:
            excel.Visible = False
        except Exception:
            # Alcune installazioni potrebbero non consentire la modifica di Visible
            pass
        try:
            excel.ScreenUpdating = False
        except Exception:
            pass
        # Disabilita avvisi ed eventi per evitare blocchi durante le operazioni COM
        try:
            excel.DisplayAlerts = False
            excel.EnableEvents = False
            # Disabilita macro/security prompt (3 = msoAutomationSecurityForceDisable)
            excel.AutomationSecurity = 3
        except Exception:
            pass

        # Apri il workbook senza aggiornare link e in sola lettura per ridurre UI
        wb = excel.Workbooks.Open(
            self.percorso_excel,
            UpdateLinks=0,  # non aggiornare link esterni
            ReadOnly=True,
            Editable=False,
            IgnoreReadOnlyRecommended=True,
            Notify=False
        )

        # Se ci sono query asincrone (Power Query/connessioni), attendi che finiscano
        try:
            excel.CalculateUntilAsyncQueriesDone()
        except Exception:
            pass

        for conn in wb.Connections:
            # Escludi Power Query (tipo "xlConnectionTypeWORKSHEET")
            if conn.Type != 7:  # 7 = xlConnectionTypeWORKSHEET (Power Query)
                info = self._estrai_info_connessione(conn)
                if info and info.get("Tipo") == "SQL":
                    self.server = info.get("Server")
                    self.database = info.get("Database")
                    self.schema = info.get("Schema")
                    self.table = info.get("Tabella")
                    self.connessioni.append(info)

        # Chiudi il workbook con un piccolo retry per gestire OLE 0x800AC472
        import time
        max_retry = 3
        for attempt in range(max_retry):
            try:
                wb.Close(SaveChanges=False)
                break
            except Exception as e:
                # Se Excel è occupato (0x800AC472), attendi e riprova
                if hasattr(e, 'hresult') and e.hresult == -2146777998:
                    time.sleep(0.5)
                    continue
                # Altrimenti rilancia
                raise

        # Prova a chiudere Excel in modo resiliente
        try:
            excel.Quit()
        except Exception as e:
            # Gestisce eccezioni COM durante Quit (es. -2146777998)
            import time
            for _ in range(2):
                time.sleep(0.5)
                try:
                    excel.Quit()
                    break
                except Exception:
                    continue
        finally:
            try:
                del wb
            except Exception:
                pass
            try:
                del excel
            except Exception:
                pass
        return self.connessioni

    def _estrai_info_connessione(self, conn):
        try:
            # Solo per connessioni OLEDB/ODBC
            if hasattr(conn, "OLEDBConnection"):
                oledb = conn.OLEDBConnection
                if "sql" in oledb.Connection.lower():
                    server = oledb.Connection.split(";")
                    server_info = {kv.split("=")[0].strip().lower(): kv.split("=")[1].strip() for kv in server if "=" in kv}
                    return {
                        "NomeConnessione": conn.Name,
                        "Server": server_info.get("data source", ""),
                        "Database": server_info.get("initial catalog", ""),
                        "Schema": oledb.CommandText.split(".")[1] if len(oledb.CommandText.split(".")) > 1 else "",
                        "Tabella": oledb.CommandText.split(".")[2] if len(oledb.CommandText.split(".")) > 2 else oledb.CommandText,
                        "Tipo": "SQL"
                    }
            elif hasattr(conn, "ODBCConnection"):
                odbc = conn.ODBCConnection
                if "sql" in odbc.Connection.lower():
                    server = odbc.Connection.split(";")
                    server_info = {kv.split("=")[0].strip().lower(): kv.split("=")[1].strip() for kv in server if "=" in kv}
                    return {
                        "NomeConnessione": conn.Name,
                        "Server": server_info.get("server", ""),
                        "Database": server_info.get("database", ""),
                        "Schema": odbc.CommandText.split(".")[1] if len(odbc.CommandText.split(".")) > 1 else "",
                        "Tabella": odbc.CommandText.split(".")[2] if len(odbc.CommandText.split(".")) > 2 else odbc.CommandText,
                        "Tipo": "SQL"
                    }
        except Exception:
            pass
        return None

# Esempio d'uso:
# cs = ConnessioniSenzaTxt("C:\\percorso\\file.xlsx")
# info = cs.estrai_connessioni()
# print(info)