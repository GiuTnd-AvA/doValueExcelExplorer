# doValue Excel Explorer

Tool per l'esplorazione e l'analisi di file Excel Power Query, con estrazione del codice M e analisi delle connessioni SQL.

## 📋 Descrizione

Questo progetto automatizza l'analisi di file Excel contenenti query Power Query, estraendo:
- **Codice M** dalle query Power Query
- **Connessioni SQL** (server, database, tabelle, join)
- **Metadati** dei file (creatore, date di modifica)
- **Report aggregati** in formato Excel

## 🗂️ Struttura del Progetto

```
doValueExcelExplorer/
├── config/                     # Configurazioni (path, variabili)
│   ├── config.py              # Configurazione Python
│   └── config.ps1             # Configurazione PowerShell
│
├── mcode_extraction/          # Estrazione codice M da Excel
│   └── Estrazione_Codice_M/
│       ├── ExportMCode.ps1    # Script PowerShell per estrazione
│       └── Excecute_Power_Shell_Script.py
│   └── Estrazione_Connessione_SQL/
│       ├── Get_SQL_Connection.py
│       └── IConnection.py
│
├── core/                      # Logica business principale
│   ├── Business_Logic.py      # Orchestrazione del workflow
│   └── Txt_Source_Lines.py    # Parsing file di testo
│
├── exporters/                 # Export dei risultati
│   └── Excel_Writer.py        # Scrittura report Excel
│
├── finders/                   # Ricerca file nel filesystem
│   ├── Excel_Finder.py        # Ricerca file Excel
│   ├── TXT_Finder.py          # Ricerca file di testo
│   └── IFinder.py             # Interfaccia base
│
├── scripts/                   # Script utility
│   ├── analyze_sql_complexity.py
│   └── extract_sql_object_from_report_connessioni.py
│
├── main.py                    # Entry point principale
└── README.md                  # Questo file
```

## 🚀 Come Usare

### Prerequisiti
- Python 3.8+
- PowerShell 5.1+
- Microsoft Excel (per estrazione codice M)
- Pacchetti Python: `openpyxl`, `pandas` (installare con `pip install -r requirements.txt`)

### Configurazione

1. **Modifica [config/config.py](config/config.py)** con i tuoi percorsi:
   ```python
   EXCEL_ROOT_PATH = r'C:\tuo\percorso\cartella_excel'
   EXPORT_MCODE_PATH = r'C:\tuo\percorso\export'
   ```

2. **Modifica [config/config.ps1](config/config.ps1)** per PowerShell:
   ```powershell
   $folder = "C:\tuo\percorso\cartella_excel"
   $exportFolder = "C:\tuo\percorso\export"
   ```

### Esecuzione

```bash
# Attiva l'ambiente virtuale (se presente)
.venv\Scripts\Activate.ps1

# Esegui il workflow completo
python main.py

# Oppure esegui script singoli
python scripts/analyze_sql_complexity.py
python scripts/extract_sql_object_from_report_connessioni.py
```

## 📊 Output

Il tool genera:
- **File .txt** con il codice M estratto (in `EXPORT_MCODE_PATH`)
- **Report_Connessioni.xlsx** con l'elenco delle connessioni SQL
- **Report_Estratto_DB.xlsx** con analisi aggregate

## 🛠️ Workflow Interno

1. **Estrazione Codice M**: PowerShell apre i file Excel e estrae le query Power Query
2. **Parsing**: Python analizza i file .txt generati
3. **Estrazione Connessioni**: Parsing del codice M per identificare Source, Server, Database, Table
4. **Aggregazione**: Collegamento tra file Excel e connessioni
5. **Export**: Generazione report Excel finale

## 📝 Note

- Il nome delle cartelle segue la convenzione Python `snake_case`
- I file `__init__.py` sono presenti per marcare le directory come package Python
- Gli script PowerShell richiedono Excel installato sul sistema

## 🤝 Contributi

Per miglioramenti o bug, modifica direttamente il codice o contatta il maintainer.

---

**Autore**: Ciro Andreano  
**Data**: Gennaio 2026
