# doValueExcelExplorer

## Cosa puoi fare in questo branch?

Questo progetto è uno strumento Python per l'analisi e l'esplorazione di file Excel contenenti codice Power Query (M Code) e connessioni SQL. È particolarmente utile per progetti di migrazione database e analisi della complessità SQL.

## Funzionalità Principali

### 1. **Estrazione e Analisi del Codice M** (`main.py`)
Questo script principale:
- Esegue uno script PowerShell per estrarre il codice M dai file Excel
- Analizza i file Excel in una directory specificata
- Estrae informazioni sulle connessioni SQL dai file .txt generati
- Genera report Excel con:
  - Lista dei file Excel trovati
  - Dettagli delle connessioni PowerQuery (server, database, schema, tabelle)
  - Confronto tra connessioni attese ed estratte

**Utilizzo:**
```python
python main.py
```

### 2. **Estrazione di Oggetti SQL da Database** (`extract_sql_object_from_report_connessioni.py`)
Questo script permette di:
- Leggere un report Excel di connessioni
- Connettersi ai database SQL Server specificati
- Estrarre tutti gli oggetti SQL (stored procedures, functions, triggers) che referenziano le tabelle specificate
- Identificare clausole T-SQL utilizzate (INSERT, UPDATE, DELETE, JOIN, ecc.)
- Esportare i risultati in file Excel parziali (per gestire grandi volumi di dati)
- Generare un log degli errori di connessione

**Caratteristiche:**
- Elaborazione batch (50 righe alla volta per default)
- Supporto per connessioni Windows Authentication
- Export incrementale per evitare limiti di memoria
- Ricerca intelligente di varianti dei nomi tabella (con/senza schema, con/senza parentesi quadre)

**Configurazione:**
- `BATCH_SIZE`: Numero di righe da processare prima di ogni checkpoint (default: 50)
- `START_ROW`: Riga di partenza nel file Excel (default: 102)
- `DRIVER`: Driver ODBC utilizzato (auto-detect tra versione 17 e 18)

### 3. **Analisi della Complessità SQL** (`analyze_sql_complexity.py`)
Questo script avanzato analizza gli oggetti SQL estratti e calcola:

**Metriche di Complessità:**
- **Score di complessità** (0-100) basato su:
  - Numero di righe di codice
  - Pattern T-SQL complessi utilizzati
  - Operazioni DML (INSERT, UPDATE, DELETE, MERGE)
  - Complessità dei JOIN
  - Numero di dipendenze

**Pattern Identificati:**
- `CURSOR`: Uso di cursori
- `DYNAMIC_SQL`: SQL dinamico (EXEC, sp_executesql)
- `TRANSACTION`: Gestione transazioni
- `TEMP_TABLE`: Tabelle temporanee (#table)
- `TABLE_VARIABLE`: Variabili tabella
- `ERROR_HANDLING`: Gestione errori (TRY-CATCH)
- `LOOP`: Cicli WHILE
- `CTE`: Common Table Expressions
- `PIVOT/UNPIVOT`: Operazioni di pivot
- `XML`: Operazioni XML
- `WINDOW_FUNCTION`: Funzioni window (ROW_NUMBER, RANK, ecc.)

**Classificazione della Criticità:**
- **ALTA**: Score ≥ 70, presenza di SQL dinamico o cursori
- **MEDIA**: Score ≥ 40 o ≥ 3 operazioni DML
- **BASSA**: Casi rimanenti

**Output:**
Per ogni oggetto SQL analizzato, genera:
- Critico per migrazione (SÌ/NO)
- Descrizione del comportamento
- Score di complessità
- Criticità tecnica
- Pattern identificati
- Numero e lista delle dipendenze
- Conteggio DML e JOIN
- Righe di codice

**Utilizzo:**
```python
python analyze_sql_complexity.py
```

## Struttura del Progetto

```
doValueExcelExplorer/
├── BusinessLogic/          # Logica di business principale
│   ├── Business_Logic.py   # Orchestrazione delle operazioni
│   └── Txt_Source_Lines.py # Parsing dei file .txt
├── Codice_M/               # Gestione codice M e connessioni
│   ├── Estrazione_Codice_M/
│   └── Estrazione_Connessione_SQL/
├── Config/                 # File di configurazione
│   ├── config.py           # Percorsi e configurazioni Python
│   └── config.ps1          # Configurazioni PowerShell
├── ExportExcel/            # Esportazione in Excel
│   └── Excel_Writer.py     # Scrittura file Excel
├── FileFinder/             # Ricerca file
│   ├── Excel_Finder.py     # Trova file Excel
│   └── TXT_Finder.py       # Trova file TXT
├── main.py                 # Script principale
├── extract_sql_object_from_report_connessioni.py  # Estrazione oggetti SQL
└── analyze_sql_complexity.py  # Analisi complessità
```

## Requisiti

### Dipendenze Python:
- `pandas`: Gestione e manipolazione dati
- `openpyxl`: Lettura/scrittura file Excel
- `sqlalchemy`: Connessione ai database
- `pyodbc`: Driver ODBC per SQL Server

### Software Richiesto:
- Python 3.x
- PowerShell (per esecuzione script di estrazione M Code)
- ODBC Driver 17 o 18 for SQL Server
- Accesso ai database SQL Server (Windows Authentication)

### Installazione Dipendenze:
```bash
pip install pandas openpyxl sqlalchemy pyodbc
```

## Configurazione

Modifica il file `Config/config.py` per specificare i percorsi:

```python
# Percorsi per main.py
POWERSHELL_SCRIPT_PATH = r'C:\...\ExportMCode.ps1'
EXCEL_ROOT_PATH = r'C:\...\doValue'
EXPORT_MCODE_PATH = r'C:\...\Export M Code'

# Percorsi per estrazione database
EXCEL_INPUT_PATH = r'C:\...\Report_Connessioni.xlsx'
EXCEL_OUTPUT_PATH = r'C:\...\Report_Estratto_DB.xlsx'
```

## Flusso di Lavoro Tipico

1. **Estrazione Codice M ed Analisi Connessioni:**
   ```bash
   python main.py
   ```
   - Output: `Report_Connessioni.xlsx` con lista file e dettagli connessioni

2. **Estrazione Oggetti SQL dai Database:**
   ```bash
   python extract_sql_object_from_report_connessioni.py
   ```
   - Input: `Report_Connessioni.xlsx`
   - Output: `Report_Estratto_DB_parziale_*.xlsx` (file multipli per batch)

3. **Analisi Complessità SQL:**
   ```bash
   python analyze_sql_complexity.py
   ```
   - Input: `Report_Estratto_DB_parziale_*.xlsx`
   - Output: `Report_Estratto_DB_parziale_*_analyzed.xlsx`

## Use Cases

### Migrazione Database
- Identifica tutti gli oggetti SQL che dipendono da specifiche tabelle
- Valuta la complessità della migrazione attraverso score e metriche
- Classifica gli oggetti per priorità di intervento

### Analisi Dipendenze
- Mappa le connessioni tra file Excel e database SQL Server
- Identifica le chiamate tra stored procedures e funzioni
- Rileva pattern complessi che richiedono attenzione

### Code Review e Refactoring
- Identifica code smell SQL (cursori, SQL dinamico, loop)
- Quantifica la complessità del codice esistente
- Prioritizza gli interventi di refactoring

### Audit e Documentazione
- Genera inventario completo delle connessioni dati
- Documenta l'uso delle tabelle nei vari oggetti database
- Identifica operazioni critiche (DML su tabelle specifiche)

## Note Importanti

- **Performance**: L'estrazione di oggetti SQL può richiedere tempo per database di grandi dimensioni. L'elaborazione batch aiuta a gestire il processo.
- **Sicurezza**: Lo script utilizza Windows Authentication per le connessioni SQL Server.
- **Limitazioni Excel**: I file vengono divisi automaticamente per evitare il limite di 1 milione di righe di Excel.
- **Errori di Connessione**: Gli errori vengono registrati in un file `_error_log.xlsx` separato.

## Contribuire

Per contribuire al progetto:
1. Crea un branch per la tua feature
2. Implementa le modifiche
3. Testa accuratamente
4. Crea una pull request

## Licenza

[Specificare la licenza del progetto]
