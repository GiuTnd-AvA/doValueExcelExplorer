# doValueExcelExplorer

## Descrizione

**doValueExcelExplorer** è uno strumento Python progettato per analizzare, esplorare ed estrarre informazioni da file Excel che contengono connessioni a database, query Power Query e sorgenti dati SQL. Il tool è particolarmente utile per la gestione e l'analisi di grandi quantità di file Excel con connessioni a database SQL Server.

## Caratteristiche Principali

### 1. Analisi delle Connessioni Excel
- **Estrazione connessioni XML**: Analizza i file Excel (.xlsx, .xls) ed estrae le informazioni di connessione dal file `xl/connections.xml`
- **Parsing Power Query**: Interpreta le sorgenti Power Query e identifica server, database, schema e tabelle
- **Supporto connessioni multiple**: Gestisce connessioni SQL, SharePoint ed Excel
- **Report aggregati**: Genera report Excel con tutte le connessioni trovate, organizzate per file

### 2. Gestione File SQL
- **Esplorazione file SQL**: Trova e analizza file .sql nella struttura delle cartelle
- **Parsing query SQL**: Estrae informazioni su tabelle referenziate (INTO, FROM, JOIN)
- **Aggregazione SQL**: Combina più file SQL in un unico file di output con commenti identificativi

### 3. Interazione Database
- **Verifica esistenza tabelle**: Controlla se le tabelle esistono nei database di destinazione
- **Estrazione DDL**: Recupera le definizioni (DDL) di tabelle e viste dai database
- **Esecuzione query**: Esegue query SELECT da file Excel e registra eventuali errori

### 4. Report e Output
- **Report in formato Excel**: Genera report strutturati con fogli multipli
- **Chunking automatico**: Divide automaticamente report di grandi dimensioni in chunk configurabili
- **Metadati file**: Include informazioni su creatore, data di creazione e ultima modifica

## Struttura del Progetto

```
doValueExcelExplorer/
├── main.py                                    # Script principale per generare report di connessioni
├── Config/
│   ├── config.py                             # Configurazione percorsi e parametri
│   └── config.ps1                            # Configurazione PowerShell
├── BusinessLogic/
│   ├── Business_Logic.py                     # Logica di business principale
│   ├── Excel_Metadata_Extractor.py          # Estrazione metadati Excel
│   ├── PowerQuerySourceConnectionParser.py   # Parser per sorgenti Power Query
│   ├── PowerQueryTxtSourceExtractor.py      # Estrazione sorgenti da file .txt
│   ├── SQL_Explorer.py                       # Esplorazione e parsing SQL
│   └── ...
├── Connection/
│   ├── Get_SQL_Connection.py                # Gestione connessioni SQL
│   ├── Get_XML_Connection.py                # Gestione connessioni XML
│   └── ...
├── Finder/
│   ├── Excel_Finder.py                      # Ricerca file Excel
│   ├── Sql_Finder.py                        # Ricerca file SQL
│   └── ...
├── Report/
│   └── Excel_Writer.py                      # Scrittura report Excel
└── tests/                                    # Suite di test
```

## Script Disponibili

### Script Principali

#### 1. `main.py`
**Scopo**: Genera report completi delle connessioni presenti nei file Excel.

**Funzionalità**:
- Scansiona una cartella per trovare tutti i file Excel
- Estrae connessioni XML e Power Query
- Analizza file SQL presenti
- Genera report Excel divisi per chunk (dimensione configurabile)
- Include fogli: Lista file, Connessioni, Connessioni Join, SQL

**Configurazione**: Modifica `Config/config.py` per impostare i percorsi

#### 2. `Export_Excel_Connections.py`
**Scopo**: Trova ed esporta tutte le connessioni da file Excel.

**Uso**:
```bash
python Export_Excel_Connections.py
```

#### 3. `Get_Table_Definitions_From_Excel.py`
**Scopo**: Legge una lista di connessioni da Excel e recupera le definizioni DDL dal database.

**Funzionalità**:
- Connessione a SQL Server tramite pyodbc
- Recupero DDL per tabelle e viste
- Gestione di diversi formati di naming (database.schema.table, database..table, ecc.)
- Fallback automatico se la connessione principale fallisce
- Output in formato Excel con colonne: Server, Database, Schema, Table, Object Type, DDL

**Configurazione**:
- Modifica `INPUT_EXCEL_PATH` e `OUTPUT_EXCEL_PATH` nello script
- Configura le credenziali del database

#### 4. `Get_Table_Views_From_Excel.py`
**Scopo**: Simile a Get_Table_Definitions ma specifico per viste.

**Funzionalità aggiuntive**:
- Batching delle query per migliorare le performance
- Filtraggio per tipo di oggetto (VIEW)
- Gestione di errori di connessione con fallback

#### 5. `Table_Existence_Checker.py`
**Scopo**: Verifica l'esistenza di tabelle e viste nei database.

**Funzionalità**:
- Connessione DSN o connection string diretta
- Query su sys.objects per verificare esistenza
- Output con flag "Exists" (Yes/No)
- Report con tabelle non trovate

#### 6. `Execute_Selects_From_Excel.py`
**Scopo**: Esegue query SELECT da un file Excel e registra eventuali errori.

**Funzionalità**:
- Lettura query da foglio Excel
- Esecuzione su database configurato
- Report di errori con messaggi dettagliati

#### 7. `Append_Sql_Files_From_Excel.py`
**Scopo**: Aggrega più file SQL in un unico file.

**Funzionalità**:
- Legge lista di file SQL da Excel
- Aggiunge commenti identificativi prima di ogni file
- Output in formato .txt e opzionalmente .sql

**Uso**:
```bash
python Append_Sql_Files_From_Excel.py --excel "percorso/file.xlsx" --output "percorso/output.txt"
```

#### 8. `Append_Views_From_Excel.py`
**Scopo**: Aggrega definizioni di viste da Excel in un unico file SQL.

**Funzionalità**:
- Filtra per Object Type = 'view'
- Formattazione con commenti server\database\schema\table.sql
- Output .sql e .txt

#### 9. `Check_Connections_From_Excel_List.py`
**Scopo**: Verifica la connettività a database da una lista Excel.

**Funzionalità**:
- Test di connessione per ogni entry
- Report con stato (Success/Failed) e messaggi di errore
- Gestione timeout

#### 10. `ExtractSqlTables.py`
**Scopo**: Estrae riferimenti a tabelle da query SQL.

**Funzionalità**:
- Parsing di clausole FROM, JOIN, INTO
- Identificazione di tabelle, CTE, subquery
- Output strutturato

#### 11. `Export_PowerQuery_Sources.py`
**Scopo**: Esporta informazioni sulle sorgenti Power Query.

**Funzionalità**:
- Analisi file .txt esportati da Power Query
- Estrazione dettagli connessione

#### 12. `Export_Parsed_PowerQuery_Connections.py`
**Scopo**: Parsing avanzato delle connessioni Power Query.

**Funzionalità**:
- Interpretazione sintassi M di Power Query
- Estrazione parametri di connessione

## Requisiti

### Dipendenze Python

```bash
pip install openpyxl pandas pyodbc sqlalchemy
```

**Pacchetti richiesti**:
- `openpyxl`: Lettura e scrittura file Excel (.xlsx)
- `pandas`: Manipolazione dati e DataFrames
- `pyodbc`: Connessione a database SQL Server
- `sqlalchemy`: ORM e gestione database (opzionale per alcune funzionalità)

### Altri Requisiti

- **Python**: 3.8 o superiore
- **PowerShell**: Per script di export M Code (opzionale)
- **SQL Server ODBC Driver**: Per connessioni database
- **Permessi**: Accesso in lettura ai file Excel e ai database

## Configurazione

### 1. Configurazione Base

Modifica il file `Config/config.py`:

```python
import os

user_folder = os.path.expanduser("~")

# Percorsi principali (usa user_folder per percorsi relativi alla home dell'utente)
EXCEL_ROOT_PATH = rf'{user_folder}\Desktop\doValue'           # Cartella con file Excel
EXPORT_MCODE_PATH = rf'{user_folder}\Desktop\Export M Code'   # Export Power Query
EXCEL_OUTPUT_PATH = rf'{user_folder}\Desktop'                  # Output report

# Dimensione chunk per report
CHUNK_SIZE = 50  # Numero di file per report Excel
```

### 2. Configurazione Database

Per gli script che si connettono al database:

```python
# Connessione diretta
connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=server_name;"
    "DATABASE=database_name;"
    "UID=username;"
    "PWD=password;"
)

# Oppure DSN
dsn_name = "nome_dsn"
```

### 3. Configurazione Script Individuali

Ogni script ha variabili di configurazione all'inizio del file. Esempio:

```python
# In Append_Sql_Files_From_Excel.py
EXCEL_LIST_PATH = r"C:\percorso\file.xlsx"
OUTPUT_TXT_PATH = r"C:\percorso\output.txt"
CREATE_SQL_COPY = True
```

## Utilizzo

### Esempio 1: Generare Report Completo

```bash
# 1. Configura Config/config.py con i percorsi corretti
# 2. Esegui lo script principale
python main.py
```

**Output**:
- File Excel nella cartella configurata: `Report_Connessioni_0-49.xlsx`, `Report_Connessioni_50-99.xlsx`, ecc.
- Ogni file contiene fogli con liste file, connessioni, SQL, ecc.

### Esempio 2: Verificare Esistenza Tabelle

```bash
# 1. Prepara Excel con colonne: Server, Database, Schema, Table
# 2. Configura INPUT_EXCEL_PATH in Table_Existence_Checker.py
# 3. Esegui
python Table_Existence_Checker.py
```

### Esempio 3: Ottenere DDL di Tabelle

```bash
# 1. Prepara Excel con lista tabelle
# 2. Configura connessioni database
# 3. Esegui
python Get_Table_Definitions_From_Excel.py
```

### Esempio 4: Aggregare File SQL

```bash
python Append_Sql_Files_From_Excel.py --excel "C:\path\report.xlsx" --output "C:\path\aggregated.txt"
```

## Struttura Output

### Report Principale (da main.py)

**Foglio "Lista file"**:
- Percorsi
- File

**Foglio "Connessioni_Senza_Power_Query"**:
- Percorso
- File_Name
- Server
- Database
- Schema
- Table

**Foglio "Connessioni_Join"**:
- File_Name
- Join (query join trovate)

**Foglio "Lista file SQL"**:
- Percorsi
- File

**Foglio "SQL_Into_From_Join"**:
- File_Name
- Into
- From
- Join

## Testing

Il progetto include una suite di test in `tests/`:

```bash
# Esegui tutti i test
python -m pytest tests/

# Esegui test specifico
python -m pytest tests/test_powerquery_source_parser.py

# O esegui direttamente
python tests/test_append_sql_appender_smoke.py
```

**Test disponibili**:
- `test_powerquery_source_parser.py`: Test parsing Power Query
- `test_get_xml_connection_db_priority.py`: Test priorità connessioni
- `test_append_sql_appender_smoke.py`: Test aggregazione SQL
- `test_table_existence_checker.py`: Test verifica esistenza tabelle
- E molti altri...

## Workflow Tipico

1. **Raccolta Dati**:
   - Posiziona i file Excel in una cartella
   - (Opzionale) Esporta M Code da Power Query

2. **Configurazione**:
   - Modifica `Config/config.py` con i percorsi corretti
   - Configura connessioni database se necessario

3. **Generazione Report**:
   ```bash
   python main.py
   ```

4. **Analisi Approfondita**:
   - Usa `Get_Table_Definitions_From_Excel.py` per DDL
   - Usa `Table_Existence_Checker.py` per verificare esistenza
   - Usa `Check_Connections_From_Excel_List.py` per test connettività

5. **Post-Elaborazione**:
   - Aggrega SQL con `Append_Sql_Files_From_Excel.py`
   - Esporta viste con `Append_Views_From_Excel.py`

## Limitazioni e Note

- **Formato Excel**: Supporta principalmente .xlsx, supporto limitato per .xls
- **Power Query**: Richiede export M Code in file .txt per analisi completa
- **Database**: Testato principalmente con SQL Server
- **Encoding**: Gestisce UTF-8 per file SQL e TXT
- **Performance**: Per grandi quantità di file, usa il chunking (CHUNK_SIZE)

## Troubleshooting

### Errore: "Missing dependency: openpyxl"
```bash
pip install openpyxl
```

### Errore: "pyodbc.Error: Data source name not found"
- Verifica la configurazione DSN in Windows ODBC Data Sources
- Oppure usa connection string diretta

### Errore: "File not found"
- Verifica i percorsi in `Config/config.py`
- Usa percorsi assoluti con raw string (r"...")

### Performance lenta con molti file
- Riduci CHUNK_SIZE in `Config/config.py`
- Processa cartelle più piccole
- Disabilita script PowerShell se non necessario

### Connessioni non trovate
- Verifica che i file Excel contengano effettivamente connessioni
- Controlla il formato del file (deve essere .xlsx, non .xlsb)
- Verifica che `xl/connections.xml` esista nel file

## Contribuire

Per contribuire al progetto:

1. Fork del repository
2. Crea un branch per la feature (`git checkout -b feature/nuova-feature`)
3. Commit delle modifiche (`git commit -am 'Aggiunta nuova feature'`)
4. Push al branch (`git push origin feature/nuova-feature`)
5. Crea una Pull Request

## Licenza

[Specificare licenza del progetto]

## Contatti e Supporto

Per domande, problemi o suggerimenti, apri una issue su GitHub.

## Changelog

### Versione Corrente
- Supporto per chunking configurabile nei report
- Parsing migliorato per connessioni XML
- Gestione normalizzazione database.schema.table
- Test suite ampliata
- Supporto per fallback nelle connessioni database

## Credits

Sviluppato per analizzare e gestire connessioni Excel in ambienti doValue.
