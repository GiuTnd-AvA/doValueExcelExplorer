# 1. SCRIPT POWERSHELL PER ESTRARRE IL CODICE M DA FILE EXCEL
# -----------------------------------------------------------
# Salva come ExportMCode.ps1 e modificalo con i tuoi percorsi

#region Parametri
param(
    [string]$Folder       = "$env:USERPROFILE\Desktop\doValue",          # <-- Cartella radice sorgente
    [string]$ExportFolder = "$env:USERPROFILE\Desktop\Export M Code xlsx",    # <-- Cartella di export
    [switch]$IncludeXlsm                                       # <-- Usa -IncludeXlsm per includere anche .xlsm
)
#endregion

#region Setup timer e contatori
$startTime = Get-Date
$sw = [System.Diagnostics.Stopwatch]::StartNew()

[int]$filesFound       = 0
[int]$filesAnalyzed    = 0
[int]$filesWithQueries = 0
[int]$queriesExported  = 0
[int]$errorCount       = 0
#endregion

Write-Host "=== Export M Code ==="
Write-Host ("Avvio analisi: {0}" -f $startTime.ToString("yyyy-MM-dd HH:mm:ss"))
Write-Host "Cartella sorgente: $Folder"
Write-Host "Cartella export:   $ExportFolder"
Write-Host ""

#region Preparazione cartella export
try {
    New-Item -ItemType Directory -Force -Path $ExportFolder | Out-Null
} catch {
    Write-Host ("[ERRORE] Impossibile creare la cartella di export: {0}" -f $ExportFolder) -ForegroundColor Red
    throw
}
#endregion

#region Inizializzazione Excel COM
$excel = $null
try {
    $excel = New-Object -ComObject Excel.Application
    $excel.Visible = $false
} catch {
    Write-Host "[ERRORE] Impossibile avviare Excel COM: $($_.Exception.Message)" -ForegroundColor Red
    throw
}
#endregion

#region Raccolta file
try {
    # Filtri: solo file, ricorsivo, estensioni consentite
    $filter = "*.xlsx"
    $files = Get-ChildItem -Path $Folder -Recurse -File -Filter $filter

    if ($IncludeXlsm.IsPresent) {
        # Aggiunge .xlsm se richiesto
        $xlsmFiles = Get-ChildItem -Path $Folder -Recurse -File -Filter "*.xlsm"
        $files = $files + $xlsmFiles
        # Rimuove duplicati se presenti
        $files = $files | Sort-Object -Property FullName -Unique
    }

    $filesFound = $files.Count
    Write-Host ("File trovati: {0}" -f $filesFound)
} catch {
    Write-Host "[ERRORE] Ricerca file fallita: $($_.Exception.Message)" -ForegroundColor Red
    $excel.Quit()
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null
    throw
}
#endregion

#region Log errori
$errorLog = Join-Path $ExportFolder "ExportMCode_errors.txt"
# Crea/intestazione del file log
"=== Log errori ExportMCode ===`r`nAvvio: $($startTime.ToString("o"))`r`n" | Set-Content -Path $errorLog
#endregion

#region Funzione di utilità
function IsNullOrWhiteSpace([string]$s) { return [string]::IsNullOrWhiteSpace($s) }
#endregion

#region Elaborazione
foreach ($file in $files) {
    try {
        # Apri workbook senza aggiornare connessioni (UpdateLinks=0)
        $wb = $excel.Workbooks.Open($file.FullName, 0)
        $filesAnalyzed++

        if ($wb.Queries -and $wb.Queries.Count -gt 0) {
            $filesWithQueries++

            foreach ($query in $wb.Queries) {
                $queryName = $query.Name
                $mCode     = $query.Formula

                # Struttura cartelle di export fedele al percorso relativo
                $relativePath     = $file.FullName.Substring($Folder.Length).TrimStart('\\','/')
                $relativeDir      = [System.IO.Path]::GetDirectoryName($relativePath)
                $baseExportFolder = if (IsNullOrWhiteSpace($relativeDir)) { $ExportFolder } else { Join-Path $ExportFolder $relativeDir }

                try {
                    New-Item -ItemType Directory -Force -Path $baseExportFolder | Out-Null
                } catch {
                    $errMsg = "[$(Get-Date -Format o)] Errore creazione cartella $baseExportFolder (origine: $($file.FullName)): $($_.Exception.Message)"
                    Add-Content -Path $errorLog -Value $errMsg
                    $errorCount++
                    continue
                }

                # Nome file export desiderato: <NomeCompletoExcel>_<QueryName>.txt
                # Dove <NomeCompletoExcel> = BaseName + '_' + estensione senza punto, es: "file" + "xlsx" => "file_xlsx"
                $safeQueryName = ($queryName -replace '[\\/:*?"<>|]', '_')
                $normalizedExcelName = "{0}_{1}" -f $file.BaseName, ($file.Extension.TrimStart('.') -replace '[^A-Za-z0-9]+','_')
                $exportFileName = "{0}_{1}.txt" -f $normalizedExcelName, $safeQueryName
                $exportPath     = Join-Path $baseExportFolder $exportFileName

                try {
                    Set-Content -Path $exportPath -Value $mCode -ErrorAction Stop -Encoding UTF8
                    $queriesExported++
                } catch {
                    $errMsg = "[$(Get-Date -Format o)] Errore scrittura $exportPath (file origine: $($file.FullName)): $($_.Exception.Message)"
                    Add-Content -Path $errorLog -Value $errMsg
                    $errorCount++
                }
            }
        }

        # Chiudi senza salvare
        $wb.Close($false)
        [System.Runtime.Interopservices.Marshal]::ReleaseComObject($wb) | Out-Null
    } catch {
        $errMsg = "[$(Get-Date -Format o)] Errore apertura/lettura file $($file.FullName): $($_.Exception.Message)"
        Add-Content -Path $errorLog -Value $errMsg
        $errorCount++
    }
}
#endregion

#region Cleanup Excel COM
try {
    $excel.Quit()
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null
} catch {
    $errMsg = "[$(Get-Date -Format o)] Errore chiusura Excel COM: $($_.Exception.Message)"
    Add-Content -Path $errorLog -Value $errMsg
    $errorCount++
}
#endregion

#region Report finale
$sw.Stop()
$endTime  = Get-Date
$duration = [System.TimeSpan]::FromMilliseconds($sw.ElapsedMilliseconds)

Write-Host ""
Write-Host "=== Report analisi ==="
Write-Host ("Fine analisi:    {0}" -f $endTime.ToString("yyyy-MM-dd HH:mm:ss"))
Write-Host ("Durata totale:   {0:hh\\:mm\\:ss\\.fff}" -f $duration)
Write-Host ("Cartella export: {0}" -f $ExportFolder)
Write-Host ("File trovati:            {0}" -f $filesFound)
Write-Host ("File analizzati:         {0}" -f $filesAnalyzed)
Write-Host ("File con Query M:        {0}" -f $filesWithQueries)
Write-Host ("Query M esportate:       {0}" -f $queriesExported)
Write-Host ("Errori registrati:       {0}" -f $errorCount)
Write-Host ""

Write-Host "Esportazione completata. I file M sono stati salvati in:" 
Write-Host $ExportFolder -ForegroundColor Green
Write-Host ("Dettagli errori (se presenti) in: {0}" -f $errorLog) -ForegroundColor Yellow
#endregion
