# 1. SCRIPT POWERSHELL PER ESTRARRE IL CODICE M DA FILE EXCEL
# -------------------------------------------------------------
# Salva questo script come ExportMCode.ps1 e modificalo con i tuoi percorsi
# Esegui da PowerShell prima di lanciare lo script Python

# Importa le variabili di configurazione
. "$PSScriptRoot\config.ps1"

$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
New-Item -ItemType Directory -Force -Path $exportFolder | Out-Null

# Ricerca ricorsiva di tutti i file .xlsx
$files = Get-ChildItem -Path $folder -Filter *.xlsx -Recurse

$errorLog = Join-Path $exportFolder "ExportMCode_errors.txt"
foreach ($file in $files) {
    try {
        # Open workbook senza aggiornare connessioni (UpdateLinks=0)
        $wb = $excel.Workbooks.Open($file.FullName, 0)
        foreach ($query in $wb.Queries) {
            $queryName = $query.Name
            $mCode = $query.Formula
            # Usa la struttura di cartelle per l'export
            $relativePath = $file.FullName.Substring($folder.Length).TrimStart('\')
            $baseExportFolder = Join-Path $exportFolder ([System.IO.Path]::GetDirectoryName($relativePath))
            New-Item -ItemType Directory -Force -Path $baseExportFolder | Out-Null
            $exportPath = Join-Path $baseExportFolder "$($file.BaseName)_$($queryName)_M.txt"
            try {
                Set-Content -Path $exportPath -Value $mCode -ErrorAction Stop
            } catch {
                $errMsg = "[$(Get-Date -Format o)] Errore Set-Content su $exportPath (file origine: $($file.FullName)): $($_.Exception.Message)"
                Add-Content -Path $errorLog -Value $errMsg
            }
        }
        $wb.Close($false)
    } catch {
        $errMsg = "[$(Get-Date -Format o)] Errore apertura file $($file.FullName): $($_.Exception.Message)"
        Add-Content -Path $errorLog -Value $errMsg
    }
}
$excel.Quit()
Write-Host "\nEsportazione completata. File M salvati in: $exportFolder"