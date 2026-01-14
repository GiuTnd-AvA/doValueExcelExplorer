# config.ps1
# Configurazione variabili per ExportMCode.ps1

# PSScriptAnalyzer: Le variabili sono usate in ExportMCode.ps1 tramite dot sourcing
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseDeclaredVarsMoreThanAssignments', 'folder')]
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseDeclaredVarsMoreThanAssignments', 'exportFolder')]
param()

$folder = "$env:USERPROFILE\Desktop\doValue"  # <-- Cartella radice
$exportFolder = "$env:USERPROFILE\Desktop\Export M Code"  # <-- Cartella export
