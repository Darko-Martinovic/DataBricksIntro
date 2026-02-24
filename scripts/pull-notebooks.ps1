Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

. "$root/sync/sync.paths.ps1"

foreach ($notebook in $NotebookMappings) {
    $localFile = Join-Path $root $notebook.Local
    $localDir = Split-Path -Parent $localFile

    if (-not (Test-Path $localDir)) {
        New-Item -ItemType Directory -Path $localDir -Force | Out-Null
    }

    Write-Host "Exporting $($notebook.Remote) -> $($notebook.Local)"
    databricks workspace export --profile $ProfileName --format $notebook.Format --file "$localFile" "$($notebook.Remote)"
}

Write-Host "Pull complete."
