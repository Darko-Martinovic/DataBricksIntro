Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

. "$root/sync/sync.paths.ps1"

$localFile = Join-Path $root $NotebookLocal
$localDir = Split-Path -Parent $localFile

if (-not (Test-Path $localDir)) {
    New-Item -ItemType Directory -Path $localDir -Force | Out-Null
}

Write-Host "Exporting $NotebookRemote -> $NotebookLocal"
databricks workspace export --profile $ProfileName --format SOURCE --file "$localFile" "$NotebookRemote"

Write-Host "Pull complete."
