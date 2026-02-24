Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

. "$root/sync/sync.paths.ps1"

$localFile = Join-Path $root $NotebookLocal

if (-not (Test-Path $localFile)) {
    throw "Local notebook does not exist: $localFile"
}

Write-Host "Importing $NotebookLocal -> $NotebookRemote"
databricks workspace import --profile $ProfileName --overwrite --format SOURCE --language PYTHON --file "$localFile" "$NotebookRemote"

Write-Host "Push complete."
