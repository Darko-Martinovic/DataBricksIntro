Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

. "$root/sync/sync.paths.ps1"

foreach ($notebook in $NotebookMappings) {
    $localFile = Join-Path $root $notebook.Local

    if (-not (Test-Path $localFile)) {
        throw "Local notebook does not exist: $localFile"
    }

    Write-Host "Importing $($notebook.Local) -> $($notebook.Remote)"
    if ($notebook.ContainsKey("Language")) {
        databricks workspace import --profile $ProfileName --overwrite --format $notebook.Format --language $notebook.Language --file "$localFile" "$($notebook.Remote)"
    }
    else {
        databricks workspace import --profile $ProfileName --overwrite --format $notebook.Format --file "$localFile" "$($notebook.Remote)"
    }
}

Write-Host "Push complete."
