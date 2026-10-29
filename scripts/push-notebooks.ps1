Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

# Resolve Databricks CLI — prefer system PATH, fall back to VSCode extension bundle
$DatabricksCli = Get-Command databricks -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty Source
if (-not $DatabricksCli) {
    $DatabricksCli = Get-ChildItem "$env:USERPROFILE\.vscode\extensions\databricks.databricks-*\bin\databricks.exe" |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1 -ExpandProperty FullName
}
if (-not $DatabricksCli) { throw "Databricks CLI not found. Install it or ensure the VSCode Databricks extension is installed." }

. "$PSScriptRoot/sync.paths.ps1"

foreach ($notebook in $NotebookMappings) {
    $localFile = Join-Path $root $notebook.Local

    if (-not (Test-Path $localFile)) {
        throw "Local notebook does not exist: $localFile"
    }

    # Notebooks in subfolders (Streaming/, SCD/, ...) need their remote folder to exist first
    $remoteDir = $notebook.Remote.Substring(0, $notebook.Remote.LastIndexOf('/'))
    & $DatabricksCli workspace mkdirs --profile $ProfileName "$remoteDir"
    if ($LASTEXITCODE -ne 0) { throw "Could not create $remoteDir" }

    Write-Host "Importing $($notebook.Local) -> $($notebook.Remote)"
    if ($notebook.ContainsKey("Language")) {
        & $DatabricksCli workspace import --profile $ProfileName --overwrite --format $notebook.Format --language $notebook.Language --file "$localFile" "$($notebook.Remote)"
    }
    else {
        & $DatabricksCli workspace import --profile $ProfileName --overwrite --format $notebook.Format --file "$localFile" "$($notebook.Remote)"
    }
    if ($LASTEXITCODE -ne 0) { throw "Import failed: $($notebook.Local)" }
}

Write-Host "Push complete."
