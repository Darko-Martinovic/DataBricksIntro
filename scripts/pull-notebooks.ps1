Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

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
    $localDir = Split-Path -Parent $localFile

    if (-not (Test-Path $localDir)) {
        New-Item -ItemType Directory -Path $localDir -Force | Out-Null
    }

    Write-Host "Exporting $($notebook.Remote) -> $($notebook.Local)"
    & $DatabricksCli workspace export --profile $ProfileName --format $notebook.Format --file "$localFile" "$($notebook.Remote)"
    # A notebook that was never pushed has no remote copy yet: keep the local one and go on
    if ($LASTEXITCODE -ne 0) { Write-Warning "Skipped $($notebook.Remote) (export failed)" }
}

Write-Host "Pull complete."
