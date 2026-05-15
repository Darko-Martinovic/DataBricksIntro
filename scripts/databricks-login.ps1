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

. "$root/sync/sync.paths.ps1"

Write-Host "Refreshing OAuth token for profile '$ProfileName' via $DatabricksCli"
Write-Host "A browser window will open — sign in to the Databricks workspace to complete login."
& $DatabricksCli auth login --profile $ProfileName
