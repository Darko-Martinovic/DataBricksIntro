param(
    [Parameter(Mandatory = $true)]
    [string]$SubPath,

    [string]$LocalRoot = "shared-selected"
)

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

. "$root/sync/sync.paths.ps1"

$sharedRoot = "/Workspace/Shared/Azure-Databricks-and-Spark-SQL-Python"
$normalizedSubPath = $SubPath.Trim('/').Replace('\\', '/')
$remotePath = "$sharedRoot/$normalizedSubPath"
$localPath = Join-Path $root (Join-Path $LocalRoot $normalizedSubPath)

if (-not (Test-Path $localPath)) {
    New-Item -ItemType Directory -Path $localPath -Force | Out-Null
}

Write-Host "Exporting $remotePath -> $localPath"
& $DatabricksCli workspace export-dir --profile $ProfileName --overwrite "$remotePath" "$localPath"
Write-Host "Shared subfolder pull complete."
