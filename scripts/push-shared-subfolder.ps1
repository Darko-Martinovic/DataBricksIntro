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

. "$PSScriptRoot/sync.paths.ps1"

$sharedRoot = "/Workspace/Shared/Azure-Databricks-and-Spark-SQL-Python"
$normalizedSubPath = $SubPath.Trim('/').Replace('\\', '/')
$remotePath = "$sharedRoot/$normalizedSubPath"
$localPath = Join-Path $root (Join-Path $LocalRoot $normalizedSubPath)

if (-not (Test-Path $localPath)) {
    throw "Local path does not exist: $localPath"
}

Write-Host "Importing $localPath -> $remotePath"
& $DatabricksCli workspace import-dir --profile $ProfileName --overwrite "$localPath" "$remotePath"
Write-Host "Shared subfolder push complete."
