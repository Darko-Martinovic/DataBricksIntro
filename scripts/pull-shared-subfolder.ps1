Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

param(
    [Parameter(Mandatory = $true)]
    [string]$SubPath,

    [string]$LocalRoot = "shared-selected"
)

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

. "$root/sync/sync.paths.ps1"

$sharedRoot = "/Workspace/Shared/Azure-Databricks-and-Spark-SQL-Python"
$normalizedSubPath = $SubPath.Trim('/').Replace('\\', '/')
$remotePath = "$sharedRoot/$normalizedSubPath"
$localPath = Join-Path $root (Join-Path $LocalRoot $normalizedSubPath)

if (-not (Test-Path $localPath)) {
    New-Item -ItemType Directory -Path $localPath -Force | Out-Null
}

Write-Host "Exporting $remotePath -> $localPath"
databricks workspace export-dir --profile $ProfileName --overwrite "$remotePath" "$localPath"
Write-Host "Shared subfolder pull complete."
