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
    throw "Local path does not exist: $localPath"
}

Write-Host "Importing $localPath -> $remotePath"
databricks workspace import-dir --profile $ProfileName --overwrite "$localPath" "$remotePath"
Write-Host "Shared subfolder push complete."
