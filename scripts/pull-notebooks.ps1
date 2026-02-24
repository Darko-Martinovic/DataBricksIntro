Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

. "$root/sync/sync.paths.ps1"

foreach ($map in $Mappings) {
    $localPath = Join-Path $root $map.Local
    if (-not (Test-Path $localPath)) {
        New-Item -ItemType Directory -Path $localPath | Out-Null
    }

    Write-Host "Exporting $($map.Remote) -> $($map.Local)"
    databricks workspace export-dir --profile $ProfileName --overwrite "$($map.Remote)" "$localPath"
}

Write-Host "Pull complete."
