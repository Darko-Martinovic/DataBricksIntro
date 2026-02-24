Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

. "$root/sync/sync.paths.ps1"

foreach ($map in $Mappings) {
    $localPath = Join-Path $root $map.Local
    if (-not (Test-Path $localPath)) {
        Write-Warning "Skipping missing local path: $($map.Local)"
        continue
    }

    Write-Host "Importing $($map.Local) -> $($map.Remote)"
    databricks workspace import-dir --profile $ProfileName --overwrite "$localPath" "$($map.Remote)"
}

Write-Host "Push complete."
