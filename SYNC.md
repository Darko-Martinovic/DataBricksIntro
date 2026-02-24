# Databricks Sync Setup

## Current default mapping

- Remote: `/Workspace/Users/darko.martinovic_outlook.com#ext#@darkomartinovicoutlook.onmicrosoft.com`
- Local: `u`

## Pull existing notebooks from Azure Databricks

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\pull-notebooks.ps1
```

## Push local edits back to Azure Databricks

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\push-notebooks.ps1
```

## Notes

- Notebooks are exported as source files (e.g. `.py`).
- `push-notebooks.ps1` uses `--overwrite` and updates existing workspace content.
- Some shared workspace folders can fail on Windows if remote names contain trailing spaces; this is a Windows filesystem limitation.
