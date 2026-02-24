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

## Optional: sync one Shared subfolder

Pull one Shared subfolder to `shared-selected/<SubPath>`:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\pull-shared-subfolder.ps1 -SubPath "Section 08"
```

Push that subfolder back:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\push-shared-subfolder.ps1 -SubPath "Section 08"
```

## Notes

- Notebooks are exported as source files (e.g. `.py`).
- `push-notebooks.ps1` uses `--overwrite` and updates existing workspace content.
- Some shared workspace folders can fail on Windows if remote names contain trailing spaces; this is a Windows filesystem limitation.
- The optional Shared sync scripts are best used with specific safe subfolders (for example `Section 08`, `Section 09`, etc.).
