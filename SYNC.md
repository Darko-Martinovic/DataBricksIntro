# Databricks Sync Setup

## Dedicated synced folder

- Synced project folder: `databricks-project/`
- Notebook allowed for sync: `databricks-project/src/Introduction to DataBricks.py`
- Remote notebook target: `/Workspace/Users/darko.martinovic_outlook.com#ext#@darkomartinovicoutlook.onmicrosoft.com/Introduction to DataBricks`

## Ignore/include guards

- `.databricks-sync-ignore` is configured to ignore everything except `src/Introduction to DataBricks.py`.
- `.databricks/project.json` includes only `src/Introduction to DataBricks.py`.

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
- `push-notebooks.ps1` uses `--overwrite` and updates only `Introduction to DataBricks` in your user workspace.
- Keep large datasets, build artifacts, and unrelated files outside `databricks-project/`.
