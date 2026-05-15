# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A personal Databricks/PySpark learning workspace. The unit of work is a numbered Jupyter notebook (e.g. `03 - DataFrames.ipynb`) under `databricks-project/src/`, executed locally via **Databricks Connect** against the Azure Databricks workspace `adb-7405614283227928`. There is no application to build, no test suite, and no lint config — just notebooks plus PowerShell sync scripts.

## Local execution model (read this before running anything)

The PySpark installed in `.venv` is **Databricks Connect**, not standard PySpark. `SparkSession.builder` is blocked; you must use `DatabricksSession.builder`. Every notebook starts with this bootstrap (also extracted into `databricks-project/src/spark_setup.py`):

```python
import os, sys
try:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()
except ImportError:
    if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
        raise RuntimeError("Wrong kernel — select .venv in VSCode")
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
```

The fallback runs only when the notebook is executed *on* a Databricks cluster (where `databricks.connect` is absent and `DATABRICKS_RUNTIME_VERSION` is set). Locally, the wrong kernel raises immediately — that guard exists because the user has standard PySpark installed under system Python and a wrong-kernel selection would silently start a local JVM and hang. **Do not weaken this guard.** Add it (or import from `spark_setup`) to every new notebook.

`spark_setup.display(df, n=None)` is a local replacement for the Databricks built-in `display()` — it returns `df.toPandas()`, or `df.limit(n).toPandas()` when `n` is given. Always pass `n` for large DataFrames; without it, the call materializes the entire result. On Databricks the built-in `display` shadows it.

### Kernel & auth

- Venv Python: `.venv\Scripts\python.exe` (Python 3.11.9). The venv does **not** auto-appear in the VSCode kernel picker — use "Enter interpreter path" manually.
- Kernel config: `.venv\share\jupyter\kernels\python3\kernel.json` sets `DATABRICKS_CLI_PATH`, `DATABRICKS_AUTH_TYPE=databricks-cli`, and **must** set `DATABRICKS_METADATA_SERVICE_URL=""` to suppress metadata-service auth that the Databricks VSCode extension otherwise injects.
- `~/.databrickscfg` profile: `adb-7405614283227928`, with `serverless_compute_id = auto`.
- The Databricks CLI used by sync scripts comes from the VSCode extension bundle (`%USERPROFILE%\.vscode\extensions\databricks.databricks-*\bin\databricks.exe`). When the extension updates, the path in `kernel.json` must be updated too — extension version is part of the path.
- **Never create clusters** without explicit user permission (compute cost). Serverless via `serverless_compute_id = auto` is the default path.
- The OAuth refresh token in `~/.databrickscfg` expires (~90 days). When `databricks-cli` auth fails with *"refresh token is invalid"*, run `powershell -ExecutionPolicy Bypass -File .\scripts\databricks-login.ps1` — it auto-discovers the CLI (surviving extension upgrades) and runs `auth login --profile $ProfileName` against the profile from `sync/sync.paths.ps1`. After it returns, restart the Jupyter kernel so the next bootstrap cell picks up the new token.

## Sync to/from the Azure workspace

Notebooks and `spark_setup.py` are synced to `/Workspace/Users/darko.martinovic_outlook.com#ext#@darkomartinovicoutlook.onmicrosoft.com/<name>`. The mapping table is the **single source of truth** at `sync/sync.paths.ps1` (`$NotebookMappings`). Adding a new notebook means appending an entry there — push/pull scripts iterate over it.

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\pull-notebooks.ps1   # remote -> local
powershell -ExecutionPolicy Bypass -File .\scripts\push-notebooks.ps1   # local -> remote (--overwrite)
```

Notebook subfolders that exist locally but aren't in `sync.paths.ps1` (currently `Streaming/`, `SCD/`, `Functions/`, `LakeFlow Jobs/`, `Optimization/`) are local-only until added to the mapping. Format is `JUPYTER` for `.ipynb`; `SOURCE` + `Language=PYTHON` for `.py` like `spark_setup.py`. Any `.py` synced as `SOURCE`/`PYTHON` must start with the `# Databricks notebook source` magic header — without it, the workspace treats the upload as a plain file rather than a notebook.

`sync.paths.ps1` also defines `$ProfileName` (currently `adb-7405614283227928` — same string as the workspace ID), which every push/pull script dot-sources to pick the `~/.databrickscfg` profile to authenticate against.

For the read-only shared Databricks Academy folder (`/Workspace/Shared/Azure-Databricks-and-Spark-SQL-Python`), use the subfolder scripts which take a `-SubPath` and default `-LocalRoot shared-selected`:

```powershell
.\scripts\pull-shared-subfolder.ps1 -SubPath "Section 08/05 Reading CSV Data into DataFrames"
.\scripts\push-shared-subfolder.ps1 -SubPath "..."
```

The legacy `sync/` folder at the repo root holds older imports of that shared content (read-only reference material), separate from the active `databricks-project/`.

The `databricks.yml` bundle config exists but the bundle workflow is not the primary path — the PowerShell scripts are.

## Repo layout (orientation)

- `databricks-project/src/` — active notebooks, numbered roughly by topic order. Subfolders group multi-notebook topics (`Streaming/`, `SCD/`, `Functions/`, `LakeFlow Jobs/`, `Optimization/`). `Functions/PLAN.md` documents that subfolder's notebook roadmap.
- `databricks-project/.databricks-sync-ignore` and the root `.databricks-sync-ignore` whitelist `databricks-project/src/**` for the Databricks VSCode extension's auto-sync (separate from the PowerShell scripts above).
- `scripts/` — PowerShell sync entry points.
- `sync/sync.paths.ps1` — dot-sourced by every script; defines `$ProfileName`, `$RemoteBase`, `$NotebookMappings`.
- `sync/shared/` — older, frozen copy of shared course material (kept for reference, not actively synced).
- `_archive/` — gitignored historical snapshots; do not edit.

## Working in notebooks

- Always import functions as `from pyspark.sql import functions as F` — this convention is followed throughout and referenced in `Functions/PLAN.md`.
- When adding a notebook in an existing series, follow the numbering (`NN - Topic.ipynb`) and place it in the matching subfolder. After creating it, add the mapping entry to `sync/sync.paths.ps1` if it should sync to the workspace.
- Datasets live under Unity Catalog volumes like `/Volumes/population_metrics/landing/datasets/...` — reuse paths from `02 - Explore DataSets.ipynb` rather than introducing new ones.
- `spark_setup.py` is a chokepoint — every notebook either copies its bootstrap inline or imports from it. Changes to the bootstrap or to `display()` affect the whole repo. When modifying it, spot-check at least one notebook from each subfolder (`Streaming/`, `SCD/`, `Functions/`) before declaring done; a regression here breaks every notebook.

### Streaming notebooks (serverless constraints)

The `Streaming/` notebooks run against the same serverless compute as the rest of the repo (`serverless_compute_id = auto`). Two constraints surface there that don't apply to batch notebooks — both are documented inside `Streaming/02 - Streaming Sources and Auto Loader.ipynb` and followed throughout the subfolder:

- **`trigger(processingTime=...)` is rejected on serverless writeStream.** Use `trigger(availableNow=True)` for finite-trigger demos. Continuous streams generally aren't viable in this workspace either — every notebook in the series uses `availableNow`.
- **Checkpoint locations must live on a Unity Catalog volume** (`/Volumes/...`), not `/tmp/` or `dbfs:/`. Demo cells append a per-run uuid suffix (`_{uuid.uuid4().hex[:8]}`) so re-running the cell doesn't collide with a prior checkpoint — copy that pattern when adding new streaming examples.

These constraints tend to bite one at a time when changing trigger mode or checkpoint paths. Account for both together when editing a streaming cell, rather than fixing the trigger and re-running into a checkpoint error on the next try.
