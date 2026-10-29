# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A personal Databricks/PySpark learning workspace. The unit of work is a numbered Jupyter notebook (e.g. `03 - DataFrames.ipynb`) under `databricks-project/src/`, executed locally via **Databricks Connect** against the Azure Databricks workspace `adb-7405614283227928`. There is no application to build and no test suite — just notebooks, PowerShell sync scripts, and an `nbstripout` pre-commit hook (`.pre-commit-config.yaml`) that commits notebooks without outputs.

## Local execution model (read this before running anything)

The PySpark installed in `.venv` is **Databricks Connect**, not standard PySpark. `SparkSession.builder` is blocked; you must use `DatabricksSession.builder`. The session is created in `databricks-project/src/spark_setup.py`, and the first code cell of every notebook is:

```python
import os
if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
    from spark_setup import spark, display  # local run through Databricks Connect

DATA_ROOT = "/Volumes/population_metrics/landing/datasets"   # only in notebooks that read the volume
```

On a Databricks cluster (`DATABRICKS_RUNTIME_VERSION` is set) the import is skipped and the built-in `spark` and `display()` are used. Locally, `spark_setup` raises immediately on the wrong kernel — that guard exists because the user has standard PySpark installed under system Python and a wrong-kernel selection would silently start a local JVM and hang. **Do not weaken this guard.** Start every new notebook with this cell; don't copy the session code inline.

Notebooks in subfolders import `spark_setup` too: `.vscode/settings.json` sets `jupyter.notebookFileRoot` to `databricks-project/src`, so every kernel starts there.

`spark_setup.display(df, n=1000)` is a local replacement for the Databricks built-in `display()` — it renders `df.limit(n).toPandas()` as a table wherever it is called in a cell. On Databricks the built-in `display` is used.

### Kernel & auth

- Venv Python: `.venv\Scripts\python.exe` (Python 3.11.9). The venv does **not** auto-appear in the VSCode kernel picker — use "Enter interpreter path" manually.
- Kernel config: `.venv\share\jupyter\kernels\python3\kernel.json` sets `DATABRICKS_CLI_PATH`, `DATABRICKS_AUTH_TYPE=databricks-cli`, and **must** set `DATABRICKS_METADATA_SERVICE_URL=""` to suppress metadata-service auth that the Databricks VSCode extension otherwise injects.
- `~/.databrickscfg` profile: `adb-7405614283227928`, with `serverless_compute_id = auto`.
- The Databricks CLI used by sync scripts comes from the VSCode extension bundle (`%USERPROFILE%\.vscode\extensions\databricks.databricks-*\bin\databricks.exe`). When the extension updates, the path in `kernel.json` must be updated too — extension version is part of the path.
- **Never create clusters** without explicit user permission (compute cost). Serverless via `serverless_compute_id = auto` is the default path.
- The OAuth refresh token in `~/.databrickscfg` expires (~90 days). When `databricks-cli` auth fails with *"refresh token is invalid"*, run `powershell -ExecutionPolicy Bypass -File .\scripts\databricks-login.ps1` — it auto-discovers the CLI (surviving extension upgrades) and runs `auth login --profile $ProfileName` against the profile from `scripts/sync.paths.ps1`. After it returns, restart the Jupyter kernel so the next bootstrap cell picks up the new token.

## Sync to/from the Azure workspace

Notebooks and `spark_setup.py` are synced to `/Workspace/Users/darko.martinovic_outlook.com#ext#@darkomartinovicoutlook.onmicrosoft.com/<name>`. The mapping is built in `scripts/sync.paths.ps1` (`$NotebookMappings`): every `.ipynb` under `databricks-project/src/` maps to the same relative path under `$RemoteBase` (subfolders included, extension dropped), plus `spark_setup.py`. A new notebook syncs without any change there.

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\pull-notebooks.ps1   # remote -> local
powershell -ExecutionPolicy Bypass -File .\scripts\push-notebooks.ps1   # local -> remote (--overwrite)
```

`push-notebooks.ps1` creates the remote subfolders before importing. Format is `JUPYTER` for `.ipynb`; `SOURCE` + `Language=PYTHON` for `.py` like `spark_setup.py`. Any `.py` synced as `SOURCE`/`PYTHON` must start with the `# Databricks notebook source` magic header — without it, the workspace treats the upload as a plain file rather than a notebook.

`scripts/sync.paths.ps1` also defines `$ProfileName` (currently `adb-7405614283227928` — same string as the workspace ID), which every push/pull script dot-sources to pick the `~/.databrickscfg` profile to authenticate against.

For the shared course folder (`/Workspace/Shared/Azure-Databricks-and-Spark-SQL-Python`, the Udemy course's material), use the subfolder scripts which take a `-SubPath` and default `-LocalRoot shared-selected`:

```powershell
.\scripts\pull-shared-subfolder.ps1 -SubPath "Section 08/05 Reading CSV Data into DataFrames"
.\scripts\push-shared-subfolder.ps1 -SubPath "..."
```

`shared-selected/` is gitignored: the repo is public and the course material must never be committed.

The `databricks.yml` bundle config exists but the bundle workflow is not the primary path — the PowerShell scripts are.

## Repo layout (orientation)

- `databricks-project/src/` — active notebooks, numbered roughly by topic order. Subfolders group multi-notebook topics (`Streaming/`, `SCD/`, `Functions/`, `LakeFlow Jobs/`, `Optimization/`). `Functions/PLAN.md` documents that subfolder's notebook roadmap.
- `databricks-project/.databricks-sync-ignore` and the root `.databricks-sync-ignore` whitelist `databricks-project/src/**` for the Databricks VSCode extension's auto-sync (separate from the PowerShell scripts above).
- `scripts/` — PowerShell sync entry points, and `sync.paths.ps1`, dot-sourced by every script; defines `$ProfileName`, `$RemoteBase`, `$NotebookMappings`.
- `data/csv_data/` — the countries CSVs that notebooks read from the volume (uploaded once; see the README).
- `requirements.txt` — the venv's packages, with `databricks-connect` pinned to the serverless version.
- `_archive/` — gitignored historical snapshots; do not edit.

## Working in notebooks

- Always import functions as `from pyspark.sql import functions as F` — this convention is followed throughout and referenced in `Functions/PLAN.md`.
- When adding a notebook in an existing series, follow the numbering (`NN - Topic.ipynb`) and place it in the matching subfolder. The sync scripts pick it up automatically.
- Datasets live under the Unity Catalog volume `DATA_ROOT` (`/Volumes/population_metrics/landing/datasets`). Build paths as `f"{DATA_ROOT}/..."`, reusing the existing folders (`countries_dataset/countries_dataset/csv_data/...`, `streaming_demo/...`) rather than introducing new ones. Use two-part table names (`default.country_population`) or `current_catalog()`, never the workspace's catalog name.
- Notebooks are committed without outputs (the pre-commit hook strips them); run `pre-commit install` once per clone.
- `spark_setup.py` is a chokepoint — every notebook imports from it. Changes to the bootstrap or to `display()` affect the whole repo. When modifying it, spot-check at least one notebook from each subfolder (`Streaming/`, `SCD/`, `Functions/`) before declaring done; a regression here breaks every notebook.

### Streaming notebooks (serverless constraints)

The `Streaming/` notebooks run against the same serverless compute as the rest of the repo (`serverless_compute_id = auto`). Two constraints surface there that don't apply to batch notebooks — both are documented inside `Streaming/02 - Streaming Sources and Auto Loader.ipynb` and followed throughout the subfolder:

- **`trigger(processingTime=...)` is rejected on serverless writeStream.** Use `trigger(availableNow=True)` for finite-trigger demos. Continuous streams generally aren't viable in this workspace either — every notebook in the series uses `availableNow`.
- **Checkpoint locations must live on a Unity Catalog volume** (`/Volumes/...`), not `/tmp/` or `dbfs:/`. Demo cells append a per-run uuid suffix (`_{uuid.uuid4().hex[:8]}`) so re-running the cell doesn't collide with a prior checkpoint — copy that pattern when adding new streaming examples.

These constraints tend to bite one at a time when changing trigger mode or checkpoint paths. Account for both together when editing a streaming cell, rather than fixing the trigger and re-running into a checkpoint error on the next try.

### Pandas UDFs and `foreachBatch` need matching client/server Python (Databricks Connect)

Cells that ship Python code to be executed on the cluster — `@pandas_udf`, `@udf`, `foreachBatch(fn)`, `foreach(...)` — fail with `[PYTHON_VERSION_MISMATCH] Python versions in the Spark Connect client and server are different` when the local venv's Python *minor* version (e.g. 3.11) doesn't match the serverless compute environment's. Catalyst built-ins (`F.when`, `F.create_map`, `F.from_json`, etc.) are evaluated server-side without shipping Python and are unaffected.

Symptoms and rule of thumb:
- Non-UDF streaming cells (filter, projection, JSON parse, explode) succeed; UDF / `foreachBatch` cells fail with the same error.
- The error names the constraint but not the server's Python version. Probe it with one cell: `import sys; print(sys.version); spark.sql("SELECT current_version()").show(truncate=False)` — the Spark version maps to a Databricks Runtime / Serverless environment, which maps to a Python version per the Databricks release notes.
- When unavoidable on serverless: prefer Catalyst built-ins, or wrap the UDF/`foreachBatch` cell in `try/except SparkConnectGrpcException` so the rest of the notebook keeps running. The repo already does this in `Streaming/03 - Streaming Transformations.ipynb` for the `foreachBatch` demo.
