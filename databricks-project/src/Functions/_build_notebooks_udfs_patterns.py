"""Generate notebooks 11-17: UDFs and patterns."""
import json, pathlib

OUT = pathlib.Path(__file__).parent

BOOTSTRAP = '''import os, sys

try:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()
except ImportError:
    if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
        raise RuntimeError(
            f"Wrong Python kernel \\u2014 'databricks.connect' not found in:\\n  {sys.executable}\\n\\n"
            "In VSCode: click the kernel selector (top-right of notebook) "
            "and choose the interpreter at:\\n"
            r"  C:\\Source\\Dsa\\DataBricks\\.venv\\Scripts\\python.exe"
        )
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

from pyspark.sql import functions as F


def display(df, n=20):
    df.show(n, truncate=False)'''


def md(text): return {"cell_type": "markdown", "metadata": {}, "source": text.splitlines(keepends=True)}
def code(text): return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": text.splitlines(keepends=True)}


META = {
    "kernelspec": {"display_name": ".venv (3.11.9)", "language": "python", "name": "python3"},
    "language_info": {
        "codemirror_mode": {"name": "ipython", "version": 3},
        "file_extension": ".py", "mimetype": "text/x-python", "name": "python",
        "nbconvert_exporter": "python", "pygments_lexer": "ipython3", "version": "3.11.9",
    },
}


def write_nb(filename, cells):
    nb = {"cells": cells, "metadata": META, "nbformat": 4, "nbformat_minor": 5}
    (OUT / filename).write_text(json.dumps(nb, indent=1, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# 11 - Python UDFs
# ---------------------------------------------------------------------------
nb11 = [
    md("""### Links and Resources
| Concept | Description |
|---|---|
| [`pyspark.sql.functions.udf`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html) | Wrap a Python function as a Spark column expr |
| [UDF programming guide](https://spark.apache.org/docs/latest/sql-ref-functions-udf-scalar.html) | Scalar UDF reference |
| [Arrow-optimized Python UDFs](https://docs.databricks.com/en/udf/python-arrow.html) | New: opt into Arrow batching for plain UDFs |
| [Why UDFs are slow](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html) | Serialization cost explained |"""),
    code(BOOTSTRAP),
    md("""# Python UDFs

A Python UDF wraps a regular Python function so it can be used as a Spark column expression. It's the **escape hatch** for logic that has no built-in equivalent: domain rules, third-party libraries, custom regex parsers.

The catch is performance. For every row, Spark has to:

1. Serialize the columns from the JVM to a Python worker.
2. Run the Python function.
3. Serialize the result back.

That's ~5–50× slower than a built-in. **Try a built-in first; reach for a UDF only when you must.**"""),
    code('''df = spark.createDataFrame([
    (1, "Alice",   "alice@example.com",  None),
    (2, "BOB",     "bob@example.org",    "1990-05-15"),
    (3, "Charlie", "charlie@x.io",       "1985-12-03"),
    (4, "Dana",    None,                  "1992-08-21"),
    (5, "  erin ", "ERIN@example.com",   "2000-01-01"),
], ["id", "name", "email", "dob"])
display(df)'''),
    md("""## Defining a UDF — three styles

All three produce the same thing. Pick whichever reads best."""),
    code('''from pyspark.sql.types import StringType, IntegerType

# Style 1: F.udf wrapping an existing function
def domain_of(email):
    if email is None:
        return None
    return email.lower().split("@")[-1]

domain_udf = F.udf(domain_of, StringType())

# Style 2: F.udf as a decorator with explicit type
@F.udf(StringType())
def name_clean(name):
    if name is None:
        return None
    return name.strip().title()

# Style 3: Register with spark.udf for SQL use
spark.udf.register("domain_sql", domain_of, StringType())

df.select(
    name_clean("name").alias("clean"),
    domain_udf("email").alias("domain"),
).show()'''),
    md("""## NULL handling — your problem, not Spark's

Built-in functions usually propagate NULL automatically. UDFs do not. **Your function will be called with `None` arguments** unless you guard or annotate with `@udf(returnType, useArrow=...)` etc. Forgetting to handle `None` is the #1 UDF bug."""),
    code('''@F.udf(IntegerType())
def name_length_unsafe(name):
    return len(name)               # raises on None

@F.udf(IntegerType())
def name_length_safe(name):
    return None if name is None else len(name)

# The unsafe one will fail at execution
try:
    df.select(name_length_unsafe("email")).show()
except Exception as e:
    print("crashed:", type(e).__name__)

df.select("email", name_length_safe("email").alias("len")).show()'''),
    md("""## Return types — strict, not inferred

Spark needs the return type at registration. If your function returns the wrong type, you'll get a confusing error or NULL. Use the `pyspark.sql.types` classes:

| Python | Spark type |
|---|---|
| `int` | `IntegerType()` / `LongType()` |
| `float` | `DoubleType()` |
| `str` | `StringType()` |
| `bool` | `BooleanType()` |
| `list[T]` | `ArrayType(T)` |
| `dict` | `MapType(K, V)` |
| `dataclass` / tuple | `StructType([...])` |"""),
    code('''from pyspark.sql.types import ArrayType, StructType, StructField

@F.udf(ArrayType(StringType()))
def split_words(s):
    return None if s is None else s.lower().split()

@F.udf(StructType([
    StructField("year",  IntegerType()),
    StructField("month", IntegerType()),
    StructField("day",   IntegerType()),
]))
def parse_dob(s):
    if s is None:
        return None
    y, m, d = s.split("-")
    return (int(y), int(m), int(d))

df.select(
    "name",
    split_words("name").alias("words"),
    parse_dob("dob").alias("parts"),
).show(truncate=False)'''),
    md("""## Using a UDF in SQL

Registered UDFs (`spark.udf.register`) are reachable from `spark.sql(...)`. Useful when the rest of the query is more readable in SQL."""),
    code('''df.createOrReplaceTempView("people")

spark.sql("""
SELECT id, name, email, domain_sql(email) AS domain
FROM people
WHERE email IS NOT NULL
""").show()'''),
    md("""## Determinism flag

By default Spark assumes UDFs are *deterministic* and may cache or reorder calls. If your UDF reads a clock, calls a service, or uses randomness, mark it non-deterministic:"""),
    code('''import random

@F.udf(IntegerType())
def coin_flip():
    return random.randint(0, 1)

# Without this flag, Spark may evaluate it once per partition and reuse
coin_flip = coin_flip.asNondeterministic()

df.select("name", coin_flip().alias("flip")).show()'''),
    md("""## Arrow-optimized Python UDFs (Databricks Runtime 14.3+)

A newer middle ground: keep the simple `@udf` decorator, but Spark uses Arrow batching under the hood. Often 2–3× faster than classic UDFs with zero code change.

```python
@F.udf(StringType(), useArrow=True)
def domain_of_arrow(email):
    return None if email is None else email.split("@")[-1]
```

Notebook 12 covers the more powerful **Pandas UDFs**, which are vectorized end-to-end."""),
    md("""## When *not* to use a UDF

| You wrote | Use this built-in instead |
|---|---|
| `lambda s: s.upper()` | `F.upper("s")` |
| `lambda s: s.split("@")[-1]` | `F.substring_index("s", "@", -1)` |
| `lambda x: round(x, 2)` | `F.round("x", 2)` |
| `lambda d: d.year` | `F.year("d")` |
| `lambda x: None if x is None else x * 2` | `F.col("x") * 2` (NULL propagates automatically) |

Quick rule: if you can express it with `F.*`, do."""),
    md("""## Takeaways

- UDFs are 5–50× slower than built-ins because of JVM↔Python serialization. Use them only when no built-in fits.
- You **must** register the return type and you **must** handle `None`.
- Use `spark.udf.register` to expose the function to SQL.
- Mark non-deterministic UDFs with `.asNondeterministic()` so Spark doesn't cache calls.
- On modern DBR, the `useArrow=True` flag gives a free speedup.
- For heavy logic, jump straight to **Pandas UDFs** (notebook 12)."""),
]
write_nb("11 - Python UDFs.ipynb", nb11)


# ---------------------------------------------------------------------------
# 12 - Pandas UDFs (Vectorized)
# ---------------------------------------------------------------------------
nb12 = [
    md("""### Links and Resources
| Concept | Description |
|---|---|
| [`pandas_udf`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html) | Decorator for vectorized UDFs |
| [Pandas Function APIs](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html) | `applyInPandas`, `mapInPandas`, `applyInPandasWithState` |
| [Apache Arrow](https://arrow.apache.org/) | Columnar interchange between JVM ↔ Python |
| [PySpark Arrow guide](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html) | Tuning + caveats |"""),
    code(BOOTSTRAP),
    md("""# Pandas UDFs (Vectorized)

A Pandas UDF receives a **batch** of rows as a `pd.Series` (or `pd.DataFrame`) and returns the same. Spark uses **Apache Arrow** to move data zero-copy between the JVM and Python, then your function runs on a real numpy array, not row-by-row Python objects.

End result: Pandas UDFs are typically **10–100× faster** than plain Python UDFs and within 2–3× of pure Spark built-ins. They're the right choice for:

- Custom math that can be expressed in numpy / pandas / scipy.
- Calling sklearn / xgboost / hugging-face models on every row.
- Anything where you'd otherwise reach for a plain UDF on >1M rows."""),
    code('''import pandas as pd
import numpy as np

df = spark.range(1_000_000).select(
    F.col("id").alias("x"),
    (F.rand(seed=1) * 100).alias("y"),
)
df.show(3)'''),
    md("""## Three flavours

| Type | Input → Output | Use for |
|---|---|---|
| **Series → Series** (Scalar) | `pd.Series` → `pd.Series`, same length | Element-wise transforms |
| **Iterator** | `Iterator[pd.Series]` → `Iterator[pd.Series]` | Expensive setup (load model once per executor) |
| **GroupedMap / Grouped Agg** | `pd.DataFrame` per group → `pd.DataFrame` / scalar | Per-group logic that doesn't fit `groupBy.agg(...)` |"""),
    md("""## Scalar Pandas UDF — Series → Series

The decorator's argument is the return type. Inside, you operate on the full vector — never a single value."""),
    code('''from pyspark.sql.types import DoubleType

@F.pandas_udf(DoubleType())
def normalize(s: pd.Series) -> pd.Series:
    return (s - s.mean()) / s.std()       # vectorized — no loop

df.select("y", normalize("y").alias("y_norm")).show(5)'''),
    md("""## Multiple input columns"""),
    code('''@F.pandas_udf(DoubleType())
def hypot(a: pd.Series, b: pd.Series) -> pd.Series:
    return pd.Series(np.hypot(a.values, b.values))

df.select(hypot("x", "y").alias("h")).show(5)'''),
    md("""## Iterator Pandas UDF — load expensive resources once per executor

If your UDF needs an ML model, a regex compiled with a 50ms cost, or any per-call setup, the iterator form lets you initialize **once per Python worker** and reuse across batches."""),
    code('''from typing import Iterator

@F.pandas_udf(DoubleType())
def expensive_score(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # Imagine: model = load_my_model("s3://...")
    weights = np.array([0.5, 0.3, 0.2])   # set up once per worker
    for batch in it:
        yield pd.Series(np.minimum(batch.values * weights[0], 100.0))

df.select(expensive_score("y").alias("score")).show(5)'''),
    md("""## Grouped Map — `applyInPandas`

When you need *one DataFrame in, one DataFrame out, per group*. Useful for per-group ML, sorting + ranking, or any logic that doesn't decompose into independent aggregates.

You must declare an **output schema** as a string or `StructType`."""),
    code('''sales = spark.createDataFrame([
    ("EMEA", "FR", 100.0),
    ("EMEA", "FR", 150.0),
    ("EMEA", "BE", 200.0),
    ("AMER", "US", 50.0),
    ("AMER", "US", 75.0),
    ("AMER", "CA", 300.0),
] * 50, ["region", "country", "amount"])

def normalize_per_region(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["amount_z"] = (pdf["amount"] - pdf["amount"].mean()) / pdf["amount"].std()
    return pdf

out_schema = "region string, country string, amount double, amount_z double"
sales.groupBy("region").applyInPandas(normalize_per_region, schema=out_schema).show(6)'''),
    md("""## Grouped Aggregate — Series → scalar

Like a built-in agg (`avg`, `sum`) but with custom logic. Use `@F.pandas_udf(returnType)` and accept a `pd.Series`, return a scalar."""),
    code('''@F.pandas_udf(DoubleType())
def trimmed_mean(s: pd.Series) -> float:
    if len(s) < 3:
        return float(s.mean())
    s = s.sort_values()
    return float(s.iloc[1:-1].mean())   # drop min and max

sales.groupBy("region").agg(
    trimmed_mean("amount").alias("trimmed_avg"),
    F.avg("amount").alias("plain_avg"),
).show()'''),
    md("""## Map in Pandas — `mapInPandas`

For when you want batches but no grouping. Receives an iterator of `pd.DataFrame`, returns the same. Output schema declared explicitly."""),
    code('''def add_label(it: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for batch in it:
        batch["label"] = np.where(batch["amount"] > 100, "big", "small")
        yield batch

mapped_schema = "region string, country string, amount double, label string"
sales.mapInPandas(add_label, schema=mapped_schema).show(6)'''),
    md("""## Memory & batch size

- **`spark.sql.execution.arrow.maxRecordsPerBatch`** (default 10,000) controls how many rows go to Python at a time. Tune down for memory-heavy UDFs, up for cheap ones.
- Each batch is a *pandas DataFrame in worker memory* — sizing matters.
- For grouped-map: **the entire group must fit in one worker**. If a region has 100M rows, this is the wrong tool; use a regular `groupBy.agg` plus higher-order functions instead."""),
    code('''print("current batch size:", spark.conf.get("spark.sql.execution.arrow.maxRecordsPerBatch"))'''),
    md("""## Type matching cheatsheet

The numpy/pandas dtype must match the Spark type:

| Spark | pandas |
|---|---|
| `IntegerType` | `int32` |
| `LongType` | `int64` |
| `DoubleType` | `float64` |
| `BooleanType` | `bool` |
| `StringType` | `object` (Python str) |
| `TimestampType` | `datetime64[ns]` |
| `ArrayType(T)` | `object` (list of T) |

Mismatches are a common source of `ArrowTypeError`."""),
    md("""## Takeaways

- Pandas UDFs are vectorized via Arrow — typically 10–100× a plain Python UDF.
- Three flavours: scalar (`Series → Series`), iterator (with reusable setup), grouped (`applyInPandas`, grouped agg, `mapInPandas`).
- Inside the function, you have full numpy/pandas/scipy/sklearn access.
- Grouped-map requires the **entire group** to fit in one worker.
- When in doubt, profile: built-in > pandas UDF > Arrow-Python UDF > plain Python UDF."""),
]
write_nb("12 - Pandas UDFs (Vectorized).ipynb", nb12)


# ---------------------------------------------------------------------------
# 13 - SQL UDFs (Unity Catalog)
# ---------------------------------------------------------------------------
nb13 = [
    md("""### Links and Resources
| Concept | Description |
|---|---|
| [`CREATE FUNCTION` (SQL)](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html) | UC SQL UDF DDL |
| [User-defined functions in UC](https://docs.databricks.com/en/udf/unity-catalog.html) | Governance & permissions |
| [Routine privileges](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#functions) | `EXECUTE` grants |"""),
    code(BOOTSTRAP),
    md("""# SQL UDFs in Unity Catalog

A **SQL UDF** is a function whose body is one or more SQL expressions. Once registered in Unity Catalog under a `catalog.schema`, it lives forever (no need to redefine per notebook), is governable with GRANT/REVOKE, and is callable from any SQL or PySpark context that can see it.

Compared to Python UDFs, SQL UDFs:

- **Run inside Catalyst** — no Python worker, no serialization, often as fast as a built-in.
- **Have no NULL trap** — they propagate NULL like every other SQL expression.
- **Are governable** — you grant `EXECUTE` per principal.
- **Are reusable** — define once, share across notebooks/jobs/dashboards/BI tools.

The trade-off: the body must be a SQL expression. No loops, no Python libraries, no model calls."""),
    md("""## Choose a catalog and schema

Set up a workspace where you have `CREATE FUNCTION` privilege. Adjust to your environment."""),
    code('''CATALOG = "main"          # change to your catalog
SCHEMA  = "default"       # change to your schema

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA  {SCHEMA}")
print("using:", CATALOG, SCHEMA)'''),
    md("""## Scalar SQL UDF — single expression

The simplest form: `RETURNS <type> RETURN <expr>`."""),
    code('''spark.sql("""
CREATE OR REPLACE FUNCTION fmt_money(amount DOUBLE, currency STRING)
RETURNS STRING
RETURN concat(currency, ' ', format_number(amount, 2))
""")

spark.sql("SELECT fmt_money(1234.5, 'EUR') AS s").show()'''),
    md("""## With NULL handling and a default

NULL propagates naturally — no Python-style `is None` guard needed."""),
    code('''spark.sql("""
CREATE OR REPLACE FUNCTION grade(score INT)
RETURNS STRING
RETURN CASE
         WHEN score IS NULL THEN 'unknown'
         WHEN score >= 90  THEN 'A'
         WHEN score >= 75  THEN 'B'
         WHEN score >= 60  THEN 'C'
         ELSE 'F'
       END
""")

spark.sql("SELECT grade(85) AS a, grade(NULL) AS b, grade(50) AS c").show()'''),
    md("""## Use it in a query — alongside built-ins"""),
    code('''people = spark.createDataFrame([
    (1, "Alice", 92), (2, "Bob", 73), (3, "Carol", None), (4, "Dana", 55),
], ["id", "name", "score"])
people.createOrReplaceTempView("people")

spark.sql("SELECT id, name, score, grade(score) AS g FROM people").show()'''),
    md("""## Functions can call other functions"""),
    code('''spark.sql("""
CREATE OR REPLACE FUNCTION net_amount(gross DOUBLE, vat_rate DOUBLE)
RETURNS DOUBLE
RETURN gross / (1 + vat_rate)
""")

spark.sql("""
CREATE OR REPLACE FUNCTION net_label(gross DOUBLE, vat_rate DOUBLE, currency STRING)
RETURNS STRING
RETURN fmt_money(net_amount(gross, vat_rate), currency)
""")

spark.sql("SELECT net_label(121.0, 0.21, 'EUR') AS net").show()'''),
    md("""## Table-valued SQL UDFs

`RETURNS TABLE` lets a function be used as a virtual table. Powerful for parameterized views."""),
    code('''spark.sql("""
CREATE OR REPLACE FUNCTION top_scorers(min_score INT)
RETURNS TABLE (id INT, name STRING, score INT)
RETURN SELECT id, name, score FROM people WHERE score >= min_score
""")

spark.sql("SELECT * FROM top_scorers(75)").show()'''),
    md("""## Discoverability — list & describe"""),
    code('''spark.sql(f"SHOW FUNCTIONS IN {CATALOG}.{SCHEMA} LIKE 'grade'").show(truncate=False)
spark.sql(f"DESCRIBE FUNCTION EXTENDED {CATALOG}.{SCHEMA}.grade").show(truncate=False)'''),
    md("""## Permissions — `GRANT EXECUTE`

UC functions are governed objects. By default, only the owner can call them. To share:

```sql
GRANT EXECUTE ON FUNCTION main.default.grade TO `data-analysts`;
GRANT USAGE   ON SCHEMA   main.default       TO `data-analysts`;
GRANT USE CATALOG ON CATALOG main             TO `data-analysts`;
```

`USE CATALOG` + `USAGE` on the schema are needed too — otherwise the principal can't *see* the function to execute it."""),
    md("""## Limitations to know

- The body is **one expression**. For multi-step logic you'd express in code, use a Python UDF (notebook 14) or a view.
- No `LOOP`, no procedural blocks (Spark SQL isn't PL/SQL).
- Recursion isn't supported.
- A SQL UDF can only call other UC functions you have `EXECUTE` on."""),
    md("""## Cleanup"""),
    code('''for fn in ["grade", "fmt_money", "net_amount", "net_label", "top_scorers"]:
    spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.{fn}")
print("dropped")'''),
    md("""## Takeaways

- SQL UDFs run inside Catalyst — no serialization cost, often as fast as a built-in.
- NULL propagates like in any SQL expression — no special handling.
- Define once in UC, share across the whole workspace via `GRANT EXECUTE`.
- `RETURNS TABLE` makes parameterized views.
- Body is a single SQL expression — no procedural code. Reach for Python UDFs (notebook 14) when that's a blocker."""),
]
write_nb("13 - SQL UDFs (Unity Catalog).ipynb", nb13)


# ---------------------------------------------------------------------------
# 14 - Python UDFs in Unity Catalog
# ---------------------------------------------------------------------------
nb14 = [
    md("""### Links and Resources
| Concept | Description |
|---|---|
| [Python UDFs in Unity Catalog](https://docs.databricks.com/en/udf/unity-catalog-python-udfs.html) | Register Python as a governed function |
| [`CREATE FUNCTION ... LANGUAGE PYTHON`](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html#parameters) | DDL syntax |
| [Limits & supported runtimes](https://docs.databricks.com/en/udf/unity-catalog-python-udfs.html#limitations) | What Python you can use |"""),
    code(BOOTSTRAP),
    md("""# Python UDFs in Unity Catalog

Notebook 11 created Python UDFs that live only in the current SparkSession. UC Python UDFs solve the same problem **persistently and securely**: register once, govern via `GRANT EXECUTE`, call from any notebook, job, SQL warehouse, or BI tool.

The DDL is the same `CREATE FUNCTION` you saw in notebook 13, but with `LANGUAGE PYTHON` and a `def` body."""),
    code('''CATALOG = "main"
SCHEMA  = "default"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA  {SCHEMA}")'''),
    md("""## Scalar Python UDF in UC"""),
    code('''spark.sql("""
CREATE OR REPLACE FUNCTION email_domain(email STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    if email is None:
        return None
    return email.lower().split("@")[-1]
$$
""")

spark.sql("SELECT email_domain('Alice@Example.COM') AS d").show()'''),
    md("""## Use in a query — like any other function"""),
    code('''people = spark.createDataFrame([
    (1, "Alice", "alice@example.com"),
    (2, "Bob",   "BOB@EXAMPLE.ORG"),
    (3, "Carol", None),
], ["id", "name", "email"])
people.createOrReplaceTempView("people")

spark.sql("SELECT id, name, email_domain(email) AS domain FROM people").show()'''),
    md("""## Multiple parameters and structured returns"""),
    code('''spark.sql("""
CREATE OR REPLACE FUNCTION classify_score(score INT, threshold INT)
RETURNS STRUCT<bucket: STRING, distance: INT>
LANGUAGE PYTHON
AS $$
    if score is None:
        return {"bucket": "unknown", "distance": None}
    if score >= threshold:
        return {"bucket": "above", "distance": score - threshold}
    return {"bucket": "below", "distance": threshold - score}
$$
""")

spark.sql("SELECT classify_score(85, 70) AS r").show(truncate=False)'''),
    md("""## What's in the Python environment

UC Python UDFs run in a **sandboxed environment** with a curated standard library. Notable points:

- The Python standard library is mostly available (`re`, `math`, `json`, `datetime`, `hashlib`, ...).
- **Network calls are blocked** by default — no `requests`, no API calls.
- **Filesystem access is blocked** — no reading workspace files / volumes from the UDF body.
- **Custom imports**: classic UC Python UDFs are stdlib-only. For sklearn/numpy/etc., use **service credentials and dependencies via Python UDFs on serverless** or fall back to a session-local `@F.udf` (notebook 11) or a Pandas UDF (notebook 12).

Always check the current Databricks docs — the supported package list expands over time."""),
    code('''spark.sql("""
CREATE OR REPLACE FUNCTION sha256_hex(s STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    import hashlib
    if s is None:
        return None
    return hashlib.sha256(s.encode("utf-8")).hexdigest()
$$
""")

spark.sql("SELECT sha256_hex('hello') AS h").show(truncate=False)'''),
    md("""## Permissions — same model as SQL UDFs

```sql
GRANT EXECUTE ON FUNCTION main.default.email_domain TO `analysts`;
```

The principal also needs `USAGE` on the schema and `USE CATALOG` on the catalog. **Owners can read the body** by default — be careful with secrets in the source."""),
    md("""## When to choose UC Python UDF vs. classic UDF vs. Pandas UDF

| Need | Pick |
|---|---|
| One-line transformation, used across many notebooks/jobs | **UC Python UDF** |
| Heavy numpy / sklearn / model inference | **Pandas UDF** (notebook 12) |
| Throwaway logic, one notebook | **Classic `@F.udf`** (notebook 11) |
| Pure SQL transformation, governable | **UC SQL UDF** (notebook 13) |
| ML model server / external API call | UDF can't do this — use a Spark Connect job or `foreachBatch` |"""),
    md("""## Limits to know

- Single-row scalar invocation — **not vectorized** like Pandas UDFs.
- Sandboxed Python — limited package set, no network, no FS.
- Body must be valid Python (the dollar-quoted block is parsed at registration).
- Versioned: `CREATE OR REPLACE` increments the version. Older queries pinned via `[VERSION ...]` for reproducibility (Databricks-specific feature)."""),
    md("""## Cleanup"""),
    code('''for fn in ["email_domain", "classify_score", "sha256_hex"]:
    spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.{fn}")
print("dropped")'''),
    md("""## Takeaways

- UC Python UDFs = `CREATE FUNCTION ... LANGUAGE PYTHON AS $$ ... $$`.
- Persisted, governed (`GRANT EXECUTE`), reusable across notebooks, jobs, SQL warehouses, BI tools.
- Sandboxed: no network, no filesystem, curated stdlib only.
- Scalar/per-row — for vectorized work use Pandas UDFs (notebook 12).
- Same NULL caveat as classic Python UDFs — guard explicitly."""),
]
write_nb("14 - Python UDFs in Unity Catalog.ipynb", nb14)


# ---------------------------------------------------------------------------
# 15 - Performance Comparison: Built-in vs UDF vs Pandas UDF
# ---------------------------------------------------------------------------
nb15 = [
    md("""### Links and Resources
| Concept | Description |
|---|---|
| [`explain`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html) | Read query plans |
| [Photon](https://www.databricks.com/product/photon) | Vectorized engine — only built-ins |
| [Catalyst](https://databricks.com/glossary/catalyst-optimizer) | Optimizes only built-ins / SQL UDFs |
| [Arrow batching](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html) | What makes Pandas UDFs fast |"""),
    code(BOOTSTRAP),
    md("""# Performance Comparison: Built-in vs UDF vs Pandas UDF

Same logic, three ways. We'll compute `(amount * 1.21) + 0.5` rounded — a trivial transform — on 5M rows and time each approach. Then we'll read the query plans and explain *why* the numbers come out the way they do."""),
    code('''import time

N = 5_000_000
df = spark.range(N).select(
    F.col("id").alias("amount"),
).repartition(8).cache()
df.count()  # materialize cache
print("rows:", N)'''),
    md("""## A small timing helper"""),
    code('''def time_query(name, build_query, runs=3):
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        build_query().write.format("noop").mode("overwrite").save()
        times.append(time.perf_counter() - t0)
    best = min(times)
    print(f"{name:30s} best of {runs}: {best*1000:7.1f} ms")
    return best'''),
    md("""## Three implementations of the same logic"""),
    code('''# 1. Pure built-in
def built_in_query():
    return df.select(F.round(F.col("amount") * 1.21 + 0.5, 2).alias("v"))

# 2. Plain Python UDF
@F.udf("double")
def py_udf(x):
    return None if x is None else round(x * 1.21 + 0.5, 2)

def py_udf_query():
    return df.select(py_udf("amount").alias("v"))

# 3. Pandas UDF
import pandas as pd
@F.pandas_udf("double")
def pd_udf(x: pd.Series) -> pd.Series:
    return (x * 1.21 + 0.5).round(2)

def pd_udf_query():
    return df.select(pd_udf("amount").alias("v"))'''),
    md("""## Run the benchmark"""),
    code('''t1 = time_query("built-in",   built_in_query)
t2 = time_query("python UDF",  py_udf_query)
t3 = time_query("pandas UDF",  pd_udf_query)

print()
print(f"python UDF / built-in: {t2/t1:.1f}x slower")
print(f"pandas UDF / built-in: {t3/t1:.1f}x slower")
print(f"python UDF / pandas UDF: {t2/t3:.1f}x slower")'''),
    md("""## Why? Read the plan.

`explain(mode="formatted")` gives a clean view of stages and the physical operators."""),
    code('''print("=" * 60, "BUILT-IN")
built_in_query().explain(mode="formatted")

print("=" * 60, "PYTHON UDF")
py_udf_query().explain(mode="formatted")

print("=" * 60, "PANDAS UDF")
pd_udf_query().explain(mode="formatted")'''),
    md("""## What to look for in those plans

- **Built-in** — single `Project` operator. Catalyst can fuse it with neighbours, push it into Parquet readers, and Photon can run it natively.
- **Python UDF** — a `BatchEvalPython` operator. Each row makes a JVM↔Python round trip. Photon can't run anything past this point.
- **Pandas UDF** — an `ArrowEvalPython` operator. Rows are batched into Arrow record batches; Python runs once per batch (10k rows at default settings)."""),
    md("""## The Photon dimension

When the cluster has **Photon** enabled (most current Databricks SQL warehouses, many DBR clusters), the gap between built-ins and UDFs widens further. Photon is a vectorized C++ engine that *only operates on built-ins*. The moment a Python UDF appears in the plan, Photon hands the data back to the JVM, runs the Python step, and tries to resume Photon afterwards. That handoff is expensive in itself.

Look for `PhotonProject` vs `Project` in the plan — Photon stages will be tagged."""),
    md("""## Practical decision tree

```
Can I express this with F.* / SQL?      ── yes ─→ Built-in (notebooks 02-10)
                │ no
                ▼
Can it be vectorized w/ pandas/numpy?  ── yes ─→ Pandas UDF (notebook 12)
                │ no
                ▼
Is the row volume small (< 100k)?       ── yes ─→ Plain Python UDF is fine
                │ no
                ▼
Reconsider — pull data out via collect, do it in pandas, push back.
```"""),
    md("""## Other levers worth measuring

- **Arrow-optimized Python UDFs**: `@F.udf(returnType, useArrow=True)` — frequently 2× the plain version, with no code change.
- **Batch size**: `spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", n)` — bigger batches amortize Python overhead but cost worker memory.
- **`repartition(n)`** — too few partitions and one slow Python worker becomes the bottleneck."""),
    code('''df.unpersist()'''),
    md("""## Takeaways

- Built-ins are the reference point. Anything else is measured *relative to them*.
- Python UDFs add a Python worker per row — typically 10–50× slower than equivalent built-ins.
- Pandas UDFs add a Python worker per *batch* via Arrow — typically 1.5–3× a built-in, an order of magnitude faster than a plain UDF.
- `explain(mode="formatted")` reveals which path you're on: `Project` (good), `ArrowEvalPython` (acceptable), `BatchEvalPython` (slow).
- Photon doubles the built-in advantage. Any Python step disables Photon for that stage."""),
]
write_nb("15 - Performance Comparison Built-in vs UDF vs Pandas UDF.ipynb", nb15)


# ---------------------------------------------------------------------------
# 16 - Functions in Streaming
# ---------------------------------------------------------------------------
nb16 = [
    md("""### Links and Resources
| Concept | Description |
|---|---|
| [Structured Streaming guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) | Streaming semantics |
| [Unsupported operations](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations) | What functions can't appear in a stream |
| [Watermarks & windows](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time) | Stateful windowing |
| [`applyInPandasWithState`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.applyInPandasWithState.html) | Stateful Pandas UDFs in streams |"""),
    code(BOOTSTRAP),
    md("""# Functions in Streaming

The `Streaming/` folder of this project covers the *mechanics* of structured streaming. This notebook is narrower: **which functions you can use inside a streaming query, which you can't, and the gotchas that don't show up in batch.**"""),
    code('''import time

events = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 20)
    .option("numPartitions", 1)
    .load()
    .withColumnRenamed("value", "event_id")
    .withColumn("user_id", (F.col("event_id") % 5).cast("int"))
    .withColumn("amount",  F.rand(seed=1) * 100))

events.printSchema()
print("isStreaming:", events.isStreaming)'''),
    code('''def peek(stream_df, name, seconds=4, limit=10):
    q = (stream_df.writeStream
         .format("memory")
         .queryName(name)
         .outputMode("append")
         .start())
    time.sleep(seconds)
    q.stop()
    return spark.sql(f"SELECT * FROM {name} LIMIT {limit}")'''),
    md("""## Stateless functions — always fine

Anything that operates on one row at a time works identically to batch: arithmetic, string functions, date functions, `when`, `coalesce`, `from_json`, higher-order array functions, scalar UDFs (Python and Pandas), SQL UDFs."""),
    code('''shaped = (events
    .select(
        "event_id", "user_id", "timestamp", "amount",
        F.round(F.col("amount") * 1.21, 2).alias("with_vat"),
        F.when(F.col("amount") > 50, "big").otherwise("small").alias("size"),
        F.date_format("timestamp", "yyyy-MM-dd HH:mm").alias("ts_min"),
    ))

display(peek(shaped, "shaped"))'''),
    md("""## The deterministic vs. non-deterministic split

This matters in streaming far more than in batch:

| Function | Deterministic? | Streaming behaviour |
|---|---|---|
| `current_timestamp()`, `current_date()` | Captured *per micro-batch*, not per row | Use freely |
| `rand()`, `randn()` (no seed) | Non-deterministic | Use freely, but mark UDFs that wrap them |
| `monotonically_increasing_id()` | Per partition counter, **not globally unique across batches** | Avoid for keys |
| `uuid()` | Truly random | Fine, but won't match across re-runs |
| Custom Python UDF reading a clock | Default: deterministic (wrong!) | Mark `.asNondeterministic()` |"""),
    code('''with_now = (events
    .withColumn("ingest_ts", F.current_timestamp())
    .withColumn("ingest_date", F.current_date())
    .withColumn("rid", F.expr("uuid()")))

display(peek(with_now.select("event_id", "ingest_ts", "ingest_date", "rid"), "with_now"))'''),
    md("""## Aggregation needs a **watermark** — and only one of them per query

Most aggregate functions (`sum`, `count`, `count_distinct`, `collect_list`, `percentile_approx`) can be used in streaming, but only:

1. **Once per query** — multiple chained aggregations are unsupported.
2. **With a watermark** if the input is unbounded — otherwise state grows forever.
3. **With windowed grouping** for proper time-bucketed aggregates.

`approx_count_distinct` is *especially* useful in streaming because exact distinct keeps every key in state forever."""),
    code('''windowed = (events
    .withWatermark("timestamp", "10 seconds")
    .groupBy(
        F.window("timestamp", "5 seconds"),
        "user_id",
    )
    .agg(
        F.count("*").alias("n"),
        F.sum("amount").alias("total"),
        F.approx_count_distinct("event_id").alias("approx_unique"),
    ))

display(peek(windowed.selectExpr("window.start AS w_start", "user_id", "n", "total", "approx_unique"),
             "windowed", seconds=8))'''),
    md("""## Functions that are illegal on streaming DataFrames

| Banned | Why |
|---|---|
| `df.count()`, `df.collect()`, `df.show()` (as actions) | Would need the whole stream |
| `df.limit(n)` | Ambiguous on unbounded data |
| `df.sort()` / `orderBy()` (top-level) | Global sort impossible |
| `df.distinct()` without watermark | Unbounded state |
| Multiple chained aggregations | One per query only |
| Some subqueries | Catalyst can't plan them streaming-safely |

These are rejected at *analysis* time — the query never starts."""),
    code('''from pyspark.errors import AnalysisException
try:
    bad = events.orderBy("amount").writeStream.format("memory").queryName("bad").start()
except (AnalysisException, Exception) as e:
    print("rejected:", type(e).__name__, str(e)[:120])'''),
    md("""## UDFs in streams — same rules, sharper consequences

Plain Python UDFs work, but the per-row Python boundary is hit on **every micro-batch**, every trigger. A 5× cost in batch becomes a 5× cost on every trigger interval — meaning your streaming job either lags or needs more nodes.

Prefer:

1. **Built-ins** — same advice as batch.
2. **Pandas UDFs** — vectorized, often the only way to keep up with high throughput.
3. **`mapInPandas` / `applyInPandasWithState`** — for stateful per-key logic that doesn't fit standard aggregates."""),
    code('''import pandas as pd

@F.pandas_udf("double")
def with_vat(s: pd.Series) -> pd.Series:
    return (s * 1.21).round(2)

priced = events.withColumn("price", with_vat("amount")).select("event_id", "amount", "price")
display(peek(priced, "priced"))'''),
    md("""## Stateful Pandas UDFs — `applyInPandasWithState`

For per-key state that you'd otherwise hand-roll: sessionization, custom counters, leaky-bucket rate limits. The state is checkpointed automatically.

```python
def update(key, pdfs, state):
    # pdfs: iterator of pd.DataFrame for this key in this batch
    # state.exists / state.get / state.update / state.remove
    ...

events.withWatermark("timestamp", "30 seconds").groupBy("user_id") \\
      .applyInPandasWithState(update, output_schema, state_schema, "Append", "EventTimeTimeout")
```

This is rich enough to deserve its own deep-dive (see `Streaming/08 - foreachBatch and Custom Sinks.ipynb` and `09 - Monitoring and Production Patterns.ipynb`)."""),
    md("""## Watermarks and `current_timestamp` — a subtle gotcha

If you watermark on a column derived from `current_timestamp()` instead of an event-time column, you get **processing time**, not event time. Late data won't be late by your watermark; you've effectively turned watermarking off. Always watermark on the actual event-time column from your source."""),
    md("""## Takeaways

- Stateless functions (the bulk of `F.*`) work identically in batch and streaming.
- Streams allow **one** aggregation per query, and need a watermark for unbounded inputs.
- Prefer `approx_count_distinct` over `count_distinct` in streams — bounded state.
- `current_timestamp()` is captured per micro-batch — fine for ingestion timestamps, dangerous for event-time logic.
- `orderBy`, `limit`, multiple aggregations: rejected at analysis time.
- For heavy per-row logic in a stream, Pandas UDFs are usually the only path that keeps up with throughput.
- Stateful per-key logic → `applyInPandasWithState`."""),
]
write_nb("16 - Functions in Streaming.ipynb", nb16)


# ---------------------------------------------------------------------------
# 17 - AI Functions
# ---------------------------------------------------------------------------
nb17 = [
    md("""### Links and Resources
| Function | Description |
|---|---|
| [`ai_query`](https://docs.databricks.com/en/large-language-models/ai-functions.html#ai_query) | Call any served LLM endpoint from SQL |
| [`ai_classify`](https://docs.databricks.com/en/large-language-models/ai-functions.html#ai_classify) | Zero-shot text classification |
| [`ai_extract`](https://docs.databricks.com/en/large-language-models/ai-functions.html#ai_extract) | Pull structured fields from text |
| [`ai_translate`, `ai_summarize`, `ai_analyze_sentiment`, `ai_fix_grammar`, `ai_mask`, `ai_similarity`, `ai_gen`](https://docs.databricks.com/en/large-language-models/ai-functions.html) | Task-specific helpers |
| [Vector Search](https://docs.databricks.com/en/generative-ai/vector-search.html) | `vector_search` SQL function |
| [Foundation Model APIs](https://docs.databricks.com/en/machine-learning/foundation-models/index.html) | Pre-deployed models — DBRX, Llama, Mixtral |"""),
    code(BOOTSTRAP),
    md("""# AI Functions

Databricks ships a family of SQL functions that call LLMs (and a vector index) inline in a query. They turn an unstructured-text problem into something you can join, filter, and aggregate over.

Two families:

1. **Task functions** — `ai_classify`, `ai_extract`, `ai_summarize`, `ai_analyze_sentiment`, `ai_translate`, `ai_fix_grammar`, `ai_mask`, `ai_similarity`. Pre-prompted, you just pass the text.
2. **`ai_query`** — generic call to *any* model serving endpoint. Used when you need a custom prompt, a fine-tuned model, or a private endpoint.

Plus **`vector_search`** for retrieval-augmented patterns.

> **Cost & latency note:** every call goes to a real LLM endpoint. Tokens are billed, latency is hundreds of ms per row. Filter aggressively before applying these — never sprinkle them through a wide query."""),
    code('''reviews = spark.createDataFrame([
    (1, "The shipping was incredibly fast and the product is amazing!"),
    (2, "Broke after two days. Customer support never replied."),
    (3, "Le produit est arrivé endommagé, je suis très déçu."),
    (4, "Mediocre. Does the job, nothing special."),
    (5, "Outstanding quality, will buy again."),
], ["id", "review"])
reviews.createOrReplaceTempView("reviews")
display(reviews)'''),
    md("""## `ai_analyze_sentiment` — single string in, label out"""),
    code('''spark.sql("""
SELECT id, review, ai_analyze_sentiment(review) AS sentiment
FROM reviews
""").show(truncate=False)'''),
    md("""## `ai_classify` — zero-shot with your own labels"""),
    code('''spark.sql("""
SELECT id, review,
       ai_classify(review, ARRAY('shipping', 'quality', 'support', 'price')) AS topic
FROM reviews
""").show(truncate=False)'''),
    md("""## `ai_translate` — language → language"""),
    code('''spark.sql("""
SELECT id, review,
       ai_translate(review, 'en') AS review_en
FROM reviews
WHERE id IN (3)
""").show(truncate=False)'''),
    md("""## `ai_extract` — structured fields from free text

Pass the text and a list of fields you want. Returns a struct."""),
    code('''spark.sql("""
SELECT id,
       ai_extract(review, ARRAY('issue', 'product_aspect', 'recommendation')) AS extracted
FROM reviews
""").show(truncate=False)'''),
    md("""## `ai_summarize` — short summary of a long passage"""),
    code('''long = spark.createDataFrame([
    (1, "I purchased the deluxe edition last month after reading several positive reviews. "
        "Initial impressions were great — the build quality felt premium and setup was a "
        "breeze. However after about three weeks the battery started draining noticeably "
        "faster, and customer support has been unresponsive to my emails. I'd recommend it "
        "with reservations until they sort out the after-sales side."),
], ["id", "text"])
long.createOrReplaceTempView("long")

spark.sql("SELECT id, ai_summarize(text, 25) AS summary FROM long").show(truncate=False)'''),
    md("""## `ai_mask` — redact PII"""),
    code('''pii = spark.createDataFrame([
    (1, "Hi, this is Alice Smith calling from +32 471 12 34 56, my email is alice@example.com."),
], ["id", "text"])
pii.createOrReplaceTempView("pii")

spark.sql("""
SELECT ai_mask(text, ARRAY('person', 'email', 'phone')) AS redacted
FROM pii
""").show(truncate=False)'''),
    md("""## `ai_similarity` — semantic similarity score (0–1)"""),
    code('''spark.sql("""
SELECT
  ai_similarity('shipping was very fast', 'package arrived quickly')      AS sim_close,
  ai_similarity('shipping was very fast', 'the colour of the box is red') AS sim_far
""").show()'''),
    md("""## `ai_query` — custom prompt against any served endpoint

Use this when:
- You want a prompt that the task helpers don't cover.
- You're calling a fine-tuned model on your workspace.
- You need a custom output schema via `responseFormat`.

Signature (simplified):

```sql
ai_query(
  endpoint => 'databricks-dbrx-instruct',
  request  => named_struct(
                'messages',  ARRAY(named_struct('role', 'user', 'content', :prompt)),
                'temperature', 0.0
              ),
  returnType => 'STRING'
)
```

Or the shorthand: `ai_query('endpoint-name', prompt_text)`."""),
    code('''# Will only work in workspaces with a serving endpoint named below.
# Replace with one available in your workspace, or skip this cell.
ENDPOINT = "databricks-meta-llama-3-1-70b-instruct"

try:
    spark.sql(f"""
    SELECT id,
           ai_query(
             '{ENDPOINT}',
             concat('In one word, what is the dominant emotion in this review? ', review)
           ) AS emotion
    FROM reviews
    """).show(truncate=False)
except Exception as e:
    print("Skipped — endpoint not available:", str(e)[:150])'''),
    md("""## `ai_query` with a structured response

Pass `responseFormat` with a JSON schema and Spark parses the response into a struct."""),
    code('''try:
    spark.sql(f"""
    SELECT id,
           ai_query(
             '{ENDPOINT}',
             concat('Extract sentiment and main_topic as JSON for: ', review),
             responseFormat => 'STRUCT<sentiment: STRING, main_topic: STRING>'
           ) AS parsed
    FROM reviews
    """).show(truncate=False)
except Exception as e:
    print("Skipped — endpoint not available:", str(e)[:150])'''),
    md("""## `vector_search` — retrieval over an indexed table

Once you've created a Vector Search index in UC, you can query it with SQL. Returns top-k matches with scores.

```sql
SELECT *
FROM vector_search(
  index => 'main.default.docs_index',
  query => 'how do I reset my password',
  num_results => 5
);
```

The typical pattern is **RAG**: `vector_search` to retrieve passages, `ai_query` to compose an answer using them as context. All inside one SQL query, no glue code."""),
    md("""## Cost-control patterns

- **Filter first.** `WHERE language = 'fr'` before `ai_translate`, etc.
- **Cache results.** AI function output is deterministic-ish per (model, prompt) pair. Materialize into a Delta table; don't re-call.
- **Batch via `MERGE`.** Run AI functions only on rows new since last run.
- **Sample before going wide.** Test on 100 rows, look at the output, *then* run on millions.
- **Watch the endpoint's rate limits.** A query against 10M rows that exceeds the endpoint's RPS will fail or throttle."""),
    md("""## When AI functions are *not* the right tool

- **Hard rules with clear semantics**: a regex or `ai_classify` with 4 labels is not better than 4 `WHEN` clauses if you can write them.
- **Joins / lookups against a known taxonomy**: just join. Don't ask the model.
- **Tight SLAs**: each call is hundreds of ms. Don't put `ai_query` in a sub-second pipeline."""),
    md("""## Takeaways

- Task helpers (`ai_analyze_sentiment`, `ai_classify`, `ai_extract`, `ai_summarize`, `ai_translate`, `ai_mask`, `ai_similarity`) cover the common cases without needing to write a prompt.
- `ai_query` is the escape hatch — any served endpoint, custom prompt, optional structured `responseFormat`.
- `vector_search` + `ai_query` = RAG in pure SQL.
- These are real network calls. Filter, cache, and merge-incrementally — never run them on a full table on every refresh.
- For deterministic, high-volume logic, prefer built-ins or SQL UDFs."""),
]
write_nb("17 - AI Functions.ipynb", nb17)

print("wrote 7 notebooks (11-17) to", OUT)
