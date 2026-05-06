"""Generate notebooks 11-17 (UDFs + Patterns)."""
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
| Concept | Reference |
|---|---|
| [`pyspark.sql.functions.udf`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html) | The UDF registration helper |
| [Python UDF programming guide](https://spark.apache.org/docs/latest/api/python/user_guide/sql/python_udf.html) | Official guide |
| [Arrow-optimized Python UDFs](https://www.databricks.com/blog/arrow-optimized-python-udfs-apache-sparktm-35) | The 3.5+ speedup |
| [Spark SQL types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html) | Return type strings |"""),
    code(BOOTSTRAP),
    md("""# Python UDFs

A **User-Defined Function** lets you wrap arbitrary Python in something that behaves like a column expression. The mechanics are simple; the cost is not.

This notebook covers:

1. The two registration styles (decorator and `udf()` call).
2. Why a plain Python UDF is **5–50× slower** than the equivalent built-in.
3. NULL handling and type pitfalls.
4. The Arrow-optimized variant (Spark 3.5+) that closes most of the gap."""),
    md("""## Why UDFs are slow

For each row, Spark must:

1. Serialize the row from JVM → bytes (via Pickle).
2. Send it to a Python worker process.
3. Deserialize, run your function, serialize the result.
4. Send bytes back to the JVM and deserialize.

That's the overhead — round-trip per row, no Catalyst optimization, no Photon. **Built-ins skip all of it.** The first rule of UDFs: *check the built-in catalog first.*"""),
    code('''# Demo data — fake "names" we want to clean up
import random
random.seed(0)
N = 50_000
words = ["alice", "BOB", "  charlie ", "dana", "ERIN", "frank", "  grace"]
df = spark.createDataFrame(
    [(i, random.choice(words)) for i in range(N)],
    ["id", "name"],
)
df.show(5, truncate=False)
print("rows:", df.count())'''),
    md("""## Style 1 — register with `udf()` and a return type"""),
    code('''from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def normalize(s):
    if s is None:
        return None
    return s.strip().title()

normalize_udf = udf(normalize, StringType())

df.select("name", normalize_udf("name").alias("clean")).show(5, truncate=False)'''),
    md("""## Style 2 — decorator with a return-type string

Equivalent, often nicer to read."""),
    code('''@udf("string")
def normalize2(s):
    return s.strip().title() if s is not None else None

df.select("name", normalize2("name").alias("clean")).show(5, truncate=False)'''),
    md("""## NULL handling — your function will receive `None`

Spark calls your UDF for every row, including those with NULL inputs. If you forget the guard, a NULL crashes the whole task with `AttributeError: 'NoneType' object has no attribute ...`."""),
    code('''@udf("string")
def safe_upper(s):
    return None if s is None else s.upper()

with_nulls = df.union(spark.createDataFrame([(99999, None)], ["id", "name"]))
with_nulls.select(safe_upper("name").alias("upper")).filter(F.col("upper").isNull()).show(2)'''),
    md("""## Return types — be explicit, be correct

The return-type string (or `DataType` object) is **not optional**. Spark uses it to allocate the output column and decide what JVM type to convert to. Common ones:

| Python value | Spark type string |
|---|---|
| `int` | `"int"` (32-bit) or `"long"` (64-bit) |
| `float` | `"double"` |
| `str` | `"string"` |
| `bool` | `"boolean"` |
| `list[int]` | `"array<int>"` |
| `dict` | `"map<string,string>"` |
| `tuple` / `dataclass` | `"struct<a:int,b:string>"` |

Mismatches don't always raise — sometimes you just get NULL columns. Test before you ship."""),
    code('''@udf("array<string>")
def split_words(s):
    if s is None:
        return None
    return [w for w in s.strip().split() if w]

spark.createDataFrame([("hello world",), ("a b c",), (None,)], ["t"]) \\
    .select("t", split_words("t").alias("words")).show(truncate=False)'''),
    md("""## A struct-returning UDF"""),
    code('''from pyspark.sql.types import StructType, StructField, StringType, IntegerType

@udf(StructType([
    StructField("clean",  StringType()),
    StructField("length", IntegerType()),
]))
def parse(s):
    if s is None:
        return None
    cleaned = s.strip().title()
    return (cleaned, len(cleaned))

df.select(parse("name").alias("p")).select("p.*").show(5)'''),
    md("""## Using a UDF in SQL — `spark.udf.register`

Once registered with a name, it works inside any SQL string."""),
    code('''spark.udf.register("normalize_sql", normalize, "string")

df.createOrReplaceTempView("v")
spark.sql("SELECT id, normalize_sql(name) AS clean FROM v LIMIT 5").show(truncate=False)'''),
    md("""## Arrow-optimized Python UDFs (Spark 3.5+ / DBR 14+)

The `useArrow=True` flag swaps the row-by-row Pickle pipe for Arrow batches — same Python function, much less serialization overhead. Often **2–5× faster** than the classic UDF, with no code changes."""),
    code('''@udf("string", useArrow=True)
def normalize_arrow(s):
    return s.strip().title() if s is not None else None

# Same call site as before
df.select(normalize_arrow("name").alias("clean")).show(5, truncate=False)'''),
    md("""## Quick benchmark — built-in vs UDF vs Arrow UDF

We expect: **built-in fastest by a wide margin**, Arrow UDF middle, classic UDF slowest. The exact ratio varies by cluster and Spark version."""),
    code('''import time

def bench(label, df_expr):
    df_expr.write.format("noop").mode("overwrite").save()  # forces full computation, no I/O
    t = time.time()
    df_expr.write.format("noop").mode("overwrite").save()
    print(f"  {label:30s} {time.time() - t:5.2f}s")

big = df.union(df).union(df).union(df)            # ~200k rows
print("rows:", big.count())

bench("built-in",       big.select(F.initcap(F.trim("name"))))
bench("classic UDF",    big.select(normalize2("name")))
bench("Arrow UDF",      big.select(normalize_arrow("name")))'''),
    md("""## When a UDF *is* the right answer

Don't over-rotate against UDFs. Reach for one when:

- The logic genuinely needs a Python library (e.g. `phonenumbers`, `dateutil`, an internal package).
- The transformation is complex enough that expressing it in built-ins becomes unreadable.
- The data volume is small (< few million rows) and developer time matters more than runtime.

In those cases, **prefer Arrow UDFs**, and **prefer Pandas UDFs** (next notebook) when you can vectorize naturally."""),
    md("""## Takeaways

- A Python UDF is the slowest way to express a transformation; check the built-in catalog first.
- Always set the return type explicitly. Always handle `None`.
- Register with `spark.udf.register("name", fn, type)` to use it in SQL.
- `useArrow=True` (Spark 3.5+) gives a free 2–5× speedup over the classic UDF.
- Save UDFs for genuine library-dependent logic; vectorize via Pandas UDFs (notebook 12) when possible."""),
]
write_nb("11 - Python UDFs.ipynb", nb11)


# ---------------------------------------------------------------------------
# 12 - Pandas UDFs (Vectorized)
# ---------------------------------------------------------------------------
nb12 = [
    md("""### Links and Resources
| Concept | Reference |
|---|---|
| [`pandas_udf`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html) | API entry point |
| [Pandas UDFs guide](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html) | Official guide |
| [Apache Arrow](https://arrow.apache.org/) | The serialization format |
| [`applyInPandas`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.applyInPandas.html) | Per-group DataFrame → DataFrame |
| [`mapInPandas`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.mapInPandas.html) | Per-partition iterator |"""),
    code(BOOTSTRAP),
    md("""# Pandas UDFs (Vectorized)

A Pandas UDF receives **a `pandas.Series`** (or `DataFrame`) instead of one Python value at a time, runs your code on the whole batch using NumPy/pandas, and returns a `Series`. Spark moves the data via **Arrow**, the columnar format pandas already speaks — so there's no row-by-row Pickle round trip.

The win: **5–100× faster than a classic Python UDF** for anything that vectorizes naturally."""),
    md("""## The four flavors

| Flavor | Input | Output | Used with |
|---|---|---|---|
| **Series → Series** (Scalar) | `pd.Series` | `pd.Series` (same length) | `.select`, `.withColumn` |
| **Iterator of Series → Iterator of Series** | iterator | iterator | Long-lived state per executor |
| **Series → scalar** (Grouped Agg) | `pd.Series` | one value | `.groupBy(...).agg(...)` |
| **DataFrame → DataFrame** (Grouped Map) | `pd.DataFrame` | `pd.DataFrame` | `.groupBy(...).applyInPandas(...)` |"""),
    code('''import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf

random.seed = lambda *_: None  # silence; not used
np.random.seed(0)

n = 100_000
df = spark.createDataFrame(
    pd.DataFrame({
        "id":      np.arange(n),
        "group":   np.random.choice(["A", "B", "C"], size=n),
        "value":   np.random.randn(n) * 10 + 50,
        "name":    np.random.choice(["alice", "BOB", "Charlie", "dana"], size=n),
    })
)
df.show(5)
print("rows:", n)'''),
    md("""## 1. Scalar Pandas UDF — Series → Series

Same length in, same length out. By far the most common pattern."""),
    code('''@pandas_udf("double")
def z_score(s: pd.Series) -> pd.Series:
    return (s - s.mean()) / s.std()

# NOTE: this computes the mean per *batch* — see the warning below
df.select("value", z_score("value").alias("z")).show(5)'''),
    md("""**Pitfall:** A scalar Pandas UDF gets called per **batch** (one chunk of a partition). Aggregations like `s.mean()` apply to that batch only — not the whole DataFrame. For "z-score against the overall mean" you'd compute the mean separately and subtract, or use a Grouped Agg UDF."""),
    md("""## 2. Iterator of Series → Iterator of Series — long-lived state

Use this when you have an expensive setup (load a model, open a connection) you only want to do **once per executor**, not once per batch."""),
    code('''from typing import Iterator

@pandas_udf("string")
def expensive_lookup(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # Setup runs ONCE per executor, before the first batch
    table = {"alice": "ALICE-EU", "bob": "BOB-US", "charlie": "CHARLIE-EU", "dana": "DANA-EU"}
    for s in it:
        yield s.str.lower().map(table).fillna("UNKNOWN")

df.select("name", expensive_lookup("name").alias("region")).show(5, truncate=False)'''),
    md("""## 3. Grouped Agg Pandas UDF — Series → scalar

Inside `.groupBy(...).agg(...)`. Receives one `Series` per group, returns one value."""),
    code('''@pandas_udf("double")
def trimmed_mean(s: pd.Series) -> float:
    # Drop top/bottom 5% then mean — not a built-in
    lo, hi = s.quantile(0.05), s.quantile(0.95)
    return s[(s >= lo) & (s <= hi)].mean()

df.groupBy("group").agg(
    F.avg("value").alias("plain_mean"),
    trimmed_mean("value").alias("trimmed_mean"),
).show()'''),
    md("""## 4. Grouped Map — `applyInPandas`

The most powerful and most expensive form. Each group is shipped to one executor as a single `pandas.DataFrame`; you return a transformed `DataFrame` of any shape. **All rows for a group must fit in memory on one executor.**

Schema of the output is required up-front."""),
    code('''def rank_within_group(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.copy()
    pdf["rank"] = pdf["value"].rank(method="min", ascending=False).astype("int64")
    pdf["pct"]  = pdf["value"].rank(pct=True)
    return pdf

out_schema = "id long, group string, value double, name string, rank long, pct double"
df.groupBy("group").applyInPandas(rank_within_group, schema=out_schema).show(5)'''),
    md("""## 5. `mapInPandas` — partition-level iterator (no grouping)

Like `applyInPandas` but at the partition level — one `DataFrame` chunk at a time, no group key. Useful for stateful streaming-style logic that doesn't need a key."""),
    code('''def add_running_id(it):
    for pdf in it:
        pdf = pdf.copy()
        pdf["row_in_partition"] = np.arange(len(pdf))
        yield pdf

df.limit(20).mapInPandas(
    add_running_id,
    schema="id long, group string, value double, name string, row_in_partition long",
).show()'''),
    md("""## Quick benchmark — classic UDF vs Pandas UDF

`Series` math is just NumPy under the hood, so the same logic runs at near-C speed."""),
    code('''import time
from pyspark.sql.functions import udf

@udf("double")
def py_normalize(v):
    return None if v is None else (v - 50) / 10

@pandas_udf("double")
def pd_normalize(s: pd.Series) -> pd.Series:
    return (s - 50) / 10

def bench(label, expr):
    expr.write.format("noop").mode("overwrite").save()
    t = time.time()
    expr.write.format("noop").mode("overwrite").save()
    print(f"  {label:18s} {time.time() - t:5.2f}s")

big = df.union(df).union(df)             # ~300k rows
bench("built-in",   big.select((F.col("value") - 50) / 10))
bench("Pandas UDF", big.select(pd_normalize("value")))
bench("classic UDF",big.select(py_normalize("value")))'''),
    md("""## Memory and batch size

- Scalar / Iterator UDFs are batched. Default batch size is `spark.sql.execution.arrow.maxRecordsPerBatch = 10000`. Larger batches → less overhead but more memory per task.
- `applyInPandas` ships the *entire group* as one DataFrame. A skewed group can OOM an executor. If groups are unbalanced, salt the key or pre-aggregate."""),
    md("""## Type hints are part of the API

Spark introspects the type hints on your function (`pd.Series`, `Iterator[pd.Series]`, `pd.DataFrame`) to pick the right flavor. Keep them — without them, the API falls back to deprecated string-mode flavors."""),
    md("""## Takeaways

- Pandas UDFs eliminate the per-row JVM↔Python round trip — typical 10× win over classic UDFs.
- Use **Scalar** for column-wise math, **Iterator** when there's expensive per-executor setup, **Grouped Agg** for custom aggregations, **Grouped Map** when the result shape differs per group.
- `applyInPandas` requires the whole group to fit in one executor's memory.
- Type hints aren't optional — they pick the API flavor.
- Built-ins still beat Pandas UDFs when one exists. Use Pandas UDFs for the gap between "built-in does it" and "needs a Python library"."""),
]
write_nb("12 - Pandas UDFs (Vectorized).ipynb", nb12)


# ---------------------------------------------------------------------------
# 13 - SQL UDFs (Unity Catalog)
# ---------------------------------------------------------------------------
nb13 = [
    md("""### Links and Resources
| Concept | Reference |
|---|---|
| [`CREATE FUNCTION` (SQL)](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function) | DDL syntax |
| [Unity Catalog functions](https://docs.databricks.com/aws/en/udf/unity-catalog) | Governance model |
| [`GRANT` on functions](https://docs.databricks.com/aws/en/sql/language-manual/security-grant) | Permissions |
| [Built-in function index](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin) | What's already there |"""),
    code(BOOTSTRAP),
    md("""# SQL UDFs in Unity Catalog

A SQL UDF is a function whose body is a **single SQL expression**. Unlike Python UDFs, it:

- Lives in Unity Catalog at `catalog.schema.function_name`.
- Is governed by `GRANT` / `REVOKE` like a table.
- Can be **inlined by Catalyst** — at execution time it disappears into the surrounding query plan, so there's **no performance penalty** vs writing the expression by hand.
- Is callable from any notebook, SQL warehouse, or BI tool that has permission.

It's the right tool for *reusable business logic* expressed in SQL: tax calculations, status mappings, normalization rules. **All cells below run as SQL** — change the catalog/schema names to match your workspace."""),
    md("""## Set the working catalog/schema

Replace these with names you can write to. The defaults below match Databricks' built-in `main.default`."""),
    code('''CATALOG = "main"
SCHEMA  = "default"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA  {SCHEMA}")
print("using:", spark.sql("SELECT current_catalog(), current_schema()").collect()[0])'''),
    md("""## Create a scalar SQL UDF

`CREATE FUNCTION ... RETURNS T RETURN <single SQL expression>`. The expression can reference parameters and any built-in function — but **not another DataFrame query**."""),
    code('''spark.sql("""
CREATE OR REPLACE FUNCTION clean_email(e STRING)
    RETURNS STRING
    COMMENT 'Lowercase, trim, mask domain after @'
    RETURN
        CASE
            WHEN e IS NULL THEN NULL
            WHEN e RLIKE '^[^@]+@[^@]+$' THEN lower(trim(regexp_replace(e, '@.*$', '@REDACTED')))
            ELSE NULL  -- malformed
        END
""")
print("created clean_email")'''),
    md("""## Use the UDF anywhere SQL runs"""),
    code('''spark.sql("""
SELECT raw,
       clean_email(raw) AS cleaned
FROM (VALUES ('  Alice@Example.COM '), ('not-an-email'), (NULL))
   AS t(raw)
""").show(truncate=False)'''),
    md("""## Multi-parameter UDF — bucketing logic"""),
    code('''spark.sql("""
CREATE OR REPLACE FUNCTION risk_band(score INT, country STRING)
    RETURNS STRING
    RETURN
        CASE
            WHEN score IS NULL                          THEN 'unknown'
            WHEN country IN ('US', 'CA') AND score < 600 THEN 'high'
            WHEN country IN ('US', 'CA') AND score < 750 THEN 'medium'
            WHEN country IN ('US', 'CA')                 THEN 'low'
            WHEN score < 500                             THEN 'high'
            WHEN score < 650                             THEN 'medium'
            ELSE 'low'
        END
""")

spark.sql("""
SELECT * FROM (VALUES
    (820, 'US'), (640, 'US'), (550, 'CA'),
    (700, 'BE'), (480, 'FR'), (NULL, 'DE')
) AS t(score, country)
ORDER BY score
""").withColumn("band", F.expr("risk_band(score, country)")).show()'''),
    md("""## Table-valued SQL UDFs (TVFs) — `RETURNS TABLE`

A TVF returns rows, not a scalar. You call it in the `FROM` clause, optionally with `LATERAL` to correlate with each outer row."""),
    code('''spark.sql("""
CREATE OR REPLACE FUNCTION date_buckets(start_d DATE, n INT)
    RETURNS TABLE (d DATE, bucket STRING)
    RETURN
        SELECT
            date_add(start_d, x) AS d,
            CASE WHEN dayofweek(date_add(start_d, x)) IN (1, 7) THEN 'weekend' ELSE 'weekday' END AS bucket
        FROM (SELECT explode(sequence(0, n - 1)) AS x)
""")

spark.sql("SELECT * FROM date_buckets(DATE '2024-01-01', 10)").show()'''),
    md("""## Inspecting and managing functions"""),
    code('''spark.sql("SHOW USER FUNCTIONS").show(20, truncate=False)
spark.sql("DESCRIBE FUNCTION EXTENDED clean_email").show(20, truncate=False)'''),
    md("""## Permissions — grant before others can use it

In Unity Catalog, even a function you created is invisible to other users until you grant on it (and on the parent schema/catalog)."""),
    code('''# Replace the principal with a real user/group/service principal in your workspace.
# Commented because the principal name is workspace-specific.

# spark.sql("GRANT EXECUTE ON FUNCTION clean_email TO `analysts@example.com`")
# spark.sql("GRANT USAGE   ON SCHEMA   default       TO `analysts@example.com`")
# spark.sql("GRANT USAGE   ON CATALOG  main          TO `analysts@example.com`")
print("see comments — fill in your principal")'''),
    md("""## Why SQL UDFs are essentially free

Unity Catalog SQL UDFs are **inlined** during query optimization. The plan for:

```sql
SELECT clean_email(raw) FROM customers
```

is identical to writing the `CASE`/`regexp_replace` chain inline. You get the *encapsulation* benefit (one definition, audit-able, granted) without paying the UDF tax.

You can confirm with `EXPLAIN`:"""),
    code('''spark.sql("EXPLAIN SELECT clean_email(raw) FROM (VALUES ('a@b.com')) t(raw)").show(truncate=False)'''),
    md("""## Limitations

- **Single SQL expression** — no statements, no loops, no `DECLARE`. For multi-step logic, build it from sub-expressions or compose multiple UDFs.
- **No DML inside the body** — purely a function over its inputs.
- **TVFs cannot reference outer correlation directly** unless invoked with `LATERAL`.
- **Full namespace** — to call from another schema, use `catalog.schema.fn(...)` or `USE CATALOG/SCHEMA` first."""),
    md("""## Cleanup"""),
    code('''spark.sql("DROP FUNCTION IF EXISTS clean_email")
spark.sql("DROP FUNCTION IF EXISTS risk_band")
spark.sql("DROP FUNCTION IF EXISTS date_buckets")
print("dropped")'''),
    md("""## Takeaways

- SQL UDFs are governed objects in Unity Catalog — name, comment, permission them like tables.
- Single-expression body, but that expression can be arbitrarily complex (CASE, regex, struct literals, all built-ins).
- Inlined by Catalyst → **zero runtime overhead** vs writing the SQL inline.
- Use `RETURNS TABLE` for TVFs; call them in `FROM`, optionally with `LATERAL`.
- Grant `EXECUTE` plus `USAGE` on the parent schema/catalog before sharing."""),
]
write_nb("13 - SQL UDFs (Unity Catalog).ipynb", nb13)


# ---------------------------------------------------------------------------
# 14 - Python UDFs in Unity Catalog
# ---------------------------------------------------------------------------
nb14 = [
    md("""### Links and Resources
| Concept | Reference |
|---|---|
| [Python UDFs in UC](https://docs.databricks.com/aws/en/udf/unity-catalog-python-udfs) | DDL & runtime model |
| [`CREATE FUNCTION` Python form](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-python-udf) | Syntax |
| [Allowed packages](https://docs.databricks.com/aws/en/udf/unity-catalog-python-udfs#dependencies) | What's importable |
| [Sandboxing](https://docs.databricks.com/aws/en/udf/unity-catalog-python-udfs#security) | Network/file restrictions |"""),
    code(BOOTSTRAP),
    md("""# Python UDFs in Unity Catalog

A UC Python UDF is a Python function whose body lives **inside the catalog**. Unlike a notebook-scoped UDF (notebook 11), it:

- Is callable from **any** session — Python, SQL warehouse, BI tool — that has `EXECUTE`.
- Survives notebook restarts; the source is stored in UC.
- Runs in a **sandboxed environment** with no network and a curated set of standard-library + data-science packages.
- Is governed by `GRANT` / `REVOKE` like a table or SQL UDF.

The trade-off: tighter sandbox, fewer packages, and the same per-row Python overhead as a notebook UDF — so prefer SQL UDFs (notebook 13) when the logic fits."""),
    md("""## Why this matters

Most projects want one of:

1. **Reusable logic across many notebooks/dashboards** without copy-paste.
2. **A single auditable definition** of a business rule.
3. **A safe way to share Python helpers** with SQL-only users.

A UC Python UDF gives you all three. It's the production-friendly cousin of the in-notebook UDF."""),
    code('''CATALOG = "main"
SCHEMA  = "default"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA  {SCHEMA}")
print("using:", spark.sql("SELECT current_catalog(), current_schema()").collect()[0])'''),
    md("""## Create a Python UDF in UC

The body is **Python code embedded in the SQL `CREATE FUNCTION` statement**. The function name in the `$$ ... $$` block must match the SQL function name."""),
    code('''spark.sql("""
CREATE OR REPLACE FUNCTION normalize_phone(raw STRING)
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Strip non-digits, keep last 10, format as (XXX) XXX-XXXX. NULL on bad input.'
    AS $$
        if raw is None:
            return None
        digits = ''.join(c for c in raw if c.isdigit())
        if len(digits) < 10:
            return None
        d = digits[-10:]
        return f"({d[0:3]}) {d[3:6]}-{d[6:10]}"
    $$
""")
print("created normalize_phone")'''),
    code('''spark.sql("""
SELECT raw, normalize_phone(raw) AS clean FROM (VALUES
    ('+1 (415) 555-0123'),
    ('415.555.0123'),
    ('555-0123'),                  -- too short
    (NULL),
    ('abc')                        -- no digits
) AS t(raw)
""").show(truncate=False)'''),
    md("""## Multi-argument UDF — currency conversion stub"""),
    code('''spark.sql("""
CREATE OR REPLACE FUNCTION convert_amount(amount DOUBLE, from_ccy STRING, to_ccy STRING)
    RETURNS DOUBLE
    LANGUAGE PYTHON
    AS $$
        # Hard-coded rate table for the demo. In production, pull from a UC table.
        rates = {
            ('USD', 'EUR'): 0.92,
            ('EUR', 'USD'): 1.09,
            ('USD', 'GBP'): 0.79,
            ('GBP', 'USD'): 1.27,
        }
        if amount is None or from_ccy is None or to_ccy is None:
            return None
        if from_ccy == to_ccy:
            return amount
        rate = rates.get((from_ccy, to_ccy))
        return None if rate is None else round(amount * rate, 2)
    $$
""")

spark.sql("""
SELECT * FROM (VALUES
    (100.0, 'USD', 'EUR'),
    (50.0,  'EUR', 'USD'),
    (10.0,  'USD', 'JPY')          -- unknown rate
) AS t(amount, from_ccy, to_ccy)
""").withColumn("converted", F.expr("convert_amount(amount, from_ccy, to_ccy)")).show()'''),
    md("""## Returning structured types

Like SQL UDFs, the return type can be `ARRAY<...>`, `MAP<...>`, or `STRUCT<...>`. Inside Python, return a list / dict / tuple."""),
    code('''spark.sql("""
CREATE OR REPLACE FUNCTION parse_name(full STRING)
    RETURNS STRUCT<first STRING, last STRING, initials STRING>
    LANGUAGE PYTHON
    AS $$
        if not full:
            return None
        parts = full.strip().split()
        if not parts:
            return None
        first = parts[0]
        last  = parts[-1] if len(parts) > 1 else None
        initials = ''.join(p[0].upper() for p in parts if p)
        return (first, last, initials)
    $$
""")

spark.sql("""
SELECT parse_name(n).* FROM (VALUES ('Alice Cooper'), ('Charlie'), ('Dana O''Neill'), (NULL)) AS t(n)
""").show()'''),
    md("""## Allowed dependencies

The sandbox ships with the Python standard library plus a curated set of data-science packages: `numpy`, `pandas`, `scipy`, `scikit-learn`, `cryptography`, etc. The exact list depends on your DBR — check the docs link at the top.

**No network**, **no arbitrary file system**, **no `pip install`**. If you need a package not on the list, use a notebook-scoped UDF (notebook 11) or a Pandas UDF on a regular cluster (notebook 12)."""),
    code('''spark.sql("""
CREATE OR REPLACE FUNCTION standardize(value DOUBLE)
    RETURNS DOUBLE
    LANGUAGE PYTHON
    AS $$
        import math
        # Hard-coded population stats for the demo
        mu, sigma = 50.0, 10.0
        if value is None or sigma == 0:
            return None
        return (value - mu) / sigma
    $$
""")

spark.sql("SELECT standardize(v) FROM (VALUES (60.0), (40.0), (70.0)) t(v)").show()'''),
    md("""## Permissions — same model as SQL UDFs"""),
    code('''# spark.sql("GRANT EXECUTE ON FUNCTION normalize_phone TO `analysts@example.com`")
# spark.sql("GRANT USAGE   ON SCHEMA   default        TO `analysts@example.com`")
# spark.sql("GRANT USAGE   ON CATALOG  main           TO `analysts@example.com`")
spark.sql("SHOW USER FUNCTIONS LIKE 'normalize_phone'").show(truncate=False)
spark.sql("DESCRIBE FUNCTION EXTENDED normalize_phone").show(20, truncate=False)'''),
    md("""## Performance

A UC Python UDF runs row-by-row through the Python sandbox — same overhead profile as a classic Python UDF. **Don't** use it as a hot-path transform on a billion rows.

It's the right choice when:

- The logic must be **shared across many queries / users / tools**.
- A SQL UDF (notebook 13) can't express it.
- The data volume is moderate, or the UDF is a small part of a heavier query.

For high-throughput vectorized work, stay on a regular cluster and use Pandas UDFs (notebook 12)."""),
    md("""## Cleanup"""),
    code('''spark.sql("DROP FUNCTION IF EXISTS normalize_phone")
spark.sql("DROP FUNCTION IF EXISTS convert_amount")
spark.sql("DROP FUNCTION IF EXISTS parse_name")
spark.sql("DROP FUNCTION IF EXISTS standardize")
print("dropped")'''),
    md("""## Takeaways

- UC Python UDFs are governed, sharable Python functions stored in Unity Catalog.
- Body is embedded Python (`$$ ... $$`); name in the body must match the SQL name.
- Sandbox: standard library + curated data-science stack, **no network**, **no pip**.
- Same row-by-row overhead as a notebook Python UDF — pick **SQL UDF first**, then UC Python UDF if SQL can't do it.
- Permissions: `GRANT EXECUTE` on the function plus `USAGE` on the parent schema/catalog."""),
]
write_nb("14 - Python UDFs in Unity Catalog.ipynb", nb14)


# ---------------------------------------------------------------------------
# 15 - Performance Comparison
# ---------------------------------------------------------------------------
nb15 = [
    md("""### Links and Resources
| Concept | Reference |
|---|---|
| [Catalyst optimizer](https://databricks.com/glossary/catalyst-optimizer) | What `EXPLAIN` shows you |
| [Photon engine](https://www.databricks.com/product/photon) | When/why built-ins go vectorized C++ |
| [`noop` sink](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.format.html) | Forces execution without writing data |
| [Spark UI](https://spark.apache.org/docs/latest/web-ui.html) | The other side of perf debugging |"""),
    code(BOOTSTRAP),
    md("""# Performance: Built-in vs Python UDF vs Pandas UDF

The folklore is "built-ins are fastest, Pandas UDFs are middle, Python UDFs are worst." This notebook **measures it on your cluster** and shows you how to read the query plan to understand why.

We'll:

1. Run the same logic four ways: built-in, Python UDF, Arrow UDF, Pandas UDF.
2. Time each with the `noop` sink (no I/O — pure compute).
3. Read `EXPLAIN` to see what Catalyst does with each.
4. Talk about Photon and what disables it."""),
    md("""## Setup — a workload big enough to matter"""),
    code('''import time
import numpy as np, pandas as pd

np.random.seed(0)
N = 2_000_000

pdf = pd.DataFrame({
    "id":    np.arange(N),
    "value": np.random.randn(N) * 10 + 50,
    "name":  np.random.choice(["alice", "BOB", "Charlie", " Dana "], size=N),
})
df = spark.createDataFrame(pdf).repartition(8).cache()
df.count()              # materialize the cache
print("rows:", N)'''),
    md("""## A reusable timer

The `noop` sink is the canonical way to benchmark Spark: it runs the full plan but writes nothing. We discard a warm-up run because the first call may include compilation / file-listing / cache priming."""),
    code('''def bench(label, df_expr, runs=3):
    df_expr.write.format("noop").mode("overwrite").save()  # warm-up
    times = []
    for _ in range(runs):
        t = time.time()
        df_expr.write.format("noop").mode("overwrite").save()
        times.append(time.time() - t)
    print(f"  {label:24s} median={np.median(times):5.2f}s  min={min(times):5.2f}s  (n={runs})")'''),
    md("""## Workload 1 — numeric: standardize a column

`(value - 50) / 10` four ways."""),
    code('''from pyspark.sql.functions import udf, pandas_udf

@udf("double")
def py_norm(v):
    return None if v is None else (v - 50) / 10

@udf("double", useArrow=True)
def arrow_norm(v):
    return None if v is None else (v - 50) / 10

@pandas_udf("double")
def pd_norm(s: pd.Series) -> pd.Series:
    return (s - 50) / 10

print("=== numeric standardize ===")
bench("built-in",   df.select((F.col("value") - 50) / 10))
bench("Python UDF", df.select(py_norm("value")))
bench("Arrow UDF",  df.select(arrow_norm("value")))
bench("Pandas UDF", df.select(pd_norm("value")))'''),
    md("""**Expected ordering** (your numbers will differ):

```
built-in   <  Pandas UDF  <  Arrow UDF  <  Python UDF
```

Built-ins win because Catalyst lowers them into Photon C++ kernels that operate on entire columns. Python UDFs lose because every row pays a serialization round-trip."""),
    md("""## Workload 2 — string: trim + title-case

`F.initcap(F.trim(...))` four ways."""),
    code('''@udf("string")
def py_clean(s):
    return None if s is None else s.strip().title()

@udf("string", useArrow=True)
def arrow_clean(s):
    return None if s is None else s.strip().title()

@pandas_udf("string")
def pd_clean(s: pd.Series) -> pd.Series:
    return s.str.strip().str.title()

print("=== string clean ===")
bench("built-in",   df.select(F.initcap(F.trim("name"))))
bench("Python UDF", df.select(py_clean("name")))
bench("Arrow UDF",  df.select(arrow_clean("name")))
bench("Pandas UDF", df.select(pd_clean("name")))'''),
    md("""## Reading the query plan — `EXPLAIN`

The plan tells you what Catalyst will actually run. Three levels of detail:

- `df.explain()` — physical plan only.
- `df.explain(True)` — parsed → analyzed → optimized → physical.
- `df.explain("formatted")` — node-by-node, easier to read."""),
    code('''print("--- built-in ---")
df.select(F.initcap(F.trim("name"))).explain()

print("--- Python UDF ---")
df.select(py_clean("name")).explain()'''),
    md("""**What to look for:**

- **`Project [initcap(trim(name#X))]`** in the built-in plan — it's a normal projection. Catalyst can fuse it with neighbours.
- **`BatchEvalPython`** in the Python UDF plan — that's the Python worker pipe. Photon can't go through it; the data exits to Python and comes back. Anything downstream of `BatchEvalPython` runs in standard Spark, not Photon, even if it could otherwise.

This is why a single Python UDF can de-optimize an *entire* query — not just its own column."""),
    md("""## Photon — when it kicks in (and when it doesn't)

Photon is a vectorized C++ engine that replaces Catalyst's whole-stage codegen for supported operations. To use it:

- Cluster must be **Photon-enabled** (a tickbox at cluster creation, or any serverless SQL warehouse).
- The plan must contain only **Photon-supported operators**. A Python/Pandas UDF or unsupported expression forces a fall-back to non-Photon for that stage.

Check `EXPLAIN` for nodes prefixed with `Photon` (e.g. `PhotonProject`, `PhotonScan`). If you see `Project` instead, Photon was disabled for that node."""),
    code('''# This will show Photon nodes only on a Photon-enabled cluster (incl. serverless).
df.select(F.initcap(F.trim("name"))).explain("formatted")'''),
    md("""## What slows things down besides UDFs

A UDF isn't the only way to lose performance. Common culprits:

- **`collect()` / `toPandas()`** on a big DataFrame — pulls the whole thing to the driver.
- **`count()` in a loop** — recomputes the plan each time.
- **`distinct()` then `count()`** — use `approx_count_distinct` if approximate is OK.
- **`groupBy().count()` with high-cardinality key** — fine, but if you only need top-N, use a window with `row_number`.
- **Large `collect_list`** — moves entire group to one executor.
- **Skewed joins** on a hot key — salt the key or enable AQE skew handling."""),
    md("""## Cleanup"""),
    code('''df.unpersist()
print("unpersisted")'''),
    md("""## Takeaways

- **Built-in > Pandas UDF > Arrow UDF > Python UDF** — and the gaps are often 5–50×.
- A single Python UDF can disable Photon for the rest of the stage. That's a bigger cost than just the UDF itself.
- Always benchmark with the `noop` sink and a warm-up run. `count()` works too but introduces extra shuffle.
- `EXPLAIN` shows `BatchEvalPython` for any node that round-trips to Python — make that your "is this fast?" smoke test.
- Tune *before* you write the Python: most "needs a UDF" cases turn out to be expressible in built-ins."""),
]
write_nb("15 - Performance Comparison.ipynb", nb15)


# ---------------------------------------------------------------------------
# 16 - Functions in Streaming
# ---------------------------------------------------------------------------
nb16 = [
    md("""### Links and Resources
| Concept | Reference |
|---|---|
| [Structured Streaming guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) | The whole streaming model |
| [Built-in functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html) | Same catalog as batch |
| [Watermarks](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking) | Bounding state |
| [`current_timestamp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_timestamp.html) | Determinism caveat |"""),
    code(BOOTSTRAP),
    md("""# Functions in Streaming

The `pyspark.sql.functions` catalog you've used in batch is **the same catalog** in Structured Streaming. Most functions work identically. A small set don't, and the difference is always about one of two things:

1. **Determinism** — the function returns a different value at different times (e.g. `current_timestamp`, `rand`).
2. **State** — the function needs to see the whole table to answer (e.g. `count_distinct` without watermark).

Notebook 03 in the Streaming folder covers what's *legal* on streams generally. This notebook is specifically about which functions need extra thought."""),
    md("""## A demo source

We'll use a `rate` stream — a synthetic source that emits `value, timestamp` rows at a fixed rate. Easy to reason about, no external setup."""),
    code('''import time

events = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 20)
    .option("numPartitions", 1)
    .load()
    .withColumnRenamed("value", "event_id")
    .withColumn("user_id", (F.col("event_id") % 5).cast("int")))

events.printSchema()
print("isStreaming:", events.isStreaming)'''),
    code('''def peek(stream_df, name, seconds=4, limit=10):
    q = (stream_df.writeStream
         .format("memory").queryName(name)
         .outputMode("append").start())
    time.sleep(seconds)
    q.stop()
    return spark.sql(f"SELECT * FROM {name} LIMIT {limit}")'''),
    md("""## Stateless functions — work exactly like batch

`select`, `withColumn`, `filter`, `concat`, `from_json`, `when`, `regexp_replace`, `length`, `cast` — all the projection / row-wise stuff just works. If the batch version is correct, the streaming version is correct."""),
    code('''stateless = (events
    .withColumn("bucket",
        F.when(F.col("event_id") % 3 == 0, "premium").otherwise("free"))
    .withColumn("user_label", F.format_string("user_%02d", "user_id"))
    .filter(F.col("user_id") < 3))

display(peek(stateless, "stateless_demo"))'''),
    md("""## Time functions — `current_timestamp` and `current_date`

These are **non-deterministic**: each micro-batch sees a different "now". That's usually exactly what you want for a streaming pipeline ("when did we ingest this row?"), but it has consequences:

- The same row replayed after a failure could get a different `current_timestamp`. If your pipeline relies on `current_timestamp` for joins or deduplication, it's broken on retry.
- For event-time logic, **always use the column from the source** (here: `timestamp`), not `current_timestamp`. The source column is what travels with the row through the system."""),
    code('''timed = events.select(
    "event_id",
    F.col("timestamp").alias("event_time"),     # event time (deterministic)
    F.current_timestamp().alias("ingest_time"), # ingest time (NOT deterministic on retry)
)
display(peek(timed, "timed_demo"))'''),
    md("""## Random functions — same warning, harder

`rand()` and `randn()` without a seed change every batch. **With a seed**, they still produce different values for the same row across replays in some Spark versions, because the seed is per-task. If you need a stable random per row, hash a stable column instead:"""),
    code('''stable = events.withColumn(
    "stable_hash",
    F.crc32(F.col("event_id").cast("string")) % 100,   # 0..99, deterministic per event_id
)
display(peek(stable, "stable_demo"))'''),
    md("""## Aggregations — fine WITH a watermark, rejected WITHOUT

A streaming aggregation needs to remember partial results until they're "done". An unbounded aggregate (`groupBy(key).count()` over all of time) requires unbounded state — never finishes, eventually OOMs.

A **watermark** tells Spark "you'll never see data older than this", which lets it close out and emit old groups. Once the watermark passes, the state is dropped.

```python
agg = (events
    .withWatermark("timestamp", "10 seconds")
    .groupBy(F.window("timestamp", "10 seconds"), "user_id")
    .agg(F.count("*").alias("n")))
```

We won't run an aggregation here (output mode complicates the demo) — see Streaming notebook 04 for the full treatment. The point: **`groupBy` is allowed; you must watermark first.**"""),
    md("""## `count_distinct` and `dropDuplicates`

- **Exact `count_distinct`** on a stream is rejected — the state is unbounded.
- **`approx_count_distinct`** works (HyperLogLog state is bounded).
- **`dropDuplicates(["id"])`** also requires a watermark, because Spark needs to remember which IDs it has seen, and the watermark bounds how far back that memory goes."""),
    code('''dedup_demo = (events
    .withWatermark("timestamp", "20 seconds")
    .dropDuplicates(["event_id"]))
print("legal because of withWatermark — would be rejected without it")
print("isStreaming:", dedup_demo.isStreaming)'''),
    md("""## UDFs in streaming — same flavors, same costs

All three UDF flavors (Python, Arrow, Pandas) work on streaming DataFrames. The performance trade-offs from notebook 15 carry over identically: prefer built-ins, then Pandas UDFs, then Arrow, then classic Python.

One extra rule: **your UDF should be deterministic and side-effect-free.** A retry will replay rows; if your UDF writes to an external system or returns different values for the same input, you'll see double-writes or non-reproducible output."""),
    code('''import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("string")
def label(uid: pd.Series) -> pd.Series:
    return uid.map({0: "alice", 1: "bob", 2: "carol", 3: "dave", 4: "eve"}).fillna("unknown")

labeled = events.withColumn("user_name", label("user_id")).select("event_id", "user_id", "user_name")
display(peek(labeled, "labeled_demo"))'''),
    md("""## Higher-order functions on arrays — fully supported

Stateless and per-row, so streaming works exactly like batch."""),
    code('''with_array = events.withColumn(
    "tags",
    F.array(
        F.lit("new"),
        F.when(F.col("event_id") % 2 == 0, F.lit("even")).otherwise(F.lit("odd")),
    ),
)

transformed = with_array.select(
    "event_id",
    F.transform("tags", lambda t: F.upper(t)).alias("upper_tags"),
    F.exists("tags", lambda t: t == F.lit("even")).alias("has_even"),
)
display(peek(transformed, "hof_demo"))'''),
    md("""## Quick rules-of-thumb table

| Function category | Streaming-safe? | Notes |
|---|---|---|
| Projection / row-wise built-ins (`when`, `concat`, `from_json`, regex…) | Yes | Identical to batch |
| Higher-order array functions (`transform`, `filter`, `aggregate`) | Yes | Per-row, stateless |
| Window over partition (`row_number`, `lag`) | **No** | Needs all rows — use stream-stream join + state instead |
| `groupBy().agg(...)` | Yes, with watermark | One streaming aggregation per query |
| `count_distinct` (exact) | **No** | Use `approx_count_distinct` |
| `dropDuplicates` | Yes, with watermark | Without watermark = unbounded state |
| `current_timestamp` / `current_date` | Yes, but non-deterministic | Use event-time column for logic |
| `rand()` / `randn()` | Yes, but non-deterministic | Use a hash of a stable column for repeatability |
| Python / Arrow / Pandas UDFs | Yes | Must be deterministic & side-effect-free |"""),
    md("""## Takeaways

- The `pyspark.sql.functions` catalog is the same in streaming. Most things just work.
- **Stateless = always fine.** Stateful = needs a **watermark** to bound state.
- Use the source's event-time column for time-based logic, not `current_timestamp`. Replays must be reproducible.
- `count_distinct` exact is forbidden; `approx_count_distinct` is your friend.
- UDFs work but should be deterministic — retries replay rows."""),
]
write_nb("16 - Functions in Streaming.ipynb", nb16)


# ---------------------------------------------------------------------------
# 17 - AI Functions
# ---------------------------------------------------------------------------
nb17 = [
    md("""### Links and Resources
| Concept | Reference |
|---|---|
| [Databricks AI Functions](https://docs.databricks.com/aws/en/large-language-models/ai-functions) | The whole catalog |
| [`ai_query`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query) | Generic LLM call |
| [`ai_classify`, `ai_extract`, `ai_translate`](https://docs.databricks.com/aws/en/large-language-models/ai-functions#task-specific-ai-functions) | Task-specific helpers |
| [`vector_search`](https://docs.databricks.com/aws/en/generative-ai/vector-search.html) | Embedding-based retrieval |
| [Foundation Model APIs](https://docs.databricks.com/aws/en/machine-learning/foundation-models/index.html) | Underlying endpoints |"""),
    code(BOOTSTRAP),
    md("""# AI Functions

Databricks AI Functions are SQL-callable wrappers around foundation models hosted on Databricks. From a function-author's perspective they're "just functions" — you call them on a column, get back a column.

What makes them special:

- **No model orchestration code** — it's all SQL/PySpark.
- **Governed by Unity Catalog** — same `GRANT EXECUTE` model.
- **Pay-per-token** against a model serving endpoint, not per-cluster.
- **Vectorized** — Databricks batches rows under the hood.

This notebook is a tour. You'll need a workspace with **Foundation Model APIs enabled** to actually run the cells; the SQL reads identically across regions but the available models differ."""),
    md("""## Setup — confirm you have a model endpoint

By default, Databricks workspaces with FM APIs enabled have endpoints like `databricks-meta-llama-3-3-70b-instruct`, `databricks-claude-3-7-sonnet`, etc. Run the cell below to list what's available in **your** workspace; pick one for the examples that follow."""),
    code('''# This requires `serving-endpoints` permission on your workspace.
# If it errors, ask your admin which FM endpoint name to use.
try:
    endpoints = spark.sql("SHOW PROVIDERS").collect()  # placeholder; real listing varies by workspace
except Exception as e:
    print("listing failed (expected if no permission):", e)

# Pick the endpoint string for ai_query examples.
ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"   # change to one available in your workspace
print("using endpoint:", ENDPOINT)'''),
    md("""## A small demo dataset"""),
    code('''reviews = spark.createDataFrame([
    (1, "The shipping was fast and the laptop arrived in perfect condition. Five stars."),
    (2, "Awful customer support. Waited 40 minutes only to be disconnected."),
    (3, "It's fine. Does what it says. Battery could be better."),
    (4, "Le produit est arrivé endommagé et le service client n'a pas répondu."),
    (5, "Simply terrible. Would not buy again. Save your money."),
], ["id", "text"])
display(reviews)'''),
    md("""## `ai_classify` — pick from a label list

You provide candidate labels; the model returns the best match. Cheap and consistent."""),
    code('''reviews.createOrReplaceTempView("reviews")

# spark.sql("""
# SELECT id, text,
#        ai_classify(text, ARRAY('positive', 'neutral', 'negative')) AS sentiment
# FROM reviews
# """).show(truncate=False)

print("uncomment when running on a workspace with FM APIs enabled")'''),
    md("""## `ai_extract` — pull structured fields

Pass a list of field names to extract; the model returns a struct."""),
    code('''# spark.sql("""
# SELECT id,
#        ai_extract(text, ARRAY('product', 'issue', 'sentiment')) AS extracted
# FROM reviews
# """).select("id", "extracted.*").show(truncate=False)

print("uncomment when running on a workspace with FM APIs enabled")'''),
    md("""## `ai_translate` — language → language"""),
    code('''# spark.sql("""
# SELECT id, text, ai_translate(text, 'en') AS text_en
# FROM reviews
# """).show(truncate=False)
print("uncomment when running on a workspace with FM APIs enabled")'''),
    md("""## `ai_summarize` — TL;DR in N tokens"""),
    code('''# spark.sql("""
# SELECT ai_summarize(
#     concat_ws(' ', collect_list(text)),
#     50    -- max tokens
# ) AS overall_summary
# FROM reviews
# """).show(truncate=False)
print("uncomment when running on a workspace with FM APIs enabled")'''),
    md("""## `ai_query` — generic prompt-on-a-column

The escape hatch for anything the task-specific functions don't cover. You write the prompt; the model answers per row.

```sql
ai_query(
    'endpoint-name',
    'prompt template referring to {value}',
    [optional schema for structured output]
)
```

Pattern: build a prompt string with `concat`/`format_string`, pass it to `ai_query`, optionally constrain the output with a `responseFormat` so you don't have to parse free text."""),
    code('''# spark.sql(f"""
# SELECT id,
#        ai_query(
#            '{ENDPOINT}',
#            CONCAT(
#                'Rate this review on a 1-5 scale and explain in one short sentence. Review: ',
#                text
#            )
#        ) AS rating
# FROM reviews
# """).show(truncate=False)
print("uncomment when running on a workspace with FM APIs enabled")'''),
    md("""### Structured output with `ai_query` — return a struct, not a string

A free-text LLM response is a parsing nightmare in production. Pass `responseFormat` (JSON schema) and the function returns a typed struct."""),
    code('''# spark.sql(f"""
# SELECT id,
#        ai_query(
#            '{ENDPOINT}',
#            CONCAT('Analyze this review: ', text),
#            responseFormat => 'STRUCT<sentiment: STRING, rating: INT, language: STRING>'
#        ) AS analysis
# FROM reviews
# """).select("id", "analysis.*").show(truncate=False)
print("uncomment when running on a workspace with FM APIs enabled")'''),
    md("""## `ai_gen` — text generation (no input column required)

For pure prompt-based generation that doesn't depend on table data. Less commonly useful in a SQL pipeline, more for ad-hoc text creation."""),
    md("""## `vector_search` — embedding retrieval

Different shape: it queries a **Vector Search index** (a managed embedding store you build separately from a Delta table). Returns the top-k nearest neighbours.

```sql
SELECT *
FROM vector_search(
    index => 'main.default.product_descriptions_idx',
    query_text => 'wireless headphones with noise cancellation',
    num_results => 5
)
```

Setup outside this notebook: create the index from a source Delta table via the Vector Search UI or `databricks.vector_search` Python SDK. Once created, `vector_search` is just a table-valued function."""),
    md("""## Cost, latency, and batching

- AI Functions call a model serving endpoint — **per-token billing**, not per-cluster. Big tables × big prompts × big models = real money. Sample first.
- They are vectorized: Databricks batches rows server-side, so 1000 rows isn't 1000 API calls. But the latency is still much higher than a built-in.
- Cache aggressively: write the AI Function output to a Delta table and join against it instead of re-calling per query.
- For high-throughput cases, consider **Foundation Model batch inference** APIs over `ai_query`."""),
    md("""## Determinism and reproducibility

LLM responses are **non-deterministic by default**. Two consequences:

- The same row recomputed tomorrow may give a different label. This breaks naive idempotence assumptions.
- Pin a model version when you can (`databricks-meta-llama-3-3-70b-instruct@version`), and persist outputs to a Delta table so the *result* is the source of truth, not the function call."""),
    md("""## Permissions and governance

Like every UC function: `GRANT EXECUTE ON FUNCTION ai_query TO ...`, plus `USAGE` on the parent schema/catalog. The model serving endpoint has its own ACL on top — having `EXECUTE` on `ai_query` doesn't automatically let you call any endpoint."""),
    md("""## Takeaways

- AI Functions turn LLMs into vectorized SQL functions. The interface is the same as a SQL UDF.
- **Task-specific** functions (`ai_classify`, `ai_extract`, `ai_translate`, `ai_summarize`) are cheaper and more consistent than generic prompts.
- Use **`ai_query` with `responseFormat`** for typed output — never parse free text in production.
- Persist results to Delta. LLM calls are slow, expensive, and non-deterministic; treat them like a costly remote API.
- `vector_search` is a separate beast — a table-valued function over a managed embedding index.
- This is the *application layer* of the function world: governed, scalable, but pay-per-token. Reach for it last, after built-ins, SQL UDFs, and Pandas UDFs have failed to express the task."""),
]
write_nb("17 - AI Functions.ipynb", nb17)

print("wrote 7 notebooks to", OUT)
