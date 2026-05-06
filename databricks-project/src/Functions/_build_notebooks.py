"""Generate the first 7 Functions notebooks. Run from anywhere."""
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
# 01 - Introduction to Functions
# ---------------------------------------------------------------------------
nb01 = [
    md("""### Links and Resources
| Concept / Command | Description |
|-------------------|-------------|
| [`pyspark.sql.functions`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html) | Master list of all built-in column functions |
| [Spark SQL function index](https://spark.apache.org/docs/latest/api/sql/index.html) | Same functions, SQL syntax |
| [Catalyst optimizer](https://databricks.com/glossary/catalyst-optimizer) | Why built-ins beat UDFs |
| [Photon engine](https://www.databricks.com/product/photon) | Vectorized C++ runtime — only accelerates built-ins |"""),
    code(BOOTSTRAP),
    md("""# Introduction to Functions

Almost every transformation you write in Spark goes through a **function** — `concat`, `when`, `to_date`, `sum`, `row_number`. They live in two places:

- **Python:** `pyspark.sql.functions` — by convention imported as `F`.
- **SQL:** the same names, called inline in `spark.sql(...)` or `selectExpr(...)`.

This notebook is the map. The next six drill into specific categories."""),
    md("""## Why this matters: built-in vs. UDF

A *built-in* function is recognized by Catalyst (Spark's optimizer) and Photon (the vectorized engine). It can be:

- Pushed into the data source (predicate / projection pushdown).
- Reordered, fused with neighbours, or eliminated entirely.
- Executed in native code without crossing the JVM↔Python boundary.

A Python UDF can do *none* of those things. Same logic, 5–50× slower. **Always reach for a built-in first.**"""),
    code('''# The single most useful import in PySpark
from pyspark.sql import functions as F

# F now exposes ~400 functions. A few you will use every day:
print([f for f in dir(F) if f.startswith("to_")][:10])
print([f for f in dir(F) if "date" in f.lower()][:10])'''),
    md("""## Three ways to call the same function

PySpark gives you parallel APIs. They compile to the same plan — pick whichever reads best."""),
    code('''df = spark.range(5).withColumnRenamed("id", "n")

# 1. DataFrame API
df.select(F.col("n"), F.col("n") * 2).show(3)

# 2. selectExpr — SQL fragments
df.selectExpr("n", "n * 2 AS doubled").show(3)

# 3. spark.sql against a temp view
df.createOrReplaceTempView("nums")
spark.sql("SELECT n, n * 2 AS doubled FROM nums LIMIT 3").show()'''),
    md("""## Function categories at a glance

| Category | Examples | Notebook |
|---|---|---|
| String | `concat`, `regexp_replace`, `substring`, `split` | 02 |
| Numeric / math | `round`, `abs`, `pow`, `sqrt`, `rand` | 03 |
| Date / timestamp | `current_date`, `date_add`, `to_date`, `date_format` | 04 |
| Conditional | `when/otherwise`, `coalesce`, `nullif` | 05 |
| Collection (arrays) | `array_contains`, `explode`, `size`, `flatten` | 06 |
| Map / struct / JSON | `map_keys`, `from_json`, `to_json`, `struct` | 07 |
| Higher-order | `transform`, `filter`, `aggregate` (lambdas) | 08 |
| Window | `row_number`, `lag`, `rank` over partitions | 09 |
| Aggregation | `sum`, `count_distinct`, `percentile_approx` | 10 |"""),
    md("""## Reading function signatures

When you don't remember a function's arguments, the cheapest lookup is `help()` in the notebook itself:"""),
    code('''help(F.regexp_replace)'''),
    md("""## A tiny demo dataset

We'll reuse this shape across notebooks 02–07. Make sure it loads."""),
    code('''people = spark.createDataFrame([
    (1, "Alice",   "alice@example.com",  "2024-03-15", 1234.50),
    (2, "Bob",     "BOB@EXAMPLE.COM",    "2024-04-02",  987.00),
    (3, "Charlie", None,                  "2023-12-31", 4500.99),
    (4, "Dana",    "dana@example.com",   "2024-06-10",    None),
    (5, "Erin",    " erin@example.com ", "2025-01-20",  150.25),
], ["id", "name", "email", "signup_date", "balance"])

display(people)'''),
    md("""## Takeaways

- `from pyspark.sql import functions as F` is the standard import.
- DataFrame API, `selectExpr`, and `spark.sql` are interchangeable — pick what reads best.
- Always prefer a built-in to a UDF: Catalyst can optimize built-ins, Photon can vectorize them.
- The function namespace is huge but well-organized by category — notebooks 02–10 walk through them."""),
]
write_nb("01 - Introduction to Functions.ipynb", nb01)


# ---------------------------------------------------------------------------
# 02 - String Functions
# ---------------------------------------------------------------------------
nb02 = [
    md("""### Links and Resources
| Function | Use |
|---|---|
| [`concat`, `concat_ws`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.concat_ws.html) | Combine strings with/without a separator |
| [`substring`, `substr`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.substring.html) | Extract a slice |
| [`regexp_replace`, `regexp_extract`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_replace.html) | Regex on strings |
| [`split`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.split.html) | String → array |
| [`format_string`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.format_string.html) | `printf`-style formatting |"""),
    code(BOOTSTRAP),
    md("""# String Functions

Strings are messy. Real-world data has trailing whitespace, mixed casing, embedded delimiters, and free-text fields begging to be parsed. Spark's string functions cover almost every cleanup you'd reach for in pandas — but vectorized, parallel, and Catalyst-aware."""),
    code('''df = spark.createDataFrame([
    (1, "  Alice Cooper  ", "alice@example.COM",  "Brussels, BE"),
    (2, "bob smith",         "BOB@example.com",    "Paris, FR"),
    (3, "CHARLIE",           "charlie@x.io",       "Berlin, DE"),
    (4, "Dana O'Neill",      "dana@example.org",   "Madrid, ES"),
], ["id", "name", "email", "location"])
display(df)'''),
    md("""## Case and whitespace — `lower`, `upper`, `trim`, `ltrim`, `rtrim`, `initcap`

The first thing you do with any string column."""),
    code('''cleaned = (df
    .withColumn("name_clean", F.initcap(F.trim("name")))
    .withColumn("email_norm", F.lower(F.trim("email"))))
display(cleaned.select("id", "name_clean", "email_norm"))'''),
    md("""## Length and padding — `length`, `lpad`, `rpad`

`lpad` is how you build fixed-width identifiers (think: `"00042"`)."""),
    code('''df.select(
    "name",
    F.length("name").alias("len"),
    F.lpad(F.col("id").cast("string"), 5, "0").alias("padded_id"),
).show()'''),
    md("""## Slicing — `substring(col, pos, len)` and `substr`

**1-indexed**, like SQL. Negative positions count from the end."""),
    code('''df.select(
    "email",
    F.substring("email", 1, 3).alias("first3"),
    F.substring_index("email", "@", 1).alias("local_part"),
    F.substring_index("email", "@", -1).alias("domain"),
).show(truncate=False)'''),
    md("""## Concatenation — `concat`, `concat_ws`

`concat` returns NULL if any argument is NULL. `concat_ws` (with-separator) skips NULLs — usually what you want."""),
    code('''df.select(
    F.concat(F.lit("user_"), F.col("id")).alias("user_key"),
    F.concat_ws(" | ", "name", "email", "location").alias("summary"),
).show(truncate=False)'''),
    md("""## Splitting — `split` returns an array

Combine with `getItem(i)` or `[i]` to pull a piece, or `explode` (notebook 06) to fan out rows."""),
    code('''df.select(
    "location",
    F.split("location", ", ").alias("parts"),
    F.split("location", ", ").getItem(0).alias("city"),
    F.split("location", ", ").getItem(1).alias("country"),
).show(truncate=False)'''),
    md("""## Replace and regex — `regexp_replace`, `regexp_extract`

Spark uses Java regex syntax. `regexp_extract(col, pattern, group_idx)` returns the captured group, `""` if no match."""),
    code('''df.select(
    "email",
    F.regexp_replace("email", r"@.*$", "@REDACTED").alias("masked"),
    F.regexp_extract("email", r"^([^@]+)@(.+)$", 2).alias("domain"),
).show(truncate=False)'''),
    md("""## Contains, startswith, endswith, like

These come back as a Boolean column — perfect for `.filter(...)`."""),
    code('''df.filter(F.col("email").endswith(".com")) \\
  .select("name", "email") \\
  .show(truncate=False)

df.filter(F.col("name").rlike(r"(?i)^char")) \\
  .show(truncate=False)'''),
    md("""## `format_string` — `printf` for columns

Works like C's `printf`. Useful for building human-readable IDs."""),
    code('''df.select(
    F.format_string("%05d - %s", "id", "name").alias("label")
).show(truncate=False)'''),
    md("""## Takeaways

- `lower(trim(...))` is the cheapest data-cleaning move you'll do.
- Substring positions are **1-indexed**.
- Prefer `concat_ws` over `concat` — it doesn't poison the result with NULLs.
- `regexp_*` functions are full Java regex — use `(?i)` for case-insensitive.
- `split` returns an array; pull elements with `.getItem(i)`."""),
]
write_nb("02 - String Functions.ipynb", nb02)


# ---------------------------------------------------------------------------
# 03 - Numeric and Math Functions
# ---------------------------------------------------------------------------
nb03 = [
    md("""### Links and Resources
| Function | Use |
|---|---|
| [`round`, `bround`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.round.html) | HALF_UP vs banker's rounding |
| [`ceil`, `floor`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ceil.html) | Round toward ±∞ |
| [`abs`, `sign`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.abs.html) | Magnitude / direction |
| [`pow`, `sqrt`, `log`, `exp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pow.html) | Exponents and logs |
| [`rand`, `randn`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rand.html) | Uniform / normal random |
| [`greatest`, `least`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.greatest.html) | Row-wise max/min across columns |"""),
    code(BOOTSTRAP),
    md("""# Numeric and Math Functions

A small but well-used corner of the API. Most are 1:1 with their `math` module counterparts, but there are two traps worth flagging up front:

- **`round` is HALF_UP, `bround` is banker's rounding.** Financial code usually wants `bround` to avoid statistical bias.
- **`rand()` is non-deterministic by default**, which breaks job re-runs. Always pass a seed."""),
    code('''df = spark.createDataFrame([
    (1, 12.345,  -7.0, 0),
    (2,  0.5,     2.5, 3),
    (3, -3.7,     1.0, 5),
    (4, 99.999,  -0.0, 2),
], ["id", "x", "y", "n"])
display(df)'''),
    md("""## Rounding — `round`, `bround`, `ceil`, `floor`

`round(0.5) = 1` (HALF_UP). `bround(0.5) = 0`, `bround(2.5) = 2` (round half to even). The difference matters when you sum many rounded values."""),
    code('''df.select(
    "x",
    F.round("x", 1).alias("round_1dp"),
    F.bround("x", 1).alias("bround_1dp"),
    F.ceil("x").alias("ceil"),
    F.floor("x").alias("floor"),
).show()'''),
    md("""## Magnitude and sign — `abs`, `signum`"""),
    code('''df.select("y", F.abs("y").alias("abs"), F.signum("y").alias("sign")).show()'''),
    md("""## Exponents and roots — `pow`, `sqrt`, `cbrt`, `exp`, `log`, `log10`, `log2`

`log(b, x)` is `log_b(x)`. Single-argument `log(x)` is natural log."""),
    code('''df.select(
    "x",
    F.pow("x", F.lit(2)).alias("squared"),
    F.sqrt(F.abs("x")).alias("sqrt_abs"),
    F.log(F.lit(10.0), F.abs("x") + F.lit(1)).alias("log10_x_plus_1"),
).show()'''),
    md("""## Modulo and division — `%`, `pmod`

`%` follows the sign of the dividend. `pmod` is always non-negative — what you usually want for hashing/bucketing."""),
    code('''df.select(
    "y",
    (F.col("y") % 3).alias("mod_3"),
    F.pmod("y", F.lit(3)).alias("pmod_3"),
).show()'''),
    md("""## Row-wise extremes — `greatest`, `least`

These take any number of columns and return the max/min **across them, per row** — different from the `max()`/`min()` aggregates."""),
    code('''df.select(
    "x", "y", "n",
    F.greatest("x", "y", "n").alias("greatest"),
    F.least("x", "y", "n").alias("least"),
).show()'''),
    md("""## Random numbers — always seed!

`rand()` and `randn()` (Gaussian) without a seed change every run, breaking idempotence. Pass an integer seed."""),
    code('''spark.range(4).select(
    "id",
    F.rand(seed=42).alias("uniform"),
    F.randn(seed=42).alias("normal"),
).show()'''),
    md("""## Casting numeric types

Use `.cast("int" / "long" / "double" / "decimal(18,2)")`. Casting a string that isn't a number returns NULL — silent — so guard with `try_cast` (Spark 3.4+) when reading dirty input."""),
    code('''spark.createDataFrame([("12.5",), ("abc",), ("3",)], ["s"]).select(
    "s",
    F.col("s").cast("double").alias("cast_double"),
    F.expr("try_cast(s AS DOUBLE)").alias("try_cast_double"),
).show()'''),
    md("""## Takeaways

- For money: `bround` over `round`, or use `decimal(p, s)` end-to-end.
- `pmod` for hashing; `%` only when sign of dividend is meaningful.
- `greatest`/`least` are row-wise — the agg counterparts (`max`/`min`) are different functions.
- Always pass a seed to `rand()` / `randn()`.
- `try_cast` returns NULL on bad input instead of failing the job."""),
]
write_nb("03 - Numeric and Math Functions.ipynb", nb03)


# ---------------------------------------------------------------------------
# 04 - Date and Timestamp Functions
# ---------------------------------------------------------------------------
nb04 = [
    md("""### Links and Resources
| Function | Use |
|---|---|
| [`current_date`, `current_timestamp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_date.html) | "Now" — captured once per query |
| [`to_date`, `to_timestamp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_date.html) | Parse strings |
| [`date_format`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_format.html) | Format dates as strings |
| [`date_add`, `date_sub`, `add_months`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_add.html) | Date arithmetic |
| [`datediff`, `months_between`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.datediff.html) | Differences |
| [Datetime patterns](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html) | `yyyy-MM-dd` etc. |"""),
    code(BOOTSTRAP),
    md("""# Date and Timestamp Functions

Spark distinguishes three temporal types:

- **`date`** — just `yyyy-MM-dd`, no time-of-day, no zone.
- **`timestamp`** — instant in UTC, displayed in the session time zone.
- **`timestamp_ntz`** (Spark 3.4+) — local time without zone.

Most bugs in this area come from confusing the second and third, or from format strings: Spark uses **Java DateTimeFormatter** patterns (`yyyy-MM-dd HH:mm:ss`), not strftime."""),
    code('''df = spark.createDataFrame([
    (1, "2024-03-15",          "2024-03-15 09:30:00"),
    (2, "2024-12-31",          "2024-12-31 23:59:59"),
    (3, "2025-01-01",          "2025-01-01 00:00:01"),
    (4, "15/03/2024",          "15-03-2024 09.30.00"),  # non-standard formats
], ["id", "date_str", "ts_str"])
display(df)'''),
    md("""## Parsing — `to_date`, `to_timestamp`

ISO 8601 strings parse without a format. Anything else needs a Java pattern."""),
    code('''df.select(
    "date_str",
    F.to_date("date_str").alias("iso_date"),
    F.to_date("date_str", "dd/MM/yyyy").alias("eu_date"),
    F.to_timestamp("ts_str").alias("iso_ts"),
    F.to_timestamp("ts_str", "dd-MM-yyyy HH.mm.ss").alias("eu_ts"),
).show(truncate=False)'''),
    md("""## "Now" — `current_date`, `current_timestamp`

Both are captured **once per query**, not once per row — important for reproducibility."""),
    code('''spark.range(1).select(
    F.current_date().alias("today"),
    F.current_timestamp().alias("now"),
).show(truncate=False)'''),
    md("""## Date arithmetic — `date_add`, `date_sub`, `add_months`, `+ INTERVAL`

`date_add(d, n)` adds n days. For other intervals, the `INTERVAL` syntax is most readable."""),
    code('''ref = df.select(F.to_date("date_str").alias("d")).filter(F.col("d").isNotNull())

ref.select(
    "d",
    F.date_add("d", 7).alias("plus_7d"),
    F.date_sub("d", 30).alias("minus_30d"),
    F.add_months("d", 6).alias("plus_6mo"),
    F.expr("d + INTERVAL 1 YEAR").alias("plus_1y"),
).show()'''),
    md("""## Differences — `datediff`, `months_between`"""),
    code('''ref.select(
    "d",
    F.datediff(F.current_date(), "d").alias("days_ago"),
    F.months_between(F.current_date(), "d").alias("months_ago"),
).show()'''),
    md("""## Components — `year`, `month`, `dayofmonth`, `dayofweek`, `weekofyear`, `quarter`, `hour`, `minute`, `second`"""),
    code('''ref.select(
    "d",
    F.year("d").alias("y"),
    F.month("d").alias("m"),
    F.dayofweek("d").alias("dow"),       # 1 = Sunday
    F.dayofyear("d").alias("doy"),
    F.weekofyear("d").alias("woy"),
    F.quarter("d").alias("q"),
).show()'''),
    md("""## Truncation — `date_trunc`, `trunc`

`date_trunc("month", ts)` zeroes out everything below the month — perfect for grouping by week/month/year."""),
    code('''spark.range(1).select(
    F.current_timestamp().alias("now"),
    F.date_trunc("hour",   F.current_timestamp()).alias("hour"),
    F.date_trunc("day",    F.current_timestamp()).alias("day"),
    F.date_trunc("month",  F.current_timestamp()).alias("month"),
    F.trunc(F.current_date(), "MM").alias("first_of_month"),
).show(truncate=False)'''),
    md("""## Formatting — `date_format`

Same Java pattern syntax as parsing."""),
    code('''ref.select(
    F.date_format("d", "yyyy-MM").alias("year_month"),
    F.date_format("d", "EEEE, d MMMM yyyy").alias("long_form"),
).show(truncate=False)'''),
    md("""## Time zones — `from_utc_timestamp`, `to_utc_timestamp`

Storage is always UTC. These functions shift the *displayed* value into a target zone, or back."""),
    code('''spark.range(1).select(
    F.current_timestamp().alias("utc"),
    F.from_utc_timestamp(F.current_timestamp(), "Europe/Brussels").alias("brussels"),
    F.from_utc_timestamp(F.current_timestamp(), "America/New_York").alias("ny"),
).show(truncate=False)'''),
    md("""## Unix timestamps

`unix_timestamp()` → bigint seconds since epoch. `from_unixtime` reverses it."""),
    code('''ref.select(
    "d",
    F.unix_timestamp("d").alias("epoch_seconds"),
    F.from_unixtime(F.unix_timestamp("d")).alias("round_trip"),
).show()'''),
    md("""## Takeaways

- ISO 8601 strings auto-parse; everything else needs a Java pattern (NOT strftime).
- `current_date()`/`current_timestamp()` are query-stable — same value for every row in one query.
- Group by `date_trunc("month", ...)` for monthly rollups.
- Time zone conversions are display-time only; the underlying timestamp is always a UTC instant.
- `dayofweek` is **1 = Sunday** (US convention), not Monday."""),
]
write_nb("04 - Date and Timestamp Functions.ipynb", nb04)


# ---------------------------------------------------------------------------
# 05 - Conditional Functions
# ---------------------------------------------------------------------------
nb05 = [
    md("""### Links and Resources
| Function | Use |
|---|---|
| [`when` / `otherwise`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.when.html) | The PySpark CASE expression |
| [`coalesce`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.coalesce.html) | First non-null |
| [`nullif`, `nvl`, `nvl2`, `ifnull`](https://spark.apache.org/docs/latest/api/sql/index.html#nullif) | NULL handling |
| [SQL `CASE`](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-case.html) | The SQL form |"""),
    code(BOOTSTRAP),
    md("""# Conditional Functions

How you express *if/else* in a Spark expression. There's only one rule worth remembering: **NULL is not False.** Most bugs in this area come from forgetting that any comparison with NULL returns NULL, not False, and that `WHEN x = NULL` never matches."""),
    code('''df = spark.createDataFrame([
    (1, "Alice",   850, "active"),
    (2, "Bob",     720, "active"),
    (3, "Charlie", None, "active"),
    (4, "Dana",    420, "churned"),
    (5, "Erin",    None, None),
], ["id", "name", "score", "status"])
display(df)'''),
    md("""## `when` / `otherwise` — the workhorse

Chain `when` calls; the first match wins. The final `otherwise` is the default — omit it and unmatched rows become NULL."""),
    code('''df.select(
    "name", "score",
    F.when(F.col("score") >= 800, "A")
     .when(F.col("score") >= 600, "B")
     .when(F.col("score") >= 400, "C")
     .otherwise("F")
     .alias("grade"),
).show()'''),
    md("""## SQL `CASE` — same thing in `selectExpr`

Reads better when there are many branches."""),
    code('''df.selectExpr("""
    name, score,
    CASE
        WHEN score >= 800 THEN 'A'
        WHEN score >= 600 THEN 'B'
        WHEN score >= 400 THEN 'C'
        ELSE 'F'
    END AS grade
""").show()'''),
    md("""## `coalesce` — first non-null

Takes any number of columns, returns the first that isn't NULL. The canonical way to apply defaults."""),
    code('''df.select(
    "name",
    F.coalesce("score", F.lit(0)).alias("score_default_0"),
    F.coalesce("status", F.lit("unknown")).alias("status_default"),
).show()'''),
    md("""## `nullif`, `nvl`, `nvl2`, `ifnull`

Less famous but handy:

- `nullif(a, b)` — returns NULL if `a == b`, else `a`. Useful to convert sentinel values back to NULL.
- `nvl(a, b)` — alias of `coalesce(a, b)`.
- `nvl2(a, b, c)` — `b` if `a` is not NULL, else `c`.
- `ifnull(a, b)` — alias of `coalesce(a, b)`.

Most live as SQL functions; call them via `F.expr(...)`."""),
    code('''df.selectExpr(
    "name", "score",
    "nullif(score, 0)         AS no_zero",
    "nvl(score, -1)            AS score_or_-1",
    "nvl2(score, 'has', 'no')  AS has_score",
).show()'''),
    md("""## The NULL trap

`x = NULL` is *never* True. Use `IS NULL` / `isNull()` / `isNotNull()`."""),
    code('''# Wrong — always returns nothing because score = NULL is NULL, not True
df.filter(F.col("score") == F.lit(None)).count()

# Right
print("nulls in score:", df.filter(F.col("score").isNull()).count())'''),
    md("""## `when` returns a Column — combine freely

You can wrap `when` around any expression, including aggregates and other `when`s."""),
    code('''df.groupBy("status").agg(
    F.count("*").alias("rows"),
    F.sum(F.when(F.col("score").isNotNull(), 1).otherwise(0)).alias("non_null_scores"),
    F.avg(F.when(F.col("score") >= 600, F.col("score"))).alias("avg_high_score"),
).show()'''),
    md("""## Takeaways

- `when().when().otherwise()` is *the* if/else — first match wins.
- Use `coalesce` for defaults; it short-circuits and accepts N arguments.
- `x = NULL` is NULL, never True. Use `isNull()` / `isNotNull()`.
- Conditional aggregates: `sum(when(cond, 1).otherwise(0))` counts matches."""),
]
write_nb("05 - Conditional Functions.ipynb", nb05)


# ---------------------------------------------------------------------------
# 06 - Collection Functions
# ---------------------------------------------------------------------------
nb06 = [
    md("""### Links and Resources
| Function | Use |
|---|---|
| [`array`, `array_contains`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_contains.html) | Build / probe arrays |
| [`size`, `array_distinct`, `sort_array`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.size.html) | Array shape & cleanup |
| [`explode`, `posexplode`, `explode_outer`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html) | Array → rows |
| [`flatten`, `arrays_zip`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.flatten.html) | Multi-array combinators |
| [`sequence`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sequence.html) | Generate ranges |"""),
    code(BOOTSTRAP),
    md("""# Collection (Array) Functions

Spark arrays are first-class column types. They show up everywhere — JSON ingestion, `split` results, multi-tag fields, time-series buckets. Most of the work is one of three things: **build**, **probe**, or **fan out**."""),
    code('''df = spark.createDataFrame([
    (1, "Alice",   ["python", "sql", "spark"]),
    (2, "Bob",     ["scala", "spark"]),
    (3, "Charlie", []),
    (4, "Dana",    None),
    (5, "Erin",    ["python", "python", "rust"]),
], ["id", "name", "skills"])
display(df)'''),
    md("""## Build — `array`, `lit`, `sequence`"""),
    code('''spark.range(3).select(
    "id",
    F.array(F.lit("a"), F.lit("b"), F.col("id").cast("string")).alias("arr"),
    F.sequence(F.lit(1), F.col("id") + 2).alias("range"),
).show(truncate=False)'''),
    md("""## Shape — `size`, `array_distinct`, `sort_array`, `array_max`, `array_min`

`size` returns -1 for NULL arrays and 0 for empty — distinguish them with `isNull()`."""),
    code('''df.select(
    "name",
    F.size("skills").alias("n"),
    F.array_distinct("skills").alias("unique"),
    F.sort_array("skills").alias("sorted"),
).show(truncate=False)'''),
    md("""## Probe — `array_contains`, `array_position`

Both return useful values for filtering."""),
    code('''df.filter(F.array_contains("skills", "spark")).select("name").show()

df.select(
    "name",
    F.array_position("skills", "python").alias("python_pos"),  # 1-based, 0 if absent
).show()'''),
    md("""## Set ops — `array_union`, `array_intersect`, `array_except`"""),
    code('''pairs = df.select("name", "skills").alias("a").crossJoin(
    df.select(F.col("name").alias("other"), F.col("skills").alias("other_skills"))
).filter("name < other").limit(3)

pairs.select(
    "name", "other",
    F.array_intersect("skills", "other_skills").alias("shared"),
    F.array_except("skills", "other_skills").alias("only_in_a"),
).show(truncate=False)'''),
    md("""## Fan out — `explode`, `explode_outer`, `posexplode`

`explode` drops rows where the array is NULL or empty. `explode_outer` keeps them with a NULL value. `posexplode` adds a position column."""),
    code('''print("explode (drops NULL/empty):")
df.select("name", F.explode("skills").alias("skill")).show()

print("explode_outer (keeps them):")
df.select("name", F.explode_outer("skills").alias("skill")).show()

print("posexplode (with index):")
df.select("name", F.posexplode("skills").alias("idx", "skill")).show()'''),
    md("""## Combinators — `flatten`, `arrays_zip`

`flatten` collapses one level of nesting. `arrays_zip` merges parallel arrays element-wise into an array of structs."""),
    code('''nested = spark.createDataFrame([
    (1, [["a", "b"], ["c"]],     ["x", "y", "z"], [10, 20, 30]),
], ["id", "nested", "letters", "numbers"])

nested.select(
    F.flatten("nested").alias("flat"),
    F.arrays_zip("letters", "numbers").alias("zipped"),
).show(truncate=False)'''),
    md("""## NULL semantics

- `size(NULL) = -1`, `size([]) = 0`. Use `isNull()` to tell them apart.
- `array_contains(NULL, "x")` returns NULL, not False — wrap with `coalesce(..., F.lit(False))` if you filter on it."""),
    code('''# Safe filter that treats NULL arrays as "doesn't contain"
df.filter(
    F.coalesce(F.array_contains("skills", "spark"), F.lit(False))
).select("name").show()'''),
    md("""## Takeaways

- `array` builds, `array_contains` / `array_position` probe, `explode` fans out.
- `size` distinguishes empty (0) from NULL (-1).
- Use `_outer` variants of `explode` to preserve rows with NULL/empty arrays.
- Set operations (`union`, `intersect`, `except`) work element-wise on arrays.
- For *element-wise transformations* (e.g. uppercase every string in an array) you want **higher-order functions** — see notebook 08."""),
]
write_nb("06 - Collection Functions.ipynb", nb06)


# ---------------------------------------------------------------------------
# 07 - Map and Struct Functions
# ---------------------------------------------------------------------------
nb07 = [
    md("""### Links and Resources
| Function | Use |
|---|---|
| [`struct`, `getField`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.struct.html) | Build / read named tuples |
| [`map`, `create_map`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.create_map.html) | Build maps |
| [`map_keys`, `map_values`, `map_entries`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.map_keys.html) | Map projection |
| [`from_json`, `to_json`, `schema_of_json`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html) | Parse / emit JSON |"""),
    code(BOOTSTRAP),
    md("""# Map, Struct, and JSON Functions

The "complex types" tier of Spark's type system:

- **struct** — fixed schema, named fields. Like a row inside a row.
- **map** — open schema, key→value, all keys same type. Like a Python dict.
- **JSON strings** — neither, until you parse them.

Knowing when to reach for which is the main skill here. Rule of thumb: if you know the field names at design time, use a **struct**. If keys are data, use a **map**."""),
    md("""## Structs — build with `struct`, read with `.field` or `getField`"""),
    code('''df = spark.createDataFrame([
    (1, "Alice", "alice@x.io",  85.0),
    (2, "Bob",   "bob@x.io",    72.5),
], ["id", "name", "email", "score"])

with_struct = df.withColumn("contact", F.struct("name", "email"))
with_struct.printSchema()
with_struct.show(truncate=False)

# Reading back
with_struct.select(
    "id",
    F.col("contact.name").alias("name"),
    F.col("contact").getField("email").alias("email"),
).show(truncate=False)'''),
    md("""## Modifying structs — `withField`, `dropFields` (Spark 3.1+)

The most common reason people regret using a struct is that adding a field used to mean rebuilding the whole thing. No longer."""),
    code('''updated = with_struct.withColumn(
    "contact",
    F.col("contact").withField("verified", F.lit(True)).dropFields("email"),
)
updated.printSchema()
updated.select("contact").show(truncate=False)'''),
    md("""## Maps — `create_map`, `map_keys`, `map_values`, `map_entries`

`create_map` takes alternating key/value column expressions."""),
    code('''m = df.withColumn(
    "props",
    F.create_map(
        F.lit("name"),  F.col("name"),
        F.lit("score"), F.col("score").cast("string"),
    ),
)
m.printSchema()
m.show(truncate=False)

m.select(
    F.col("props")["name"].alias("name_via_map"),
    F.map_keys("props").alias("keys"),
    F.map_values("props").alias("values"),
).show(truncate=False)'''),
    md("""## When to pick map vs struct

| Use a **struct** when… | Use a **map** when… |
|---|---|
| Keys are known at design time | Keys are data (variable per row) |
| You want per-field types (`int`, `string`, `array`...) | All values share a type |
| You want column-style autocomplete & schema enforcement | You're modeling sparse / extensible properties |
| You'll write SQL like `WHERE contact.email LIKE ...` | You'll iterate over keys |"""),
    md("""## JSON ingestion — `from_json` with a schema

Almost every Kafka/Auto Loader pipeline does this. **Always pass an explicit schema**: relying on schema inference blows up cost and yields surprising types."""),
    code('''from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

raw = spark.createDataFrame([
    (1, '{"name": "Alice", "score": 85.0, "tags": ["a", "b"]}'),
    (2, '{"name": "Bob",   "score": 72.5, "tags": []}'),
    (3, '{"name": "Eve",   "score": null, "tags": null}'),
    (4, '{not valid json}'),  # malformed → all fields NULL
], ["id", "payload"])

schema = StructType([
    StructField("name",  StringType()),
    StructField("score", DoubleType()),
    StructField("tags",  ArrayType(StringType())),
])

parsed = raw.withColumn("p", F.from_json("payload", schema))
parsed.select("id", "p.name", "p.score", "p.tags").show(truncate=False)'''),
    md("""## Inferring a schema once with `schema_of_json`

If you really don't know the shape, sample one row, infer the schema, then *reuse* it. Don't infer per-row."""),
    code('''sample = '{"name": "Alice", "score": 85.0, "tags": ["a"]}'

inferred = spark.range(1).select(F.schema_of_json(F.lit(sample))).collect()[0][0]
print("inferred:", inferred)

# Then plug it in
raw.withColumn("p", F.from_json("payload", inferred)).select("id", "p.*").show()'''),
    md("""## `to_json` — serialize back

Useful for emitting to Kafka or for compact storage of variable-shape data."""),
    code('''with_struct.select(
    "id",
    F.to_json("contact").alias("contact_json"),
).show(truncate=False)'''),
    md("""## `get_json_object` and `json_tuple` — for the lazy path

If you only want one or two fields out of a deeply nested JSON, you can skip the schema:

- `get_json_object(col, "$.path.to.field")` — JSONPath, returns string.
- `json_tuple(col, "k1", "k2", ...)` — pulls multiple top-level keys at once."""),
    code('''raw.select(
    "id",
    F.get_json_object("payload", "$.name").alias("name"),
    F.get_json_object("payload", "$.tags[0]").alias("first_tag"),
).show(truncate=False)'''),
    md("""## Takeaways

- **Struct** when keys are fixed; **map** when keys are data.
- `withField` / `dropFields` make struct evolution painless.
- `from_json` with an **explicit schema** is the right way to parse JSON columns at scale.
- Use `schema_of_json` once on a sample, reuse the result — don't infer per row.
- `get_json_object` is a fine shortcut for a few fields, but the typed `from_json` path is faster and safer for production."""),
]
write_nb("07 - Map and Struct Functions.ipynb", nb07)

print("wrote 7 notebooks to", OUT)
