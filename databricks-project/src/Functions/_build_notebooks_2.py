"""Generate notebooks 08-10 (Higher-Order & Advanced)."""
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
# 08 - Higher-Order Functions on Arrays
# ---------------------------------------------------------------------------
nb08 = [
    md("""### Links and Resources
| Function | Use |
|---|---|
| [`transform`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.transform.html) | Apply a lambda to every element |
| [`filter`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.filter.html) | Keep elements matching a predicate |
| [`aggregate`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.aggregate.html) | Reduce / fold an array to a single value |
| [`exists`, `forall`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.exists.html) | Boolean checks |
| [`zip_with`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.zip_with.html) | Pairwise combine two arrays |
| [Higher-order functions blog](https://www.databricks.com/blog/2017/05/24/working-with-nested-data-using-higher-order-functions-in-sql-on-databricks.html) | Background |"""),
    code(BOOTSTRAP),
    md("""# Higher-Order Functions on Arrays

A *higher-order function* takes another function — a **lambda** — as input. Spark's higher-order functions let you transform arrays element-by-element **without exploding to rows and re-collecting**. That matters because:

- `explode` → `groupBy` → `collect_list` is expensive (shuffle, aggregation, state).
- A higher-order function is one Catalyst expression that runs on the same row, often vectorized by Photon.

If you've ever written a Python UDF just to map over a list column, this notebook is the replacement."""),
    code('''df = spark.createDataFrame([
    (1, [10, 20, 30, 40]),
    (2, [5, 15, 25]),
    (3, [100]),
    (4, []),
    (5, None),
], ["id", "vals"])
display(df)'''),
    md("""## `transform` — map over elements

Lambda signature: `(element) -> new_element`. Returns a new array of the same length."""),
    code('''df.select(
    "id", "vals",
    F.transform("vals", lambda x: x * 2).alias("doubled"),
    F.transform("vals", lambda x: F.format_string("v=%d", x)).alias("labeled"),
).show(truncate=False)'''),
    md("""### `transform` with index

Pass a 2-arg lambda `(element, index) -> new_element` to get the position. Index is 0-based."""),
    code('''df.select(
    "id",
    F.transform("vals", lambda x, i: F.struct(i.alias("pos"), x.alias("val"))).alias("indexed"),
).show(truncate=False)'''),
    md("""## `filter` — keep elements matching a predicate

The HOF, not `DataFrame.filter`. Lambda returns a Boolean."""),
    code('''df.select(
    "id", "vals",
    F.filter("vals", lambda x: x > 15).alias("over_15"),
).show(truncate=False)'''),
    md("""## `exists` and `forall` — Boolean checks

- `exists(arr, p)` — at least one element satisfies `p`.
- `forall(arr, p)` — every element satisfies `p` (vacuously True for empty arrays).

Cheaper than `array_contains` when the predicate is more than equality, and works on arrays of structs."""),
    code('''df.select(
    "id", "vals",
    F.exists("vals",  lambda x: x > 50).alias("any_big"),
    F.forall("vals",  lambda x: x > 0).alias("all_positive"),
).show()'''),
    md("""## `aggregate` — fold into a single value

Lambda signature: `(accumulator, element) -> new_accumulator`. You provide the **initial value** (which determines the result type) and optionally a **finisher** that runs on the final accumulator.

Think of it as `reduce` with a starting seed."""),
    code('''df.select(
    "id", "vals",
    F.aggregate("vals", F.lit(0), lambda acc, x: acc + x).alias("sum"),
    F.aggregate("vals", F.lit(1.0), lambda acc, x: acc * x).alias("product"),
).show()'''),
    md("""### `aggregate` with a finisher — computing a mean

The finisher converts the working accumulator (here a struct of running sum + count) into the final value (the mean)."""),
    code('''mean_arr = F.aggregate(
    "vals",
    F.struct(F.lit(0.0).alias("s"), F.lit(0).alias("n")),
    lambda acc, x: F.struct((acc.s + x).alias("s"), (acc.n + 1).alias("n")),
    lambda acc: F.when(acc.n > 0, acc.s / acc.n),  # finisher
)

df.select("id", "vals", mean_arr.alias("mean")).show()'''),
    md("""## `zip_with` — combine two arrays element-wise

Pairs up elements by index. The result is as long as the **shorter** of the two."""),
    code('''pairs = spark.createDataFrame([
    (1, [1, 2, 3], [10, 20, 30]),
    (2, [1, 2],    [100, 200, 300]),  # extra element in second array is dropped
], ["id", "a", "b"])

pairs.select(
    "id",
    F.zip_with("a", "b", lambda x, y: x + y).alias("sum"),
    F.zip_with("a", "b", lambda x, y: F.struct(x.alias("a"), y.alias("b"))).alias("paired"),
).show(truncate=False)'''),
    md("""## SQL syntax — same functions, lambda arrows

In SQL you write `x -> ...` instead of a Python `lambda`. Same semantics, often cleaner inside a long query."""),
    code('''df.createOrReplaceTempView("v")

spark.sql("""
SELECT id,
       transform(vals, x -> x * 2)              AS doubled,
       filter(vals,    x -> x > 15)             AS over_15,
       aggregate(vals, 0, (acc, x) -> acc + x)  AS sum
FROM v
""").show(truncate=False)'''),
    md("""## Why HOFs beat the explode→groupBy pattern

Two equivalent ways to sum each row's array:

```python
# Old way — shuffle and rebuild
(df.select("id", F.explode("vals").alias("v"))
   .groupBy("id").agg(F.sum("v").alias("sum")))

# HOF way — one expression, no shuffle, stays per-row
df.select("id", F.aggregate("vals", F.lit(0), lambda a, x: a + x).alias("sum"))
```

The HOF version:
- Stays inside the row — no shuffle, no extra stage.
- Fuses with surrounding projections.
- Photon can JIT it in C++.

Use the explode pattern only when you genuinely need rows (e.g. to join against another table)."""),
    md("""## NULL semantics

- HOFs on a NULL array return NULL. Empty array stays empty.
- The lambda *body* can return NULL — those NULLs end up in the result array unchanged.
- `forall` on an empty array is **True** (vacuously). `exists` on empty is **False**."""),
    md("""## Takeaways

- Use `transform` / `filter` instead of explode-and-rebuild for array element work.
- `aggregate(arr, init, merge_fn, finisher_fn)` is `reduce` with an optional finisher — perfect for sums, products, means, counts.
- `exists` / `forall` are the `any` / `all` of arrays.
- `zip_with` pairs two arrays by index; result length = min(len_a, len_b).
- In SQL, lambdas use `->` syntax; everything else is identical."""),
]
write_nb("08 - Higher-Order Functions on Arrays.ipynb", nb08)


# ---------------------------------------------------------------------------
# 09 - Window Functions
# ---------------------------------------------------------------------------
nb09 = [
    md("""### Links and Resources
| Function | Use |
|---|---|
| [`Window`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html) | Define partition / order / frame |
| [`row_number`, `rank`, `dense_rank`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.row_number.html) | Position within a partition |
| [`lag`, `lead`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lag.html) | Look back / forward |
| [`percent_rank`, `cume_dist`, `ntile`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percent_rank.html) | Quantile-like positions |
| [Window function intro](https://www.databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html) | Background |"""),
    code(BOOTSTRAP),
    md("""# Window Functions

A window function computes a value **for each row** based on a **window of related rows** — without collapsing rows the way `groupBy` does. They answer questions like:

- "What's each employee's rank within their department?"
- "What's the running 7-day total of sales per store?"
- "What was the previous order date for this customer?"

A window has three parts:

1. **`partitionBy`** — group the rows (e.g. per department).
2. **`orderBy`** — order within the partition (often required).
3. **Frame** — `rowsBetween(...)` or `rangeBetween(...)` — which rows around the current row are visible. Defaults vary by function."""),
    code('''sales = spark.createDataFrame([
    ("ACME", "2024-01-01", 100),
    ("ACME", "2024-01-02", 150),
    ("ACME", "2024-01-03", 120),
    ("ACME", "2024-01-04", 200),
    ("ACME", "2024-01-05",  80),
    ("BETA", "2024-01-01", 300),
    ("BETA", "2024-01-02", 250),
    ("BETA", "2024-01-03", 400),
    ("BETA", "2024-01-04", 350),
], ["store", "date", "sales"]).withColumn("date", F.to_date("date"))

display(sales)'''),
    md("""## `Window.partitionBy(...).orderBy(...)`"""),
    code('''from pyspark.sql.window import Window

w = Window.partitionBy("store").orderBy("date")

sales.select(
    "store", "date", "sales",
    F.row_number().over(w).alias("row_num"),
    F.rank().over(w).alias("rank"),
    F.dense_rank().over(w).alias("dense_rank"),
).show()'''),
    md("""### `row_number` vs `rank` vs `dense_rank`

| Function | Ties | Gaps after ties |
|---|---|---|
| `row_number` | Broken arbitrarily — every row gets a unique number | n/a |
| `rank` | Same value for ties | Yes — next rank skips |
| `dense_rank` | Same value for ties | No |

Use `row_number` to pick "the one row per group" (e.g. latest record per customer). Use `rank` / `dense_rank` for leaderboards."""),
    md("""## Pick the latest row per group — the canonical window pattern

This pattern shows up in every project. Memorize it."""),
    code('''latest_per_store = (sales
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("store").orderBy(F.col("date").desc())
    ))
    .filter(F.col("rn") == 1)
    .drop("rn"))

display(latest_per_store)'''),
    md("""## `lag` and `lead` — look back / forward

`lag(col, n=1)` returns the value from `n` rows earlier in the partition. `lead` looks forward. Both return NULL when no such row exists."""),
    code('''sales.select(
    "store", "date", "sales",
    F.lag("sales", 1).over(w).alias("prev_sales"),
    (F.col("sales") - F.lag("sales", 1).over(w)).alias("delta"),
    F.lead("sales", 1).over(w).alias("next_sales"),
).show()'''),
    md("""## Aggregations over a window — running totals & moving averages

When you put `sum`, `avg`, `count`, etc. `.over(window)`, you get a per-row aggregate computed over the window's frame.

**Important: the default frame depends on whether you ordered.**

- **No `orderBy`** → unbounded frame (whole partition).
- **With `orderBy`** → `rangeBetween(unboundedPreceding, currentRow)` — i.e. running aggregate.

To get something else (rolling window of N rows, centered window, etc.), set the frame explicitly."""),
    code('''# Running total per store
sales.select(
    "store", "date", "sales",
    F.sum("sales").over(w).alias("running_total"),
    F.avg("sales").over(w).alias("running_avg"),
).show()'''),
    md("""## Frames — `rowsBetween` vs `rangeBetween`

- **`rowsBetween(a, b)`** — physical row offsets. `rowsBetween(-2, 0)` = current row plus the two preceding rows.
- **`rangeBetween(a, b)`** — logical offsets on the *value* of the order column. `rangeBetween(-7, 0)` over a date column = last 7 calendar days.

Constants: `Window.unboundedPreceding`, `Window.unboundedFollowing`, `Window.currentRow`."""),
    code('''rolling3 = w.rowsBetween(-2, 0)        # 3-row window: prev 2 + current
sales.select(
    "store", "date", "sales",
    F.avg("sales").over(rolling3).alias("avg_3row"),
    F.max("sales").over(rolling3).alias("max_3row"),
).show()'''),
    code('''# 7-day rolling sum using rangeBetween on a date column.
# Spark needs the order column expressed as a number for rangeBetween — cast to long days since epoch.
days = F.col("date").cast("long") / (24 * 3600)

w_days = (Window.partitionBy("store")
    .orderBy(days)
    .rangeBetween(-6, 0))                 # last 6 days + today = 7 calendar days

sales.select(
    "store", "date", "sales",
    F.sum("sales").over(w_days).alias("rolling_7d_sum"),
).show()'''),
    md("""## Distribution — `percent_rank`, `cume_dist`, `ntile`

- `percent_rank` — `(rank - 1) / (n - 1)`, ranges 0..1.
- `cume_dist` — fraction of rows with value ≤ current.
- `ntile(k)` — split partition into `k` roughly-equal buckets."""),
    code('''sales.select(
    "store", "date", "sales",
    F.percent_rank().over(w).alias("pct_rank"),
    F.cume_dist().over(w).alias("cume"),
    F.ntile(3).over(w).alias("tertile"),
).show()'''),
    md("""## SQL form — same thing with `OVER (...)`"""),
    code('''sales.createOrReplaceTempView("sales")

spark.sql("""
SELECT store, date, sales,
       row_number()                        OVER (PARTITION BY store ORDER BY date)        AS rn,
       sales - LAG(sales) OVER (PARTITION BY store ORDER BY date)                         AS delta,
       AVG(sales) OVER (PARTITION BY store ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_3row
FROM sales
""").show()'''),
    md("""## Performance notes

- Each distinct window spec triggers a **shuffle and sort** by the partition keys. Reuse the same `Window` object when columns share a spec.
- Skewed partition keys (one store with millions of rows) hurt — same problem as a skewed `groupBy`. Salting helps if it's a hot spot.
- `row_number` + filter (`rn == 1`) is often beaten by a `groupBy(...).agg(max_struct(...))` trick when you only need a few columns."""),
    md("""## Takeaways

- Three knobs: **partition** (which rows are siblings), **order** (their sequence), **frame** (which siblings are visible).
- `row_number` for "pick one per group", `rank/dense_rank` for leaderboards.
- `lag/lead` for previous/next row deltas.
- Running aggregate is the default when you `orderBy`; set `rowsBetween`/`rangeBetween` for anything else.
- Reuse `Window` objects to avoid duplicate shuffles."""),
]
write_nb("09 - Window Functions.ipynb", nb09)


# ---------------------------------------------------------------------------
# 10 - Aggregation Functions Deep Dive
# ---------------------------------------------------------------------------
nb10 = [
    md("""### Links and Resources
| Function | Use |
|---|---|
| [`count`, `count_distinct`, `approx_count_distinct`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.approx_count_distinct.html) | Counting, exact and approximate |
| [`sum`, `avg`, `min`, `max`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sum.html) | The basics |
| [`collect_list`, `collect_set`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.collect_list.html) | Aggregate to an array |
| [`percentile_approx`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile_approx.html) | Quantiles (approximate) |
| [`stddev`, `variance`, `corr`, `covar_samp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.stddev.html) | Statistics |
| [GROUPING SETS / CUBE / ROLLUP](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-groupby.html) | Multi-level grouping |"""),
    code(BOOTSTRAP),
    md("""# Aggregation Functions Deep Dive

`groupBy().agg(...)` is one of the most-used patterns in Spark. The basics (`sum`, `count`, `avg`) are obvious; what this notebook covers is everything else: distinct counts, percentiles, list aggregates, statistics, and the `CUBE`/`ROLLUP` family for multi-level summaries."""),
    code('''sales = spark.createDataFrame([
    ("ACME", "EU", "2024-01-01", "alice",   100),
    ("ACME", "EU", "2024-01-02", "bob",     150),
    ("ACME", "EU", "2024-01-02", "alice",   120),
    ("ACME", "US", "2024-01-01", "carol",   200),
    ("ACME", "US", "2024-01-03", "bob",      80),
    ("BETA", "EU", "2024-01-01", "alice",   300),
    ("BETA", "EU", "2024-01-02", "dave",    250),
    ("BETA", "US", "2024-01-03", "carol",   400),
    ("BETA", "US", "2024-01-04", "carol",   350),
], ["store", "region", "date", "customer", "amount"])
display(sales)'''),
    md("""## The basics — `sum`, `avg`, `min`, `max`, `count`

Two `count` flavors:

- `count("*")` / `count(F.lit(1))` — counts rows including those with NULLs.
- `count("col")` — counts only **non-null** values in that column."""),
    code('''sales.groupBy("store").agg(
    F.count("*").alias("rows"),
    F.sum("amount").alias("total"),
    F.avg("amount").alias("avg"),
    F.min("amount").alias("min"),
    F.max("amount").alias("max"),
).show()'''),
    md("""## Distinct counts — exact vs approximate

`count_distinct` is **expensive** at scale: it requires a shuffle keyed on the column to deduplicate. For analytics dashboards over billions of rows, **`approx_count_distinct`** uses HyperLogLog++ and is dramatically faster, with a tunable error rate (default ~5%).

Use exact only when correctness demands it (audit, billing). Use approximate for "how many unique users today"."""),
    code('''sales.groupBy("store").agg(
    F.count_distinct("customer").alias("unique_customers_exact"),
    F.approx_count_distinct("customer", rsd=0.05).alias("unique_customers_approx"),
).show()'''),
    md("""## `collect_list` and `collect_set` — aggregate to an array

- `collect_list` — preserves duplicates and order is *not* guaranteed.
- `collect_set` — deduplicates, also unordered.

Both materialize the whole group in memory on one executor, so watch out for huge groups."""),
    code('''sales.groupBy("store").agg(
    F.collect_list("customer").alias("customer_list"),
    F.collect_set("customer").alias("customer_set"),
).show(truncate=False)'''),
    md("""### Combining `collect_list` with `sort_array` to get a deterministic order

If you need the list ordered, sort by another column *before* collecting (Spark 3.0+ keeps order from the input partition only when run with `spark.sql.execution.sortBeforeRepartition=true`, which is fragile). Safer: collect a struct and sort the array afterwards."""),
    code('''sales.groupBy("store").agg(
    F.array_sort(
        F.collect_list(F.struct("date", "customer", "amount"))
    ).alias("events_sorted")
).show(truncate=False)'''),
    md("""## Percentiles — `percentile_approx`

Median, quartiles, p99 latency. Pass a single percentile or an array."""),
    code('''sales.groupBy("store").agg(
    F.percentile_approx("amount", 0.5).alias("median"),
    F.percentile_approx("amount", [0.25, 0.5, 0.75, 0.99]).alias("quartiles_p99"),
).show(truncate=False)'''),
    md("""## Statistics — `stddev`, `variance`, `skewness`, `kurtosis`, `corr`, `covar_samp`

`stddev` / `variance` are the *sample* versions (Bessel's correction). Use `stddev_pop` / `var_pop` for population statistics."""),
    code('''sales.groupBy("store").agg(
    F.stddev("amount").alias("stddev"),
    F.variance("amount").alias("variance"),
    F.skewness("amount").alias("skewness"),
).show()

# Pairwise stats — same group key, two columns
sales.groupBy("store").agg(
    F.corr(F.length("customer"), "amount").alias("namelen_amount_corr"),
).show()'''),
    md("""## Conditional aggregates — `sum(when(...))`

The classic trick: count or sum only rows that match a condition, without splitting into multiple queries."""),
    code('''sales.groupBy("store").agg(
    F.sum(F.when(F.col("region") == "EU", F.col("amount"))).alias("eu_total"),
    F.sum(F.when(F.col("region") == "US", F.col("amount"))).alias("us_total"),
    F.count(F.when(F.col("amount") > 200, 1)).alias("big_orders"),
).show()'''),
    md("""## `agg` with a dict — quick & dirty

For one-off summaries with default column names, you can pass a dict of `column -> agg_name`:"""),
    code('''sales.groupBy("store").agg({
    "amount":   "sum",
    "customer": "count",
    "*":        "count",
}).show()'''),
    md("""## Multi-level totals — `CUBE`, `ROLLUP`, `GROUPING SETS`

Three ways to compute aggregates at multiple levels in a single pass:

- **`ROLLUP(a, b, c)`** — `(a, b, c)`, `(a, b)`, `(a)`, `()` — hierarchical totals.
- **`CUBE(a, b, c)`** — every subset — full cross-tab.
- **`GROUPING SETS((a, b), (a), ())`** — explicit list of groupings.

Rows where a key is "the grand total" have `NULL` in that column. Use `grouping(col)` to tell apart "real NULL" from "this is a subtotal row"."""),
    code('''sales.rollup("store", "region").agg(
    F.sum("amount").alias("total"),
    F.grouping("store").alias("g_store"),
    F.grouping("region").alias("g_region"),
).orderBy("g_store", "g_region", "store", "region").show()'''),
    code('''sales.cube("store", "region").agg(F.sum("amount").alias("total")) \\
     .orderBy("store", "region").show()'''),
    md("""### `GROUPING SETS` in SQL — the most flexible form"""),
    code('''sales.createOrReplaceTempView("sales")

spark.sql("""
SELECT store, region, SUM(amount) AS total
FROM sales
GROUP BY GROUPING SETS (
    (store, region),
    (store),
    (region),
    ()
)
ORDER BY store, region
""").show()'''),
    md("""## `pivot` — wide-format aggregates

A specialised aggregation that turns distinct values of a column into new columns. Always pass the explicit value list — without it, Spark runs an extra pass to discover them."""),
    code('''sales.groupBy("store").pivot("region", ["EU", "US"]).agg(F.sum("amount")).show()'''),
    md("""## Performance notes

- Distinct counts shuffle by the column being counted — slow with high-cardinality data. Reach for `approx_count_distinct` first.
- `collect_list` / `collect_set` move all values for one group to a single executor — bound the group size or it OOMs.
- Aggregations are partial-aggregated before shuffle (a "combine" step), so `sum`/`count`/`avg` over big data are cheap.
- `count(distinct ...)` cannot partial-aggregate — that's why it's the slow one. `approx_count_distinct` *can* partial-aggregate, hence the speedup."""),
    md("""## Takeaways

- `count("*")` counts rows; `count("col")` counts non-NULLs.
- `approx_count_distinct` is the right default for distinct counts at scale.
- `collect_list` / `collect_set` are powerful but bounded by group size.
- `percentile_approx` for medians/quartiles/p99 — the exact `percentile` function exists but is far slower.
- `sum(when(cond, x))` lets you compute many conditional metrics in one pass.
- `ROLLUP` / `CUBE` / `GROUPING SETS` give multi-level totals without UNION ALL gymnastics."""),
]
write_nb("10 - Aggregation Functions Deep Dive.ipynb", nb10)

print("wrote 3 notebooks to", OUT)
