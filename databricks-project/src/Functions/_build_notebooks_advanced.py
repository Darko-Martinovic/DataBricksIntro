"""Generate notebooks 08-10: higher-order, window, aggregation."""
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
| [`transform`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.transform.html) | Map a lambda over each element |
| [`filter`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.filter.html) | Keep elements that match a predicate |
| [`aggregate`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.aggregate.html) | Fold an array into a single value |
| [`exists`, `forall`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.exists.html) | Any/all predicates |
| [`zip_with`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.zip_with.html) | Pair-wise combine two arrays |
| [`transform_keys`, `transform_values`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.transform_keys.html) | Lambdas on maps |"""),
    code(BOOTSTRAP),
    md("""# Higher-Order Functions on Arrays

Notebook 06 covered *coarse* array operations — sizes, set ops, contains. This notebook is about **per-element transformations** without leaving the JVM.

Before higher-order functions existed, the only way to e.g. uppercase every string in an array was `explode → upper → collect_list`. That works but is shuffle-heavy. Higher-order functions let Catalyst do it in place.

The key trick is the **lambda**: a small Python expression that becomes a Spark expression, not a UDF. No Python serialization cost, full Catalyst optimization."""),
    code('''df = spark.createDataFrame([
    (1, "Alice",   [10, 25, 50, 70]),
    (2, "Bob",     [5, 15]),
    (3, "Charlie", []),
    (4, "Dana",    [100, 200, 300, 400, 500]),
], ["id", "name", "scores"])
display(df)'''),
    md("""## `transform` — element-wise map

Pass a lambda `x -> expr`. The expression is in Spark's column language — use `F.` functions inside, NOT plain Python."""),
    code('''df.select(
    "name", "scores",
    F.transform("scores", lambda x: x * 2).alias("doubled"),
    F.transform("scores", lambda x: F.when(x > 50, "high").otherwise("low")).alias("labels"),
).show(truncate=False)'''),
    md("""## `filter` — keep matching elements

This is `F.filter` — *not* the DataFrame's `.filter` method. Same name, different operand."""),
    code('''df.select(
    "name",
    F.filter("scores", lambda x: x > 20).alias("over_20"),
    F.filter("scores", lambda x, i: i % 2 == 0).alias("even_idx"),  # second arg = index
).show(truncate=False)'''),
    md("""## `exists` and `forall` — any/all predicates

Return a Boolean column. Cheaper than `array_contains(filter(...))` for the same logic."""),
    code('''df.select(
    "name",
    F.exists("scores", lambda x: x > 100).alias("has_big"),
    F.forall("scores", lambda x: x > 0).alias("all_positive"),
).show()'''),
    md("""## `aggregate` — fold the array to a single value

Three arguments: column, zero/initial value, accumulator lambda. Optional fourth lambda runs on the final accumulator (handy for averaging)."""),
    code('''df.select(
    "name", "scores",
    F.aggregate("scores", F.lit(0), lambda acc, x: acc + x).alias("sum"),
    F.aggregate(
        "scores",
        F.struct(F.lit(0).alias("s"), F.lit(0).alias("n")),
        lambda acc, x: F.struct((acc["s"] + x).alias("s"), (acc["n"] + 1).alias("n")),
        lambda acc: F.when(acc["n"] > 0, acc["s"] / acc["n"]).otherwise(F.lit(None)),
    ).alias("avg"),
).show(truncate=False)'''),
    md("""## `zip_with` — element-wise combine two arrays"""),
    code('''pairs = spark.createDataFrame([
    (1, [1, 2, 3], [10, 20, 30]),
    (2, [5, 5],    [1, 1]),
], ["id", "a", "b"])

pairs.select(
    "a", "b",
    F.zip_with("a", "b", lambda x, y: x + y).alias("sum"),
    F.zip_with("a", "b", lambda x, y: x * y).alias("product"),
).show(truncate=False)'''),
    md("""## Map lambdas — `transform_keys`, `transform_values`, `map_filter`, `map_zip_with`"""),
    code('''m = spark.createDataFrame([
    (1, {"a": 1, "b": 2, "c": 3}),
    (2, {"x": 10, "y": 20}),
], ["id", "props"])

m.select(
    F.transform_values("props", lambda k, v: v * 100).alias("scaled"),
    F.transform_keys("props",   lambda k, v: F.upper(k)).alias("upper_keys"),
    F.map_filter("props",       lambda k, v: v > 1).alias("filtered"),
).show(truncate=False)'''),
    md("""## Why this beats `explode` + `collect_list`

For pure per-element transforms, an `explode → groupBy → collect_list` round-trip:

1. Generates one row per element (memory blow-up).
2. Triggers a shuffle on `groupBy`.
3. Re-aggregates with `collect_list`, which has no preserved order guarantee.

Higher-order functions do all of it in a single project — no shuffle, order preserved, fully optimizable."""),
    code('''# OLD WAY — slow, lossy on order
old = (df.select("name", F.posexplode("scores").alias("pos", "x"))
         .groupBy("name")
         .agg(F.collect_list(F.col("x") * 2).alias("doubled_unordered")))
display(old)

# NEW WAY — fast, ordered
new = df.select("name", F.transform("scores", lambda x: x * 2).alias("doubled"))
display(new)'''),
    md("""## SQL form

The same primitives in SQL syntax, useful when writing inline expressions or `selectExpr`:"""),
    code('''df.selectExpr(
    "name",
    "transform(scores, x -> x * 2)              AS doubled",
    "filter(scores, x -> x > 20)                AS over_20",
    "aggregate(scores, 0, (acc, x) -> acc + x)  AS total",
    "exists(scores, x -> x > 100)               AS has_big",
).show(truncate=False)'''),
    md("""## Takeaways

- A *lambda* here is a Spark expression, not Python — use `F.when`, `+`, etc., inside.
- `transform`, `filter`, `aggregate`, `exists`, `forall`, `zip_with` cover ~95% of element-wise needs.
- Map equivalents: `transform_keys`, `transform_values`, `map_filter`, `map_zip_with`.
- Always prefer these over `explode → groupBy → collect_list` — no shuffle, preserved order.
- Order of arguments to lambdas: `(value)` for unary, `(value, index)` for indexed variants, `(acc, x)` for `aggregate`."""),
]
write_nb("08 - Higher-Order Functions on Arrays.ipynb", nb08)


# ---------------------------------------------------------------------------
# 09 - Window Functions
# ---------------------------------------------------------------------------
nb09 = [
    md("""### Links and Resources
| Function | Use |
|---|---|
| [`Window`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html) | Defines partition / order / frame |
| [`row_number`, `rank`, `dense_rank`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.row_number.html) | Ranking |
| [`lag`, `lead`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lag.html) | Previous / next row |
| [`first`, `last`, `nth_value`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.first.html) | Frame-bounded picks |
| [`rowsBetween`, `rangeBetween`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.rowsBetween.html) | Frame definitions |
| [SQL `OVER` clause](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html) | SQL form |"""),
    code(BOOTSTRAP),
    md("""# Window Functions

A window function computes a value for each row **based on a set of related rows** (the window) — without collapsing the rows like a `groupBy` does. They're how you express:

- "What rank is this customer's order within their own history?"
- "What was their previous order's amount?"
- "What's the running total of revenue per region?"
- "What's the 7-day moving average of sales?"

Three pieces define a window:

1. **Partition** — independent groups (`partitionBy`).
2. **Order** — within each partition (`orderBy`).
3. **Frame** — which rows of the partition contribute (`rowsBetween` / `rangeBetween`).

Most ranking functions ignore the frame; aggregate windows depend on it heavily."""),
    code('''from pyspark.sql.window import Window

orders = spark.createDataFrame([
    (1, "alice", "2024-01-05",  120.0),
    (2, "alice", "2024-02-10",   80.0),
    (3, "alice", "2024-03-15",  200.0),
    (4, "alice", "2024-04-20",   50.0),
    (5, "bob",   "2024-01-12",  300.0),
    (6, "bob",   "2024-02-18",  150.0),
    (7, "bob",   "2024-03-22",  250.0),
    (8, "carol", "2024-02-08",   90.0),
], ["order_id", "customer", "order_date", "amount"]).withColumn(
    "order_date", F.to_date("order_date"))
display(orders)'''),
    md("""## Ranking — `row_number`, `rank`, `dense_rank`

The classic "top N per group" pattern.

| Function | Ties | Gaps |
|---|---|---|
| `row_number` | broken arbitrarily | n/a |
| `rank` | same rank | yes (1, 2, 2, 4) |
| `dense_rank` | same rank | no (1, 2, 2, 3) |"""),
    code('''w = Window.partitionBy("customer").orderBy(F.col("amount").desc())

ranked = orders.select(
    "customer", "order_id", "amount",
    F.row_number().over(w).alias("rn"),
    F.rank().over(w).alias("rk"),
    F.dense_rank().over(w).alias("dr"),
)
display(ranked.orderBy("customer", "rn"))'''),
    md("""## Top-N per group

Filter on `row_number` after the window."""),
    code('''top2_per_customer = ranked.filter(F.col("rn") <= 2).drop("rk", "dr")
display(top2_per_customer.orderBy("customer", "rn"))'''),
    md("""## `lag` and `lead` — previous / next row

Both take an optional offset (default 1) and default value (default NULL)."""),
    code('''w_time = Window.partitionBy("customer").orderBy("order_date")

orders.select(
    "customer", "order_date", "amount",
    F.lag("amount").over(w_time).alias("prev_amount"),
    F.lead("amount").over(w_time).alias("next_amount"),
    (F.col("amount") - F.lag("amount").over(w_time)).alias("delta_vs_prev"),
).orderBy("customer", "order_date").show()'''),
    md("""## Running totals — frame `rowsBetween(unboundedPreceding, currentRow)`

When you add `rowsBetween`, the aggregate becomes a *window aggregate*."""),
    code('''w_running = (Window
    .partitionBy("customer")
    .orderBy("order_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow))

orders.select(
    "customer", "order_date", "amount",
    F.sum("amount").over(w_running).alias("running_total"),
    F.avg("amount").over(w_running).alias("running_avg"),
).orderBy("customer", "order_date").show()'''),
    md("""## Moving average — symmetric `rowsBetween`

`rowsBetween(-1, 1)` is a 3-row centered window: previous, current, next."""),
    code('''w_3row = (Window
    .partitionBy("customer")
    .orderBy("order_date")
    .rowsBetween(-1, 1))

orders.select(
    "customer", "order_date", "amount",
    F.avg("amount").over(w_3row).alias("3row_avg"),
).orderBy("customer", "order_date").show()'''),
    md("""## `rowsBetween` vs `rangeBetween`

- **`rowsBetween`** counts physical rows. `rowsBetween(-2, 0)` = the previous 2 rows + current.
- **`rangeBetween`** uses the value of the ORDER BY column. `rangeBetween(-7, 0)` with an int `day_offset` = "everything within 7 of the current row's value".

Range windows are how you build true *time-bounded* moving averages — "average over the last 7 days regardless of how many orders there were"."""),
    code('''# Convert order_date to a numeric day offset for rangeBetween
o_num = orders.withColumn("day", F.datediff("order_date", F.lit("2024-01-01")))

w_7day = (Window
    .partitionBy("customer")
    .orderBy("day")
    .rangeBetween(-7, 0))

o_num.select(
    "customer", "order_date", "amount",
    F.sum("amount").over(w_7day).alias("sum_last_7d"),
    F.count("*").over(w_7day).alias("orders_last_7d"),
).orderBy("customer", "order_date").show()'''),
    md("""## Window aggregates without a frame

If you specify `partitionBy` + `orderBy` but no `rowsBetween`, the **default frame is `unboundedPreceding` to `currentRow`** for ordered windows. If you omit `orderBy` too, the frame is the entire partition.

This catches people out — a `sum().over(Window.partitionBy("c").orderBy("d"))` is a *running* sum, not a partition total."""),
    code('''w_part = Window.partitionBy("customer")                    # whole partition
w_ord  = Window.partitionBy("customer").orderBy("order_date")  # running

orders.select(
    "customer", "order_date", "amount",
    F.sum("amount").over(w_part).alias("partition_total"),
    F.sum("amount").over(w_ord).alias("running_total"),
).orderBy("customer", "order_date").show()'''),
    md("""## SQL form — the `OVER` clause"""),
    code('''orders.createOrReplaceTempView("orders")

spark.sql("""
SELECT customer, order_date, amount,
       row_number()              OVER (PARTITION BY customer ORDER BY amount DESC) AS rn,
       sum(amount)               OVER (PARTITION BY customer ORDER BY order_date)  AS running,
       avg(amount)               OVER (PARTITION BY customer ORDER BY order_date
                                       ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)   AS smoothed
FROM orders
ORDER BY customer, order_date
""").show()'''),
    md("""## Performance notes

- Every distinct `Window` spec triggers a shuffle + sort. Reuse window definitions where possible.
- A `partitionBy` with very few keys (or none!) sends all data to one executor — `Window.orderBy("d")` without `partitionBy` is a notorious OOM trigger.
- Define windows once, reuse with `.over(w)`."""),
    md("""## Takeaways

- Three knobs: **partition** (groups), **order** (sequencing), **frame** (which rows).
- Ranking functions: `row_number` (always unique), `rank` (gaps), `dense_rank` (no gaps).
- `lag`/`lead` for previous/next; subtract them for deltas.
- Aggregate + `rowsBetween` = moving-window stats.
- Default frame for an ordered window is `unboundedPreceding → currentRow` — running, not total.
- Window without `partitionBy` collapses to one partition and will OOM on large data."""),
]
write_nb("09 - Window Functions.ipynb", nb09)


# ---------------------------------------------------------------------------
# 10 - Aggregation Functions Deep Dive
# ---------------------------------------------------------------------------
nb10 = [
    md("""### Links and Resources
| Function | Use |
|---|---|
| [`count`, `count_distinct`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.count_distinct.html) | Counts |
| [`approx_count_distinct`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.approx_count_distinct.html) | HyperLogLog — fast, fuzzy |
| [`collect_list`, `collect_set`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.collect_list.html) | Aggregate to array |
| [`percentile_approx`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile_approx.html) | Quantiles via t-digest |
| [`stddev`, `variance`, `corr`, `covar_samp`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.stddev.html) | Stats |
| [`grouping_id`, `cube`, `rollup`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.grouping_id.html) | Multi-level group-by |"""),
    code(BOOTSTRAP),
    md("""# Aggregation Functions Deep Dive

Notebook 11 in this project (`Grouping and Aggregation`) covers `groupBy + agg` mechanics. This notebook focuses on the **agg functions themselves** — the ones that go inside `.agg(...)` — and the trade-offs between exact and approximate variants on big data."""),
    code('''sales = spark.createDataFrame([
    ("EMEA", "FR", "alice",   120.0),
    ("EMEA", "FR", "alice",    80.0),
    ("EMEA", "FR", "bob",     200.0),
    ("EMEA", "BE", "alice",    50.0),
    ("EMEA", "BE", "carol",    75.0),
    ("AMER", "US", "dave",    300.0),
    ("AMER", "US", "dave",    300.0),
    ("AMER", "US", "eve",     150.0),
    ("AMER", "CA", "frank",   250.0),
    ("APAC", "JP", "grace",    90.0),
] * 100, ["region", "country", "rep", "amount"])
print("rows:", sales.count())
display(sales, 5)'''),
    md("""## Counts: `count`, `count_distinct`, `approx_count_distinct`

- `count("*")` — total rows.
- `count("col")` — rows where `col IS NOT NULL`.
- `count_distinct(...)` — exact distinct, requires a shuffle that holds every key in memory. **Expensive** on high-cardinality columns.
- `approx_count_distinct(col, rsd=0.05)` — HyperLogLog, ~5% relative error by default, runs in roughly constant memory."""),
    code('''sales.agg(
    F.count("*").alias("rows"),
    F.count("amount").alias("non_null_amount"),
    F.count_distinct("rep").alias("exact_reps"),
    F.approx_count_distinct("rep").alias("approx_reps"),
    F.approx_count_distinct("rep", rsd=0.01).alias("approx_reps_1pct"),
).show()'''),
    md("""## Sum, avg, min, max, and the conditional pattern

The `sum(when(cond, x))` idiom does conditional aggregates without a subquery."""),
    code('''sales.groupBy("region").agg(
    F.sum("amount").alias("total"),
    F.avg("amount").alias("avg"),
    F.min("amount").alias("min"),
    F.max("amount").alias("max"),
    F.sum(F.when(F.col("amount") > 200, F.col("amount"))).alias("big_orders_total"),
    F.count(F.when(F.col("amount") > 200, 1)).alias("big_orders_count"),
).show()'''),
    md("""## `collect_list` vs `collect_set`

- **`collect_list`** — keeps duplicates and arrival order (per-partition, NOT global).
- **`collect_set`** — deduplicates, no order guarantee.

Both load the entire group into a single executor's memory. Watch your group sizes — these are how OOMs happen on real data."""),
    code('''sales.groupBy("region").agg(
    F.collect_list("rep").alias("all_reps"),
    F.collect_set("rep").alias("distinct_reps"),
    F.size(F.collect_set("rep")).alias("n_distinct"),
).show(truncate=False)'''),
    md("""## Quantiles — `percentile_approx`

Exact quantiles need a global sort (huge shuffle). `percentile_approx` uses Greenwald-Khanna and runs in bounded memory.

Pass a single percentile or an array."""),
    code('''sales.groupBy("region").agg(
    F.percentile_approx("amount", 0.5).alias("median"),
    F.percentile_approx("amount", [0.25, 0.5, 0.75, 0.95]).alias("quartiles"),
    F.percentile_approx("amount", 0.99, accuracy=10000).alias("p99_high_acc"),
).show(truncate=False)'''),
    md("""## Spread and shape — `stddev`, `variance`, `skewness`, `kurtosis`

Sample (`stddev_samp`, `var_samp`) are the defaults; population (`stddev_pop`, `var_pop`) divide by `n` instead of `n-1`."""),
    code('''sales.groupBy("region").agg(
    F.round(F.stddev("amount"), 2).alias("std"),
    F.round(F.variance("amount"), 2).alias("var"),
    F.round(F.skewness("amount"), 2).alias("skew"),
    F.round(F.kurtosis("amount"), 2).alias("kurt"),
).show()'''),
    md("""## Correlation and covariance — `corr`, `covar_samp`, `covar_pop`"""),
    code('''pairs = (sales
    .withColumn("discount", F.col("amount") * F.lit(0.1) + F.rand(seed=7))
    .select("region", "amount", "discount"))

pairs.groupBy("region").agg(
    F.round(F.corr("amount", "discount"), 3).alias("r"),
    F.round(F.covar_samp("amount", "discount"), 3).alias("cov"),
).show()'''),
    md("""## Multi-level rollups — `cube`, `rollup`, `grouping_sets`

These compute multiple `groupBy` levels in one pass. Use `grouping_id` (or `grouping`) to tell the levels apart in the output.

- **`rollup(a, b)`** — totals at `(a, b)`, `(a)`, `()` — a hierarchy.
- **`cube(a, b)`** — all 4 combinations: `(a, b)`, `(a)`, `(b)`, `()`.
- **`grouping_sets`** — explicit list."""),
    code('''sales.rollup("region", "country").agg(
    F.sum("amount").alias("total"),
    F.grouping_id().alias("level"),  # 0=detail, 1=region only, 3=grand total
).orderBy("region", "country").show()'''),
    md("""## `agg` with a dict shorthand

For one-function-per-column rollups, this is shorter than `agg(F.sum("a"), F.avg("b"))`."""),
    code('''sales.groupBy("region").agg({"amount": "sum", "rep": "count_distinct"}).show()'''),
    md("""## Multiple aggregates per column — keep them in one pass

Spark only re-reads the source if you split the work. Bundle aggregates in a single `.agg(...)` call so they share a scan and a shuffle."""),
    code('''# GOOD — one pass
sales.groupBy("region").agg(
    F.sum("amount").alias("sum"),
    F.avg("amount").alias("avg"),
    F.min("amount").alias("min"),
    F.max("amount").alias("max"),
).show()

# BAD — four passes
# sales.groupBy("region").agg(F.sum("amount")).show()
# sales.groupBy("region").agg(F.avg("amount")).show()
# ... etc'''),
    md("""## NULL behaviour cheatsheet

| Function | Treats NULL as |
|---|---|
| `count("*")` | counted |
| `count(col)` | skipped |
| `sum`, `avg`, `min`, `max` | skipped (NULL if all are NULL) |
| `collect_list`, `collect_set` | skipped |
| `count_distinct` | skipped |
| `first`/`last` | skipped only if `ignorenulls=True` |"""),
    md("""## Takeaways

- For high-cardinality counts, prefer `approx_count_distinct` — orders of magnitude cheaper.
- `percentile_approx` is the right call for quantiles on big data; pass an array for multiple percentiles in one pass.
- `collect_list`/`collect_set` materialise full groups in one executor — bounded groups only.
- Conditional aggregates: `sum/count(when(cond, ...))` beats joining a filtered subquery.
- Bundle all aggregates in a single `.agg(...)` to share the scan/shuffle.
- `rollup`, `cube`, `grouping_sets` produce multi-level summaries in one pass — pair with `grouping_id` to tag the level."""),
]
write_nb("10 - Aggregation Functions Deep Dive.ipynb", nb10)

print("wrote 3 notebooks (08-10) to", OUT)
