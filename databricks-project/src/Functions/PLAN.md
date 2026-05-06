# Functions in Databricks — Coverage Plan

This folder covers **functions** in Databricks/Spark SQL & PySpark: built-ins, user-defined, higher-order, window, and Unity Catalog SQL functions. Each topic is one notebook following the existing numbered convention (`01 - Topic.ipynb`).

## Goals

- Show idiomatic PySpark and Spark SQL usage side-by-side.
- Explain when to prefer built-ins over UDFs (Catalyst optimization, Photon, serialization cost).
- Cover Unity Catalog–governed SQL functions and Python UDFs.
- Compare performance: built-in vs. Python UDF vs. Pandas UDF vs. Arrow-optimized UDF.

## Notebook Roadmap

### Built-in Functions

1. **01 - Introduction to Functions** — `pyspark.sql.functions` overview, importing conventions (`F.`), function categories, docs navigation.
2. **02 - String Functions** — `concat`, `substring`, `regexp_replace`, `split`, `trim`, `lower/upper`, `format_string`, `lpad/rpad`, `instr`.
3. **03 - Numeric and Math Functions** — `round`, `ceil/floor`, `abs`, `pow`, `sqrt`, `log`, `rand`, `bround`, `greatest/least`.
4. **04 - Date and Timestamp Functions** — `current_date`, `date_add`, `datediff`, `date_format`, `to_date`, `to_timestamp`, `unix_timestamp`, `from_utc_timestamp`, time zones.
5. **05 - Conditional Functions** — `when/otherwise`, `coalesce`, `nullif`, `nvl`, `ifnull`, `case` expressions in SQL.
6. **06 - Collection Functions** — `array`, `array_contains`, `size`, `explode`, `explode_outer`, `posexplode`, `flatten`, `array_distinct`, `sequence`.
7. **07 - Map and Struct Functions** — `map_keys`, `map_values`, `struct`, `getField`, `to_json`, `from_json`, `schema_of_json`.

### Higher-Order & Advanced

8. **08 - Higher-Order Functions on Arrays** — `transform`, `filter`, `aggregate`, `exists`, `forall`, `zip_with`, lambda syntax.
9. **09 - Window Functions** — `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `sum/avg over`, partitioning, ordering, frames (`rowsBetween`, `rangeBetween`).
10. **10 - Aggregation Functions Deep Dive** — `count_distinct`, `approx_count_distinct`, `collect_list`, `collect_set`, `percentile_approx`, `corr`, `stddev`.

### User-Defined Functions

11. **11 - Python UDFs** — registering with `udf()`, return types, null handling, why they're slow (Python serialization), when to avoid.
12. **12 - Pandas UDFs (Vectorized)** — `@pandas_udf`, scalar / grouped map / grouped agg, Arrow optimization, performance comparison vs. plain UDF.
13. **13 - SQL UDFs (Unity Catalog)** — `CREATE FUNCTION ... RETURNS ... RETURN`, governance, sharing across notebooks, permissions.
14. **14 - Python UDFs in Unity Catalog** — registered Python functions, environment, current limits.

### Practical Patterns

15. **15 - Performance Comparison: Built-in vs UDF vs Pandas UDF** — benchmark same logic three ways, read query plans, Photon impact.
16. **16 - Functions in Streaming** — using functions inside structured streaming queries, stateful caveats, deterministic vs non-deterministic.
17. **17 - AI Functions** — `ai_query`, `ai_classify`, `ai_extract`, `vector_search` — Databricks SQL AI functions.

## Conventions

- Use the `DatabricksSession` / `SparkSession` bootstrap pattern from the project memory at the top of each notebook.
- Import as `from pyspark.sql import functions as F`.
- Each notebook ends with a "Key Takeaways" markdown cell and a small exercise.
- Sample data: reuse existing datasets from `02 - Explore DataSets.ipynb` where possible to avoid duplication.

## Out of Scope (covered elsewhere)

- DataFrame transformations themselves → see `07 - DataFrame Transformations.ipynb`.
- Joins → see notebooks 16–19.
- SCD merge logic → see `SCD/` folder.
