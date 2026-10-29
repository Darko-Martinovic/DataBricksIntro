# Databricks notebook source
"""Spark session and display() for running the notebooks locally through Databricks Connect.

Every notebook starts with:

    import os
    if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
        from spark_setup import spark, display

On a Databricks cluster the import is skipped and the built-in spark and display() are used.
"""
import os, sys

try:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()
except ImportError:
    if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
        raise RuntimeError(
            f"Wrong Python kernel — 'databricks.connect' not found in:\n  {sys.executable}\n\n"
            "In VS Code: click the kernel selector (top-right of the notebook) "
            "and choose the .venv interpreter at the root of this repository "
            r"(.venv\Scripts\python.exe on Windows, .venv/bin/python elsewhere)."
        )
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()


def display(df, n=1000):
    """Local replacement for the Databricks built-in display().
    Renders the first n rows as a table in the notebook output, wherever it is called in a cell.
    On Databricks the built-in display() is used instead (never imported from here).
    """
    from IPython.display import display as show

    show(df.limit(n).toPandas())
