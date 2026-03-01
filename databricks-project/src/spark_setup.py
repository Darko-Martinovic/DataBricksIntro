# Databricks notebook source
import os, sys

try:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()
except ImportError:
    if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
        raise RuntimeError(
            f"Wrong Python kernel — 'databricks.connect' not found in:\n  {sys.executable}\n\n"
            "In VSCode: click the kernel selector (top-right of notebook) "
            "and choose the interpreter at:\n"
            r"  C:\Source\Dsa\DataBricks\.venv\Scripts\python.exe"
        )
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()


def display(df, n=None):
    """Local replacement for the Databricks built-in display().
    Renders the DataFrame as a pandas table in the VS Code notebook output.
    On Databricks the built-in display() is used instead (never imported from here).
    """
    return df.toPandas() if n is None else df.limit(n).toPandas()
