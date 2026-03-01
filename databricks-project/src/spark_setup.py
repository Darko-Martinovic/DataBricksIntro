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
