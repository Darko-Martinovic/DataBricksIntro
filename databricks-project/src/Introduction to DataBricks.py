# Databricks notebook source
path = "/Volumes/population_metrics/landing/datasets/countries_dataset/countries_dataset/csv_data/countries_population/"
outputPath = "/Volumes/population_metrics/landing/datasets/output_dataset/delta/country_population"





# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType

schema = StructType([ \
    StructField("COUNTRY_ID",IntegerType(),True), \
    StructField("NAME",StringType(),True), \
    StructField("NATIONALITY",StringType(),True), \
    StructField("CODE",StringType(),True), \
    StructField("ISO_ALPHA3",StringType(),True), \
    StructField("CAPITAL",StringType(),True), \
    StructField("POPULATION",IntegerType(),True), \
    StructField("AREA_KM2",DoubleType(),True), \
    StructField("REGION_ID",IntegerType(),True), \
    StructField("SUB_REGION_ID",IntegerType(),True) \
  ])

# COMMAND ----------

df=spark.read.format("csv").options(header='true').schema(schema).load(path)


# COMMAND ----------

#df.write.format("orc").options(header=True).mode("overwrite").save(outputPath)
df.write.format("delta").mode("overwrite").save(outputPath)
#spark.read.format("delta").load(outputPath).display()
#df.show(truncate=False, n=300)
df.display()
#display(df) # it is possible to profile data 
