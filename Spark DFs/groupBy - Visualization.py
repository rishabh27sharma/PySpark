# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import sum, avg, max, min, count
spark = SparkSession.builder.appName("groupBy - Visualization in DF").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/OfficeData.csv')
df.show()

# COMMAND ----------

df.groupBy("department").count().show()

# COMMAND ----------

df.groupBy("department").sum().show()

# COMMAND ----------

df.groupBy("department").sum("salary").show()

# COMMAND ----------

df.groupBy("department").agg(sum("salary").alias("total_salary"), min("salary").alias("min_salary")).show()

# COMMAND ----------

df.groupBy("department", "state").agg(count("*")).show()

# COMMAND ----------

df.groupBy("department", "state").agg(sum("salary").alias("total_salary"), min("salary").alias("min_salary")).show()

# COMMAND ----------


