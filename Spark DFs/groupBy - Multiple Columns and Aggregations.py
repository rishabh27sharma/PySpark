# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("groupBy Multiple Columns and Aggregations in DF").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.groupBy("course").count().show()

# COMMAND ----------

df.groupBy("course", "gender").count().show()

# COMMAND ----------

from pyspark.sql.functions import sum, avg, max, min, count

# COMMAND ----------

df.groupBy("course").agg(count("*"), sum("marks"), min("marks"), max("marks"), avg("marks")).show()

# COMMAND ----------

df.groupBy("course").agg(count("*").alias("total_enrollment"), sum("marks").alias("total_marks"), min("marks").alias("min_marks"), max("marks").alias("max_marks"), avg("marks").alias("average_marks")).show()

# COMMAND ----------

df.groupBy("course", "gender").agg(count("*").alias("total_enrollment"), sum("marks").alias("total_marks"), min("marks").alias("min_marks"), max("marks").alias("max_marks"), avg("marks").alias("average_marks")).show()

# COMMAND ----------


