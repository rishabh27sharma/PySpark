# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import sum, avg, max, min, count
spark = SparkSession.builder.appName("groupBy - Quiz").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

# df.groupBy("course").count().show()
df.groupBy("course").agg(count("*").alias("total_enrollment")).show()

# COMMAND ----------

# df.groupBy("course", "gender").count().show()
df.groupBy("course", "gender").agg(count("*").alias("total_enrollment")).show()

# COMMAND ----------

# df.groupBy("course", "gender").sum("marks").show()
df.groupBy("course", "gender").agg(sum("marks")).show()

# COMMAND ----------

df.groupBy("course", "age").agg(avg("marks").alias("avg_marks"), min("marks").alias("min_marks"), max("marks").alias("max_marks")).show()

# COMMAND ----------


