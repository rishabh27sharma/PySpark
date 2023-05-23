# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("withColumnRenamed in DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema='True', header='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df = df.withColumnRenamed("gender", "sex")
df.show()

# COMMAND ----------

df = df.withColumnRenamed("gender", "sex").withColumnRenamed("roll", "roll_no")
df.show()

# COMMAND ----------

df.select(col("name").alias("full Name")).show()

# COMMAND ----------


