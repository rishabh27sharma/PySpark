# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema='True', header='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.filter(df.course == "Cloud").show()

# COMMAND ----------

df.filter(col("course") == "DB").show()

# COMMAND ----------

df.filter( (df.course == "Cloud") & (df.marks > 50) ).show()

# COMMAND ----------

courses = ["DB", "Cloud", "OOP", "DSA"]
df.filter(df.course.isin(courses)).show()

# COMMAND ----------

df.filter(df.course.startswith("D")).show()

# COMMAND ----------

df.filter(df.name.endswith("e")).show()

# COMMAND ----------

df.filter(df.name.contains("se")).show()

# COMMAND ----------

df.filter(df.name.like('%se%')).show()

# COMMAND ----------


