# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DFs").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.count() # Action

# COMMAND ----------

df.filter(df.course == "DB").count()

# COMMAND ----------

# distinct to get unique rows
df.distinct().count()

# COMMAND ----------

df.select("gender", "age").distinct().show()

# COMMAND ----------

df.select("gender", "age").distinct().count()

# COMMAND ----------

df.select("gender").distinct().show()

# COMMAND ----------

df.dropDuplicates(["gender"]).show()

# COMMAND ----------

df.dropDuplicates(["gender", "course"]).show()

# COMMAND ----------

# Quiz
df.select("gender", "age", "course").distinct().show()

# COMMAND ----------

df.select("gender", "age", "course").distinct().count()

# COMMAND ----------

df.dropDuplicates(["age", "gender", "course"]).show()

# COMMAND ----------

df.dropDuplicates(["age", "gender", "course"]).count()

# COMMAND ----------


