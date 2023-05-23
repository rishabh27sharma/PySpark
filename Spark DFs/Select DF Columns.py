# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema='True', header='True').csv('StudentData.csv')
df.show()

# COMMAND ----------

df.select('name', 'gender').show()

# COMMAND ----------

# df.select(df.name, df.email).show()

# COMMAND ----------

# from pyspark.sql.functions import col
# df.select(col("roll"), col("name"), col("marks")).show()

# COMMAND ----------

# df.select('*').show()

# COMMAND ----------

print(df.columns)

# COMMAND ----------

# df.select(df.columns[2:5]).show()

# COMMAND ----------

# df.select('email').show()

# COMMAND ----------

# filtering
# df2 = df.select(col('roll'), col('name'))
# df2.show()

# COMMAND ----------


