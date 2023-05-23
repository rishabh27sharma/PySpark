# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("sort or orderBy in DF").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.sort("marks").show()

# COMMAND ----------

df.sort("marks", "age").show() 
# df.sort(df.marks, df.age).show() 

# COMMAND ----------

df.orderBy(df.marks, df.age).show() 

# COMMAND ----------

# sort in ascending - decending order (data must be in integer)
df.sort(df.marks.desc(), df.age.asc()).show() 

# COMMAND ----------

df.orderBy(df.marks.desc(), df.age.asc()).show() 

# COMMAND ----------


