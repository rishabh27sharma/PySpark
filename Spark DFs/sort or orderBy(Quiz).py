# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("sort or orderBy Quiz").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/OfficeData.csv')
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.sort(df.bonus).show()

# COMMAND ----------

df.sort(df.age.desc(), df.salary.asc()).show()

# COMMAND ----------

df.sort(df.age.desc(), df.bonus.desc(), df.salary.asc()).show()

# COMMAND ----------


