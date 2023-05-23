# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
spark = SparkSession.builder.appName("Writing DataFrame").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MODES
# overwrite
# append
# ignore
# error - bydefault mode

# COMMAND ----------

df.write.options(header='True').csv('/FileStore/tables/StudentData/out')

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData/out')
df.show()

# COMMAND ----------

df2 =  df.groupBy("course", "gender").count()
df2.show()

# COMMAND ----------

df2.write.mode("overwrite").options(header='True').csv('/FileStore/tables/StudentData/out')

# COMMAND ----------


