# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
spark = SparkSession.builder.appName("DF to RDD").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

type(df)

# COMMAND ----------

rdd = df.rdd

# COMMAND ----------

type(rdd)

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd.filter(lambda x: x[1] == "Male").collect()

# COMMAND ----------

rdd.filter(lambda x: x[0] == 28).collect()

# COMMAND ----------

rdd.filter(lambda x: x["age"] == 28).collect()

# COMMAND ----------


