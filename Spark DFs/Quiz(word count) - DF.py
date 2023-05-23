# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Quiz - word count").getOrCreate()

# COMMAND ----------

df = spark.read.options().text('WordData.txt')
print(df.show())

# COMMAND ----------

df.groupBy("value").count().show()

# COMMAND ----------


