# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Quiz").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df = df.withColumn("total_marks", lit(120))
df.show()

# COMMAND ----------

df = df.withColumn("average", (col("marks") / col("total_marks")) * 100 )
df.show()

# COMMAND ----------

df_OOP = df.filter((df.course == "OOP") & (df.average > 80))
df_OOP.show()

# COMMAND ----------

df_Cloud = df.filter((df.course == "Cloud") & (df.average > 60))
df_Cloud.show()

# COMMAND ----------

df_OOP.select("name", "marks").show()

# COMMAND ----------

# bydefault shows top 20 rows
df_Cloud.select("name", "marks").show()

# COMMAND ----------


