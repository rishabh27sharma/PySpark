# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import sum, avg, max, min, count
spark = SparkSession.builder.appName("groupBy - Filtering in DF").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('StudentData.csv')
print(df.show())

# COMMAND ----------

df.filter(df.gender == "Male").show()

# COMMAND ----------

# filtering before performing groupBy
df2 = df.filter(df.gender == "Male")
df3 = df2.groupBy("course", "gender").agg(count("*").alias("total_enrollments"))

# COMMAND ----------

print(df3.show())

# COMMAND ----------

# filtering after performing groupBy

# where/filter are same
df3.where(df3.total_enrollments > 85).show()
# df3.filter(df3.total_enrollments > 85).show()

# COMMAND ----------

# Alternate way
print(df.filter(df.gender == "Male").groupBy("course", "gender").agg(count("*").alias("total_enrollments")).where(col("total_enrollments") > 85).show())

# COMMAND ----------


