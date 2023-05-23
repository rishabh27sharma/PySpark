# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
spark = SparkSession.builder.appName("Spark SQL").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

# Creating a Temp View or Table from DataFrame named Student
df.createOrReplaceTempView("Student")

# COMMAND ----------

df = spark.sql("select * from Student")
df.show()

# COMMAND ----------

df = spark.sql("select distinct(course) from Student")
df.show()

# COMMAND ----------

# df.select("course").filter("age" > 20).show()
df = spark.sql("select course from Student where age > 20")
df.show()

# COMMAND ----------

df = spark.sql("select * from Student where marks > 85")
df.show()

# COMMAND ----------

df = spark.sql("select course, count(*) from Student group by course")
df.show()

# COMMAND ----------

df = spark.sql("select course, gender, count(*) from Student group by course, gender")
df.show()

# COMMAND ----------

df = spark.sql("select course, gender, sum(marks) from Student group by course, gender")
df.show()

# COMMAND ----------


