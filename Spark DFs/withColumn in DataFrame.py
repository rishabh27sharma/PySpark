# Databricks notebook source
# Manipulating the column values, column datatypes, create new column using withColumn

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("withColumn in DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema='True', header='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

# it will manipulate the column and return a new df so we have to capture it
df = df.withColumn("roll", col("roll").cast("String"))
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("marks", col('marks') + 10)
df.show()

# COMMAND ----------

df = df.withColumn("aggregated marks", col('marks') - 10)
df.show()

# COMMAND ----------

df = df.withColumn("Country", lit('USA'))
df.show()

# COMMAND ----------

# multiple transformations
df = df.withColumn("marks", col("marks") - 10).withColumn("updated marks", col("marks") + 20).withColumn("Country", lit("USA"))
df.show()

# COMMAND ----------


