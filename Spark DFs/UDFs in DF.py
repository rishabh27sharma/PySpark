# Databricks notebook source
# UDFs -> User Defined Functions

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import IntegerType
spark = SparkSession.builder.appName("UDFs in DataFrame").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/OfficeData.csv')
df.show()

# COMMAND ----------

# UDF
def get_total_salary(salary, bonus):
    return salary + bonus

# registering function as UDF and its return type
totalSalaryUDF = udf(lambda x, y: get_total_salary(x, y), IntegerType())
    
df.withColumn("total_salary", totalSalaryUDF(df.salary, df.bonus)).show()

# COMMAND ----------


