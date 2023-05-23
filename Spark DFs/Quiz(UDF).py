# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import DoubleType
spark = SparkSession.builder.appName("Quiz UDFs").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/OfficeData.csv')
df.show()

# COMMAND ----------

def get_increment(state, salary, bonus):
    sum = 0
    if state == "NY":
        sum = salary * 0.10 
        sum += bonus * 0.05
    elif state == "CA":
        sum = salary * 0.12
        sum += bonus * 0.03
        
    return sum   

incrementUDF = udf(lambda x, y, z: get_increment(x, y, z), DoubleType())

df.withColumn("increment", incrementUDF(df.state, df.salary, df.bonus)).show()

# COMMAND ----------


