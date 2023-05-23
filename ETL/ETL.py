# Databricks notebook source
# dbutils.fs.rm('/FileStore/tables', True)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, explode
import pyspark.sql.functions as f

# COMMAND ----------

# Extract
spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
df = spark.read.text('/FileStore/tables/WordData.txt')
df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

s = "This is a Japanese doll"
s.split(" ")

# COMMAND ----------

# Transform
df2 = df.withColumn("splittedData", f.split("value", " "))
df3 = df2.withColumn("words", explode("splittedData"))
display(df3)

# COMMAND ----------

wordsDF = df3.select("words")
display(wordsDF)

# COMMAND ----------

# word count
wordCount = wordsDF.groupBy("words").count()
display(wordCount)

# COMMAND ----------

# Load
# DataBricks bydefault provides you drivers
# for working locally you have to just download the specific driver

driver = "org.postgresql.Driver"
url = "jdbc:postgresql://endpoint/"
table = "schema.table"
user = "postgres"
password = "password"

wordCount.write.format("jdbc").option("driver", driver).option("url", url).option("dbtable", table).option("mode", "append").option("user", user).option("password", password).save()

# COMMAND ----------


