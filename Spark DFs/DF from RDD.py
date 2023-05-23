# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrames from RDD").getOrCreate()

# COMMAND ----------

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("RDD")
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile('/FileStore/tables/StudentData.csv')
headers = rdd.first()
# filtering out header
rdd = rdd.filter(lambda x: x != headers).map(lambda x: x.split(','))
rdd = rdd.map(lambda x: [int(x[0]), x[1], x[2], x[3], x[4], x[5], x[6]])
rdd.collect()

# COMMAND ----------

columns = headers.split(',')
dfRdd = rdd.toDF(columns)
dfRdd.show()
dfRdd.printSchema()

# COMMAND ----------

# CREATING A SCHEMA
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
                    StructField("age", IntegerType(), True), 
                    StructField("gender", StringType(), True), 
                    StructField("name", StringType(), True),
                    StructField("course", StringType(), True), 
                    StructField("roll", StringType(), True), 
                    StructField("marks", IntegerType(), True), 
                    StructField("email", StringType(), True)     
])

# COMMAND ----------

dfRdd = spark.createDataFrame(rdd, schema=schema)
dfRdd.show()
dfRdd.printSchema()

# COMMAND ----------


