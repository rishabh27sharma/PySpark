# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Read file")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('sample.txt')

# COMMAND ----------

rdd2  = rdd.map(lambda x: x.split(' '))

# COMMAND ----------

print(rdd.collect())

# COMMAND ----------

print(rdd2.collect())

# COMMAND ----------


