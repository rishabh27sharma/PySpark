# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Flap Map")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('sample.txt')
print(rdd.collect())

# COMMAND ----------

mapRdd = rdd.map(lambda x: x.split(' '))
print(mapRdd.collect())

# COMMAND ----------

flatmapRdd = rdd.flatMap(lambda x: x.split(' '))
print(flatmapRdd.collect())

# COMMAND ----------


