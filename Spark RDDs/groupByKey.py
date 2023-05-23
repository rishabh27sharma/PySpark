# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("groupByKey")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/groupByKey.txt')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd2.collect()

# COMMAND ----------

rdd3 = rdd2.map(lambda x: (x, len(x)))
rdd3.collect()

# COMMAND ----------

# mapValues(list) are usually used to get the group data
# groupByKey is used to create groups based on Keys in RDD
# data must be in (key, value) format
rdd3.groupByKey().mapValues(list).collect()

# COMMAND ----------


