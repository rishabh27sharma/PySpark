# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("count")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('FileStore/tables/groupByKey.txt')
rdd.collect()

# COMMAND ----------

# count returns the number of elements in RDD
# count is an action
rdd.count()

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd2.collect()

# COMMAND ----------

rdd2.count()

# COMMAND ----------

# countByValue() provide how many times each value occure in RDD
# countByValue is an action

rdd.countByValue()

# COMMAND ----------

rdd2.countByValue()

# COMMAND ----------


