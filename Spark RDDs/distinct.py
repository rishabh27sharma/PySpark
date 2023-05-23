# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Distinct")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample.txt')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd2.collect()

# COMMAND ----------

rdd3 = rdd2.distinct()
rdd3.collect()

# COMMAND ----------

rdd.flatMap(lambda x: x.split(' ')).distinct().collect()

# COMMAND ----------


