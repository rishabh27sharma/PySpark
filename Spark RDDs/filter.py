# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Filter")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample.txt')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.filter(lambda x: x != '12 12 33')
rdd2.collect()

# COMMAND ----------

def func(x):
    if x == '12 12 33':
        return False
    else:
        return True
    
rdd3 = rdd.filter(func)
rdd3.collect()

# COMMAND ----------


