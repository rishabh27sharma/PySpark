# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("saveAsTextFile")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('FileStore/tables/groupByKey.txt')
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,1))

# COMMAND ----------

rdd3.collect()  # combine data from all partitions and provide result

# COMMAND ----------

# Bydefault RDD consists of 2 Partitions
# Apply faltMap on both Partitions independently
# Apply map on both Partitions independently
rdd.getNumPartitions()

# COMMAND ----------

# saveAsTextFile() is used to save the RDD in the file
# saveAsTextFile is an action
rdd3.saveAsTextFile('FileStore/tables/output/output4')

# COMMAND ----------


