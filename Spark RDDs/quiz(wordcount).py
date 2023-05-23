# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("wordcount")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/quiz_wordcount_.txt')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd2.collect()

# COMMAND ----------

rdd3 = rdd2.filter(lambda x: len(x) != 0)
rdd3.collect()

# COMMAND ----------

rdd4 = rdd3.map(lambda x: (x, 1))
rdd4.collect()

# COMMAND ----------

rdd5 = rdd4.reduceByKey(lambda x,y: x + y)
rdd5.collect()

# COMMAND ----------

rdd.flatMap(lambda x: x.split(' ')).filter(lambda x: len(x) != 0).map(lambda x: (x, 1)).reduceByKey(lambda x,y: x + y).collect()

# COMMAND ----------


