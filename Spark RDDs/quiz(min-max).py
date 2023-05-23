# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("MINMAX(quiz)")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/average_quiz_sample.csv')
rdd.collect()

# COMMAND ----------

x = 'JAN,NY,3.0'
(x.split(',')[0], float(x.split(',')[2]))

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[1], float(x.split(',')[2])))
rdd2.collect()

# COMMAND ----------

# MIN
rdd3 = rdd2.reduceByKey(lambda x, y : x if x < y else y)
rdd3.collect()

# COMMAND ----------

# MAX
rdd4 = rdd2.reduceByKey(lambda x, y : x if x > y else y)
rdd4.collect()

# COMMAND ----------


