# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("reduceByKey")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample.txt')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd2.collect()

# COMMAND ----------

rdd3 = rdd2.map(lambda x: (x, 1))
rdd3.collect()

# COMMAND ----------

# reduceByKey(lambda x,y: x + y)
#                    4,3 => 7
#                    7,3 => 10
#                    10,5 => 15
#                    15,6 => 21
                      
#                    ('this', 21) => reduction -> reduceByKey(lambda: .....)
#                    ('this', [4,3,3,5,6]) -> grouping -> groupByKey()

# COMMAND ----------

rdd3.reduceByKey(lambda x,y :  x + y).collect()

# COMMAND ----------


