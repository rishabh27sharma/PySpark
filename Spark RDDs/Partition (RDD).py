# Databricks notebook source
# repartition() -> used to change the number of partitions in RDD
# It will create a new RDD
# rdd.repartition(no_of_partitions)

# COMMAND ----------

# coalesce() -> used to decrease the number of partitions in RDD
# It will create a new RDD
# rdd.coalesce(no_of_partitions)
# coalesce is only used to decrease the number of partition

# COMMAND ----------

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Partition")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('FileStore/tables/groupByKey.txt')
# rdd.getNumPartitions()
rdd = rdd.repartition(5)
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,1))

# COMMAND ----------

rdd3.saveAsTextFile('FileStore/tables/output/5partitionOutput')

# COMMAND ----------

rdd = sc.textFile('FileStore/tables/groupByKey.txt')
rdd = rdd.repartition(5)
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,1))
rdd3 = rdd3.coalesce(3)
rdd3.getNumPartitions()

# COMMAND ----------

rdd3.saveAsTextFile('FileStore/tables/output/3partitionOutput')

# COMMAND ----------

rdd = sc.textFile('FileStore/tables/output/5partitionOutput')
rdd.collect()

# COMMAND ----------


