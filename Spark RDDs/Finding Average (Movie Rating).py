# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("findingAverage")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/movie_ratings.csv')
rdd.collect()

# COMMAND ----------

x = 'The Shawshank Redemption,3'
(x.split(',')[0],int(x.split(',')[1]))

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[0], (int(x.split(',')[1]), 1) ))
rdd2.collect()

# COMMAND ----------

# ('The Shawshank Redemption', (3, 1)
# ('The Shawshank Redemption', (2, 1)
# ('The Shawshank Redemption', (2, 1)
# ('The Shawshank Redemption', (5, 1)
 
# lambda x, y: (x[0]+y[0], x[1]+y[1])
#      (3,1) (2,1) => (5,2)
#      (5,2) (2,1) => (7,3)
#      (7,3) (5,1) => (12,4)

# ('The Shawshank Redemption', (12, 4))

# COMMAND ----------

rdd3 = rdd2.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
rdd3.collect()

# COMMAND ----------

y = ('The Shawshank Redemption', (12, 4))
(y[0],y[1][0]/y[1][1])

# COMMAND ----------

rdd4 = rdd3.map(lambda x: (x[0],x[1][0]/x[1][1]))
rdd4.collect()

# COMMAND ----------


