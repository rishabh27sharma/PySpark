# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("MinandMax")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/movie_ratings.csv')
rdd.collect()

# COMMAND ----------

x = 'The Shawshank Redemption,3'
(x.split(',')[0],int(x.split(',')[1]))

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[0], int(x.split(',')[1]) ))
rdd2.collect()

# COMMAND ----------

"yes" if 1 < 2 else "no"

# COMMAND ----------

x = 5
y = 3
x if x < y else y

# COMMAND ----------

# ('The Matrix', 5)
# ('The Matrix', 3)
# ('The Matrix', 4)

# lambda x, y : x if x < y else y
#        5, 3 => 3
#        3, 4 => 3
        
# ('The Matrix', 3)        

# COMMAND ----------

# MIN
rdd3 = rdd2.reduceByKey(lambda x, y : x if x < y else y)
rdd3.collect()

# COMMAND ----------

# MAX
rdd4 = rdd2.reduceByKey(lambda x, y : x if x > y else y)
rdd4.collect()

# COMMAND ----------


