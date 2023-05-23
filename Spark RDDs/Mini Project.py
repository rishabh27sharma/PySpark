# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Mini Project")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/StudentData.csv')
headers = rdd.first()
# filtering out header
rdd = rdd.filter(lambda x: x != headers)
rdd = rdd.map(lambda x: x.split(','))
rdd.collect()

# COMMAND ----------

# 1] show the number of students in the file
rdd.count()

# COMMAND ----------

# 2] show the total marks achieved by Female and Male students
rdd2 = rdd
rdd2 = rdd2.map(lambda x: (x[1], int(x[5])))
rdd2 = rdd2.reduceByKey(lambda x, y: x + y)
rdd2.collect()

# COMMAND ----------

# 3] show the total number of students that have passed and failed. 50+ marks are required to pass the course.

rdd3 = rdd
passedStudents = rdd3.filter(lambda x: int(x[5]) > 50).count()
failedStudents = rdd3.filter(lambda x: int(x[5]) <= 50).count()
print(passedStudents, failedStudents)

# or failedStudents = rdd.count() - passedStudents

# COMMAND ----------

# 4] show the total number of students enrolled per course
rdd4 = rdd
rdd4 = rdd4.map(lambda x: (x[3], 1))
rdd4 = rdd4.reduceByKey(lambda x, y: x + y)
rdd4.collect()

# COMMAND ----------

# 5] show the total marks that students have achieved per course 
rdd5 = rdd
rdd5 = rdd5.map(lambda x: (x[3], int(x[5])))
rdd5 = rdd5.reduceByKey(lambda x, y: x + y)
rdd5.collect()

# COMMAND ----------

# 6] show the average marks that students have achieved per course
rdd6= rdd
rdd6 = rdd6.map(lambda x: (x[3], (int(x[5]), 1) ))
rdd6 = rdd6.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
# rdd6 = rdd6.map(lambda x: (x[0], x[1][0]/x[1][1]))
rdd6.mapValues(lambda x: (x[0] / x[1])).collect()

# COMMAND ----------

# 7] show the minimum and maximum marks achieved per course
rdd7 = rdd
rdd7 = rdd7.map(lambda x: (x[3], int(x[5])))
print(rdd7.reduceByKey(lambda x, y: x if x > y else y).collect())
print(rdd7.reduceByKey(lambda x, y: x if x < y else y).collect())

# COMMAND ----------

# 8] show the average age of male and female students
rdd8 = rdd
rdd8 = rdd8.map(lambda x: (x[1], (int(x[0]), 1)))
rdd8 = rdd8.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
rdd8.mapValues(lambda x: x[0] / x[1]).collect()

# COMMAND ----------


