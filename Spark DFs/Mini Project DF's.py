# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.functions import sum, avg, min, max, mean, count
spark = SparkSession.builder.appName("Mini Project DataFrames").getOrCreate()

# COMMAND ----------

 df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/OfficeDataProject.csv')
df.show()

# COMMAND ----------

# 1] Total number of employees in the company
df.count()

# COMMAND ----------

# 2] Total number of departments in the company
# df.groupBy("department").count().count()
df.select("department").dropDuplicates(['department']).count()

# COMMAND ----------

# 3] Department names of the company
df.select("department").dropDuplicates(['department']).show()

# COMMAND ----------

# 4] total number of employees in each department
df.groupBy('department').count().show()

# COMMAND ----------

# 5] total number of employees in each state
df.groupBy('state').count().show()

# COMMAND ----------

# 6] total number of employees in each state in each department
df.groupBy('state', 'department').count().show()

# COMMAND ----------

# 7] minimum and maximum salaries in each department and sort salaries in asc order
df.groupBy('department').agg(min("salary").alias("min"), max("salary").alias("max")).orderBy(col("max"), col("min")).show()

# COMMAND ----------

df.filter(df.state == "NY").show()

# COMMAND ----------

# 8] Print the names of employees working in NY state under Finanace department whose bonuses are greater than the average bonuses of employees in NY state

# collect will provide us the actual data removes abstraction

avgBonus = df.filter(df.state == "NY").groupBy('state').agg(avg("bonus").alias("avg_bonus")).select("avg_bonus").collect()[0]['avg_bonus']
# type(avgBonus)

df.filter((df.state == "NY") & (df.department == "Finance") & (df.bonus > avgBonus) ).show()

# COMMAND ----------

# 9] Raise the salaries $500 of all the employees whose age is greater than 45 

def incr_salary(age, currentSalary):
    if age > 45:
        return currentSalary + 500
    else:
        return currentSalary
    
incrSalaryUDF = udf(lambda x, y: incr_salary(x,y), IntegerType())  

df.withColumn('salary', incrSalaryUDF(col("age"), col("salary"))).show()

# COMMAND ----------

# 10] Create DF of all those employees whose age is greater than 45 and save them in a file

df.filter(df.age > 45).write.csv('/FileStore/tables/output_45')

# COMMAND ----------


