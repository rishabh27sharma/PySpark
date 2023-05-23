# Databricks notebook source
# dbutils.fs.rm("/FileStore/tables/", True)

# COMMAND ----------

from pyspark.sql import SparkSession

from pyspark.sql.functions import sum, avg, max, min, count
spark = SparkSession.builder.appName("DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/diamonds.csv')
df.show()

# COMMAND ----------

dfGood = df.filter(df.cut == "Good")
updatedDFGood = dfGood.withColumn("price", df.price + 20)
updatedDFGood.show()

# COMMAND ----------

parquetDF = updatedDFGood.write.partitionBy('cut').parquet('/FileStore/tables/diamonds.parquet')

# COMMAND ----------

df1 = spark.read.options(header='True', inferSchema='True').parquet('/FileStore/tables/diamonds.parquet/cut=Good/part-00000-tid-2878300474223788724-11c4bc4f-0faa-4050-80d1-8b91ea2b23d1-157-1.c000.snappy.parquet')

# COMMAND ----------

df1.show()

# COMMAND ----------

df2 = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/emp_details.csv')
df2.show()

# COMMAND ----------

df2 = df2.withColumnRenamed("emp_nm", "emp_full_name")
df2.show()

# COMMAND ----------

df3=df2.filter(df2.dept == 'IT').withColumn("salary", df2.salary)
df3.show()

# COMMAND ----------

df4 = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/product.csv')
df4.show()

# COMMAND ----------

df4.groupBy('category').agg({'rRevenue': 'max'}).show()

# COMMAND ----------

df4.groupBy('category').max().show()

# COMMAND ----------

emp_cols = ["emp_id", "emp_name", "dept_id", "salary"]
emp_data = [("1", "Employee 1", "Dept_1", 20000),
           ("2", "Employee 2", "Dept_1", 30000),
           ("3", "Employee 3", "Dept_2", 20000),
           ("3", "Employee 3", "Dept_3", 20000)]


# COMMAND ----------

emp_df = spark.createDataFrame(data=emp_data, schema = emp_cols)
emp_df.show(truncate=False)

# COMMAND ----------

dept_cols = ["dept_id", "dept_name"]
dept_data = [("Dept_1", "Operations"),
            ("Dept_2", "IT")]

# COMMAND ----------

dept_df = spark.createDataFrame(data=dept_data, schema = dept_cols)
dept_df.show(truncate=False)

# COMMAND ----------

joinedDF = emp_df.join(dept_df,emp_df.dept_id ==  dept_df.dept_id,"left")
joinedDF.show(truncate=False)

# COMMAND ----------

joinedDF.fillna(value='Unknown', subset=['dept_name']).show()

# COMMAND ----------


