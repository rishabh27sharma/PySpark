# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('PySpark ML').getOrCreate()

# COMMAND ----------

training = spark.read.csv('/FileStore/tables/test.csv', header=True, inferSchema=True)

# COMMAND ----------

training.printSchema()

# COMMAND ----------

training.show()

# COMMAND ----------

training.columns

# COMMAND ----------

# [Age, Experience] -> new feature -> independent feature

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
featureAssembler = VectorAssembler(inputCols=['age', 'Experience'], outputCol='Independent Features') 

# COMMAND ----------

output = featureAssembler.transform(training)

# COMMAND ----------

output.show()

# COMMAND ----------

output.columns

# COMMAND ----------

finalized_data = output.select("Independent Features", "Salary")

# COMMAND ----------

finalized_data.show()

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

#train-test split
train_data, test_data = finalized_data.randomSplit([0.75,0.25])

regressor = LinearRegression(featuresCol='Independent Features', labelCol='Salary')

regressor = regressor.fit(train_data)

# COMMAND ----------

### intercepts
regressor.intercept

# COMMAND ----------

### coefficients
regressor.coefficients

# COMMAND ----------

# Prediction
pred = regressor.evaluate(test_data)

# COMMAND ----------

pred.predictions.show()

# COMMAND ----------

pred.meanAbsoluteError, pred.meanSquaredError

# COMMAND ----------


