# Databricks notebook source
# Delete files from DBFS
# dbutils.fs.rm("/FileStore/tables/", True)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

spark = SparkSession.builder.appName("Collaborative Filtering").getOrCreate()

# COMMAND ----------

moviesDF = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/movies.csv')

ratingsDF = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/ratings.csv')

# COMMAND ----------

moviesDF.printSchema()
moviesDF.show()

# COMMAND ----------

ratingsDF.printSchema()
ratingsDF.show()

# COMMAND ----------

display(moviesDF) # display() only for databricks

# COMMAND ----------

display(ratingsDF)

# COMMAND ----------

# JOINING DFs
ratings = ratingsDF.join(moviesDF, 'movieId', 'left')

# COMMAND ----------

display(ratings)

# COMMAND ----------

ratings.count()

# COMMAND ----------

type(ratings)

# COMMAND ----------

# CREATE TRAIN AND TEST DATA
(train, test) = ratings.randomSplit([0.8, 0.2])

# COMMAND ----------

print(train.count())
train.show()

# COMMAND ----------

print(test.count())
test.show()

# COMMAND ----------

# ALS MODEL

als = ALS(userCol = "userId", itemCol = "movieId", ratingCol = "rating", nonnegative=True, implicitPrefs=False, coldStartStrategy="drop")

# COMMAND ----------

# HYPERPARAMETER TUNING AND CROSS VALIDATION
# first create ParamGrid

param_grid = ParamGridBuilder() \
            .addGrid(als.rank, [10, 50, 100, 150]) \
            .addGrid(als.regParam, [.01, .05, .1, .15]) \
            .build()

# COMMAND ----------

# it will create (4*4) -> 16 models
len(param_grid)

# COMMAND ----------

# then create evaluator
evaluator = RegressionEvaluator(
           metricName="rmse", 
           labelCol="rating", 
           predictionCol="prediction")

# COMMAND ----------

# Cross Validator consider param_grid and evaluator and provide us the best model
cv = CrossValidator(estimator=als, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=5)

# COMMAND ----------

# BUILD MODEL
model = cv.fit(train)
best_model = model.bestModel
test_predictions = best_model.transform(test)
RMSE = evaluator.evaluate(test_predictions)

# COMMAND ----------

print(RMSE)

# COMMAND ----------

# Recommendations
recommendations = best_model.recommendForAllUsers(5)

# COMMAND ----------

df = recommendations

# COMMAND ----------

display(df)

# COMMAND ----------

# 1,["A", "B", "cat"]
# 2,["AA", "W", "bat"]

# explode()

# 1,A
# 1,B
# 1,cat
# 2,AA
# 2,W
# 3,bat

# COMMAND ----------

df2 = df.withColumn("movieid_rating", explode("recommendations"))

# COMMAND ----------

display(df2)

# COMMAND ----------

display(df2.select("userId", col("movieid_rating.movieId"), col("movieid_rating.rating")))

# COMMAND ----------


