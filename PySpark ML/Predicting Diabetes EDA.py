# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('diabetes').getOrCreate()

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/diabetes.csv', header=True, inferSchema=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show(2)

# COMMAND ----------

display(df)

# COMMAND ----------

df.describe().show(2)

# COMMAND ----------

df.describe().toPandas()

# COMMAND ----------

df.groupby('outcome').count().show()

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

fig = plt.figure(figsize=(25,15))
st = fig.suptitle('Distribution of Features', fontsize=50, verticalalignment="center")

# COMMAND ----------

df.toPandas().describe().columns

# COMMAND ----------

for col, num in zip(df.toPandas().describe().columns, range(1,11)):
    print(col,num)

# COMMAND ----------

fig = plt.figure(figsize=(25, 15))
st = fig.suptitle("Distribution of Features", fontsize=50, verticalalignment="center")
for col, num in zip(df.toPandas().describe().columns, range(1,11)):
    ax = fig.add_subplot(3,4, num)
    ax.hist(df.toPandas()[col])
    plt.grid(False)
    plt.xticks(rotation=45, fontsize=20)
    plt.yticks(fontsize=15)
    plt.title(col.upper(), fontsize=20)

plt.tight_layout()
st.set_y(0.95)
fig.subplots_adjust(top=0.85, hspace=0.4)
plt.show()

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count, col

# COMMAND ----------

# checking missing values
df.select([count(when(isnan(c), c)).alias(c) for c in df.columns]).toPandas().head()

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# COMMAND ----------

y_udf = udf(lambda y: "no" if y == 0 else "yes", StringType())

# COMMAND ----------

df = df.withColumn("HasDiabetes",y_udf('Outcome')).drop("Outcome")

# COMMAND ----------

df.show()

# COMMAND ----------

def udf_multi(age):
      if (age < 25):
        return "Under 25"
      elif (age >=25 and age <= 35):
        return "Between 25 and 35"
      elif (age > 35 and age < 50):
        return "Between 36 and 50"
      elif (age >= 50):
        return "Over 50"
      else: return "NA"

# COMMAND ----------

age_udf = udf(udf_multi)
df = df.withColumn('age_udf', age_udf('Age'))

# COMMAND ----------

df.show(2)

# COMMAND ----------

df.show(3)

# COMMAND ----------

df_groupAge = df.groupBy('age_udf').count()

# COMMAND ----------

df_groupAge.toPandas()

# COMMAND ----------

sns.barplot(x="age_udf", y="count",data=df_groupAge.toPandas())

# COMMAND ----------

for t in df.dtypes:
    print(t)

# COMMAND ----------

numeric_features = [t[0] for t in df.dtypes if t[1] !='string']
numeric_features_df = df.select(numeric_features)
numeric_features_df.toPandas().head()

# COMMAND ----------


