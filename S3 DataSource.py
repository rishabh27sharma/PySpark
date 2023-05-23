# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in AWS S3. There are two ways to establish access to S3: [IAM roles](https://docs.databricks.com/user-guide/cloud-configurations/aws/iam-roles.html) and access keys.
# MAGIC
# MAGIC *We recommend using IAM roles to specify which cluster can access which buckets. Keys can show up in logs and table metadata and are therefore fundamentally insecure.* If you do use keys, you'll have to escape the `/` in your keys with `%2F`.
# MAGIC
# MAGIC This is a **Python** notebook so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` magic command. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# remove files
dbutils.fs.rm("/FileStore/tables/", True)

# COMMAND ----------

# list files
dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# COMMAND ----------

# let’s read the csv file with AWS keys to Databricks
# Define file type
file_type = 'csv'
# Whether the file has a header
first_row_is_header = 'true'
# Delimiter used in the file
delimiter = ','
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option('header', first_row_is_header)\
.option('sep', delimiter)\
.load('/FileStore/tables/pyspark_s3_databricks_accessKeys.csv')

# COMMAND ----------

aws_keys_df.show()

# COMMAND ----------

ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
ACCESS_KEY

# COMMAND ----------

SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
SECRET_KEY

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe='')  
# The secret key was encoded using urllib.parse.quote for security purposes. safe="" means every character in the secret key is encoded.

# COMMAND ----------

ENCODED_SECRET_KEY

# COMMAND ----------

# It’s time to mount the S3 bucket! We can mount the bucket by passing in the S3 url and the desired mount name to dbutils.fs.mount(). It returns Ture if the bucket is mounted successfully.

# AWS S3 bucket name
AWS_S3_BUCKET = 'pyspark-s3-databricks'
# Mount name for the bucket
MOUNT_NAME = '/mnt/pyspark-s3-databricks'
# Source url
SOURCE_URL = 's3n://{0}:{1}@{2}'.format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# dbutils.fs.unmount("/mnt/pyspark-databricks-s3")

# COMMAND ----------

# Check if the AWS S3 bucket was mounted successfully
display(dbutils.fs.ls('/mnt/pyspark-s3-databricks'))

# COMMAND ----------

# File location and type
file_location = "/mnt/pyspark-s3-databricks/StudentData.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "Student"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# /* Query the created temp table in a SQL cell */

spark.sql('select * from Student where course="Cloud"').show()

# COMMAND ----------

# Since this table is registered as a temp view, it will only be available to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "{{table_name}}"

# df.write.format("{{table_import_type}}").saveAsTable(permanent_table_name)
