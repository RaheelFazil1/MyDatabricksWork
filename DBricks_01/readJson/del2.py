# Databricks notebook source
storage_account_name = "alstoragecontainer2"
storage_account_access_key = "nTqUBSv0HFlR0yeQYb5a+9w5oOzOxbkW43ik+UG16wvv1HLNnVqurelO2LDQsxJYDgj3svFQF39N+AStIscMIg=="
container = "autoloader-webinar-container"
spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net",storage_account_access_key)


# COMMAND ----------

from pyspark.sql.functions import input_file_name, regexp_extract
# import pyspark.sql.functions as F
df = spark.read \
.option("delimiter", ",") \
.schema("Idx int, Volume int, StockName String, StockPrice Double, timestamp TIMESTAMP") \
.csv(f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/stocks/APPL_1388.csv") \
.withColumn("filePath", input_file_name())

# df = df.withColumn("fileName", F.regexp_extract("filePath", r"([^/]+$)", 1))
df = df.withColumn("fileName", regexp_extract("filePath", r"([^/]+$)", 1)) \
       .withColumn("fileId", regexp_extract("fileName", r".*_(\d+)\.csv", 1))
display(df)

# .withColumn("filePath",input_file_name())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stocks_tble
