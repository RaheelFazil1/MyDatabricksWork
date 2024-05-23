# Databricks notebook source
# checkpoint_path = "abfss://dev-bucket@<storage-account>.dfs.core.windows.net/_checkpoint/dev_table"

# (spark.readStream
#   .format("cloudFiles")
#   .option("cloudFiles.format", "json")
#   .option("cloudFiles.schemaLocation", checkpoint_path)
#   .load("abfss://autoloader-source@<storage-account>.dfs.core.windows.net/json-data")
#   .writeStream
#   .option("checkpointLocation", checkpoint_path)
#   .trigger(availableNow=True)
#   .toTable("dev_catalog.dev_database.dev_table"))

# COMMAND ----------

storage_account_name = "alstoragecontainer"
storage_account_access_key = "f0Yub7Z2fLFbWwXJ3GqcVl66GbjuMdg3niXLLwKXlX4h4YlY3ZzMxwIywnoU3IidZ8wFnOdAcJ4K+AStsB5HqQ=="
container = "zeeshan-arif-webinar-container"

# COMMAND ----------

spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net",storage_account_access_key)

# COMMAND ----------

dbutils.fs.ls(f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/")

# COMMAND ----------

# MAGIC %fs ls wasbs://zeeshan-arif-webinar-container@alstoragecontainer.blob.core.windows.net/

# COMMAND ----------

df = spark.read \
  .option("header", "true") \
  .option("inferSchema", "False") \
  .option("delimiter", ",") \
  .csv(f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/stocks/APPL_1.csv")

df.show()

# COMMAND ----------

import dlt

@dlt.table
def bronze_tble():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
    #   .schema("A", "B", "C", "D")
      .load(f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/stocks/")
  )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.r_schema.stocks_tble
# MAGIC where `StockName` = 'APPL'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from hive_metastore.r_schema.stocks_tble
# MAGIC select * from hive_metastore.r_schema.appl_stock

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED appl_stock

# COMMAND ----------

# MAGIC %sql
# MAGIC show views
