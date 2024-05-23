# Databricks notebook source
# MAGIC %md
# MAGIC # 5 Minute Batch Aggregation

# COMMAND ----------

# Containername='nbl'
# StorageAccount= 'dlsniagara'
# configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": "8183ec7b-0640-401c-a4c0-3482614584f8",
#           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="databricks-keyvault-scope",key="databricks-adls"),
#           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/773bda68-87c6-401a-b9a4-3dfb1f92e189/oauth2/token"}

# # Optionally, you can add <directory-name> to the source URI of your mount point.
# mountsSources = [mount.source for mount in dbutils.fs.mounts()]
# mountsMountPoint = [mount.mountPoint for mount in dbutils.fs.mounts()]
# source = f"abfss://{Containername}@{StorageAccount}.dfs.core.windows.net/"
# mount_point = f"/mnt/adls-dlsniagara-mount"
# if source in mountsSources:
#   print(f"Source: {source} already mounted at Mount Point: {dbutils.fs.mounts()[mountsSources.index(source)].mountPoint}")
# else:
#   dbutils.fs.mount(
#   source = source,
#   mount_point = mount_point,
#   extra_configs = configs)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,max,min,col,desc

"""
Create widgets for starttime and end time
"""

start=dbutils.widgets.text("Start time", "2023-01-23 19:10:00")
end=dbutils.widgets.text("End time", "2023-01-23 19:15:00")
start_time = dbutils.widgets.get("Start time")
end_time =dbutils.widgets.get("End time")

# COMMAND ----------

from datetime import datetime as dt
start_time = dt.strptime(dbutils.widgets.get('Start time'),'%Y-%m-%d %H:%M:%S')
end_time = dt.strptime(dbutils.widgets.get('End time'),'%Y-%m-%d %H:%M:%S')

watermark = 1

# Debug settings
interactive = False

# COMMAND ----------

# MAGIC %md
# MAGIC # Source Dataset

# COMMAND ----------

# Query data and get the last value of each second -- handles multiple readings per second
query = (
  f'select distinct devicename,tagname,  timestamp, last(value) over( partition by devicename,tagname,  timestamp order by timestamp) as value from\
  ( SELECT devicename,tagname, date_trunc("second",timestamp) as timestamp,  value \
      FROM databricks_poc.bronze_abq_litmus \
      WHERE tagname like "A32%" \
      AND timestamp >= to_timestamp("{start_time}") \
      AND timestamp < to_timestamp("{end_time}"))' \
)
# Create the source dataframe
try:
 srcDf = spark.sql(query).orderBy("devicename","tagname","timestamp").distinct()
 
except Exception as e:
  dbutils.notebook.exit(e)
if(interactive):
  display(srcDf.select("devicename","tagname").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC # "Full" Dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current Tags

# COMMAND ----------

# Distinct Device/Tag Dataframe from current dataframe

currDf = srcDf.select("deviceName", "tagName").distinct()

if(interactive):
  print("current tag count: " + str(currDf.count()))
  display(currDf)
 # this was commented as we will be reading the distinct device tags from tags dimension

# COMMAND ----------

# MAGIC %md
# MAGIC ## Previous Tags

# COMMAND ----------

from pyspark.sql.functions import date_trunc,expr,window,col
from datetime import datetime, timedelta
# Query previous dataframe for device/tag combination
query = (
  f'SELECT distinct deviceName, tagName, lastValue,timestamp \
  FROM databricks_poc.abq_litmus_agg_zorder \
  WHERE 1=1\
  AND timestamp > to_timestamp("{start_time}") - interval 5 minutes \
  AND timestamp <= to_timestamp("{end_time}") - interval 5 minutes'
)

prevDf = spark.sql(query)

if(interactive):
  print("previous tag count: " + str(prevDf.count()))
  display(prevDf)

# COMMAND ----------

tagsDf = currDf.union(prevDf.select("deviceName","tagName")).distinct()

if(interactive):
  print("total tags: " + str(tagsDf.count()))

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import to_timestamp, year, month, dayofmonth, hour, minute, second,lit

# reformat datetime strings to epoch seconds
start = ((start_time - datetime(1970, 1, 1)).total_seconds())
end = ((end_time - datetime(1970, 1, 1)).total_seconds())
print(f'{start} and {end}')
# use spark.range to create a time dataframe
timeDf = spark.range(start, end, 1)
timeDf = timeDf.select(to_timestamp("id").alias("timestamp"))
timeDf = timeDf.select(year("timestamp").alias("year"),
                      month("timestamp").alias("month"),
                      dayofmonth("timestamp").alias("day"),
                      hour("timestamp").alias("hour"),
                      minute("timestamp").alias("minute"),
                      second("timestamp").alias("second"),
                      "timestamp")

if(interactive):
  display(timeDf.orderBy("timestamp"))

# COMMAND ----------

# Create a "full" dataframe by crossjoining tags with 300s of data
fullDf = tagsDf.crossJoin(timeDf)

if(interactive):
  print("total records: " + str(fullDf.count()))
  display(fullDf)

# COMMAND ----------

from pyspark.sql.functions import col, when, coalesce

# Join condition for every minute and second
joinCondition = ["deviceName", "tagName", "timestamp"]

# Join the dataset dataframe with the "full" dataframe
stageDf = fullDf.alias("fullDf").join(
    srcDf.alias("srcDf"),
    joinCondition,
    "full_outer"
  ).select(
    coalesce(col("srcDf.devicename"), col("fullDf.deviceName")).alias("deviceName"),
    coalesce(col("srcDf.tagname"), col("fullDf.tagName")).alias("tagName"),
    coalesce(col("srcDf.timestamp"), col("fullDf.timestamp")).alias("timestamp"),     
    when(col("srcDf.value").alias("value").cast("float").isNotNull(),col("srcDf.value").alias("value").cast("double")).otherwise(col("srcDf.value").cast("boolean").cast("float")).alias("value")
)


if(interactive):
  print('total records:'+str(stageDf.count()))
  display(stageDf)

# COMMAND ----------

from pyspark.sql.functions import when, lit

# Join the last value
joinCondition = ["deviceName", "tagName"]

# If the start value is null, pull in the last value
dataDf = stageDf.alias("dataDf").join(
    prevDf.alias("prevDf"),
    joinCondition,
    "left_outer").select(
      col("dataDf.deviceName"),
      col("dataDf.tagName"),
      col("dataDf.timestamp"),
      col("dataDf.value").alias("srcValue"),
      col("prevDf.lastValue").alias("lastValue"), \
      when((col("dataDf.value").isNull()) &
           (minute(col("dataDf.timestamp")) == minute(to_timestamp(lit(f'{start_time}')))) &
           (second(col("dataDf.timestamp")) == lit(0)), when(col("prevDf.lastValue").isNull(), lit(0)).otherwise(col("prevDf.lastValue"))).otherwise(col("dataDf.value")).alias("value")
  )
#why do we need second and minute condition -- minute ans second conditioin is needed to get the value from previous only window only onto 0th second if it is null

if(interactive):
  print('total records:'+str(dataDf.count()))
  display(dataDf)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import last

# define the window
window = Window.partitionBy('deviceName', 'tagName')\
               .orderBy('timestamp')\
               .rowsBetween(-300, 0)

# define the forward-filled column
filled_column = last(dataDf['value'], ignorenulls=True).over(window)

# forward fill the data
filledDf = dataDf.withColumn('filled_value', filled_column)

if(interactive):
  # use a tagName not in the source data to check if lastValue is populating value properly
  print('total records:'+str(filledDf.count()))
  display(filledDf)

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregate Results

# COMMAND ----------

from pyspark.sql.functions import asc, first, min, max, avg,last,expr,col
from pyspark.sql import Window
# Produce aggregation results and write to silver table
aggDf = (
  filledDf
    .orderBy("deviceName","tagName")
    .groupBy("deviceName", "tagName")
    .agg(
      (last(col("timestamp")+ expr('INTERVAL 1 second'))).alias("timestamp"),
      last("filled_value").alias("lastValue"),
      first("filled_value").alias("firstValue"),
      min("filled_value").alias("minValue"),
      max("filled_value").alias("maxValue"),
      avg("filled_value").alias("avgValue")
    )
)

if(interactive):
  print('total records:'+str(aggDf.count()))
  display(aggDf)


# COMMAND ----------

from pyspark.sql.functions import struct

# calculate the mode
modeDf = (
  filledDf
  .groupBy("deviceName", "tagName", "filled_value")
  .count()
  .groupBy("deviceName","tagName")
  .agg(
    max(struct(
      col("count"),
      col("filled_value")
    )).alias('max'))
  .select("deviceName", "tagName", col("max.filled_value").alias("modeValue"))
 )

# COMMAND ----------

from pyspark.sql.functions import concat,col,lit,year,month,dayofmonth, concat_ws
# Prepare the final silver dataframe
silverDf = (
  aggDf.join(
    modeDf,
    [
      aggDf.deviceName == modeDf.deviceName,
      aggDf.tagName == modeDf.tagName
    ],
    "inner")
).select(
  aggDf.deviceName,
  aggDf.tagName,
  "timestamp",
  col("lastValue").cast('double'),
  col("firstValue").cast('double'),
  col("minValue").cast('double'),
  col("modeValue").cast('double'),
  col("maxValue").cast('double'),
  col("avgValue").cast("double")
)

if(interactive):
  print("agg records: " + str(silverDf.count()))


# COMMAND ----------

silverDf=silverDf.withColumn("insertdate",lit(datetime.now())).withColumn("PROC",lit('ADB')).withColumn("devicetag",concat(col("deviceName"),lit('~'),col("tagName")))

# COMMAND ----------

# df_tagt = spark.read.table("databricks_poc.abq_litmus_agg_tagt")

# df_ksql = silverDf.join(df_tagt,"devicetag").select(concat_ws('~',silverDf.devicetag, silverDf.timestamp).alias("Unique_key") , silverDf.deviceName, silverDf.tagName, silverDf.timestamp, silverDf.lastValue, silverDf.firstValue, silverDf.minValue, silverDf.modeValue, silverDf.maxValue, silverDf.avgValue, silverDf.insertdate, silverDf.PROC, silverDf.devicetag, df_tagt.description )


# COMMAND ----------

# confluentClusterName : "litmus"
# confluentBootstrapServers = "pkc-41973.westus2.azure.confluent.cloud:9092"
# schemaRegistryUrl = "https://psrc-gq7pv.westus2.azure.confluent.cloud"
# confluentApiKey = "5JJK7QKEM5CPXF7K"
# confluentSecret = "zwvmHVVU8JOaruVpeQdzzNvAhdDvHEH+QP/DA/SxZgD+y+CATvpfpduobGBSJocc"


# topicProduce = "abq-litmus-agg-ksql"
# df_ksql.selectExpr(f'CAST(concat(deviceTag,"~", timestamp) AS STRING) AS key', 'to_json(struct(*)) AS value') \
#   .write \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", confluentBootstrapServers) \
#   .option("kafka.security.protocol", "SASL_SSL") \
#   .option("kafka.sasl.jaas.config", f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{confluentApiKey}' password='{confluentSecret}';") \
#   .option("kafka.ssl.endpoint.identification.algorithm", "https") \
#   .option("kafka.sasl.mechanism", "PLAIN") \
#   .option("topic", topicProduce) \
#   .save()

# COMMAND ----------

#final_df=silverDf.createOrReplaceTempView('final_df_table')

# COMMAND ----------

# %sql
# merge into `databricks_poc`.`abq_litmus_agg_zorder` target 
# using final_df_table source
# ON target.devicename=source.devicename
# AND target.tagname=source.tagname
# and target.timestamp=source.timestamp
# when matched then update set * 
# when not matched then insert *

# COMMAND ----------

# Write data to silver agg table
silverDf.write.mode("append").saveAsTable("databricks_poc.abq_litmus_agg_zorder")
