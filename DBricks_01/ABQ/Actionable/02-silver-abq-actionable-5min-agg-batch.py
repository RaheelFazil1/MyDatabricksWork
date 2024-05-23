# Databricks notebook source
# MAGIC %md
# MAGIC # 5 Minute Batch Aggregation of ABQ Actionable Data

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,max,min

"""
Create widgets for starttime and end time
"""

start=dbutils.widgets.text("Start time", "2023-01-23 19:10:00")
end=dbutils.widgets.text("End time", "2023-01-23 19:15:00")
start_time = dbutils.widgets.get("Start time")
end_time =dbutils.widgets.get("End time")
interactive=False

# COMMAND ----------

from datetime import datetime as dt
start_time = dt.strptime(dbutils.widgets.get('Start time'),'%Y-%m-%d %H:%M:%S')
end_time = dt.strptime(dbutils.widgets.get('End time'),'%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# MAGIC %md
# MAGIC # Source Dataset

# COMMAND ----------

from pyspark.sql.functions import date_trunc,expr,window,col
query = (
  f'select distinct devicename,tagname,  timestamp, first_value(value) over( partition by devicename,tagname,  timestamp order by raw_timestamp desc) as value from\
  ( SELECT devicename,tagname,timestamp as raw_timestamp, window(date_trunc("second",timestamp),"5 minutes").end as timestamp,  value \
      FROM databricks_poc.bronze_abq_litmus_action \
      WHERE tagname like "A32%"\
      AND timestamp >= to_timestamp("{start_time}") \
      AND timestamp < to_timestamp("{end_time}"))' \
)
# Create the source dataframe
srcDf = spark.sql(query).orderBy("devicename","tagname","timestamp")
srcDf= srcDf.withColumn("timestamp_dxm",window(date_trunc("second", col('timestamp')- expr("interval 1 second")),"5 minutes").end )\
            .withColumn("lastValue",col("value").cast('double'))\
            .withColumn("firstValue",col("value").cast('double'))\
            .withColumn("minValue",col("value").cast('double'))\
            .withColumn("modeValue",col("value").cast('double'))\
            .withColumn("maxValue",col("value").cast('double'))\
            .withColumn("avgValue",col("value").cast('double'))\
            .select("devicename","tagname",col("timestamp_dxm").alias("timestamp"),"lastValue","firstValue","minValue","modeValue","maxValue","avgValue")

# COMMAND ----------

from pyspark.sql.functions import *
srcDf = (
  srcDf
    .groupBy("deviceName", "tagName","timestamp")
    .agg(
      last("lastValue").alias("lastValue"),
      first("lastValue").alias("firstValue"),
      min("lastValue").alias("minValue"),
      max("lastValue").alias("maxValue"),
      avg("lastValue").alias("avgValue")
    )
)

# COMMAND ----------

from pyspark.sql.functions import struct

# calculate the mode
modeDf = (
  srcDf
  .groupBy("deviceName", "tagName", "lastValue")
  .count()
  .groupBy("deviceName","tagName")
  .agg(
    max(struct(
      col("count"),
      col("lastValue")
    )).alias('max'))
  .select(col("deviceName").alias("deviceName1"), col("tagName").alias("tagName1"), col("max.lastValue").alias("modeValue"))
 )

# COMMAND ----------

from pyspark.sql.functions import concat,col,lit,year,month,dayofmonth
# Prepare the final silver dataframe
silverDf = (
  srcDf.alias("srcDf").join(
    modeDf,
    [
      srcDf.deviceName == modeDf.deviceName1,
      srcDf.tagName == modeDf.tagName1
    ],
    "inner")
).select(
  srcDf.deviceName,
  srcDf.tagName,
  "timestamp",
  col("lastValue").cast('double'),
  col("firstValue").cast('double'),
  col("minValue").cast('double'),
  col("modeValue").cast('double'),
   col("maxValue").cast('double'),
  col("avgValue").cast("double")
   
)

# COMMAND ----------

from pyspark.sql.functions import date_trunc,expr,window,col,lit,concat, concat_ws
from datetime import datetime 
silverDf=silverDf.withColumn("insertdate",lit(datetime.now())).withColumn("PROC",lit('ADB')).withColumn("devicetag",concat(col("deviceName"),lit('~'),col("tagName")))
#display(silverDf)

# COMMAND ----------

# df_tagt = spark.read.table("databricks_poc.abq_litmus_agg_tagt")

# df_ksql = silverDf.join(df_tagt,"devicetag").select(concat_ws('~',silverDf.devicetag, silverDf.timestamp).alias("Unique_key") , silverDf.deviceName, silverDf.tagName, silverDf.timestamp, silverDf.lastValue, silverDf.firstValue, silverDf.minValue, silverDf.modeValue, silverDf.maxValue, silverDf.avgValue, silverDf.insertdate, silverDf.PROC, silverDf.devicetag, df_tagt.description )


# COMMAND ----------

# confluentClusterName : "litmus"
# confluentBootstrapServers = "pkc-41973.westus2.azure.confluent.cloud:9092"
# schemaRegistryUrl = "https://psrc-gq7pv.westus2.azure.confluent.cloud"
# confluentApiKey = "5JJK7QKEM5CPXF7K"
# confluentSecret = "zwvmHVVU8JOaruVpeQdzzNvAhdDvHEH+QP/DA/SxZgD+y+CATvpfpduobGBSJocc"



# topicProduce = "abq-litmus-actionable-agg-ksql"
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

silverDf.write.mode("append").saveAsTable("databricks_poc.abq_litmus_action_agg_zorder")
