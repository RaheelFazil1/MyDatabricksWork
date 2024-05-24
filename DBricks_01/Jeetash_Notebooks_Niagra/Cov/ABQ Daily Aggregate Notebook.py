# Databricks notebook source
# MAGIC %md
# MAGIC Daily Aggregation
# MAGIC

# COMMAND ----------

from datetime import timedelta, datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
start_time_hour=spark.sql("select nvl(max(timestamp),to_timestamp('2021-01-01 00:00:00')) as timestamp from `databricks_poc`.`abq_litmus_agg_zorder_daily` where timestamp<'2339-03-21 22:20:00'")
initial_start_time = datetime.strptime("2021-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
start_time_hour=start_time_hour.withColumn("DiffInDays",round((current_timestamp().cast("long") - col("timestamp").cast("long"))/(24*3600)))
DiffInDays=start_time_hour.head()[1]
start_time = initial_start_time if start_time_hour.head()[0]  is None else start_time_hour.head()[0] 
end_time= datetime.now() if start_time_hour.head()[0]  is None else start_time+timedelta(days=DiffInDays)
print(start_time)
print(end_time)

# COMMAND ----------

from pyspark.sql.functions import window,date_trunc,col,desc,expr,lit
df=spark.sql(f'select devicetag,timestamp,avgvalue,lastvalue,firstvalue,minvalue,maxvalue,modevalue from `databricks_poc`.`abq_litmus_agg_zorder_hour` where timestamp>"{start_time}" and timestamp<="{end_time}" ')
df=df.withColumn("day_end",window(date_trunc("second", col('timestamp')- expr("interval 1 second")),"1 day").start )
df=df.filter(col("timestamp") > lit('1970-01-01 00:00:00'))

# display(df)

# COMMAND ----------

from pyspark.sql.functions import asc,last,min,max,first,last,avg,to_timestamp,lit,count,struct
df_mode=(df.groupBy("devicetag","day_end","modevalue")
     .count()
     .groupBy("devicetag","day_end")
     .agg(max(struct(col("count"),col("modevalue"))).alias("max"))
     .select(col("devicetag").alias("mode_devicetag"),col("day_end").alias("mode_day_end"), col("max.modevalue"))
     )


# COMMAND ----------

from pyspark.sql.functions import asc,last,min,max,first,last,avg,to_timestamp,lit
df=df.orderBy(asc("timestamp"))\
     .groupBy("devicetag","day_end")\
     .agg(\
       last(col('day_end') ).alias("timestamp"),
       last("lastvalue").alias("lastValue"),
       first("firstvalue").alias("firstValue"),
       min("minvalue").alias("minValue"),
       max("maxvalue").alias("maxValue"),
       avg("avgvalue").alias("avgValue")
    ).select("devicetag", "timestamp", "firstValue", "lastValue", "minValue","maxValue","avgValue")

#display(df)


# COMMAND ----------

final_df=df.join(df_mode,[df_mode.mode_devicetag==df.devicetag , df_mode.mode_day_end==df.timestamp],"inner").select("devicetag","timestamp","firstValue","lastValue","minValue","maxValue","modevalue","avgvalue")

# COMMAND ----------

final_df=final_df.createOrReplaceTempView('final_df_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into `databricks_poc`.`abq_litmus_agg_zorder_daily` target 
# MAGIC using final_df_table source
# MAGIC ON target.devicetag=source.devicetag 
# MAGIC and target.timestamp=source.timestamp
# MAGIC when matched then update set * 
# MAGIC when not matched then insert *
