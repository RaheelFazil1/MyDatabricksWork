# Databricks notebook source
from datetime import timedelta, datetime
from pyspark.sql.functions import coalesce,current_date,col

start_time = spark.sql(
    'SELECT date_trunc("second", max(timestamp))  as start_window FROM databricks_poc.abq_litmus_action_agg_zorder where timestamp<"2339-03-21 22:20:00"'
  )
initial_start_time = datetime.strptime("2023-05-24 21:00:12", "%Y-%m-%d %H:%M:%S")
initial_extract_date=initial_start_time
start_time = initial_extract_date if start_time.head()[0] is None else start_time.head()[0] 
end_time= start_time+timedelta(minutes=5)
start_time=start_time.strftime("%Y-%m-%d %H:%M:%S")
end_time=end_time.strftime("%Y-%m-%d %H:%M:%S")
end_time1=datetime.strptime(end_time,'%Y-%m-%d %H:%M:%S')
end_time1
#comment

# COMMAND ----------

from datetime import datetime as dt
Cutoff_time=datetime.now()
#print(Cutoff_time)
while  end_time1<= Cutoff_time: 
     status=dbutils.notebook.run("/Repos/Master/ADB/ABQ/Actionable/02-silver-abq-actionable-5min-agg-batch",1000, {'Start time':start_time,'End time':end_time})
     start_time= dt.strptime(start_time,'%Y-%m-%d %H:%M:%S')+timedelta(minutes=5)
     start_time=start_time.strftime("%Y-%m-%d %H:%M:%S")
     end_time= dt.strptime(end_time,'%Y-%m-%d %H:%M:%S')+timedelta(minutes=5)
     end_time=end_time.strftime("%Y-%m-%d %H:%M:%S")
     end_time1=dt.strptime(end_time,'%Y-%m-%d %H:%M:%S')
