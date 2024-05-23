# Databricks notebook source
from datetime import timedelta, datetime
from pyspark.sql.functions import coalesce,current_date,col

start_time = spark.sql(
    'SELECT date_trunc("second", max(timestamp))- INTERVAL 2 hours  as start_window FROM databricks_poc.abq_litmus_agg_zorder where timestamp<"2339-03-21 22:20:00"'
  )
#+ interval 1 second

initial_start_time = datetime.strptime("2023-01-23 19:10:00", "%Y-%m-%d %H:%M:%S")
initial_extract_date=initial_start_time
start_time = initial_extract_date if start_time.head()[0]  is None else start_time.head()[0] 
end_time= datetime.now()-timedelta(minutes=1)
start_time=start_time.strftime("%Y-%m-%d %H:%M:%S")
end_time=end_time.strftime("%Y-%m-%d %H:%M:%S")
end_time1=datetime.strptime(end_time,'%Y-%m-%d %H:%M:%S')
print(start_time)
print(end_time1)

# COMMAND ----------

from datetime import datetime as dt
Cutoff_time=datetime.now()
while  end_time1<= Cutoff_time:  
     status=dbutils.notebook.run("/Repos/Master/ADB/ABQ/General/abq-Device-Tags-Dimension",1000, {'Start time':start_time,'End time':end_time})
     start_time= dt.strptime(start_time,'%Y-%m-%d %H:%M:%S')+timedelta(minutes=5)
     start_time=start_time.strftime("%Y-%m-%d %H:%M:%S")
     end_time= dt.strptime(end_time,'%Y-%m-%d %H:%M:%S')+timedelta(minutes=5)
     end_time=end_time.strftime("%Y-%m-%d %H:%M:%S")
     end_time1=dt.strptime(end_time,'%Y-%m-%d %H:%M:%S')
