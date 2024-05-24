# Databricks notebook source
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

from datetime import timedelta, datetime
from pyspark.sql.functions import coalesce,current_date,col

start_time = spark.sql(
    'SELECT date_trunc("second", max(timestamp))- INTERVAL 2 hours  as start_window FROM databricks_poc.abq_litmus_agg_zorder where timestamp<"2339-03-21 22:20:00"'
  )

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

# from datetime import datetime as dt
# start_time = dt.strptime(dbutils.widgets.get('Start time'),'%Y-%m-%d %H:%M:%S')
# end_time = dt.strptime(dbutils.widgets.get('End time'),'%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# Query data and get the last value of each second -- handles multiple readings per second
symbol='~'
query = (
  f'merge into databricks_poc.abq_litmus_agg_tagt target \
  USING (select distinct devicename,tagname,  description,devicename||"{symbol}"||tagname as devicetag from\
  ( SELECT distinct devicename,tagname, first_value(description) over( partition by devicename,tagname order by timestamp desc) as description  \
      FROM databricks_poc.bronze_abq_litmus \
      WHERE  1=1\
      AND tagname like "A32%"\
      AND timestamp >= to_timestamp("{start_time}") \
      AND timestamp < to_timestamp("{end_time}"))\
  UNION \
 select distinct devicename,tagname,  description,devicename||"{symbol}"||tagname as devicetag from\
  ( SELECT distinct devicename,tagname, first_value(description) over( partition by devicename,tagname order by timestamp desc) as description  \
      FROM databricks_poc.bronze_abq_litmus_action \
      WHERE  1=1\
      AND tagname like "A32%"\
      AND timestamp >= to_timestamp("{start_time}") \
      AND timestamp < to_timestamp("{end_time}"))) source \
  ON target.devicetag=source.devicetag\
  when matched then \
  update set target.description=source.description\
  when not matched then\
  insert  (devicename,tagname,description,devicetag) values (source.devicename,source.tagname,source.description,source.devicetag)\
  ' \
)
# Create the source dataframe
try:
  df=spark.sql(query)
except Exception as e:
  dbutils.notebook.exit(e)
