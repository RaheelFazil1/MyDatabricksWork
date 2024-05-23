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
    'SELECT date_trunc("second", max(timestamp))  as start_window FROM databricks_poc.abq_litmus_agg_zorder where timestamp<"2339-03-21 22:20:00"'
  )
initial_start_time = datetime.strptime("2021-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
initial_extract_date=initial_start_time
start_time = initial_extract_date if start_time.head()[0] is None else start_time.head()[0] 
end_time= start_time+timedelta(minutes=5)
start_time=start_time.strftime("%Y-%m-%d %H:%M:%S")
end_time=end_time.strftime("%Y-%m-%d %H:%M:%S")
end_time1=datetime.strptime(end_time,'%Y-%m-%d %H:%M:%S')
end_time1
####

# COMMAND ----------

from datetime import datetime as dt
Cutoff_time=datetime.now()
while  end_time1<= Cutoff_time:
     status=dbutils.notebook.run("/Repos/Master/ADB/ABQ/Cov/02-abq-silver-5min-agg-batch",1000, {'Start time':start_time,'End time':end_time})
     start_time= dt.strptime(start_time,'%Y-%m-%d %H:%M:%S')+timedelta(minutes=5)
     start_time=start_time.strftime("%Y-%m-%d %H:%M:%S")
     end_time= dt.strptime(end_time,'%Y-%m-%d %H:%M:%S')+timedelta(minutes=5)
     end_time=end_time.strftime("%Y-%m-%d %H:%M:%S")
     end_time1=dt.strptime(end_time,'%Y-%m-%d %H:%M:%S')
