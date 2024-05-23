# Databricks notebook source
# from pyspark.sql.types import *

# stockSchema = StructType([
#   StructField("Idx", LongType(), True), 
#   StructField("StockName", StringType(), False), 
#   StructField("StockPrice", DoubleType(), False), 
#   StructField("timestamp", TimestampType(), False)])

# COMMAND ----------

storage_account_name = "alstoragecontainer2"
storage_account_access_key = "nTqUBSv0HFlR0yeQYb5a+9w5oOzOxbkW43ik+UG16wvv1HLNnVqurelO2LDQsxJYDgj3svFQF39N+AStIscMIg=="
container = "autoloader-webinar-container"

# COMMAND ----------

spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net",storage_account_access_key)

# COMMAND ----------

# %sql
# CREATE STREAMING LIVE TABLE raw_stocks
# AS SELECT * FROM cloud_files("wasbs://zeeshan-arif-webinar-container@alstoragecontainer.blob.core.windows.net/stocks/", 'csv')

# COMMAND ----------

import dlt
from pyspark.sql.functions import input_file_name, regexp_extract

@dlt.table
def Stocks_tble():
  df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("header", False) \
  .schema("Idx int, Volume int, StockName String, StockPrice Double, timestamp TIMESTAMP") \
  .load(f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/stocks/") \
  .withColumn("filePath",input_file_name()) \
  .withColumn("fileName", regexp_extract("filePath", r"([^/]+$)", 1)) \
  .withColumn("fileId", regexp_extract("fileName", r".*_(\d+)\.csv", 1).cast("bigint"))
  df = df.drop("filePath", "fileName")
  return df

# COMMAND ----------

from pyspark.sql import functions as F

@dlt.table
def appl_stock():
    apl = dlt.read("Stocks_tble").where(F.col("StockName") == 'APPL')
    return apl

@dlt.table
def msft_stock():
    apl = dlt.read("Stocks_tble").where(F.col("StockName") == 'MSFT')
    return apl

@dlt.table
def tsla_stock():
    apl = dlt.read("Stocks_tble").where(F.col("StockName") == 'TSLA')
    return apl

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def stock_agg(shares, tableName):
    df = dlt.read(tableName)
    df = df.orderBy('timestamp')
    df = df.groupBy('fileId').agg(F.first('StockName').alias('StockName'),
                                    F.first('StockPrice').alias('Open'),
                                    F.min('StockPrice').alias('Low'), 
                                    F.max('StockPrice').alias('High'), 
                                    F.last('StockPrice').alias('Closing'),
                                    F.sum('Volume').alias('Volume'))
    return df

apl_shares = 3750
tsla_shares = 6750
msft_shares = 4500

apl_table = "appl_stock"
tsla_table = "tsla_stock"
msft_table = "msft_stock"

@dlt.table
def appl_agg():
    apl_df = stock_agg(apl_shares, apl_table)
    return apl_df

@dlt.table
def tsla_agg():
    tsla_df = stock_agg(tsla_shares, tsla_table)
    return tsla_df

@dlt.table
def msft_agg():
    msft_df = stock_agg(msft_shares, msft_table)
    return msft_df

# COMMAND ----------


@dlt.table
def portfolio_value():
    appl_df = dlt.read("appl_agg")
    tsla_df = dlt.read("tsla_agg")
    msft_df = dlt.read("msft_agg")
    
    portfolio_df = appl_df.join(tsla_df, tsla_df.fileId == appl_df.fileId).join(msft_df, tsla_df.fileId == msft_df.fileId) \
        .select(appl_df.fileId.alias('fileId'), 
            appl_df.Open.alias('appl_Open'),
            appl_df.Low.alias('appl_Low'), 
            appl_df.High.alias('appl_High'), 
            appl_df.Closing.alias('appl_Closing'), 
            tsla_df.Open.alias('tsla_Open'),
            tsla_df.Low.alias('tsla_Low'), 
            tsla_df.High.alias('tsla_High'), 
            tsla_df.Closing.alias('tsla_Closing'), 
            msft_df.Open.alias('msft_Open'),
            msft_df.Low.alias('msft_Low'), 
            msft_df.High.alias('msft_High'), 
            msft_df.Closing.alias('msft_Closing'), )
        
    portfolio_Open_df = portfolio_df.withColumn('PortfolioOpen', (F.col('appl_Open') * apl_shares) + (F.col('msft_Open') * msft_shares) + (F.col('tsla_Open') * tsla_shares))
    portfolio_Open_df = portfolio_Open_df.select(F.col('fileId').alias('Open_fileId'), 'PortfolioOpen')

    portfolio_Low_df = portfolio_df.withColumn('PortfolioLow', (F.col('appl_Low') * apl_shares) + (F.col('msft_Low') * msft_shares) + (F.col('tsla_Low') * tsla_shares))
    portfolio_Low_df = portfolio_Low_df.select(F.col('fileId').alias('Low_fileId'), 'PortfolioLow')

    portfolio_High_df = portfolio_df.withColumn('PortfolioHigh', (F.col('appl_High') * apl_shares) + (F.col('msft_High') * msft_shares) + (F.col('tsla_High') * tsla_shares))
    portfolio_High_df = portfolio_High_df.select(F.col('fileId').alias('High_fileId'), 'PortfolioHigh')

    portfolio_Closing_df = portfolio_df.withColumn('PortfolioClosing', (F.col('appl_Closing') * apl_shares) + (F.col('msft_Closing') * msft_shares) + (F.col('tsla_Closing') * tsla_shares))
    portfolio_Closing_df = portfolio_Closing_df.select(F.col('fileId').alias('Closing_fileId'), 'PortfolioClosing')

    portfolio_value_df = portfolio_Open_df.join(portfolio_Low_df, portfolio_Open_df.Open_fileId == portfolio_Low_df.Low_fileId) \
    .join(portfolio_High_df, portfolio_Open_df.Open_fileId == portfolio_High_df.High_fileId) \
    .join(portfolio_Closing_df, portfolio_Open_df.Open_fileId == portfolio_Closing_df.Closing_fileId).select(F.col('Open_fileId').alias('fileId'), 'PortfolioOpen', 'PortfolioLow', 'PortfolioHigh', 'PortfolioClosing')
    return portfolio_value_df

# COMMAND ----------

# CREATE MATERIALIZED VIEW APPL_stock
# AS SELECT * FROM live.Stocks_tble
# WHERE 'StockName' = 'APPL'

# COMMAND ----------

# CREATE MATERIALIZED VIEW MSFT_stock
# AS SELECT * FROM live.Stocks_tble
# WHERE 'StockName' = 'MSFT'

# COMMAND ----------

# CREATE MATERIALIZED VIEW TSLA_stock
# AS SELECT * FROM live.Stocks_tble
# WHERE 'StockName' = 'TSLA'
