# Databricks notebook source
# MAGIC %sql
# MAGIC select * from appl_stock
# MAGIC -- where `timestamp`>= '2024-05-13T18:30:52' and `timestamp` <= '2024-05-13T18:30:53'
# MAGIC order by `timestamp`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from msft_agg

# COMMAND ----------

# from pyspark.sql import functions as F
# from pyspark.sql.window import Window
# apl_df = spark.table("appl_stock")
# apl_df = apl_df.filter((apl_df['timestamp'] >= '2024-05-13T18:30:52') & (apl_df['timestamp'] <= '2024-05-13T18:30:53'))
# df = df.withColumn('Minutes', F.date_format('timestamp', 'mm'))
# df = df.withColumn('Minutes', F.minute(F.col('timestamp')))
# display(apl_df)

# COMMAND ----------

# win = Window.partitionBy('Minutes').orderBy('timestamp')
# df = df.withColumn('High', F.max('StockPrice').over(win))

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def stock_agg(shares, tableName):
    df = spark.table(tableName)
    df = df.orderBy('timestamp')
    df = df.groupBy('fileName').agg(F.first('StockName').alias('StockName'),
                                    F.first('StockPrice').alias('Open'),
                                    F.min('StockPrice').alias('Low'), 
                                    F.max('StockPrice').alias('High'), 
                                    F.last('StockPrice').alias('Closing'),
                                    F.sum('Volume').alias('Volume'))
    df = df.withColumn('PortfolioOpenPrice', (df['Open'] * apl_shares)) \
        .withColumn('PortfolioLowPrice', (df['Low'] * apl_shares)) \
        .withColumn('PortfolioHighPrice', (df['High'] * apl_shares)) \
        .withColumn('PortfolioClosingPrice', (df['Closing'] * apl_shares))
    return df
    

# Number of share are as under
apl_shares = 10
tsla_shares = 35
msft_shares = 55

apl_table = "appl_stock"
tsla_table = "tsla_stock"
msft_table = "msft_stock"

out_df = stock_agg(tsla_shares, msft_table)
display(out_df)

# COMMAND ----------

# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install plotly
# MAGIC
# MAGIC import plotly.graph_objects as go
# MAGIC
# MAGIC out_agg = out_df.toPandas()
# MAGIC
# MAGIC fig = go.Figure(data=[go.Candlestick(x=out_agg['fileName'],
# MAGIC                 open=out_agg['Open'],
# MAGIC                 high=out_agg['High'],
# MAGIC                 low=out_agg['Low'],
# MAGIC                 close=out_agg['Closing'])])
# MAGIC
# MAGIC fig.update_layout(title='Stock Price', xaxis_title='fileName', yaxis_title='Price ($)',
# MAGIC                   xaxis_rangeslider_visible=False)  # Disable range slider for better clarity
# MAGIC
# MAGIC fig.show()

# COMMAND ----------

# %sql
# select * from appl_agg

# COMMAND ----------

# %pip install plotly

# import plotly.graph_objects as go

# appl_agg = df.toPandas()

# fig = go.Figure(data=[go.Candlestick(x=appl_agg['Minutes'],
#                 open=appl_agg['Open'],
#                 high=appl_agg['High'],
#                 low=appl_agg['Low'],
#                 close=appl_agg['Closing'])])

# fig.update_layout(title='APPL Stock Price', xaxis_title='Minute', yaxis_title='Price ($)',
#                   xaxis_rangeslider_visible=False)  # Disable range slider for better clarity

# fig.show()
