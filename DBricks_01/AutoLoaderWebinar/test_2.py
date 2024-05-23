# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def stock_agg(shares, tableName):
    df = spark.table(tableName)
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

apl_shares_percent = 0.25
tsla_shares_percent = 0.3
msft_shares_percent = 0.45

apl_table = "appl_stock"
tsla_table = "tsla_stock"
msft_table = "msft_stock"

appl_df = stock_agg(apl_shares, apl_table)
tsla_df = stock_agg(tsla_shares, tsla_table)
msft_df = stock_agg(msft_shares, msft_table)
display(appl_df)

# COMMAND ----------


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
# portfolio_df.show()
# portfolio_df = portfolio_df.join(msft_df, tsla_df.fileId == msft_df.fileId)
display(portfolio_df)

# COMMAND ----------

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



display(portfolio_value_df)

# COMMAND ----------

# MAGIC %pip install plotly
# MAGIC
# MAGIC import plotly.graph_objects as go
# MAGIC
# MAGIC # Assuming the plotly.graph_objects module is already imported
# MAGIC
# MAGIC # Retrieve the data for candlestick chart
# MAGIC out_df = portfolio_value_df.orderBy('fileId')
# MAGIC out_agg = out_df.toPandas()
# MAGIC
# MAGIC # Create the candlestick chart
# MAGIC fig = go.Figure(data=[go.Candlestick(x=out_agg['fileId'],
# MAGIC                 open=out_agg['PortfolioOpen'],
# MAGIC                 high=out_agg['PortfolioHigh'],
# MAGIC                 low=out_agg['PortfolioLow'],
# MAGIC                 close=out_agg['PortfolioClosing'])])
# MAGIC
# MAGIC # Customize the layout
# MAGIC fig.update_layout(xaxis_title='fileId',
# MAGIC                   xaxis_rangeslider_visible=False)
# MAGIC
# MAGIC # Show the chart
# MAGIC fig.show()

# COMMAND ----------

import plotly.graph_objects as go

# Retrieve the data for candlestick chart
out_df = portfolio_value_df.orderBy('fileId')
out_agg = out_df.toPandas()

# Create the candlestick chart
fig = go.Figure(data=[go.Candlestick(x=out_agg['fileId'],
                                    open=out_agg['PortfolioOpen'],
                                    high=out_agg['PortfolioHigh'],
                                    low=out_agg['PortfolioLow'],
                                    close=out_agg['PortfolioClosing'])])

# Customize the layout
fig.update_layout(
    title="Portfolio Value Candlestick Chart",     # Add chart title
    xaxis_title='fileId',    # Add x-axis title
    xaxis_rangeslider_visible=False,
    yaxis_title='Portfolio Value'    # Add y-axis title
)

# Show the chart
fig.show()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import desc
out_df = spark.table('portfolio_value').orderBy(desc('fileId'))
display(out_df)
