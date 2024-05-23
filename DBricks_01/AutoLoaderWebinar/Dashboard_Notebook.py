# Databricks notebook source
import plotly.graph_objects as go

# Retrieve the data for the candlestick chart
out_df = spark.table('portfolio_value').orderBy('fileId')
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
    xaxis_title='BatchId',    # Add x-axis title
    xaxis_rangeslider_visible=False,
    yaxis_title='Portfolio Value'    # Add y-axis title
)

# Show the chart
fig.show()

# COMMAND ----------

# import plotly.graph_objects as go

# Retrieve the data for the candlestick chart
appl_df = spark.table('appl_agg').orderBy('fileId')
appl_agg = appl_df.toPandas()

# Create the candlestick chart
fig = go.Figure(data=[go.Candlestick(x=appl_agg['fileId'],
                                    open=appl_agg['Open'],
                                    high=appl_agg['High'],
                                    low=appl_agg['Low'],
                                    close=appl_agg['Closing'])])

# Customize the layout
fig.update_layout(
    title="APPL Candlestick Chart",     # Add chart title
    xaxis_title='BatchId',    # Add x-axis title
    xaxis_rangeslider_visible=False,
    yaxis_title='Price'    # Add y-axis title
)

# Show the chart
fig.show()

# COMMAND ----------

# Retrieve the data for the candlestick chart
msft_df = spark.table('msft_agg').orderBy('fileId')
msft_agg = msft_df.toPandas()

# Create the candlestick chart
fig = go.Figure(data=[go.Candlestick(x=msft_agg['fileId'],
                                    open=msft_agg['Open'],
                                    high=msft_agg['High'],
                                    low=msft_agg['Low'],
                                    close=msft_agg['Closing'])])

# Customize the layout
fig.update_layout(
    title="MSFT Candlestick Chart",     # Add chart title
    xaxis_title='BatchId',    # Add x-axis title
    xaxis_rangeslider_visible=False,
    yaxis_title='Price'    # Add y-axis title
)

# Show the chart
fig.show()

# COMMAND ----------

# Retrieve the data for the candlestick chart
tsla_df = spark.table('tsla_agg').orderBy('fileId')
tsla_agg = tsla_df.toPandas()

# Create the candlestick chart
fig = go.Figure(data=[go.Candlestick(x=tsla_agg['fileId'],
                                    open=tsla_agg['Open'],
                                    high=tsla_agg['High'],
                                    low=tsla_agg['Low'],
                                    close=tsla_agg['Closing'])])

# Customize the layout
fig.update_layout(
    title="TSLA Candlestick Chart",     # Add chart title
    xaxis_title='BatchId',    # Add x-axis title
    xaxis_rangeslider_visible=False,
    yaxis_title='Price'    # Add y-axis title
)

# Show the chart
fig.show()
