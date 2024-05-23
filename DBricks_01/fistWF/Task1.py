# Databricks notebook source
import requests

response = requests.get('https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv')
csvfile = response.content.decode('utf-8')
# dbutils.fs.put("/Volumes/main/default/my-volume/babynames.csv", csvfile, True)
dbutils.fs.put("/user/hive/warehouse/r_data/babynames.csv", csvfile, True)

