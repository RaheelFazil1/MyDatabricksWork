# Databricks notebook source
dbutils.widgets.text('MyTable', '')
dbutils.widgets.text('MyStage', '')
a = dbutils.widgets.get('MyTable')
b = dbutils.widgets.get('MyStage')
print(a + ' _ ' + b)
