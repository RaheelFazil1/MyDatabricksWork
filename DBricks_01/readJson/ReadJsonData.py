# Databricks notebook source
# Read JSON file using Spark
df = spark.read.option("multiline", "true").json("/FileStore/jsonData/file1.json")
# df = spark.read.json("/FileStore/jsonData/file1.json")
display(df)




# COMMAND ----------

# Python Function to Flatten Json File:

from pyspark.sql.types import *
from pyspark.sql.functions import *

def flatten(df):
#    %md compute Complex Fields (Lists and Structs) in Schema   
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
    #   %md if StructType then convert all sub element to columns.
    #   %md i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
    
    #   %md if ArrayType then add the Array Elements as Rows using the explode function
    #   %md i.e. explode Arrays
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))
    
    #   %md recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df
print('Function run')

# COMMAND ----------


flatten_df = flatten(df)
display(flatten_df)

# COMMAND ----------

# flatten_df.select('quiz_maths_q1_answer', 'quiz_maths_q1_options', 'quiz_maths_q1_question').write.json('/FileStore/jsonData/output')

# COMMAND ----------

# flatten_df.select('quiz_maths_q1_answer', 'quiz_maths_q1_options', 'quiz_maths_q1_question').write.saveAsTable("jsonTotble")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jsontotble
