# Databricks notebook source
# MAGIC %sql
# MAGIC use `dbdemos`.`r_schema`

# COMMAND ----------

babynames = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/hive/warehouse/r_data/babynames.csv")
babynames = babynames.withColumnRenamed("First Name", "First_Name")
babynames.write.saveAsTable("tblBabynames")
# babynames.write.saveAsTable("`dbdemos`.`r_schema`.`tbl_babynames`")
# babynames.write.option("path", "dbdemos/r_schema").saveAsTable("tbl_babynames")
# display(babynames)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tblBabynames

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into tblbabynames
# MAGIC values (2015, 'AARON', 'ALBANY', 'F', 3)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tblBabynames
# MAGIC where First_Name = 'AARON' and County = 'ALBANY'
