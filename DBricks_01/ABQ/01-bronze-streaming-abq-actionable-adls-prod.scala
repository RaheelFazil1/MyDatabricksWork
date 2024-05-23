// Databricks notebook source
// MAGIC %md
// MAGIC # ABQ Plant Actionable Data

// COMMAND ----------

// MAGIC %md
// MAGIC ## Spark Configurations

// COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true") // added 1/18/22
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 1073741824)
//spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

// COMMAND ----------

println(spark.conf.get("spark.databricks.delta.optimizeWrite.enabled"))
println(spark.conf.get("spark.databricks.delta.autoCompact.enabled"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Kafka Configurations

// COMMAND ----------

val confluentClusterName = "litmus"
val confluentBootstrapServers = "pkc-41973.westus2.azure.confluent.cloud:9092"
val confluentTopicName = "abq-litmus-flows-actionable"
val schemaRegistryUrl = "https://psrc-gq7pv.westus2.azure.confluent.cloud"
val confluentApiKey = "LDUZZKTQTCLFQKYY"
val confluentSecret = "XB0sm3RZqtaVAKNBIN140Rh7bYAmgGCFSsRrNBLzBi52FtKFS3x96bbJoHmmI7UB"
val adlsGen2Key = "UgDFEkdIJpyDQ+oK0ir2RMcLvXBjbDAcTEqn4TFzwxjV+HUG3UpTeHflhI+wI1IPOzlqx8qB8zqT45BOMs2NLw=="
val deltaTablePath=spark.conf.set("fs.azure.account.key.dlsniagara.dfs.core.windows.net", adlsGen2Key)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Source Stream

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val df =
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", confluentBootstrapServers)
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.jaas.config", s"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='$confluentApiKey' password='$confluentSecret';")
  .option("kafka.ssl.endpoint.identification.algorithm", "https")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("subscribe", confluentTopicName)
  .option("startingOffsets", "latest")
  .option("failOnDataLoss", "false")
  .load()
  .select("topic", "partition", "offset", "timestamp", "timestampType", "value")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Data Format and JSON Extraction

// COMMAND ----------

val schema =  new StructType()
  .add("deviceName", StringType, false)
  .add("tagName", StringType, false)
  .add("deviceID", StringType, false)
  .add("success", BooleanType, false)
  .add("datatype", StringType, false)
  .add("timestamp",LongType, false)
  .add("value", StringType, false)
  .add("registerId", StringType, false)
  .add("description", StringType, false)

val selectDf = 
  df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("temp"))
    .select("temp.*") 
    .withColumn("timestamp", col("timestamp")/1000)
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .na.drop()

val outDf = selectDf
      .withColumn("year",  year(col("timestamp")))
      .withColumn("month", month(col("timestamp")))
      .withColumn("day",   dayofmonth(col("timestamp")))
      .withColumn("hour",  hour(col("timestamp")))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Output Stream to Delta Bronze

// COMMAND ----------

import org.apache.spark.sql.DataFrame

outDf.writeStream.format("delta").outputMode("append").option("checkpointLocation", "abfss://nbl@dlsniagara.dfs.core.windows.net/checkpoints/databricks-poc/abq/action").toTable("databricks_poc.bronze_abq_litmus_action")

/*
  .foreachBatch{ (batchDF: DataFrame, batchId: Long) =>
    batchDF.persist()
    if(batchId % 40 == 0){
      spark.sql("optimize `databricks_poc`.`bronze_hou_litmus` WHERE year = year(current_timestamp()) and month = month(current_timestamp()) and day >= day(current_timestamp())-1")
    }
    if(batchId % 105 == 0){
      spark.sql("optimize `databricks_poc`.`bronze_hou_litmus` WHERE year = year(current_timestamp()) and month = month(current_timestamp()) and day >= day(current_timestamp())-1 zorder by (deviceName, tagName)")
    }
    batchDF.write.format("delta").mode("append").saveAsTable("databricks_poc.bronze_hou_litmus")
    batchDF.unpersist()
    ()
  }.outputMode("update")
  .start()
  */
