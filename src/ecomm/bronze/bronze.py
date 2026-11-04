# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql.window import *


# COMMAND ----------



# COMMAND ----------

create_sink_table()

# COMMAND ----------

#bronze layer
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumnsWithTypeWidening") #addNewColumnsWithTypeWidening,addNewColumns,failOnNewColumns,rescue,none
    .option("cloudFiles.inferColumnTypes",True)
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/e-commerce/schemalocation")
    .load("/Volumes/workspace/e-commerce/e-commerce_sales_analysis/")
    .withColumn("_file_name",col("_metadata.file_name"))
    .withColumn("_file_path",col("_metadata.file_path"))
    .withColumn("_file_size",col("_metadata.file_size"))
    .withColumn("_file_mod",col("_metadata.file_modification_time"))
    .withColumn("event_time",col("order_date"))
    .withColumn("_processing_time",current_timestamp())
    )

(
    df.writeStream
    .outputMode("append")
    .option("mergeSchema",True)
    .option("checkpointLocation", "/Volumes/workspace/e-commerce/checkpoint")
    .trigger(availableNow=True)
    .toTable("workspace.`e-commerce`.sink")

    )
