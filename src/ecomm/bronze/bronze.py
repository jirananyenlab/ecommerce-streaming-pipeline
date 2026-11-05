# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql.window import *

# COMMAND ----------

# MAGIC %run ../fw

# COMMAND ----------

file_format="csv"
schema_evolution_mode="addNewColumnsWithTypeWidening"
header=True
infer_column_types=True
path_file = "global_ecommerce_sales_*.csv"
trigger={"availableNow": True}
output_mode="append"
volume = "/Volumes/workspace/e-commerce/e-commerce_sales_analysis"
bronze_table = "workspace.`e-commerce`.sink"
checkpoint = "/Volumes/workspace/e-commerce/checkpoint"
schemalocation = "/Volumes/workspace/e-commerce/schemalocation"
merge_schema=True

# COMMAND ----------

bronze = BronzeLayer(
    # Read configuration
    file_format=file_format,
    schema_evolution_mode=schema_evolution_mode,
    header=header,
    infer_column_types=infer_column_types,
    schema_location=schemalocation,
    load_location=f"{volume}/raw_data",
    path_file=path_file,

    # Write configuration
    output_mode=output_mode,
    checkpoint_location=checkpoint,
    trigger=trigger,
    merge_schema=merge_schema,
    table_bronze=bronze_table
    )

df = bronze.read_stream(spark)

query = bronze.write_stream(df)