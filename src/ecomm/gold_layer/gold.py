# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import *

# COMMAND ----------


VOLUME = "/Volumes/workspace/e-commerce/e-commerce_sales_analysis"
SILVER = "workspace.`e-commerce`.silver_orders"
GOLD = "workspace.`e-commerce`.gold_daily_sales_cdf"
CHECKPOINT = f"{VOLUME}/_pipeline/checkpoints/gold_daily_sales_cdf"
GROUP_KEYS = ("order_date", "country", "category")


# COMMAND ----------

def merge_daily_sales(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    session = batch_df.sparkSession
    # Keep all CDF row types, including both images of each update.
    affected_groups = batch_df.select(*GROUP_KEYS).distinct()

    # Recompute absolute totals from current Silver. The left join below
    # preserves affected groups that now have no orders.
    group_totals = (
        session.table(SILVER)
        .join(affected_groups,  list(GROUP_KEYS), "left_semi")
        .groupBy(*GROUP_KEYS)
        .agg(
            count("*").alias("order_count"),
            sum(col("quantity").cast("bigint")).alias("units_sold"),
            sum("total_amount").alias("revenue_usd"),
        )
    )
    changes = (
        affected_groups.join(group_totals, list(GROUP_KEYS), "left")
        .fillna(0, subset=["order_count"])
    )

    merge_condition = " AND ".join([
        f"source.`{key}` = target.`{key}`" for key in GROUP_KEYS
    ])
    (
        DeltaTable.forName(session, GOLD)
        .alias("target")
        .merge(changes.alias("source"), merge_condition)
        .whenMatchedDelete(condition=col("source.order_count") == 0)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll(condition=col("source.order_count") > 0)
        .execute()
    )
    print(f"Processed Gold micro-batch {batch_id}")

# COMMAND ----------

# DBTITLE 1,Import transform function
query = (
    spark.readStream
    .option("readChangeFeed", "true")
    .table(SILVER)
    .writeStream.foreachBatch(merge_daily_sales)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

totals = spark.table(GOLD).agg(
    coalesce(sum("order_count"), lit(0)).alias("orders"),
    coalesce(sum("revenue_usd"), lit(0)).alias("revenue_usd"),
).first()
