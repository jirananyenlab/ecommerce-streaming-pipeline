# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
def create_sink_table() -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS workspace.`e-commerce`.`sink` (
            order_id BIGINT,
            country STRING,
            category STRING,
            unit_price DOUBLE,
            quantity BIGINT,
            order_date TIMESTAMP,
            total_amount DOUBLE,
            event_time TIMESTAMP
        )
        USING DELTA
    """)
    print("Created table e-commerce.default.sink successfully")