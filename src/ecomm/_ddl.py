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

# COMMAND ----------

def create_silver_orders_table() -> None:
    spark.sql("""
            CREATE TABLE IF NOT EXISTS workspace.`e-commerce`.silver_orders (
                order_id BIGINT,
                country STRING,
                category STRING,
                unit_price DECIMAL(18,2),
                quantity INT,
                order_date DATE,
                total_amount DECIMAL(18,2),
                source_file STRING,
                _processing_time TIMESTAMP
            )
            USING DELTA
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true'
            );
    """)
    print("Created table workspace.e-commerce.silver_orders successfully")


def create_silver_bad_record_table() -> None:
    spark.sql("""
            CREATE TABLE IF NOT EXISTS workspace.`e-commerce`.silver_sales_bad_record (
                order_id STRING,
                country STRING,
                category STRING,
                unit_price STRING,
                quantity STRING,
                order_date STRING,
                total_amount STRING,
                source_file STRING,
                _processing_time TIMESTAMP,
                csv_corrupt_record STRING,
                rescued_data STRING,
                reason ARRAY<STRING>
            )
            USING DELTA;
    """)
    print("Created table workspace.e-commerce.silver_sales_bad_record successfully")

# COMMAND ----------

def create_gold_daily_sales_table() -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS workspace.`e-commerce`.gold_daily_sales_cdf (
            order_date DATE,
            country STRING,
            category STRING,
            order_count BIGINT,
            units_sold BIGINT,
            revenue_usd DECIMAL(28,2)
        )
        USING DELTA
    """)
    print("Created table workspace.e-commerce.gold_daily_sales_cdf successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE workspace.`e-commerce`.sink
# MAGIC SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');

# COMMAND ----------

create_sink_table()

# COMMAND ----------

create_silver_orders_table()
create_silver_bad_record_table()

# COMMAND ----------

create_gold_daily_sales_table()