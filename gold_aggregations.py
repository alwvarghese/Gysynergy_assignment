# Databricks notebook source
# MAGIC %run ./Config

# COMMAND ----------

spark.sql(f"CREATE TABLE {enterprise_schema}.mview_weekly_sales location '{table_location}gold/mview_weekly_sales' AS SELECT pos_site_id, sku_id, fscldt_id, price_substate_id, type, SUM(sales_units) AS total_sales_units, SUM(sales_dollars) AS total_sales_dollars, SUM(discount_dollars) AS total_discount_dollars FROM {staging_schema}.fact_transactions_cleaned GROUP BY pos_site_id, sku_id, fscldt_id, price_substate_id, type")

# COMMAND ----------

