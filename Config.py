# Databricks notebook source
import pyspark
from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use catalog catalog_name

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create database if not exists bronze_db;
# MAGIC create database if not exists silver_db;
# MAGIC create database if not exists gold_db;

# COMMAND ----------

container='your_container'
storage_account='your_storage_account'
landing_schema='bronze_db'
staging_schema='silver_db'
enterprise_schema='gold_db'
base_path=f'abfss://{container}@{storage_account}.dfs.core.windows.net/'
landing_location=f'{base_path}Alwin_V/gsynergy_assignment/landing/'
user='ingestion_user'
audit_columns=['CREATE_DATE','CREATED_BY','UPDATE_DATE','UPDATED_BY','FILE_PATH']
table_location = f'{base_path}Alwin_V/gsynergy_assignment/tables/'