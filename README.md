# Gysynergy_assignment

# Azure ETL Pipeline
## Overview
This project aims to demonstrate the process of performing ETL on raw data from Azure Blob storage to databricks for analysis purposes. The ETL process involves using Azure Databricks for data extraction, transformation and agrregation. I have used databricks workflows to orchestrate data movement, and Delta Lake for storing staged data for analytical purposes.

## ER Diagram
![image](https://github.com/user-attachments/assets/3716f4cf-760b-4867-9fd6-ff27ad799a4f)

fact.transactions has a many-to-one relationship with fact.averagecosts on fscldt_id and sku_id.

fact.transactions has a many-to-one relationship with hier.clnd on fscldt_id.

fact.transactions has a many-to-one relationship with hier.possite on pos_site_id.

fact.transactions has a many-to-one relationship with hier.prod on sku_id.

fact.transactions has a many-to-one relationship with hier.pricestate on price_substate_id.

fact.averagecosts has a many-to-one relationship with hier.prod on sku_id.

## ETL Methodology

I have used Medallion Architecture to perform ETL operations on the data. 
1. The bronze layer creates a raw copy of the data in the ADLS location along with adding audit columns. No other operation is performed on this layer. The ingestion hapeens in append mode so that new data can be incrementally stored in the bronze layer. Files get pushed to archive location after it's pushed to delta table as an archiving strategy. So, landing would be empty after each run.
2. The silver layer intends to clean the data, perform DQ checks and standardize the data. The write strategy for this layer is overwrite and this acts as a materialized view for the gold layer.
3. The gold layer is aggregation of the cleaned data to get meaningful insights into the data.

Below is the screenshot for job run based on the architecture:
![image](https://github.com/user-attachments/assets/0d77c13b-8bb9-40ed-947c-293aca77a7b5)

