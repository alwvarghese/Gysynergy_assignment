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
