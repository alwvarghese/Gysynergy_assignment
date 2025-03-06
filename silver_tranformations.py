# Databricks notebook source
# MAGIC %run ./Config

# COMMAND ----------

def fill_null_with_zero(df):
    return df.fillna(0)

# COMMAND ----------

tables=[]
list_Tables = spark.catalog.listTables(landing_schema)
for table in list_Tables:
    if not table.isTemporary:
        tables.append(table.name)

for table_name in tables:
    if spark.catalog.tableExists(f"{staging_schema}.{table_name}"):
        max_value = spark.read.table(f"{staging_schema}.{table_name}").agg(max("UPDATE_DATE")).collect()[0][0]
        df=spark.sql(f"select * from {landing_schema}.{table_name} where UPDATE_DATE > '{max_value}'")
    else:
        df=spark.read.table(f"{landing_schema}.{table_name}")
    pkey=df.columns[0]
    print(f"table name: {table_name}, primary key: {pkey}")
    df=df.dropDuplicates([pkey])
    df=fill_null_with_zero(df)
    df.write.format('delta').mode('overwrite').option('mergeSchema',True).option('path',table_location+'silver/'+table_name+'_cleaned').saveAsTable(staging_schema+'.'+table_name+'_cleaned')

# COMMAND ----------

"""Data Quality Check for foreign key constraints vetween txn table and dim clnd table"""

def check_foreign_key(fact_df, fact_fk_col, dim_df, dim_pk_col):
    unmatched_count = fact_df.join(dim_df, fact_df[fact_fk_col] == dim_df[dim_pk_col], "left_anti").count()
    if unmatched_count > 0:
        raise ValueError(f"Foreign key constraint violation: {unmatched_count} unmatched {fact_fk_col} values in fact table")
    print(f"Foreign key constraint check passed for {fact_fk_col}")

# Example usage
fact_transactions_df=spark.read.table(f"{staging_schema}.fact_transactions_cleaned")
hier_clnd_df=spark.read.table(f"{staging_schema}.hier_clnd_cleaned")
try:
    check_foreign_key(fact_transactions_df, "fscldt_id", hier_clnd_df, "fscldt_id")
except ValueError as e:
    print(e)