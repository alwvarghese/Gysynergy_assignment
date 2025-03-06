# Databricks notebook source
# MAGIC %run ./Config

# COMMAND ----------

today_utc=datetime.now()
for data in dbutils.fs.ls(landing_location):
    print(f'Processing: {data.name}')
    name=data.name.split('.')
    table_name=name[0]+'_'+name[1]
    print(table_name)
    df=spark.read.option("header", True).option('inferSchema', True).option("delimiter", "|").csv(data.path)
    df=(df.withColumn('CREATE_DATE',lit(today_utc).cast('timestamp'))
          .withColumn('CREATED_BY',lit(user).cast('string'))
          .withColumn('UPDATE_DATE',lit(today_utc).cast('timestamp'))
          .withColumn('UPDATED_BY',lit(user).cast('string'))
          .withColumn('FILE_PATH',input_file_name()))
    df.write.format('delta').mode('overwrite').option('mergeSchema',True).option('path',table_location+'/bronze/'+table_name).saveAsTable(landing_schema+'.'+table_name)
    dbutils.fs.mv(data.path,landing_location.replace('landing','archive'))

# COMMAND ----------

