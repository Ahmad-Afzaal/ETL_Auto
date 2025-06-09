# Databricks notebook source
# DBTITLE 1,Parameters
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('s3_location', 's3://gia-stg-oh-ue1-data-raw/haven/inbound/VE_EDW/process/ETL_Ref')  
# dbutils.widgets.text('received_date', '2024-02-27')  
dbutils.widgets.text('catalog', 'oh_apm_stg')  
dbutils.widgets.text('schema_name', 'archive_vendor_extracts')  
dbutils.widgets.text('Clng_Month_Gap', '-120')


#-------------------
# EDW Staging Tables
#-------------------
dbutils.widgets.text('VEN130FA', 'EDW_VEN130FA_Staging')


#--------------------
# EDW Historic Tables
#--------------------
dbutils.widgets.text('VEN130FA_hist', 'EDW_VEN130FA_Historic')


# COMMAND ----------

# DBTITLE 1,Get Parameters Values
#-----------
# DBX Parms
#-----------
Clng_Month_Gap = dbutils.widgets.get('Clng_Month_Gap')
s3_location = dbutils.widgets.get('s3_location')
# received_date = dbutils.widgets.get('received_date')
catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')

#-------------------
# EDW Staging Tables
#-------------------
VEN130FA = dbutils.widgets.get('VEN130FA')

#--------------------
# EDW Historic Tables
#--------------------
VEN130FA_hist = dbutils.widgets.get('VEN130FA_hist')


# COMMAND ----------

# DBTITLE 1,Show Parms
print("Location Path:", s3_location)
# print("Location Dte:", received_date)
print("catalog:", catalog)
print("schema:", schema_name)
print("Cleaning Month Gap:", Clng_Month_Gap)

print()
print("EDW Staging Tables")
print("------------------")
print(VEN130FA)

print()
print("EDW Historic Tables")
print("-------------------")
print(VEN130FA_hist)


# COMMAND ----------

# DBTITLE 1,EDW_VEN130FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN130FA} (
# MAGIC  EDW_SEQUENCE_ID             DECIMAL (15,0)
# MAGIC ,RDM_SEQUENCE_ID             STRING
# MAGIC ,CODESET_NAME                STRING
# MAGIC ,CODE                        STRING
# MAGIC ,DESCRIPTION                 STRING
# MAGIC ,CREATED_DATE                STRING
# MAGIC ,CREATED_BY                  STRING
# MAGIC ,LAST_UPDATED_DATE           STRING
# MAGIC ,LAST_UPDATED_BY             STRING
# MAGIC ,EFFECTIVE_DATE              STRING
# MAGIC ,END_DATE                    STRING
# MAGIC ,RECORD_STATUS               STRING
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN130FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}/*130FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("EDW_SEQUENCE_ID"             ,DecimalType (15,0), True)
,StructField("RDM_SEQUENCE_ID"             ,StringType(), True)
,StructField("CODESET_NAME"                ,StringType(), True)
,StructField("CODE"                        ,StringType(), True)
,StructField("DESCRIPTION"                 ,StringType(), True)
,StructField("CREATED_DATE"                ,StringType(), True)
,StructField("CREATED_BY"                  ,StringType(), True)
,StructField("LAST_UPDATED_DATE"           ,StringType(), True)
,StructField("LAST_UPDATED_BY"             ,StringType(), True)
,StructField("EFFECTIVE_DATE"              ,StringType(), True)
,StructField("END_DATE"                    ,StringType(), True)
,StructField("RECORD_STATUS"               ,StringType(), True)
])

table_name = f"{catalog}.{schema_name}.{VEN130FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,Load EDW VEN130FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN130FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN130FA} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN130FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN130FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC
