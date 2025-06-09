# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     ETL_Reference_Load_VE_Ctl.                                                                                       *
#*                                                                                                                                  *
#*   DESCRIPTION:                                                                                                                   *
#*                                                                                                                                  *
#*                                                                                                                                  *
#*   INPUT PARMS:                                                                                                                   *
#*                                                                                                                                  *
#*                                                                                                                                  *
#*   INPUT FILES:                                                                                                                   *
#*                                                                                                                                  *
#*                                                                                                                                  *
#*   OUTPUT FILE:                                                                                                                   *
#*                                                                                                                                  *
#*   EXITS:       0 - success                                                                                                       *
#*                <> 0 - failure                                                                                                    *
#*                                                                                                                                  *
#************************************************************************************************************************************
#*                                                                                                                                  *
#*                                                 Modification Log                                                                 *
#*                                                                                                                                  *
#*    Date     CO                 Author              Description                                                                   *
#* ---------- ------------------  -----------------   ------------------------------------------------------------------------------*
#* 04/01/2024 CCRB70930/CO#43342  Jaime Zavala        Initial Release.                                                              *
#************************************************************************************************************************************


# COMMAND ----------

# DBTITLE 1,Parameters
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('s3_location', 's3://gia-stg-oh-ue1-data-raw/haven/inbound/VE_EDW/process/ETL_Ref') 
# dbutils.widgets.text('received_date', '2024-03-20')  
dbutils.widgets.text('catalog', 'oh_apm_stg')  
dbutils.widgets.text('schema_name', 'archive_vendor_extracts')  
dbutils.widgets.text('Clng_Month_Gap', '-120')
  
#-------------------
# EDW Staging Tables
#-------------------
dbutils.widgets.text('EDW_CtlLog_tmp', 'EDW_CtlLog_Rfrnc_tmp')
dbutils.widgets.text('EDW_CtlLog', 'EDW_CtlLog_Rfrnc_Staging')

#--------------------
# EDW Historic Tables
#--------------------
dbutils.widgets.text('EDW_CtlLog_hist', 'EDW_CtlLog_Historic')

# COMMAND ----------

# DBTITLE 1,Get Parameters Values
#-------------------
# EDW Staging Tables
#-------------------
EDW_CtlLog_tmp = dbutils.widgets.get('EDW_CtlLog_tmp')
EDW_CtlLog = dbutils.widgets.get('EDW_CtlLog')

#--------------------
# EDW Historic Tables
#--------------------

EDW_CtlLog_hist = dbutils.widgets.get('EDW_CtlLog_hist')

#-----------
# DBX Parms
#-----------
Clng_Month_Gap = dbutils.widgets.get('Clng_Month_Gap')
s3_location = dbutils.widgets.get('s3_location')
# received_date = dbutils.widgets.get('received_date')
catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')


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
print(EDW_CtlLog_tmp)
print(EDW_CtlLog)

print()
print("EDW Historic Tables")
print("-------------------")
print(EDW_CtlLog_hist)


# COMMAND ----------

# DBTITLE 1,EDW_CtlLog_tmp DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${EDW_CtlLog_tmp} (
# MAGIC  FileNm                     STRING
# MAGIC ,Row_Count                  DECIMAL (18,0)
# MAGIC ) 
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Load EDW Control Log Temporary Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}/*.REFERENCE.WEEKLY.*.ctl" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("FileNm"                      ,StringType(), True)
,StructField("Row_Count"                   ,DecimalType (18,0), True)
])

table_name = f"{catalog}.{schema_name}.{EDW_CtlLog_tmp}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "false") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)



# COMMAND ----------

# DBTITLE 1,EDW_CtlLog DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${EDW_CtlLog} (
# MAGIC  FileType       STRING
# MAGIC ,FileDte        STRING
# MAGIC ,FileNm         STRING
# MAGIC ,Row_Count      DECIMAL (18,0)
# MAGIC ) 
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW Control Log Staging Table
# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.${EDW_CtlLog}
# MAGIC select 
# MAGIC  split_part(FileNm,'.' ,3),
# MAGIC  split_part(FileNm,'.' ,7),
# MAGIC  * from  ${catalog}.${schema_name}.${EDW_CtlLog_tmp}
# MAGIC  ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${catalog}.${schema_name}.${EDW_CtlLog}
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop tmp table
# MAGIC  Drop Table ${catalog}.${schema_name}.${EDW_CtlLog_tmp}
# MAGIC  ;

# COMMAND ----------

# DBTITLE 1,Load EDW Control Log Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${EDW_CtlLog_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${EDW_CtlLog} )
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Clean EDW Control Log Historic Table
# MAGIC %sql
# MAGIC delete  from   ${catalog}.${schema_name}.${EDW_CtlLog_hist}
# MAGIC where EXTRACTION_DATE < ADD_MONTHS(current_timestamp(),${Clng_Month_Gap})
