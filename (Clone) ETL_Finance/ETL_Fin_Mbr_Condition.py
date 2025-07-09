# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     ETL_Fin_Mbr_Condition.                                                                                           *
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
#* 01/03/2025 CCRB70930/CO#43342  Jaime Zavala        Initial Release.                                                              *
#************************************************************************************************************************************


# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Parameters
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('s3_location', 's3://gia-stg-oh-ue1-data-raw/haven/inbound/VE_EDW')  
dbutils.widgets.text('received_date', '/process/ETL_Cap')
dbutils.widgets.text('catalog', 'oh_apm_stg')  
dbutils.widgets.text('schema_name', 'vendor_extracts')  
dbutils.widgets.text('Clng_Month_Gap', '-120')


#-------------------
# EDW Staging Tables
#-------------------
dbutils.widgets.text('VEN16FB', 'EDW_VEN16FB_Staging')

#--------------------
# EDW Historic Tables
#--------------------
dbutils.widgets.text('VEN16FB_hist', 'EDW_VEN16FB_Historic')


# COMMAND ----------

# DBTITLE 1,Get Parameters Values
#-----------
# DBX Parms
#-----------
Clng_Month_Gap = dbutils.widgets.get('Clng_Month_Gap')
s3_location = dbutils.widgets.get('s3_location')
received_date = dbutils.widgets.get('received_date')
catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')

#-------------------
# EDW Staging Tables
#-------------------
VEN16FB = dbutils.widgets.get('VEN16FB')

#--------------------
# EDW Historic Tables
#--------------------
VEN16FB_hist = dbutils.widgets.get('VEN16FB_hist')


# COMMAND ----------

# DBTITLE 1,Show Parms
print("Location Path:", s3_location)
print("Location Dte:", received_date)
print("catalog:", catalog)
print("schema:", schema_name)
print("Cleaning Month Gap:", Clng_Month_Gap)

print()
print("EDW Staging Tables")
print("------------------")
print(VEN16FB)

print()
print("EDW Historic Tables")
print("-------------------")
print(VEN16FB_hist)


# COMMAND ----------

# DBTITLE 1,EDW_VEN16FB_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN16FB} (
# MAGIC  FI_MBR_CONDITION_EXT_SK	DECIMAL	(18,0)
# MAGIC ,MEMBER_SAK_ID			      STRING
# MAGIC ,CONDITION_SAK_ID		      STRING
# MAGIC ,EFFECTIVE_DT			        TIMESTAMP	
# MAGIC ,EFFECTIVE_DT_NUM		      DECIMAL	(8,0)
# MAGIC ,TERMINATION_DT			      TIMESTAMP	
# MAGIC ,TERMINATION_DT_NUM		    DECIMAL	(8,0)
# MAGIC ,PRE_EXISTING			        STRING
# MAGIC ,RECONSIDER_DT			      TIMESTAMP	
# MAGIC ,RECONSIDER_DT_NUM		    DECIMAL	(8,0)
# MAGIC ,CONDITION_TYP			      STRING
# MAGIC ,CONDITION_DESCRIPTION	  STRING
# MAGIC ,SAK_RECIPIENT_ID		      DECIMAL	(18,0)
# MAGIC ,MEDICAID_ID				      STRING
# MAGIC ,LAST_NAME				        STRING
# MAGIC ,FIRST_NAME				        STRING
# MAGIC ,MIDDLE_NAME				      STRING
# MAGIC ,SORT_ORDER				        DECIMAL	(18,0)
# MAGIC ,REPORT_DTE				        TIMESTAMP	
# MAGIC ,REPORT_DTE_NBR			      DECIMAL	(8,0)
# MAGIC ,STATUS					          STRING
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN16FB Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*16FB*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("FI_MBR_CONDITION_EXT_SK"	,DecimalType (18,0), True)
,StructField("MEMBER_SAK_ID"		    ,StringType(), True)
,StructField("CONDITION_SAK_ID"		    ,StringType(), True)
,StructField("EFFECTIVE_DT"			    ,TimestampType(), True)
,StructField("EFFECTIVE_DT_NUM"		    ,DecimalType (8,0), True)
,StructField("TERMINATION_DT"		    ,TimestampType(), True)
,StructField("TERMINATION_DT_NUM"	    ,DecimalType (8,0), True)
,StructField("PRE_EXISTING"			    ,StringType(), True)
,StructField("RECONSIDER_DT"		    ,TimestampType(), True)
,StructField("RECONSIDER_DT_NUM"	    ,DecimalType (8,0), True)
,StructField("CONDITION_TYP"		    ,StringType(), True)
,StructField("CONDITION_DESCRIPTION"    ,StringType(), True)
,StructField("SAK_RECIPIENT_ID"		    ,DecimalType (18,0), True)
,StructField("MEDICAID_ID"				,StringType(), True)
,StructField("LAST_NAME"			    ,StringType(), True)
,StructField("FIRST_NAME"			    ,StringType(), True)
,StructField("MIDDLE_NAME"				,StringType(), True)
,StructField("SORT_ORDER"			    ,DecimalType (18,0), True)
,StructField("REPORT_DTE"			    ,TimestampType(), True)
,StructField("REPORT_DTE_NBR"		    ,DecimalType (8,0), True)
,StructField("STATUS"				    ,StringType(), True)
])

table_name = f"{catalog}.{schema_name}.{VEN16FB}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,Choose Clustering Keys
# Use Spark SQL to describe the table
columnsInfo = spark.sql(f"DESCRIBE {catalog}.{schema_name}.{VEN16FB}")
  
# Show the schema including column names
# columnsInfo.show(truncate=False)

# Count the number of columns
numColumns = columnsInfo.count()
print(f"Number of columns in the table {catalog}.{schema_name}.{VEN16FB}: {numColumns}")

spark.sql(f"ALTER TABLE {catalog}.{schema_name}.{VEN16FB} SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '{numColumns}')")

# Manually trigger the recomputation of statistics for the Delta table
spark.sql(f"ANALYZE TABLE {catalog}.{schema_name}.{VEN16FB} COMPUTE STATISTICS")
print(f"Updated statistics for {catalog}.{schema_name}.{VEN16FB}")


# COMMAND ----------

# DBTITLE 1,Clustering and Optimize Table
# MAGIC %sql
# MAGIC -- Clustering and optimizing the  table
# MAGIC ALTER TABLE ${catalog}.${schema_name}.${VEN16FB}
# MAGIC CLUSTER BY (MEDICAID_ID,SAK_RECIPIENT_ID,CONDITION_DESCRIPTION,CONDITION_TYP);
# MAGIC --
# MAGIC -- Optimize Table
# MAGIC OPTIMIZE ${catalog}.${schema_name}.${VEN16FB};
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN16FB Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN16FB_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN16FB} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN16FB Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN16FB_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC
