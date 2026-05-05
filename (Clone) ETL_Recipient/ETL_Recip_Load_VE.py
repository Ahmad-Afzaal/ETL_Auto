# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_Recipient_Load_VE.                                                                                           *
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
#*    Date     CO            Author          Description                                                                            *
#* ---------- ---------  -----------------   -------------------------------------------------------------------------------------- *
#* 03/01/2024            Suman Ettedi        Initial Release.                                                                       *
#* 03/25/2024            Jaime Zavala        Removed Load VE from Analytics.                                                        *
#************************************************************************************************************************************


# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text('received_date', '/process/ETL_Rec')
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('ven_114fa', 'EDW_ven114fa_staging')
dbutils.widgets.text('ven_115fa', 'EDW_ven115fa_staging')
dbutils.widgets.text('s3_location', 's3://gia-stg-oh-ue1-data-raw/haven/inbound/VE_EDW')
dbutils.widgets.text('ven_114fa_hist', 'EDW_ven114fa_historic')
dbutils.widgets.text('ven_115fa_hist', 'EDW_ven115fa_historic')
dbutils.widgets.text('Clng_Month_Gap', '-120')

# COMMAND ----------

received_date = dbutils.widgets.get('received_date')
schema_name = dbutils.widgets.get('schema_name')
ven_114fa = dbutils.widgets.get('ven_114fa')
ven_115fa=dbutils.widgets.get('ven_115fa')
s3_location =dbutils.widgets.get('s3_location')
ven_114fa_hist =dbutils.widgets.get('ven_114fa_hist')
ven_115fa_hist=dbutils.widgets.get('ven_115fa_hist')
catalog = dbutils.widgets.get('catalog')
Clng_Month_Gap = dbutils.widgets.get('Clng_Month_Gap')

print("Location Path:", s3_location)
print("Location Dte:", received_date)
print("Cleaning Month Gap: ", Clng_Month_Gap)
print("VEN114FA :",ven_114fa)
print("VEN114FA HIST :",ven_114fa_hist)
print("VEN115FA :",ven_115fa)
print("VEN114FA HIST :",ven_115fa_hist)

# COMMAND ----------

# DBTITLE 1,EDW_VEN114FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${ven_114fa} (
# MAGIC   SAK_RECIP DECIMAL(18, 0),
# MAGIC   ID_MEDICAID STRING,
# MAGIC   NAM_LAST STRING,
# MAGIC   NAM_FIRST STRING,
# MAGIC   NAM_MID_INIT STRING,
# MAGIC   NUM_SSN STRING,
# MAGIC   CDE_RACE STRING,
# MAGIC   CDE_RACE_2 STRING,
# MAGIC   CDE_RACE_3 STRING,
# MAGIC   CDE_RACE_4 STRING,
# MAGIC   CDE_RACE_5 STRING,
# MAGIC   CDE_RACE_6 STRING,
# MAGIC   CDE_RACE_7 STRING,
# MAGIC   CDE_ETHNIC STRING,
# MAGIC   CDE_SOURCE STRING,
# MAGIC   CDE_SEX STRING,
# MAGIC   DTE_BIRTH DATE,
# MAGIC   DTE_BIRTH_NBR DECIMAL(8, 0),
# MAGIC   DTE_DEATH DATE,
# MAGIC   DTE_DEATH_NBR DECIMAL(8, 0),
# MAGIC   NUM_CASE STRING,
# MAGIC   CDE_LANGUAGE STRING,
# MAGIC   CDE_LIV_ARNG STRING,
# MAGIC   CDE_SSI_STATUS STRING,
# MAGIC   CDE_MARITAL STRING,
# MAGIC   DTE_EFFECTIVE DATE,
# MAGIC   DTE_EFFECTIVE_NBR DECIMAL(8, 0),
# MAGIC   DTE_END DATE,
# MAGIC   DTE_END_NBR DECIMAL(8, 0),
# MAGIC   REPORT_DTE DATE,
# MAGIC   REPORT_DTE_NBR DECIMAL(8, 0)
# MAGIC ) 

# COMMAND ----------

# DBTITLE 1,Load EDW VEN114FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType
s3_location_final = f"{s3_location}{received_date}/*114FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
    StructField("SAK_RECIP", DecimalType(18, 0), True),
    StructField("ID_MEDICAID", StringType(), True),
    StructField("NAM_LAST", StringType(), True),
    StructField("NAM_FIRST", StringType(), True),
    StructField("NAM_MID_INIT", StringType(), True),
    StructField("NUM_SSN", StringType(), True),
    StructField("CDE_RACE", StringType(), True),
    StructField("CDE_RACE_2", StringType(), True),
    StructField("CDE_RACE_3", StringType(), True),
    StructField("CDE_RACE_4", StringType(), True),
    StructField("CDE_RACE_5", StringType(), True),
    StructField("CDE_RACE_6", StringType(), True),
    StructField("CDE_RACE_7", StringType(), True),
    StructField("CDE_ETHNIC", StringType(), True),
    StructField("CDE_SOURCE", StringType(), True),
    StructField("CDE_SEX", StringType(), True),
    StructField("DTE_BIRTH", DateType(), True),
    StructField("DTE_BIRTH_NBR", DecimalType(8, 0), True),
    StructField("DTE_DEATH", DateType(), True),
    StructField("DTE_DEATH_NBR", DecimalType(8, 0), True),
    StructField("NUM_CASE", StringType(), True),
    StructField("CDE_LANGUAGE", StringType(), True),
    StructField("CDE_LIV_ARNG", StringType(), True),
    StructField("CDE_SSI_STATUS", StringType(), True),
    StructField("CDE_MARITAL", StringType(), True),
    StructField("DTE_EFFECTIVE", DateType(), True),
    StructField("DTE_EFFECTIVE_NBR", DecimalType(8, 0), True),
    StructField("DTE_END", DateType(), True),
    StructField("DTE_END_NBR", DecimalType(8, 0), True),
    StructField("REPORT_DTE", DateType(), True),
    StructField("REPORT_DTE_NBR", DecimalType(8, 0), True)
])

table_name = f"{catalog}.{schema_name}.{ven_114fa}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,EDW_VEN115FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${ven_115fa} (
# MAGIC  SAK_RECIP            DECIMAL (18,0)
# MAGIC ,ADR_STREET_1         STRING
# MAGIC ,ADR_STREET_2         STRING
# MAGIC ,ADR_CTY              STRING
# MAGIC ,ADR_STATE            STRING
# MAGIC ,ADR_ZIP_CODE         STRING
# MAGIC ,ADR_ZIP_CODE_4       STRING
# MAGIC ,NUM_PHONE            STRING
# MAGIC ,NUM_PHONE_EXT        STRING
# MAGIC ,NUM_PHONE_INTL       STRING
# MAGIC ,NUM_PHONE_INTL_EXT   STRING
# MAGIC ,NUM_FAX              STRING
# MAGIC ,NUM_FAX_INTL         STRING
# MAGIC ,NUM_ADD_PHONE        STRING
# MAGIC ,CDE_COUNTY           STRING
# MAGIC ,ADDR_TYP_CD          STRING
# MAGIC ,ADDR_TYP_DESC        STRING
# MAGIC ,NUM_LONGITUDE        DECIMAL (19,8)
# MAGIC ,NUM_LATITUDE         DECIMAL (19,8)
# MAGIC ,REPORT_DTE           DATE
# MAGIC ,REPORT_DTE_NBR       DECIMAL (8,0)
# MAGIC ) USING DELTA;

# COMMAND ----------

# DBTITLE 1,Load EDW VEN115FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType
s3_location_final = f"{s3_location}{received_date}/*115FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
    StructField("SAK_RECIP", DecimalType(18, 0), True),
    StructField("ADR_STREET_1", StringType(), True),
    StructField("ADR_STREET_2", StringType(), True),
    StructField("ADR_CTY", StringType(), True),
    StructField("ADR_STATE", StringType(), True),
    StructField("ADR_ZIP_CODE", StringType(), True),
    StructField("ADR_ZIP_CODE_4", StringType(), True),
    StructField("NUM_PHONE", StringType(), True),
    StructField("NUM_PHONE_EXT", StringType(), True),
    StructField("NUM_PHONE_INTL", StringType(), True),
    StructField("NUM_PHONE_INTL_EXT", StringType(), True),
    StructField("NUM_FAX", StringType(), True),
    StructField("NUM_FAX_INTL", StringType(), True),
    StructField("NUM_ADD_PHONE", StringType(), True),
    StructField("CDE_COUNTY", StringType(), True),
    StructField("ADDR_TYP_CD", StringType(), True),
    StructField("ADDR_TYP_DESC", StringType(), True),
    StructField("NUM_LONGITUDE", DecimalType(19, 8), True),
    StructField("NUM_LATITUDE", DecimalType(19, 8), True),
    StructField("REPORT_DTE", DateType(), True),
    StructField("REPORT_DTE_NBR", DecimalType(8, 0), True)
])

table_name = f"{catalog}.{schema_name}.{ven_115fa}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Load EDW VEN115FA Historic Table
# MAGIC %sql
# MAGIC insert into ${catalog}.${schema_name}.${ven_115fa_hist} (
# MAGIC select current_timestamp() AS EXTRACTION_DATE, * from ${catalog}.${schema_name}.${ven_115fa} )

# COMMAND ----------

# DBTITLE 1,Load EDW VEN114FA Historic Table
# MAGIC %sql
# MAGIC insert into ${catalog}.${schema_name}.${ven_114fa_hist} (
# MAGIC select current_timestamp() AS EXTRACTION_DATE, * from ${catalog}.${schema_name}.${ven_114fa} )

# COMMAND ----------

# DBTITLE 1,Clean EDW VEN115FA Historic Table
# MAGIC %sql
# MAGIC delete  from  ${catalog}.${schema_name}.${ven_115fa_hist}
# MAGIC where EXTRACTION_DATE < ADD_MONTHS(current_timestamp(), ${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Clean EDW VEN114FA Historic Table
# MAGIC %sql
# MAGIC delete  from  ${catalog}.${schema_name}.${ven_114fa_hist}
# MAGIC where EXTRACTION_DATE < ADD_MONTHS(current_timestamp(), ${Clng_Month_Gap})
# MAGIC ;
