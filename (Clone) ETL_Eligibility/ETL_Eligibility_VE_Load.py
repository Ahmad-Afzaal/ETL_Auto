# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     ETL_Eligibility_VE_Load.                                                                                         *
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
#* 06/06/2024 CCRB70930/CO#43342  Jaime Zavala        Added "MONTHLY" to VEN101FA's file mask.                                      *
#* 08/19/2025 CCRB70930/CO#43342  Jaime Zavala        Modified to met new Layout under RECIPIENT_ELIG_FULL_EXTRACT/VEN101FA.        *
#*                                                    Add 28 new data elements.                                                     *
#************************************************************************************************************************************


# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Parameters
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('s3_location', 's3://gia-stg-oh-ue1-data-raw/haven/inbound/VE_EDW')  
dbutils.widgets.text('received_date', '/process/ETL_Elig')  
dbutils.widgets.text('catalog', 'oh_apm_stg')  
dbutils.widgets.text('schema_name', 'vendor_extracts')  
dbutils.widgets.text('Clng_Month_Gap', '-120')


#-------------------
# EDW Staging Tables
#-------------------
dbutils.widgets.text('VEN116FA_PartA',   'EDW_VEN116FA_PartA_Staging')
dbutils.widgets.text('VEN116FA_PartC',   'EDW_VEN116FA_PartC_Staging')
dbutils.widgets.text('VEN116FA_PartD01', 'EDW_VEN116FA_PartD01_Staging')
dbutils.widgets.text('VEN116FA_PartD02', 'EDW_VEN116FA_PartD02_Staging')
dbutils.widgets.text('VEN116FA_PartD03', 'EDW_VEN116FA_PartD03_Staging')
dbutils.widgets.text('VEN116FA_PartD04', 'EDW_VEN116FA_PartD04_Staging')
dbutils.widgets.text('VEN116FA_PartD05', 'EDW_VEN116FA_PartD05_Staging')
dbutils.widgets.text('VEN116FA_PartE',   'EDW_VEN116FA_PartE_Staging')
dbutils.widgets.text('VEN116FA_PartH',   'EDW_VEN116FA_PartH_Staging')
dbutils.widgets.text('VEN116FA_PartI',   'EDW_VEN116FA_PartI_Staging')
dbutils.widgets.text('VEN116FA_PartJ',   'EDW_VEN116FA_PartJ_Staging')
dbutils.widgets.text('VEN116FA_PartK',   'EDW_VEN116FA_PartK_Staging')
dbutils.widgets.text('VEN116FA_PartL',   'EDW_VEN116FA_PartL_Staging')
dbutils.widgets.text('VEN116FA_PartN',   'EDW_VEN116FA_PartN_Staging')
dbutils.widgets.text('VEN116FA_PartG',   'EDW_VEN116FA_PartG_Staging')
dbutils.widgets.text('VEN116FA_PartO',   'EDW_VEN116FA_PartO_Staging')
dbutils.widgets.text('VEN101FA'      ,   'EDW_VEN101FA_Staging')

#--------------------
# EDW Historic Tables
#--------------------
dbutils.widgets.text('VEN116FA_PartA_hist',   'EDW_VEN116FA_PartA_Historic')
dbutils.widgets.text('VEN116FA_PartC_hist',   'EDW_VEN116FA_PartC_Historic')
dbutils.widgets.text('VEN116FA_PartD01_hist', 'EDW_VEN116FA_PartD01_Historic')
dbutils.widgets.text('VEN116FA_PartD02_hist', 'EDW_VEN116FA_PartD02_Historic')
dbutils.widgets.text('VEN116FA_PartD03_hist', 'EDW_VEN116FA_PartD03_Historic')
dbutils.widgets.text('VEN116FA_PartD04_hist', 'EDW_VEN116FA_PartD04_Historic')
dbutils.widgets.text('VEN116FA_PartD05_hist', 'EDW_VEN116FA_PartD05_Historic')
dbutils.widgets.text('VEN116FA_PartE_hist',   'EDW_VEN116FA_PartE_Historic')
dbutils.widgets.text('VEN116FA_PartH_hist',   'EDW_VEN116FA_PartH_Historic')
dbutils.widgets.text('VEN116FA_PartI_hist',   'EDW_VEN116FA_PartI_Historic')
dbutils.widgets.text('VEN116FA_PartJ_hist',   'EDW_VEN116FA_PartJ_Historic')
dbutils.widgets.text('VEN116FA_PartK_hist',   'EDW_VEN116FA_PartK_Historic')
dbutils.widgets.text('VEN116FA_PartL_hist',   'EDW_VEN116FA_PartL_Historic')
dbutils.widgets.text('VEN116FA_PartN_hist',   'EDW_VEN116FA_PartN_Historic')
dbutils.widgets.text('VEN116FA_PartG_hist',   'EDW_VEN116FA_PartG_Historic')
dbutils.widgets.text('VEN116FA_PartO_hist',   'EDW_VEN116FA_PartO_Historic')
dbutils.widgets.text('VEN101FA_hist'      ,   'EDW_VEN101FA_Historic')


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
VEN116FA_PartA   = dbutils.widgets.get('VEN116FA_PartA')
VEN116FA_PartC   = dbutils.widgets.get('VEN116FA_PartC')
VEN116FA_PartD01 = dbutils.widgets.get('VEN116FA_PartD01')
VEN116FA_PartD02 = dbutils.widgets.get('VEN116FA_PartD02')
VEN116FA_PartD03 = dbutils.widgets.get('VEN116FA_PartD03')
VEN116FA_PartD04 = dbutils.widgets.get('VEN116FA_PartD04')
VEN116FA_PartD05 = dbutils.widgets.get('VEN116FA_PartD05')
VEN116FA_PartE   = dbutils.widgets.get('VEN116FA_PartE')
VEN116FA_PartH   = dbutils.widgets.get('VEN116FA_PartH')
VEN116FA_PartI   = dbutils.widgets.get('VEN116FA_PartI')
VEN116FA_PartJ   = dbutils.widgets.get('VEN116FA_PartJ')
VEN116FA_PartK   = dbutils.widgets.get('VEN116FA_PartK')
VEN116FA_PartL   = dbutils.widgets.get('VEN116FA_PartL')
VEN116FA_PartN   = dbutils.widgets.get('VEN116FA_PartN')
VEN116FA_PartG   = dbutils.widgets.get('VEN116FA_PartG')
VEN116FA_PartO   = dbutils.widgets.get('VEN116FA_PartO')
VEN101FA         = dbutils.widgets.get('VEN101FA')

#--------------------
# EDW Historic Tables
#--------------------
VEN116FA_PartA_hist   = dbutils.widgets.get('VEN116FA_PartA_hist')
VEN116FA_PartC_hist   = dbutils.widgets.get('VEN116FA_PartC_hist')
VEN116FA_PartD01_hist = dbutils.widgets.get('VEN116FA_PartD01_hist')
VEN116FA_PartD02_hist = dbutils.widgets.get('VEN116FA_PartD02_hist')
VEN116FA_PartD03_hist = dbutils.widgets.get('VEN116FA_PartD03_hist')
VEN116FA_PartD04_hist = dbutils.widgets.get('VEN116FA_PartD04_hist')
VEN116FA_PartD05_hist = dbutils.widgets.get('VEN116FA_PartD05_hist')
VEN116FA_PartE_hist   = dbutils.widgets.get('VEN116FA_PartE_hist')
VEN116FA_PartH_hist   = dbutils.widgets.get('VEN116FA_PartH_hist')
VEN116FA_PartI_hist   = dbutils.widgets.get('VEN116FA_PartI_hist')
VEN116FA_PartJ_hist   = dbutils.widgets.get('VEN116FA_PartJ_hist')
VEN116FA_PartK_hist   = dbutils.widgets.get('VEN116FA_PartK_hist')
VEN116FA_PartL_hist   = dbutils.widgets.get('VEN116FA_PartL_hist')
VEN116FA_PartN_hist   = dbutils.widgets.get('VEN116FA_PartN_hist')
VEN116FA_PartG_hist   = dbutils.widgets.get('VEN116FA_PartG_hist')
VEN116FA_PartO_hist   = dbutils.widgets.get('VEN116FA_PartO_hist')
VEN101FA_hist         = dbutils.widgets.get('VEN101FA_hist')


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
print(VEN116FA_PartA)
print(VEN116FA_PartC)
print(VEN116FA_PartD01)
print(VEN116FA_PartD02)
print(VEN116FA_PartD03)
print(VEN116FA_PartD04)
print(VEN116FA_PartD05)
print(VEN116FA_PartE)
print(VEN116FA_PartH)
print(VEN116FA_PartI)
print(VEN116FA_PartJ)
print(VEN116FA_PartK)
print(VEN116FA_PartL)
print(VEN116FA_PartN)
print(VEN116FA_PartG)
print(VEN116FA_PartO)
print(VEN101FA)

print()
print("EDW Historic Tables")
print("-------------------")
print(VEN116FA_PartA_hist)
print(VEN116FA_PartC_hist)
print(VEN116FA_PartD01_hist)
print(VEN116FA_PartD02_hist)
print(VEN116FA_PartD03_hist)
print(VEN116FA_PartD04_hist)
print(VEN116FA_PartD05_hist)
print(VEN116FA_PartE_hist)
print(VEN116FA_PartH_hist)
print(VEN116FA_PartI_hist)
print(VEN116FA_PartJ_hist)
print(VEN116FA_PartK_hist)
print(VEN116FA_PartL_hist)
print(VEN116FA_PartN_hist)
print(VEN116FA_PartG_hist)
print(VEN116FA_PartO_hist)
print(VEN101FA_hist)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartA} (
# MAGIC  SAK_RECIP           DECIMAL (18,0)
# MAGIC ,ID_MEDICAID         STRING
# MAGIC ,IND_ACTIVE          STRING
# MAGIC ,ID_LINKED           DECIMAL (18,0)
# MAGIC ,ID_MBI_CMS          STRING
# MAGIC ,DTE_APPLICATION     DATE
# MAGIC ,DTE_APPLICATION_NBR DECIMAL (8,0)
# MAGIC ,NUM_CASE            STRING
# MAGIC ,ENRL_SPAN_TYP       STRING
# MAGIC ,CDE_STATUS          STRING
# MAGIC ,SAK_PUB_HLTH        DECIMAL (18,0)
# MAGIC ,ASSIGN_PLAN         DECIMAL (9,0)
# MAGIC ,DTE_EFFECTIVE       DATE
# MAGIC ,DTE_EFFECTIVE_NBR   DECIMAL (8,0)
# MAGIC ,DTE_END             DATE
# MAGIC ,DTE_END_NBR         DECIMAL (8,0)
# MAGIC ,SAK_PROV_LOC        DECIMAL (18,0)
# MAGIC ,CDE_REASON          STRING
# MAGIC ,REPORT_DTE          DATE
# MAGIC ,REPORT_DTE_NBR      DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA_PartA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.A*"
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_RECIP"           ,DecimalType (18,0), True)
,StructField("ID_MEDICAID"         ,StringType(), True)
,StructField("IND_ACTIVE"          ,StringType(), True)
,StructField("ID_LINKED"           ,DecimalType (18,0), True)
,StructField("ID_MBI_CMS"          ,StringType(), True)
,StructField("DTE_APPLICATION"     ,DateType(), True)
,StructField("DTE_APPLICATION_NBR" ,DecimalType (8,0), True)
,StructField("NUM_CASE"            ,StringType(), True)
,StructField("ENRL_SPAN_TYP"       ,StringType(), True)
,StructField("CDE_STATUS"          ,StringType(), True)
,StructField("SAK_PUB_HLTH"        ,DecimalType (18,0), True)
,StructField("ASSIGN_PLAN"         ,DecimalType (9,0), True)
,StructField("DTE_EFFECTIVE"       ,DateType(), True)
,StructField("DTE_EFFECTIVE_NBR"   ,DecimalType (8,0), True)
,StructField("DTE_END"             ,DateType(), True)
,StructField("DTE_END_NBR"         ,DecimalType (8,0), True)
,StructField("SAK_PROV_LOC"        ,DecimalType (18,0), True)
,StructField("CDE_REASON"          ,StringType(), True)
,StructField("REPORT_DTE"          ,DateType(), True)
,StructField("REPORT_DTE_NBR"      ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartC_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartC} (
# MAGIC  SAK_RECIP           DECIMAL (18,0)
# MAGIC ,ID_MEDICAID         STRING
# MAGIC ,IND_ACTIVE          STRING
# MAGIC ,ID_LINKED           DECIMAL (18,0)
# MAGIC ,ID_MBI_CMS          STRING
# MAGIC ,DTE_APPLICATION     DATE
# MAGIC ,DTE_APPLICATION_NBR DECIMAL (8,0)
# MAGIC ,NUM_CASE            STRING
# MAGIC ,ENRL_SPAN_TYP       STRING
# MAGIC ,CDE_SPEC_COND       STRING
# MAGIC ,DTE_EFFECTIVE       DATE
# MAGIC ,DTE_EFFECTIVE_NBR   DECIMAL (8,0)
# MAGIC ,DTE_END             DATE
# MAGIC ,DTE_END_NBR         DECIMAL (8,0)
# MAGIC ,REPORT_DTE          DATE
# MAGIC ,REPORT_DTE_NBR      DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartC Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.C*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_RECIP"           ,DecimalType (18,0), True)
,StructField("ID_MEDICAID"         ,StringType(), True)
,StructField("IND_ACTIVE"          ,StringType(), True)
,StructField("ID_LINKED"           ,DecimalType (18,0), True)
,StructField("ID_MBI_CMS"          ,StringType(), True)
,StructField("DTE_APPLICATION"     ,DateType(), True)
,StructField("DTE_APPLICATION_NBR" ,DecimalType (8,0), True)
,StructField("NUM_CASE"            ,StringType(), True)
,StructField("ENRL_SPAN_TYP"       ,StringType(), True)
,StructField("CDE_SPEC_COND"       ,StringType(), True)
,StructField("DTE_EFFECTIVE"       ,DateType(), True)
,StructField("DTE_EFFECTIVE_NBR"   ,DecimalType (8,0), True)
,StructField("DTE_END"             ,DateType(), True)
,StructField("DTE_END_NBR"         ,DecimalType (8,0), True)
,StructField("REPORT_DTE"          ,DateType(), True)
,StructField("REPORT_DTE_NBR"      ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartC}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartD01_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartD01} (
# MAGIC  SAK_RECIP                 DECIMAL (18,0)
# MAGIC ,ID_MEDICAID               STRING
# MAGIC ,IND_ACTIVE                STRING
# MAGIC ,ID_LINKED                 DECIMAL (18,0)
# MAGIC ,ID_MBI_CMS                STRING
# MAGIC ,DTE_APPLICATION           DATE
# MAGIC ,DTE_APPLICATION_NBR       DECIMAL (8,0)
# MAGIC ,NUM_CASE                  STRING
# MAGIC ,ENRL_SPAN_TYP             STRING
# MAGIC ,SAK_AID_ELIG              DECIMAL (12,0)
# MAGIC ,SAK_PGM_ELIG              DECIMAL (6,0)
# MAGIC ,DTE_EFFECTIVE             DATE
# MAGIC ,DTE_EFFECTIVE_NBR         DECIMAL (8,0)
# MAGIC ,DTE_END                   DATE
# MAGIC ,DTE_END_NBR               DECIMAL (8,0)
# MAGIC ,CDE_COUNTY                STRING
# MAGIC ,CDE_LIV_ARNG              STRING
# MAGIC ,CDE_AID_CATEGORY          DECIMAL (9,0)
# MAGIC ,SAK_CDE_AID               STRING
# MAGIC ,CDE_SSI_STATUS            STRING
# MAGIC ,CDE_RETRO_BCKDT           STRING
# MAGIC ,DTE_LIVARNG_EFFECTIVE     DATE
# MAGIC ,DTE_LIVARNG_EFFECTIVE_NBR DECIMAL (8,0)
# MAGIC ,DTE_LIVARNG_END           DATE
# MAGIC ,DTE_LIVARNG_END_NBR       DECIMAL (8,0)
# MAGIC ,DTE_LAST_UPDATE           DATE
# MAGIC ,DTE_LAST_UPDATE_NBR       DECIMAL (8,0)
# MAGIC ,REPORT_DTE                DATE
# MAGIC ,REPORT_DTE_NBR            DECIMAL (8,0)
# MAGIC ,SAK_PUB_HLTH              DECIMAL (9,0)
# MAGIC ,SAK_FIN_PAYER             DECIMAL (9,0)
# MAGIC ,CDE_PGM_HEALTH            STRING
# MAGIC ,DSC_PGM_HEALTH            STRING
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartD01 Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.D01*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_RECIP"                 ,DecimalType (18,0), True)
,StructField("ID_MEDICAID"               ,StringType(), True)
,StructField("IND_ACTIVE"                ,StringType(), True)
,StructField("ID_LINKED"                 ,DecimalType (18,0), True)
,StructField("ID_MBI_CMS"                ,StringType(), True)
,StructField("DTE_APPLICATION"           ,DateType(), True)
,StructField("DTE_APPLICATION_NBR"       ,DecimalType (8,0), True)
,StructField("NUM_CASE"                  ,StringType(), True)
,StructField("ENRL_SPAN_TYP"             ,StringType(), True)
,StructField("SAK_AID_ELIG"              ,DecimalType (12,0), True)
,StructField("SAK_PGM_ELIG"              ,DecimalType (6,0), True)
,StructField("DTE_EFFECTIVE"             ,DateType(), True)
,StructField("DTE_EFFECTIVE_NBR"         ,DecimalType (8,0), True)
,StructField("DTE_END"                   ,DateType(), True)
,StructField("DTE_END_NBR"               ,DecimalType (8,0), True)
,StructField("CDE_COUNTY"                ,StringType(), True)
,StructField("CDE_LIV_ARNG"              ,StringType(), True)
,StructField("CDE_AID_CATEGORY"          ,DecimalType (9,0), True)
,StructField("SAK_CDE_AID"               ,StringType(), True)
,StructField("CDE_SSI_STATUS"            ,StringType(), True)
,StructField("CDE_RETRO_BCKDT"           ,StringType(), True)
,StructField("DTE_LIVARNG_EFFECTIVE"     ,DateType(), True)
,StructField("DTE_LIVARNG_EFFECTIVE_NBR" ,DecimalType (8,0), True)
,StructField("DTE_LIVARNG_END"           ,DateType(), True)
,StructField("DTE_LIVARNG_END_NBR"       ,DecimalType (8,0), True)
,StructField("DTE_LAST_UPDATE"           ,DateType(), True)
,StructField("DTE_LAST_UPDATE_NBR"       ,DecimalType (8,0), True)
,StructField("REPORT_DTE"                ,DateType(), True)
,StructField("REPORT_DTE_NBR"            ,DecimalType (8,0), True)
,StructField("SAK_PUB_HLTH"              ,DecimalType (9,0), True)
,StructField("SAK_FIN_PAYER"             ,DecimalType (9,0), True)
,StructField("CDE_PGM_HEALTH"            ,StringType(), True)
,StructField("DSC_PGM_HEALTH"            ,StringType(), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartD01}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartD02_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartD02} (
# MAGIC  SAK_RECIP          DECIMAL (18,0)
# MAGIC ,SAK_PGM_ELIG       DECIMAL (6,0)
# MAGIC ,SAK_PUB_HLTH       DECIMAL (9,0)
# MAGIC ,SAK_FIN_PAYER      DECIMAL (9,0)
# MAGIC ,CDE_PGM_HEALTH     STRING
# MAGIC ,PGM_HEALTH_DESC    STRING
# MAGIC ,REPORT_DTE         DATE
# MAGIC ,REPORT_DTE_NBR     DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartD02 Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.D02*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_RECIP"                 ,DecimalType (18,0), True)
,StructField("SAK_PGM_ELIG"              ,DecimalType (6,0), True)
,StructField("SAK_PUB_HLTH"              ,DecimalType (9,0), True)
,StructField("SAK_FIN_PAYER"             ,DecimalType (9,0), True)
,StructField("CDE_PGM_HEALTH"            ,StringType(), True)
,StructField("PGM_HEALTH_DESC"           ,StringType(), True)
,StructField("REPORT_DTE"                ,DateType(), True)
,StructField("REPORT_DTE_NBR"            ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartD02}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartD03_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartD03} (
# MAGIC  SAK_RECIP               DECIMAL (18,0)
# MAGIC ,CDE_MISC_IND_TYPE       STRING
# MAGIC ,CDE_MISC_IND            STRING
# MAGIC ,DTE_BUYIN_EFFECTIVE     DATE
# MAGIC ,DTE_BUYIN_EFFECTIVE_NBR DECIMAL (8,0)
# MAGIC ,DTE_BUYIN_END           DATE
# MAGIC ,DTE_BUYIN_END_NBR       DECIMAL (8,0)
# MAGIC ,REPORT_DTE              DATE
# MAGIC ,REPORT_DTE_NBR          DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartD03 Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.D03*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_RECIP"               ,DecimalType (18,0), True)
,StructField("CDE_MISC_IND_TYPE"       ,StringType(), True)
,StructField("CDE_MISC_IND"            ,StringType(), True)
,StructField("DTE_BUYIN_EFFECTIVE"     ,DateType(), True)
,StructField("DTE_BUYIN_EFFECTIVE_NBR" ,DecimalType (8,0), True)
,StructField("DTE_BUYIN_END"           ,DateType(), True)
,StructField("DTE_BUYIN_END_NBR"       ,DecimalType (8,0), True)
,StructField("REPORT_DTE"              ,DateType(), True)
,StructField("REPORT_DTE_NBR"          ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartD03}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartD04_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartD04} (
# MAGIC  SAK_AID_ELIG      DECIMAL (12,0)
# MAGIC ,SAK_CASE_XREF     DECIMAL (9,0)
# MAGIC ,IND_HEALTH_INS    STRING
# MAGIC ,CDE_FIAT          STRING
# MAGIC ,CDE_CATEGORY      STRING
# MAGIC ,DTE_EFFECTIVE     DATE
# MAGIC ,DTE_EFFECTIVE_NBR DECIMAL (8,0)
# MAGIC ,DTE_END           DATE
# MAGIC ,DTE_END_NBR       DECIMAL (8,0)
# MAGIC ,CDE_STATUS        STRING
# MAGIC ,CDE_SOURCE        STRING
# MAGIC ,REPORT_DTE        DATE
# MAGIC ,REPORT_DTE_NBR    DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartD04 Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.D04*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_AID_ELIG"      ,DecimalType (12,0), True)
,StructField("SAK_CASE_XREF"     ,DecimalType (9,0), True)
,StructField("IND_HEALTH_INS"    ,StringType(), True)
,StructField("CDE_FIAT"          ,StringType(), True)
,StructField("CDE_CATEGORY"      ,StringType(), True)
,StructField("DTE_EFFECTIVE"     ,DateType(), True)
,StructField("DTE_EFFECTIVE_NBR" ,DecimalType (8,0), True)
,StructField("DTE_END"           ,DateType(), True)
,StructField("DTE_END_NBR"       ,DecimalType (8,0), True)
,StructField("CDE_STATUS"        ,StringType(), True)
,StructField("CDE_SOURCE"        ,StringType(), True)
,StructField("REPORT_DTE"        ,DateType(), True)
,StructField("REPORT_DTE_NBR"    ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartD04}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartD05_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartD05} (
# MAGIC  SAK_AID_ELIG        DECIMAL (12,0)
# MAGIC ,CDE_AID_ELIG_REASON STRING
# MAGIC ,DTE_ADDED           DATE
# MAGIC ,DTE_ADDED_NBR       DECIMAL (8,0)
# MAGIC ,REPORT_DTE          DATE
# MAGIC ,REPORT_DTE_NBR      DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartD05 Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.D05*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_AID_ELIG"        ,DecimalType (12,0), True)
,StructField("CDE_AID_ELIG_REASON" ,StringType(), True)
,StructField("DTE_ADDED"           ,DateType(), True)
,StructField("DTE_ADDED_NBR"       ,DecimalType (8,0), True)
,StructField("REPORT_DTE"          ,DateType(), True)
,StructField("REPORT_DTE_NBR"      ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartD05}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartE_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartE} (
# MAGIC  SAK_RECIP           DECIMAL (18,0)
# MAGIC ,ID_MEDICAID         STRING
# MAGIC ,IND_ACTIVE          STRING
# MAGIC ,ID_LINKED           DECIMAL (18,0)
# MAGIC ,ID_MBI_CMS          STRING
# MAGIC ,DTE_APPLICATION     DATE
# MAGIC ,DTE_APPLICATION_NBR DECIMAL (8,0)
# MAGIC ,NUM_CASE            STRING
# MAGIC ,ENRL_SPAN_TYP       STRING
# MAGIC ,DTE_EFFECTIVE       DATE
# MAGIC ,DTE_EFFECTIVE_NBR   DECIMAL (8,0)
# MAGIC ,DTE_END             DATE
# MAGIC ,DTE_END_NBR         DECIMAL (8,0)
# MAGIC ,ID_PROVIDER_MCAID   STRING
# MAGIC ,CDE_RSN_MC_STOP     STRING
# MAGIC ,CDE_RSN_MC_START    STRING
# MAGIC ,REPORT_DTE          DATE
# MAGIC ,REPORT_DTE_NBR      DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartE Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.E*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_RECIP"           ,DecimalType (18,0), True)
,StructField("ID_MEDICAID"         ,StringType(), True)
,StructField("IND_ACTIVE"          ,StringType(), True)
,StructField("ID_LINKED"           ,DecimalType (18,0), True)
,StructField("ID_MBI_CMS"          ,StringType(), True)
,StructField("DTE_APPLICATION"     ,DateType(), True)
,StructField("DTE_APPLICATION_NBR" ,DecimalType (8,0), True)
,StructField("NUM_CASE"            ,StringType(), True)
,StructField("ENRL_SPAN_TYP"       ,StringType(), True)
,StructField("DTE_EFFECTIVE"       ,DateType(), True)
,StructField("DTE_EFFECTIVE_NBR"   ,DecimalType (8,0), True)
,StructField("DTE_END"             ,DateType(), True)
,StructField("DTE_END_NBR"         ,DecimalType (8,0), True)
,StructField("ID_PROVIDER_MCAID"   ,StringType(), True)
,StructField("CDE_RSN_MC_STOP"     ,StringType(), True)
,StructField("CDE_RSN_MC_START"    ,StringType(), True)
,StructField("REPORT_DTE"          ,DateType(), True)
,StructField("REPORT_DTE_NBR"      ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartE}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartG_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartG} (
# MAGIC  SAK_RECIP            DECIMAL (18,0)
# MAGIC ,ID_MEDICAID          STRING
# MAGIC ,IND_ACTIVE           STRING
# MAGIC ,ID_LINKED            DECIMAL (18,0)
# MAGIC ,ID_MBI_CMS           STRING
# MAGIC ,DTE_APPLICATION      DATE
# MAGIC ,DTE_APPLICATION_NBR  DECIMAL (8,0)
# MAGIC ,NUM_CASE             STRING
# MAGIC ,ENRL_SPAN_TYP        STRING
# MAGIC ,DTE_EFFECTIVE        DATE
# MAGIC ,DTE_EFFECTIVE_NBR    DECIMAL (8,0)
# MAGIC ,DTE_END              DATE
# MAGIC ,DTE_END_NBR          DECIMAL (8,0)
# MAGIC ,DTE_LAST_UPDATED     DATE
# MAGIC ,DTE_LAST_UPDATED_NBR DECIMAL (8,0)
# MAGIC ,AMT_CLAWBACK         DECIMAL (8,2)
# MAGIC ,ID_PLAN              STRING
# MAGIC ,NAM_PLAN             STRING
# MAGIC ,CDE_DUAL_STATUS      STRING
# MAGIC ,CDE_REC_TYPE         STRING
# MAGIC ,DTE_COPAY_START      DATE
# MAGIC ,DTE_COPAY_START_NBR  DECIMAL (8,0)
# MAGIC ,DTE_COPAY_END        DATE
# MAGIC ,DTE_COPAY_END_NBR    DECIMAL (8,0)
# MAGIC ,TXT_LIS              STRING
# MAGIC ,TXT_COPAY            STRING
# MAGIC ,ID_CONTRACT          STRING
# MAGIC ,DTE_1ST_PARTD        DATE
# MAGIC ,DTE_1ST_PARTD_NBR    DECIMAL (8,0)
# MAGIC ,REPORT_DTE           DATE
# MAGIC ,REPORT_DTE_NBR       DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartG Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.G*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_RECIP"            ,DecimalType (18,0), True)
,StructField("ID_MEDICAID"          ,StringType(), True)
,StructField("IND_ACTIVE"           ,StringType(), True)
,StructField("ID_LINKED"            ,DecimalType (18,0), True)
,StructField("ID_MBI_CMS"           ,StringType(), True)
,StructField("DTE_APPLICATION"      ,DateType(), True)
,StructField("DTE_APPLICATION_NBR"  ,DecimalType (8,0), True)
,StructField("NUM_CASE"             ,StringType(), True)
,StructField("ENRL_SPAN_TYP"        ,StringType(), True)
,StructField("DTE_EFFECTIVE"        ,DateType(), True)
,StructField("DTE_EFFECTIVE_NBR"    ,DecimalType (8,0), True)
,StructField("DTE_END"              ,DateType(), True)
,StructField("DTE_END_NBR"          ,DecimalType (8,0), True)
,StructField("DTE_LAST_UPDATED"     ,DateType(), True)
,StructField("DTE_LAST_UPDATED_NBR" ,DecimalType (8,0), True)
,StructField("AMT_CLAWBACK"         ,DecimalType (8,2), True)
,StructField("ID_PLAN"              ,StringType(), True)
,StructField("NAM_PLAN"             ,StringType(), True)
,StructField("CDE_DUAL_STATUS"      ,StringType(), True)
,StructField("CDE_REC_TYPE"         ,StringType(), True)
,StructField("DTE_COPAY_START"      ,DateType(), True)
,StructField("DTE_COPAY_START_NBR"  ,DecimalType (8,0), True)
,StructField("DTE_COPAY_END"        ,DateType(), True)
,StructField("DTE_COPAY_END_NBR"    ,DecimalType (8,0), True)
,StructField("TXT_LIS"              ,StringType(), True)
,StructField("TXT_COPAY"            ,StringType(), True)
,StructField("ID_CONTRACT"          ,StringType(), True)
,StructField("DTE_1ST_PARTD"        ,DateType(), True)
,StructField("DTE_1ST_PARTD_NBR"    ,DecimalType (8,0), True)
,StructField("REPORT_DTE"           ,DateType(), True)
,StructField("REPORT_DTE_NBR"       ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartG}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartH_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartH} (
# MAGIC  SAK_RECIP            DECIMAL (18,0)
# MAGIC ,ID_MEDICAID          STRING
# MAGIC ,IND_ACTIVE           STRING
# MAGIC ,ID_LINKED            DECIMAL (18,0)
# MAGIC ,ID_MBI_CMS           STRING
# MAGIC ,DTE_APPLICATION      DATE
# MAGIC ,DTE_APPLICATION_NBR  DECIMAL (8,0)
# MAGIC ,NUM_CASE             STRING
# MAGIC ,ENRL_SPAN_TYP        STRING
# MAGIC ,DTE_EFFECTIVE        DATE
# MAGIC ,DTE_EFFECTIVE_NBR    DECIMAL (8,0)
# MAGIC ,DTE_END              DATE
# MAGIC ,DTE_END_NBR          DECIMAL (8,0)
# MAGIC ,CDE_WAIVER           STRING
# MAGIC ,CDE_LOC              STRING
# MAGIC ,REPORT_DTE           DATE
# MAGIC ,REPORT_DTE_NBR       DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartH Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.H*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_RECIP"            ,DecimalType (18,0), True)
,StructField("ID_MEDICAID"          ,StringType(), True)
,StructField("IND_ACTIVE"           ,StringType(), True)
,StructField("ID_LINKED"            ,DecimalType (18,0), True)
,StructField("ID_MBI_CMS"           ,StringType(), True)
,StructField("DTE_APPLICATION"      ,DateType(), True)
,StructField("DTE_APPLICATION_NBR"  ,DecimalType (8,0), True)
,StructField("NUM_CASE"             ,StringType(), True)
,StructField("ENRL_SPAN_TYP"        ,StringType(), True)
,StructField("DTE_EFFECTIVE"        ,DateType(), True)
,StructField("DTE_EFFECTIVE_NBR"    ,DecimalType (8,0), True)
,StructField("DTE_END"              ,DateType(), True)
,StructField("DTE_END_NBR"          ,DecimalType (8,0), True)
,StructField("CDE_WAIVER"           ,StringType(), True)
,StructField("CDE_LOC"              ,StringType(), True)
,StructField("REPORT_DTE"           ,DateType(), True)
,StructField("REPORT_DTE_NBR"       ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartH}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartI_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartI} (
# MAGIC  SAK_RECIP            DECIMAL (18,0)
# MAGIC ,ID_MEDICAID          STRING
# MAGIC ,IND_ACTIVE           STRING
# MAGIC ,ID_LINKED            DECIMAL (18,0)
# MAGIC ,ID_MBI_CMS           STRING
# MAGIC ,DTE_APPLICATION      DATE
# MAGIC ,DTE_APPLICATION_NBR  DECIMAL (8,0)
# MAGIC ,NUM_CASE             STRING
# MAGIC ,ENRL_SPAN_TYP        STRING
# MAGIC ,DTE_EFFECTIVE        DATE
# MAGIC ,DTE_EFFECTIVE_NBR    DECIMAL (8,0)
# MAGIC ,DTE_END              DATE
# MAGIC ,DTE_END_NBR          DECIMAL (8,0)
# MAGIC ,IND_RETRO            STRING
# MAGIC ,ID_MEDICARE          STRING
# MAGIC ,DTE_LAST_UPDATE      DATE
# MAGIC ,DTE_LAST_UPDATE_NBR  DECIMAL (8,0)
# MAGIC ,CDE_SOURCE           STRING
# MAGIC ,REPORT_DTE           DATE
# MAGIC ,REPORT_DTE_NBR       DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartI Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.I*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_RECIP"            ,DecimalType (18,0), True)
,StructField("ID_MEDICAID"          ,StringType(), True)
,StructField("IND_ACTIVE"           ,StringType(), True)
,StructField("ID_LINKED"            ,DecimalType (18,0), True)
,StructField("ID_MBI_CMS"           ,StringType(), True)
,StructField("DTE_APPLICATION"      ,DateType(), True)
,StructField("DTE_APPLICATION_NBR"  ,DecimalType (8,0), True)
,StructField("NUM_CASE"             ,StringType(), True)
,StructField("ENRL_SPAN_TYP"        ,StringType(), True)
,StructField("DTE_EFFECTIVE"        ,DateType(), True)
,StructField("DTE_EFFECTIVE_NBR"    ,DecimalType (8,0), True)
,StructField("DTE_END"              ,DateType(), True)
,StructField("DTE_END_NBR"          ,DecimalType (8,0), True)
,StructField("IND_RETRO"            ,StringType(), True)
,StructField("ID_MEDICARE"          ,StringType(), True)
,StructField("DTE_LAST_UPDATE"      ,DateType(), True)
,StructField("DTE_LAST_UPDATE_NBR"  ,DecimalType (8,0), True)
,StructField("CDE_SOURCE"           ,StringType(), True)
,StructField("REPORT_DTE"           ,DateType(), True)
,StructField("REPORT_DTE_NBR"       ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartI}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartJ_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartJ} (
# MAGIC  SAK_RECIP                DECIMAL (18,0)
# MAGIC ,ID_MEDICAID              STRING
# MAGIC ,IND_ACTIVE               STRING
# MAGIC ,ID_LINKED                DECIMAL (18,0)
# MAGIC ,ID_MBI_CMS               STRING
# MAGIC ,DTE_APPLICATION          DATE
# MAGIC ,DTE_APPLICATION_NBR      DECIMAL (8,0)
# MAGIC ,NUM_CASE                 STRING
# MAGIC ,ENRL_SPAN_TYP            STRING
# MAGIC ,DTE_VENDOR_PAY_BEGIN     DATE
# MAGIC ,DTE_VENDOR_PAY_BEGIN_NBR DECIMAL (8,0)
# MAGIC ,DTE_VENDOR_PAY_END       DATE
# MAGIC ,DTE_VENDOR_PAY_END_NBR   DECIMAL (8,0)
# MAGIC ,CDE_LEVEL_OF_CARE        STRING
# MAGIC ,ID_PROVIDER_MCAID        STRING
# MAGIC ,CDE_STATUS               STRING
# MAGIC ,IND_BACKPAY              STRING
# MAGIC ,REPORT_DTE               DATE
# MAGIC ,REPORT_DTE_NBR           DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartJ Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.J*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_RECIP"                ,DecimalType (18,0), True)
,StructField("ID_MEDICAID"              ,StringType(), True)
,StructField("IND_ACTIVE"               ,StringType(), True)
,StructField("ID_LINKED"                ,DecimalType (18,0), True)
,StructField("ID_MBI_CMS"               ,StringType(), True)
,StructField("DTE_APPLICATION"          ,DateType(), True)
,StructField("DTE_APPLICATION_NBR"      ,DecimalType (8,0), True)
,StructField("NUM_CASE"                 ,StringType(), True)
,StructField("ENRL_SPAN_TYP"            ,StringType(), True)
,StructField("DTE_VENDOR_PAY_BEGIN"     ,DateType(), True)
,StructField("DTE_VENDOR_PAY_BEGIN_NBR" ,DecimalType (8,0), True)
,StructField("DTE_VENDOR_PAY_END"       ,DateType(), True)
,StructField("DTE_VENDOR_PAY_END_NBR"   ,DecimalType (8,0), True)
,StructField("CDE_LEVEL_OF_CARE"        ,StringType(), True)
,StructField("ID_PROVIDER_MCAID"        ,StringType(), True)
,StructField("CDE_STATUS"               ,StringType(), True)
,StructField("IND_BACKPAY"              ,StringType(), True)
,StructField("REPORT_DTE"               ,DateType(), True)
,StructField("REPORT_DTE_NBR"           ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartJ}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartK_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartK} (
# MAGIC  SAK_RECIP                DECIMAL (18,0)
# MAGIC ,ID_MEDICAID              STRING
# MAGIC ,IND_ACTIVE               STRING
# MAGIC ,ID_LINKED                DECIMAL (18,0)
# MAGIC ,ID_MBI_CMS               STRING
# MAGIC ,DTE_APPLICATION          DATE
# MAGIC ,DTE_APPLICATION_NBR      DECIMAL (8,0)
# MAGIC ,NUM_CASE                 STRING
# MAGIC ,ENRL_SPAN_TYP            STRING
# MAGIC ,DTE_EFFECTIVE            DATE
# MAGIC ,DTE_EFFECTIVE_NBR        DECIMAL (8,0)
# MAGIC ,DTE_END                  DATE
# MAGIC ,DTE_END_NBR              DECIMAL (8,0)
# MAGIC ,CDE_PROV_TYPE_PRIM       STRING
# MAGIC ,ID_PROVIDER_MCAID        STRING
# MAGIC ,REPORT_DTE               DATE
# MAGIC ,REPORT_DTE_NBR           DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartK Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.K*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_RECIP"                ,DecimalType (18,0), True)
,StructField("ID_MEDICAID"              ,StringType(), True)
,StructField("IND_ACTIVE"               ,StringType(), True)
,StructField("ID_LINKED"                ,DecimalType (18,0), True)
,StructField("ID_MBI_CMS"               ,StringType(), True)
,StructField("DTE_APPLICATION"          ,DateType(), True)
,StructField("DTE_APPLICATION_NBR"      ,DecimalType (8,0), True)
,StructField("NUM_CASE"                 ,StringType(), True)
,StructField("ENRL_SPAN_TYP"            ,StringType(), True)
,StructField("DTE_EFFECTIVE"            ,DateType(), True)
,StructField("DTE_EFFECTIVE_NBR"        ,DecimalType (8,0), True)
,StructField("DTE_END"                  ,DateType(), True)
,StructField("DTE_END_NBR"              ,DecimalType (8,0), True)
,StructField("CDE_PROV_TYPE_PRIM"       ,StringType(), True)
,StructField("ID_PROVIDER_MCAID"        ,StringType(), True)
,StructField("REPORT_DTE"               ,DateType(), True)
,StructField("REPORT_DTE_NBR"           ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartK}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartL_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartL} (
# MAGIC  SAK_RECIP                DECIMAL (18,0)
# MAGIC ,ID_MEDICAID              STRING
# MAGIC ,IND_ACTIVE               STRING
# MAGIC ,ID_LINKED                DECIMAL (18,0)
# MAGIC ,ID_MBI_CMS               STRING
# MAGIC ,DTE_APPLICATION          DATE
# MAGIC ,DTE_APPLICATION_NBR      DECIMAL (8,0)
# MAGIC ,NUM_CASE                 STRING
# MAGIC ,ENRL_SPAN_TYP            STRING
# MAGIC ,DTE_EFFECTIVE            DATE
# MAGIC ,DTE_EFFECTIVE_NBR        DECIMAL (8,0)
# MAGIC ,DTE_END                  DATE
# MAGIC ,DTE_END_NBR              DECIMAL (8,0)
# MAGIC ,IND_RETRO                STRING
# MAGIC ,ID_MEDICARE              STRING
# MAGIC ,DTE_LAST_UPDATE          DATE
# MAGIC ,DTE_LAST_UPDATE_NBR      DECIMAL (8,0)
# MAGIC ,CDE_SOURCE               STRING
# MAGIC ,IND_FREE                 STRING
# MAGIC ,REPORT_DTE               DATE
# MAGIC ,REPORT_DTE_NBR           DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartL Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.L*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_RECIP"                ,DecimalType (18,0), True)
,StructField("ID_MEDICAID"              ,StringType(), True)
,StructField("IND_ACTIVE"               ,StringType(), True)
,StructField("ID_LINKED"                ,DecimalType (18,0), True)
,StructField("ID_MBI_CMS"               ,StringType(), True)
,StructField("DTE_APPLICATION"          ,DateType(), True)
,StructField("DTE_APPLICATION_NBR"      ,DecimalType (8,0), True)
,StructField("NUM_CASE"                 ,StringType(), True)
,StructField("ENRL_SPAN_TYP"            ,StringType(), True)
,StructField("DTE_EFFECTIVE"            ,DateType(), True)
,StructField("DTE_EFFECTIVE_NBR"        ,DecimalType (8,0), True)
,StructField("DTE_END"                  ,DateType(), True)
,StructField("DTE_END_NBR"              ,DecimalType (8,0), True)
,StructField("IND_RETRO"                ,StringType(), True)
,StructField("ID_MEDICARE"              ,StringType(), True)
,StructField("DTE_LAST_UPDATE"          ,DateType(), True)
,StructField("DTE_LAST_UPDATE_NBR"      ,DecimalType (8,0), True)
,StructField("CDE_SOURCE"               ,StringType(), True)
,StructField("IND_FREE"                 ,StringType(), True)
,StructField("REPORT_DTE"               ,DateType(), True)
,StructField("REPORT_DTE_NBR"           ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartL}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartN_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartN} (
# MAGIC  SAK_RECIP                DECIMAL (18,0)
# MAGIC ,ID_MEDICAID              STRING
# MAGIC ,IND_ACTIVE               STRING
# MAGIC ,ID_LINKED                DECIMAL (18,0)
# MAGIC ,ID_MBI_CMS               STRING
# MAGIC ,DTE_APPLICATION          DATE
# MAGIC ,DTE_APPLICATION_NBR      DECIMAL (8,0)
# MAGIC ,NUM_CASE                 STRING
# MAGIC ,ENRL_SPAN_TYP            STRING
# MAGIC ,DTE_TPL_EFFECTIVE        DATE
# MAGIC ,DTE_TPL_EFFECTIVE_NBR    DECIMAL (8,0)
# MAGIC ,DTE_TPL_END              DATE
# MAGIC ,DTE_TPL_END_NBR          DECIMAL (8,0)
# MAGIC ,CDE_COVERAGE             STRING
# MAGIC ,REPORT_DTE               DATE
# MAGIC ,REPORT_DTE_NBR           DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartN Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.N*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_RECIP"                ,DecimalType (18,0), True)
,StructField("ID_MEDICAID"              ,StringType(), True)
,StructField("IND_ACTIVE"               ,StringType(), True)
,StructField("ID_LINKED"                ,DecimalType (18,0), True)
,StructField("ID_MBI_CMS"               ,StringType(), True)
,StructField("DTE_APPLICATION"          ,DateType(), True)
,StructField("DTE_APPLICATION_NBR"      ,DecimalType (8,0), True)
,StructField("NUM_CASE"                 ,StringType(), True)
,StructField("ENRL_SPAN_TYP"            ,StringType(), True)
,StructField("DTE_TPL_EFFECTIVE"        ,DateType(), True)
,StructField("DTE_TPL_EFFECTIVE_NBR"    ,DecimalType (8,0), True)
,StructField("DTE_TPL_END"              ,DateType(), True)
,StructField("DTE_TPL_END_NBR"          ,DecimalType (8,0), True)
,StructField("CDE_COVERAGE"             ,StringType(), True)
,StructField("REPORT_DTE"               ,DateType(), True)
,StructField("REPORT_DTE_NBR"           ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartN}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN116FA_PartO_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN116FA_PartO} (
# MAGIC  SAK_RECIP                DECIMAL (18,0)
# MAGIC ,ID_MEDICAID              STRING
# MAGIC ,IND_ACTIVE               STRING
# MAGIC ,ID_LINKED                DECIMAL (18,0)
# MAGIC ,ID_MBI_CMS               STRING
# MAGIC ,DTE_APPLICATION          DATE
# MAGIC ,DTE_APPLICATION_NBR      DECIMAL (8,0)
# MAGIC ,NUM_CASE                 STRING
# MAGIC ,ENRL_SPAN_TYP            STRING
# MAGIC ,NUM_CONTRACT             STRING
# MAGIC ,DTE_EFFECTIVE            DATE
# MAGIC ,DTE_EFFECTIVE_NBR        DECIMAL (8,0)
# MAGIC ,DTE_END                  DATE
# MAGIC ,DTE_END_NBR              DECIMAL (8,0)
# MAGIC ,IND_SNP                  STRING
# MAGIC ,DTE_LAST_UPDATE          DATE
# MAGIC ,DTE_LAST_UPDATE_NBR      DECIMAL (8,0)
# MAGIC ,TXT_ORGANIZATION_NAME    STRING
# MAGIC ,TXT_PLAN_NAME            STRING
# MAGIC ,REPORT_DTE               DATE
# MAGIC ,REPORT_DTE_NBR           DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartO Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*116FA*PART.O*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("SAK_RECIP"             ,DecimalType (18,0), True)
,StructField("ID_MEDICAID"           ,StringType(), True)
,StructField("IND_ACTIVE"            ,StringType(), True)
,StructField("ID_LINKED"             ,DecimalType (18,0), True)
,StructField("ID_MBI_CMS"            ,StringType(), True)
,StructField("DTE_APPLICATION"       ,DateType(), True)
,StructField("DTE_APPLICATION_NBR"   ,DecimalType (8,0), True)
,StructField("NUM_CASE"              ,StringType(), True)
,StructField("ENRL_SPAN_TYP"         ,StringType(), True)
,StructField("NUM_CONTRACT"          ,StringType(), True)
,StructField("DTE_EFFECTIVE"         ,DateType(), True)
,StructField("DTE_EFFECTIVE_NBR"     ,DecimalType (8,0), True)
,StructField("DTE_END"               ,DateType(), True)
,StructField("DTE_END_NBR"           ,DecimalType (8,0), True)
,StructField("IND_SNP"               ,StringType(), True)
,StructField("DTE_LAST_UPDATE"       ,DateType(), True)
,StructField("DTE_LAST_UPDATE_NBR"   ,DecimalType (8,0), True)
,StructField("TXT_ORGANIZATION_NAME" ,StringType(), True)
,StructField("TXT_PLAN_NAME"         ,StringType(), True)
,StructField("REPORT_DTE"            ,DateType(), True)
,StructField("REPORT_DTE_NBR"        ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN116FA_PartO}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN101FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN101FA} (
# MAGIC  ELIG_MONTH                             DECIMAL(6,0)
# MAGIC ,SAK_RECIP                              DECIMAL(18,0)
# MAGIC ,REPORTING_MONTH                        DECIMAL(6,0)
# MAGIC ,ID_MEDICAID                            STRING
# MAGIC ,MM_PHASE                               DECIMAL(4,0)
# MAGIC ,DTE_BIRTH_NBR                          DECIMAL(8,0)
# MAGIC ,DTE_DEATH_NBR                          DECIMAL(8,0)
# MAGIC ,AGE_YEARS                              DECIMAL(3,0)
# MAGIC ,AGE_MONTHS                             DECIMAL(4,0)
# MAGIC ,CDE_SEX                                STRING
# MAGIC ,CDE_RES_COUNTY                         STRING
# MAGIC ,CDE_ELIG_COUNTY                        STRING
# MAGIC ,ELIG_COUNTY_NAME                       STRING
# MAGIC ,NUM_SSN                                STRING
# MAGIC ,NAM_FIRST                              STRING
# MAGIC ,NAM_MID_INIT                           STRING
# MAGIC ,NAM_LAST                               STRING
# MAGIC ,ADR_STREET_1                           STRING
# MAGIC ,ADR_STREET_2                           STRING
# MAGIC ,ADR_CTY                                STRING
# MAGIC ,ADR_STATE                              STRING
# MAGIC ,ADR_ZIP_CODE                           STRING
# MAGIC ,CDE_RACE                               STRING
# MAGIC ,ETHNICITY                              STRING
# MAGIC ,CDE_SAK_AID_CTG                        DECIMAL(9,0)
# MAGIC ,AID_CTG_DESC                           STRING
# MAGIC ,BP_ABP                                 DECIMAL(1,0)
# MAGIC ,BP_ALCRX                               DECIMAL(1,0)
# MAGIC ,BP_ALIEN                               DECIMAL(1,0)
# MAGIC ,BP_ASL                                 DECIMAL(1,0)
# MAGIC ,BP_CHOIC                               DECIMAL(1,0)
# MAGIC ,BP_DDSLF                               DECIMAL(1,0)
# MAGIC ,BP_HSPCB                               DECIMAL(1,0)
# MAGIC ,BP_ICWVR                               DECIMAL(1,0)
# MAGIC ,BP_MCAID                               DECIMAL(1,0)
# MAGIC ,BP_MRIO                                DECIMAL(1,0)
# MAGIC ,BP_MRLV1                               DECIMAL(1,0)
# MAGIC ,BP_MRTCM                               DECIMAL(1,0)
# MAGIC ,BP_MSP                                 DECIMAL(1,0)
# MAGIC ,BP_OHC                                 DECIMAL(1,0)
# MAGIC ,BP_OMH                                 DECIMAL(1,0)
# MAGIC ,BP_PACEB                               DECIMAL(1,0)
# MAGIC ,BP_PASSP                               DECIMAL(1,0)
# MAGIC ,BP_QI_1                                DECIMAL(1,0)
# MAGIC ,BP_QMB                                 DECIMAL(1,0)
# MAGIC ,BP_QWDI                                DECIMAL(1,0)
# MAGIC ,BP_REF                                 DECIMAL(1,0)
# MAGIC ,BP_SLMB                                DECIMAL(1,0)
# MAGIC ,BP_TMRDD                               DECIMAL(1,0)
# MAGIC ,BP_TRCO                                DECIMAL(1,0)
# MAGIC ,BP_OTHER                               DECIMAL(1,0)
# MAGIC ,BP_UNRECOGN                            DECIMAL(1,0)
# MAGIC ,AP_CSPP                                DECIMAL(1,0)
# MAGIC ,AP_CSPD                                DECIMAL(1,0)
# MAGIC ,AP_HSPCA                               DECIMAL(1,0)
# MAGIC ,AP_MHOME                               DECIMAL(1,0)
# MAGIC ,AP_PACEA                               DECIMAL(1,0)
# MAGIC ,AP_OTHER                               DECIMAL(1,0)
# MAGIC ,AP_UNRECOGN                            DECIMAL(1,0)
# MAGIC ,CDE_WAIVER                             STRING
# MAGIC ,CDE_WAIVER_DESC                        STRING
# MAGIC ,WAIVER_GRP                             STRING
# MAGIC ,ID_PMP_PLAN                            STRING
# MAGIC ,PMP_REGION                             STRING
# MAGIC ,PMP_PLAN                               STRING
# MAGIC ,CDE_PMP_ENROLL                         STRING
# MAGIC ,IND_PMP_ENROLL                         DECIMAL(1,0)
# MAGIC ,CAPT_AID_CTG                           DECIMAL(9,0)
# MAGIC ,CAPT_RATE                              STRING
# MAGIC ,CAPT_AMT                               DECIMAL(19,2)
# MAGIC ,DELV_RATE                              STRING
# MAGIC ,DELV_AMT                               DECIMAL(19,2)
# MAGIC ,CAPT_RATE_DESC                         STRING
# MAGIC ,DELV_RATE_DESC                         STRING
# MAGIC ,CDE_CAPT_FUND                          STRING
# MAGIC ,CDE_CAPT_FUND_DESC                     STRING
# MAGIC ,CDE_SPEC_COND                          STRING
# MAGIC ,IND_ICF_NONDC                          DECIMAL(1,0)
# MAGIC ,IND_ICF_DC                             DECIMAL(1,0)
# MAGIC ,IND_FFS_NF                             DECIMAL(1,0)
# MAGIC ,ID_PROVIDER_MCAID_ICF_DC               STRING
# MAGIC ,ID_PROVIDER_MCAID_ICF_NON_DC           STRING
# MAGIC ,ID_PROVIDER_MCAID_NF                   STRING
# MAGIC ,MEDICARE_A                             STRING
# MAGIC ,MEDICARE_B                             STRING
# MAGIC ,MEDICARE_C                             STRING
# MAGIC ,MEDICARE_D                             STRING
# MAGIC ,IND_MEDICARE                           STRING
# MAGIC ,IND_FULL_MEDICAID                      DECIMAL(1,0)
# MAGIC ,IND_MFP                                STRING
# MAGIC ,IND_SPENDDOWN                          STRING
# MAGIC ,AMT_SPENDDOWN                          DECIMAL(9,2)
# MAGIC ,PTNT_LIAB_AMT_SBMT                     DECIMAL(9,2)
# MAGIC ,IND_PTNT_LIAB_SBMT                     STRING
# MAGIC ,BP_ACT                                 DECIMAL(1,0)
# MAGIC ,BP_IHBT                                DECIMAL(1,0)
# MAGIC ,BP_IHSP                                DECIMAL(1,0)
# MAGIC ,BP_SRSP                                DECIMAL(1,0)
# MAGIC ,RES_COUNTY_NAME                        STRING
# MAGIC ,MEMBER_MONTH                           DECIMAL(1,0)
# MAGIC ,CDE_CAPT_REGION                        STRING
# MAGIC ,CDE_DELIV_REGION                       STRING
# MAGIC ,CDE_CAPT_REGION_DESC                   STRING
# MAGIC ,CDE_DELIV_REGION_DESC                  STRING
# MAGIC ,CAPT_AMT_SUM                           DECIMAL(19,2)
# MAGIC ,CAPT_AMT_VAR                           DECIMAL(19,2)
# MAGIC ,CDE_SPEC_COND_DESC                     STRING
# MAGIC ,CDE_ROLLUP_1                           STRING
# MAGIC ,CDE_ROLLUP_2                           STRING
# MAGIC ,CDE_ROLLUP_3                           STRING
# MAGIC ,CDE_ROLLUP_4                           STRING
# MAGIC ,CDE_ROLLUP_5                           STRING
# MAGIC ,CASELOAD_MJR_GROUP                     STRING
# MAGIC ,SAK_RECIP_PRI                          DECIMAL(9,0)
# MAGIC ,ID_PRI_RECIP_MCAID                     STRING
# MAGIC ,IND_SAK_RECIP_PRI                      STRING
# MAGIC ,DTE_RECIP_LINK_PRCS                    TIMESTAMP
# MAGIC ,DTE_RECIP_LINK_PRCS_NBR                DECIMAL(8,0)
# MAGIC ,CDE_ELIGIBILITY_SOURCE                 STRING
# MAGIC ,REPORT_DTE                             TIMESTAMP
# MAGIC ,REPORT_DTE_NBR                         DECIMAL(8,0)
# MAGIC ,CAPT_AID_CTG_DESC                      STRING
# MAGIC ,OHRISE_ID_PMP_PLAN                     STRING
# MAGIC ,OHRISE_PMP_PLAN                        STRING
# MAGIC ,OHRISE_IND_PMP_ENROLL                  DECIMAL(1,0)
# MAGIC ,OHRISE_CDE_PMP_ENROLL                  STRING
# MAGIC ,OHRISE_PMP_REGION                      STRING
# MAGIC ,OHRISE_CAPT_RATE                       STRING
# MAGIC ,OHRISE_CAPT_AMT                        DECIMAL(19,2)
# MAGIC ,OHRISE_CDE_CAPT_FUND                   STRING
# MAGIC ,OHRISE_CDE_CAPT_FUND_DESC              STRING
# MAGIC ,OHRISE_CAPT_AID_CTG                    DECIMAL(9,0)
# MAGIC ,OHRISE_CAPT_AID_CTG_DESC               STRING
# MAGIC ,OHRISE_CAPT_RATE_DESC                  STRING
# MAGIC ,OHRISE_CAPT_AMT_SUM                    DECIMAL(19,2)
# MAGIC ,OHRISE_IND_MCP                         DECIMAL(1,0)
# MAGIC ,OHRISE_IND_FFS                         DECIMAL(1,0)
# MAGIC ,BP_OHRISE_WVR                          DECIMAL(1,0)
# MAGIC ,MEMID                                  STRING
# MAGIC ,ENROLL_ID                              STRING
# MAGIC ,PLAN_ID                                STRING
# MAGIC ,RATE_ID                                STRING
# MAGIC ,BP_FMPLN                               DECIMAL(1,0)
# MAGIC ,BP_MCABD                               DECIMAL(1,0)
# MAGIC ,BP_MCCFC                               DECIMAL(1,0)
# MAGIC ,BP_MCICD                               DECIMAL(1,0)
# MAGIC ,BP_MCLTC                               DECIMAL(1,0)
# MAGIC ,BP_MCOHR                               DECIMAL(1,0)
# MAGIC ,BP_MFP                                 DECIMAL(1,0)
# MAGIC ,BP_PEPW                                DECIMAL(1,0)
# MAGIC ,PROGRAMID                              STRING
# MAGIC ,PROGRAM_DESC                           STRING
# MAGIC ,PLAN_ID_DESC                           STRING
# MAGIC ,OHRISE_ENROLL_ID                       STRING
# MAGIC ,OHRISE_PLAN_ID                         STRING
# MAGIC ,OHRISE_PLAN_ID_DESC                    STRING
# MAGIC ,MED_A_HOSPITALONLY                     DECIMAL(1,0)
# MAGIC ,MED_B_PROFONLY                         DECIMAL(1,0)
# MAGIC ,MED_C_MEDICAL                          DECIMAL(1,0)
# MAGIC ,MED_D_PHARMACY                         DECIMAL(1,0)
# MAGIC ,COB01_COMPREHENSIVE                    DECIMAL(1,0)
# MAGIC ,COB02_HOSPITALONLY                     DECIMAL(1,0)
# MAGIC ,COB05_PHARMACY                         DECIMAL(1,0)
# MAGIC ,COB04_COMPDENTAL                       DECIMAL(1,0)
# MAGIC ,COB06_VISION                           DECIMAL(1,0)
# MAGIC ,COB07_LONGTERMCARE                     DECIMAL(1,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN101FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*101FA.MONTHLY*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("ELIG_MONTH"                   ,DecimalType(6,0), True)
,StructField("SAK_RECIP"                    ,DecimalType(18,0), True)
,StructField("REPORTING_MONTH"              ,DecimalType(6,0), True)
,StructField("ID_MEDICAID"                  ,StringType(), True)
,StructField("MM_PHASE"                     ,DecimalType(4,0), True)
,StructField("DTE_BIRTH_NBR"                ,DecimalType(8,0), True)
,StructField("DTE_DEATH_NBR"                ,DecimalType(8,0), True)
,StructField("AGE_YEARS"                    ,DecimalType(3,0), True)
,StructField("AGE_MONTHS"                   ,DecimalType(4,0), True)
,StructField("CDE_SEX"                      ,StringType(), True)
,StructField("CDE_RES_COUNTY"               ,StringType(), True)
,StructField("CDE_ELIG_COUNTY"              ,StringType(), True)
,StructField("ELIG_COUNTY_NAME"             ,StringType(), True)
,StructField("NUM_SSN"                      ,StringType(), True)
,StructField("NAM_FIRST"                    ,StringType(), True)
,StructField("NAM_MID_INIT"                 ,StringType(), True)
,StructField("NAM_LAST"                     ,StringType(), True)
,StructField("ADR_STREET_1"                 ,StringType(), True)
,StructField("ADR_STREET_2"                 ,StringType(), True)
,StructField("ADR_CTY"                      ,StringType(), True)
,StructField("ADR_STATE"                    ,StringType(), True)
,StructField("ADR_ZIP_CODE"                 ,StringType(), True)
,StructField("CDE_RACE"                     ,StringType(), True)
,StructField("ETHNICITY"                    ,StringType(), True)
,StructField("CDE_SAK_AID_CTG"              ,DecimalType(9,0), True)
,StructField("AID_CTG_DESC"                 ,StringType(), True)
,StructField("BP_ABP"                       ,DecimalType(1,0), True)
,StructField("BP_ALCRX"                     ,DecimalType(1,0), True)
,StructField("BP_ALIEN"                     ,DecimalType(1,0), True)
,StructField("BP_ASL"                       ,DecimalType(1,0), True)
,StructField("BP_CHOIC"                     ,DecimalType(1,0), True)
,StructField("BP_DDSLF"                     ,DecimalType(1,0), True)
,StructField("BP_HSPCB"                     ,DecimalType(1,0), True)
,StructField("BP_ICWVR"                     ,DecimalType(1,0), True)
,StructField("BP_MCAID"                     ,DecimalType(1,0), True)
,StructField("BP_MRIO"                      ,DecimalType(1,0), True)
,StructField("BP_MRLV1"                     ,DecimalType(1,0), True)
,StructField("BP_MRTCM"                     ,DecimalType(1,0), True)
,StructField("BP_MSP"                       ,DecimalType(1,0), True)
,StructField("BP_OHC"                       ,DecimalType(1,0), True)
,StructField("BP_OMH"                       ,DecimalType(1,0), True)
,StructField("BP_PACEB"                     ,DecimalType(1,0), True)
,StructField("BP_PASSP"                     ,DecimalType(1,0), True)
,StructField("BP_QI_1"                      ,DecimalType(1,0), True)
,StructField("BP_QMB"                       ,DecimalType(1,0), True)
,StructField("BP_QWDI"                      ,DecimalType(1,0), True)
,StructField("BP_REF"                       ,DecimalType(1,0), True)
,StructField("BP_SLMB"                      ,DecimalType(1,0), True)
,StructField("BP_TMRDD"                     ,DecimalType(1,0), True)
,StructField("BP_TRCO"                      ,DecimalType(1,0), True)
,StructField("BP_OTHER"                     ,DecimalType(1,0), True)
,StructField("BP_UNRECOGN"                  ,DecimalType(1,0), True)
,StructField("AP_CSPP"                      ,DecimalType(1,0), True)
,StructField("AP_CSPD"                      ,DecimalType(1,0), True)
,StructField("AP_HSPCA"                     ,DecimalType(1,0), True)
,StructField("AP_MHOME"                     ,DecimalType(1,0), True)
,StructField("AP_PACEA"                     ,DecimalType(1,0), True)
,StructField("AP_OTHER"                     ,DecimalType(1,0), True)
,StructField("AP_UNRECOGN"                  ,DecimalType(1,0), True)
,StructField("CDE_WAIVER"                   ,StringType(), True)
,StructField("CDE_WAIVER_DESC"              ,StringType(), True)
,StructField("WAIVER_GRP"                   ,StringType(), True)
,StructField("ID_PMP_PLAN"                  ,StringType(), True)
,StructField("PMP_REGION"                   ,StringType(), True)
,StructField("PMP_PLAN"                     ,StringType(), True)
,StructField("CDE_PMP_ENROLL"               ,StringType(), True)
,StructField("IND_PMP_ENROLL"               ,DecimalType(1,0), True)
,StructField("CAPT_AID_CTG"                 ,DecimalType(9,0), True)
,StructField("CAPT_RATE"                    ,StringType(), True)
,StructField("CAPT_AMT"                     ,DecimalType(19,2), True)
,StructField("DELV_RATE"                    ,StringType(), True)
,StructField("DELV_AMT"                     ,DecimalType(19,2), True)
,StructField("CAPT_RATE_DESC"               ,StringType(), True)
,StructField("DELV_RATE_DESC"               ,StringType(), True)
,StructField("CDE_CAPT_FUND"                ,StringType(), True)
,StructField("CDE_CAPT_FUND_DESC"           ,StringType(), True)
,StructField("CDE_SPEC_COND"                ,StringType(), True)
,StructField("IND_ICF_NONDC"                ,DecimalType(1,0), True)
,StructField("IND_ICF_DC"                   ,DecimalType(1,0), True)
,StructField("IND_FFS_NF"                   ,DecimalType(1,0), True)
,StructField("ID_PROVIDER_MCAID_ICF_DC"     ,StringType(), True)
,StructField("ID_PROVIDER_MCAID_ICF_NON_DC" ,StringType(), True)
,StructField("ID_PROVIDER_MCAID_NF"         ,StringType(), True)
,StructField("MEDICARE_A"                   ,StringType(), True)
,StructField("MEDICARE_B"                   ,StringType(), True)
,StructField("MEDICARE_C"                   ,StringType(), True)
,StructField("MEDICARE_D"                   ,StringType(), True)
,StructField("IND_MEDICARE"                 ,StringType(), True)
,StructField("IND_FULL_MEDICAID"            ,DecimalType(1,0), True)
,StructField("IND_MFP"                      ,StringType(), True)
,StructField("IND_SPENDDOWN"                ,StringType(), True)
,StructField("AMT_SPENDDOWN"                ,DecimalType(9,2), True)
,StructField("PTNT_LIAB_AMT_SBMT"           ,DecimalType(9,2), True)
,StructField("IND_PTNT_LIAB_SBMT"           ,StringType(), True)
,StructField("BP_ACT"                       ,DecimalType(1,0), True)
,StructField("BP_IHBT"                      ,DecimalType(1,0), True)
,StructField("BP_IHSP"                      ,DecimalType(1,0), True)
,StructField("BP_SRSP"                      ,DecimalType(1,0), True)
,StructField("RES_COUNTY_NAME"              ,StringType(), True)
,StructField("MEMBER_MONTH"                 ,DecimalType(1,0), True)
,StructField("CDE_CAPT_REGION"              ,StringType(), True)
,StructField("CDE_DELIV_REGION"             ,StringType(), True)
,StructField("CDE_CAPT_REGION_DESC"         ,StringType(), True)
,StructField("CDE_DELIV_REGION_DESC"        ,StringType(), True)
,StructField("CAPT_AMT_SUM"                 ,DecimalType(19,2), True)
,StructField("CAPT_AMT_VAR"                 ,DecimalType(19,2), True)
,StructField("CDE_SPEC_COND_DESC"           ,StringType(), True)
,StructField("CDE_ROLLUP_1"                 ,StringType(), True)
,StructField("CDE_ROLLUP_2"                 ,StringType(), True)
,StructField("CDE_ROLLUP_3"                 ,StringType(), True)
,StructField("CDE_ROLLUP_4"                 ,StringType(), True)
,StructField("CDE_ROLLUP_5"                 ,StringType(), True)
,StructField("CASELOAD_MJR_GROUP"           ,StringType(), True)
,StructField("SAK_RECIP_PRI"                ,DecimalType(9,0), True)
,StructField("ID_PRI_RECIP_MCAID"           ,StringType(), True)
,StructField("IND_SAK_RECIP_PRI"            ,StringType(), True)
,StructField("DTE_RECIP_LINK_PRCS"          ,TimestampType(), True)
,StructField("DTE_RECIP_LINK_PRCS_NBR"      ,DecimalType(8,0), True)
,StructField("CDE_ELIGIBILITY_SOURCE"       ,StringType(), True)
,StructField("REPORT_DTE"                   ,TimestampType(), True)
,StructField("REPORT_DTE_NBR"               ,DecimalType(8,0), True)
,StructField("CAPT_AID_CTG_DESC"            ,StringType(), True)
,StructField("OHRISE_ID_PMP_PLAN"           ,StringType(), True)
,StructField("OHRISE_PMP_PLAN"              ,StringType(), True)
,StructField("OHRISE_IND_PMP_ENROLL"        ,DecimalType(1,0), True)
,StructField("OHRISE_CDE_PMP_ENROLL"        ,StringType(), True)
,StructField("OHRISE_PMP_REGION"            ,StringType(), True)
,StructField("OHRISE_CAPT_RATE"             ,StringType(), True)
,StructField("OHRISE_CAPT_AMT"              ,DecimalType(19,2), True)
,StructField("OHRISE_CDE_CAPT_FUND"         ,StringType(), True)
,StructField("OHRISE_CDE_CAPT_FUND_DESC"    ,StringType(), True)
,StructField("OHRISE_CAPT_AID_CTG"          ,DecimalType(9,0), True)
,StructField("OHRISE_CAPT_AID_CTG_DESC"     ,StringType(), True)
,StructField("OHRISE_CAPT_RATE_DESC"        ,StringType(), True)
,StructField("OHRISE_CAPT_AMT_SUM"          ,DecimalType(19,2), True)
,StructField("OHRISE_IND_MCP"               ,DecimalType(1,0), True)
,StructField("OHRISE_IND_FFS"               ,DecimalType(1,0), True)
,StructField("BP_OHRISE_WVR"                ,DecimalType(1,0), True)
,StructField("MEMID"                        ,StringType(), True)
,StructField("ENROLL_ID"                    ,StringType(), True)
,StructField("PLAN_ID"                      ,StringType(), True)
,StructField("RATE_ID"                      ,StringType(), True)
,StructField("BP_FMPLN"                     ,DecimalType(1,0), True)
,StructField("BP_MCABD"                     ,DecimalType(1,0), True)
,StructField("BP_MCCFC"                     ,DecimalType(1,0), True)
,StructField("BP_MCICD"                     ,DecimalType(1,0), True)
,StructField("BP_MCLTC"                     ,DecimalType(1,0), True)
,StructField("BP_MCOHR"                     ,DecimalType(1,0), True)
,StructField("BP_MFP"                       ,DecimalType(1,0), True)
,StructField("BP_PEPW"                      ,DecimalType(1,0), True)
,StructField("PROGRAMID"                    ,StringType(), True)
,StructField("PROGRAM_DESC"                 ,StringType(), True)
,StructField("PLAN_ID_DESC"                 ,StringType(), True)
,StructField("OHRISE_ENROLL_ID"             ,StringType(), True)
,StructField("OHRISE_PLAN_ID"               ,StringType(), True)
,StructField("OHRISE_PLAN_ID_DESC"          ,StringType(), True)
,StructField("MED_A_HOSPITALONLY"           ,DecimalType(1,0), True)
,StructField("MED_B_PROFONLY"               ,DecimalType(1,0), True)
,StructField("MED_C_MEDICAL"                ,DecimalType(1,0), True)
,StructField("MED_D_PHARMACY"               ,DecimalType(1,0), True)
,StructField("COB01_COMPREHENSIVE"          ,DecimalType(1,0), True)
,StructField("COB02_HOSPITALONLY"           ,DecimalType(1,0), True)
,StructField("COB05_PHARMACY"               ,DecimalType(1,0), True)
,StructField("COB04_COMPDENTAL"             ,DecimalType(1,0), True)
,StructField("COB06_VISION"                 ,DecimalType(1,0), True)
,StructField("COB07_LONGTERMCARE"           ,DecimalType(1,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN101FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartA} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartC Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartC_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartC} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartD01 Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartD01_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartD01} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartD02 Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartD02_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartD02} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartD03 Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartD03_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartD03} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartD04 Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartD04_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartD04} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartD05 Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartD05_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartD05} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartE Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartE_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartE} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartG Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartG_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartG} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartH Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartH_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartH} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartI Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartI_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartI} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartJ Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartJ_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartJ} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartK Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartK_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartK} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartL Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartL_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartL} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartN Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartN_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartN} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN116FA PartO Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN116FA_PartO_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN116FA_PartO} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN101FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN101FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN101FA} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartC Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartC_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartD01 Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartD01_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartD02 Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartD02_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartD03 Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartD03_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartD04 Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartD04_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartD05 Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartD05_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartE Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartE_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartG Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartG_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartH Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartH_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartI Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartI_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartJ Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartJ_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartK Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartK_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartL Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartL_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartN Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartN_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleaning VEN116FA PartO Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN116FA_PartO_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleaning VEN101FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN101FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
