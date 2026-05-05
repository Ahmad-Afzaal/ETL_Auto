# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     ETL_Finance.                                                                                                     *
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
#* 12/02/2024 CCRB70930/CO#43342  Jaime Zavala        Modified to meet the following Layout Changes on VEN113FB (11/07/2024).       *
#*                                                    --- New Field- 12  ---                                                        *
#*                                                    PMT_PAID_YEAR_MNTH                                                            *
#*                                                    CAP_YEAR_MNTH                                                                 *
#*                                                    NOTWITHHOLD_CAP_TYP                                                           *
#*                                                    WITHHOLD_CAP_TYP                                                              *
#*                                                    CAP_PROC_DT                                                                   *
#*                                                    CAP_PROC_DT_NBR                                                               *
#*                                                    CAP_PROC_YEAR_MNTH                                                            *
#*                                                    CAPFCTR_EFF_DT                                                                *
#*                                                    CAPFCTR_EFF_DT_NBR                                                            *
#*                                                    CAPFCTR_TERM_DT                                                               *
#*                                                    CAPFCTR_TERM_DT_NBR                                                           *
#*                                                    CAPFCTR_RATE_AMT                                                              *
#*                                                                                                                                  *
#*                                                    --- Deleted Fields - 50  ---                                                  *
#*                                                    SAK_CPTN                                                                      *
#*                                                    CPTN_ADJ_ID                                                                   *
#*                                                    CPTN_ADJ_TYP                                                                  *
#*                                                    CPTN_ADJ_STS                                                                  *
#*                                                    ADJ_FUND_ID                                                                   *
#*                                                    ADJ_FUND_DESC                                                                 *
#*                                                    MBR_ID                                                                        *
#*                                                    MBR_STS                                                                       *
#*                                                    MBR_COND_TYP                                                                  *
#*                                                    ADJ_RATE_CD_DESC                                                              *
#*                                                    CPTN_TXN_DT                                                                   *
#*                                                    CPTN_TXN_DT_NBR                                                               *
#*                                                    CPTN_DESC                                                                     *
#*                                                    EFF_DT                                                                        *
#*                                                    EFF_DT_NBR                                                                    *
#*                                                    END_DT                                                                        *
#*                                                    END_DT_NBR                                                                    *
#*                                                    CPTN_RATE_ID                                                                  *
#*                                                    CPTN_TERM_ID                                                                  *
#*                                                    CPTN_DT_NBR                                                                   *
#*                                                    VCHR_AFLT_ID                                                                  *
#*                                                    VCHR_DAY_CT                                                                   *
#*                                                    VCHR_RFND_AMT                                                                 *
#*                                                    VCHR_ENRL_ID                                                                  *
#*                                                    RCPNT_ENT_ID                                                                  *
#*                                                    PAID_AMT                                                                      *
#*                                                    PAYCHK_PAY_DSCNT                                                              *
#*                                                    PAYCHK_CHK_AMT                                                                *
#*                                                    CHK_NBR                                                                       *
#*                                                    PAYCHK_ISSUE_DT                                                               *
#*                                                    PAYCHK_ISSUE_DT_NBR                                                           *
#*                                                    CAP_YR_MNTH_NUM                                                               *
#*                                                    CPTN_TYP                                                                      *
#*                                                    FUND_CPTN_CD                                                                  *
#*                                                    FUND_WITHHOLD_CD                                                              *
#*                                                    MBR_COND_DESC                                                                 *
#*                                                    SAK_CONDITION                                                                 *
#*                                                    CONDITION_DESC                                                                *
#*                                                    CONDITION_TYP                                                                 *
#*                                                    COND_EFF_DT                                                                   *
#*                                                    COND_EFF_DT_NBR                                                               *
#*                                                    COND_TERM_DT                                                                  *
#*                                                    COND_TERM_DT_NBR                                                              *
#*                                                    SAK_SPCL_CONDITION                                                            *
#*                                                    SPCL_COND_CD                                                                  *
#*                                                    SPCL_COND_DESC                                                                *
#*                                                    SPCL_COND_EFF_DT                                                              *
#*                                                    SPCL_COND_EFF_DT_NBR                                                          *
#*                                                    SPCL_COND_TERM_DT                                                             *
#*                                                    SPCL_COND_TERM_DT_NBR                                                         *
#*                                                                                                                                  *
#*                                                    Also added Clustering and Optimize logic.                                     *
#* 12/06/2024 CCRB70930/CO#43342  Jaime Zavala        Removed logic for VEN114FB.                                                   *
#* 03/26/2025 CCRB70930/CO#43342  Jaime Zavala        Added logic for VEN16FB.                                                      *
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
dbutils.widgets.text('schema_name', 'archive_vendor_extracts')  
dbutils.widgets.text('Clng_Month_Gap', '-120')


#-------------------
# EDW Staging Tables
#-------------------
dbutils.widgets.text('VEN113FB', 'EDW_VEN113FB_Staging')
dbutils.widgets.text('VEN16FB', 'EDW_VEN16FB_Staging')

#--------------------
# EDW Historic Tables
#--------------------
dbutils.widgets.text('VEN113FB_hist', 'EDW_VEN113FB_Historic')
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
VEN113FB = dbutils.widgets.get('VEN113FB')
VEN16FB = dbutils.widgets.get('VEN16FB')

#--------------------
# EDW Historic Tables
#--------------------
VEN113FB_hist = dbutils.widgets.get('VEN113FB_hist')
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
print(VEN113FB)
print(VEN16FB)

print()
print("EDW Historic Tables")
print("-------------------")
print(VEN113FB_hist)
print(VEN16FB_hist)


# COMMAND ----------

# DBTITLE 1,EDW_VEN113FB_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN113FB} (
# MAGIC  FI_CAPITATION_EXT_SK  DECIMAL (18,0)
# MAGIC ,SAK_PROV              DECIMAL (18,0)
# MAGIC ,PROV_NAME             STRING
# MAGIC ,SAK_RECIP             DECIMAL (18,0)
# MAGIC ,MEDICAID_ID           STRING
# MAGIC ,MCD_PROV_ID           STRING
# MAGIC ,NPI_PROV_ID           STRING
# MAGIC ,PGM_ID                STRING
# MAGIC ,PGM_DESC              STRING
# MAGIC ,RATE_CELL_DESC        STRING
# MAGIC ,MBR_FRST_NM           STRING
# MAGIC ,MBR_LST_NM            STRING
# MAGIC ,MBR_MIDDLE_NM         STRING
# MAGIC ,MBR_SEX               STRING
# MAGIC ,RATE_CELL_CD          STRING
# MAGIC ,ENRL_TYP              STRING
# MAGIC ,ENRL_EFF_DT           TIMESTAMP
# MAGIC ,ENRL_EFF_DT_NBR       DECIMAL (8,0)
# MAGIC ,ENRL_TERM_DT          TIMESTAMP
# MAGIC ,ENRL_TERM_DT_NBR      DECIMAL (8,0)
# MAGIC ,ENRL_RATE_CD          STRING
# MAGIC ,PAYCHK_PMT_ID         STRING
# MAGIC ,PMT_PAID_DT           TIMESTAMP
# MAGIC ,PMT_PAID_DT_NBR       DECIMAL (8,0)
# MAGIC ,REPORT_DTE            TIMESTAMP
# MAGIC ,REPORT_DTE_NBR        DECIMAL (8,0)
# MAGIC ,CAP_DT                TIMESTAMP 
# MAGIC ,CAP_DT_NBR            DECIMAL (8,0)
# MAGIC ,CAP_MNTH_NUM          DECIMAL (10,0)
# MAGIC ,CAP_YEAR_NUM          DECIMAL (8,0)
# MAGIC ,FUND_CD               STRING
# MAGIC ,FUND_DESC             STRING
# MAGIC ,CPTN_NET_AMT          DECIMAL (19,4)
# MAGIC ,CPTN_WITHHOLD_AMT     DECIMAL (19,4)
# MAGIC ,FEDERAL_SHARE_AMT     DECIMAL (28,2)
# MAGIC ,FEDERAL_COVID_AMT     DECIMAL (28,2)
# MAGIC ,MFP_AMT               DECIMAL (28,2)
# MAGIC ,STATE_SHARE_AMT       DECIMAL (28,2)
# MAGIC ,ENRL_RATE_DESC        STRING
# MAGIC ,PMT_PAID_YEAR_MNTH    DECIMAL (10,0)
# MAGIC ,CAP_YEAR_MNTH         DECIMAL (8,0)
# MAGIC ,NOTWITHHOLD_CAP_TYP   STRING
# MAGIC ,WITHHOLD_CAP_TYP      STRING
# MAGIC ,CAP_PROC_DT           TIMESTAMP
# MAGIC ,CAP_PROC_DT_NBR       DECIMAL (8,0)
# MAGIC ,CAP_PROC_YEAR_MNTH    DECIMAL (8,0)
# MAGIC ,CAPFCTR_EFF_DT        TIMESTAMP
# MAGIC ,CAPFCTR_EFF_DT_NBR    DECIMAL (8,0)
# MAGIC ,CAPFCTR_TERM_DT       TIMESTAMP
# MAGIC ,CAPFCTR_TERM_DT_NBR   DECIMAL (8,0)
# MAGIC ,CAPFCTR_RATE_AMT      DECIMAL (19,4)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN113FB Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*113FB*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("FI_CAPITATION_EXT_SK"  ,DecimalType (18,0), True)
,StructField("SAK_PROV"              ,DecimalType (18,0), True)
,StructField("PROV_NAME"             ,StringType(), True)
,StructField("SAK_RECIP"             ,DecimalType (18,0), True)
,StructField("MEDICAID_ID"           ,StringType(), True)
,StructField("MCD_PROV_ID"           ,StringType(), True)
,StructField("NPI_PROV_ID"           ,StringType(), True)
,StructField("PGM_ID"                ,StringType(), True)
,StructField("PGM_DESC"              ,StringType(), True)
,StructField("RATE_CELL_DESC"        ,StringType(), True)
,StructField("MBR_FRST_NM"           ,StringType(), True)
,StructField("MBR_LST_NM"            ,StringType(), True)
,StructField("MBR_MIDDLE_NM"         ,StringType(), True)
,StructField("MBR_SEX"               ,StringType(), True)
,StructField("RATE_CELL_CD"          ,StringType(), True)
,StructField("ENRL_TYP"              ,StringType(), True)
,StructField("ENRL_EFF_DT"           ,TimestampType(), True)
,StructField("ENRL_EFF_DT_NBR"       ,DecimalType (8,0), True)
,StructField("ENRL_TERM_DT"          ,TimestampType(), True)
,StructField("ENRL_TERM_DT_NBR"      ,DecimalType (8,0), True)
,StructField("ENRL_RATE_CD"          ,StringType(), True)
,StructField("PAYCHK_PMT_ID"         ,StringType(), True)
,StructField("PMT_PAID_DT"           ,TimestampType(), True)
,StructField("PMT_PAID_DT_NBR"       ,DecimalType (8,0), True)
,StructField("REPORT_DTE"            ,TimestampType(), True)
,StructField("REPORT_DTE_NBR"        ,DecimalType (8,0), True)
,StructField("CAP_DT"                ,TimestampType(), True)
,StructField("CAP_DT_NBR"            ,DecimalType (8,0), True)
,StructField("CAP_MNTH_NUM"          ,DecimalType (10,0), True)
,StructField("CAP_YEAR_NUM"          ,DecimalType (8,0), True)
,StructField("FUND_CD"               ,StringType(), True)
,StructField("FUND_DESC"             ,StringType(), True)
,StructField("CPTN_NET_AMT"          ,DecimalType (19,4), True)
,StructField("CPTN_WITHHOLD_AMT"     ,DecimalType (19,4), True)
,StructField("FEDERAL_SHARE_AMT"     ,DecimalType (28,2), True)
,StructField("FEDERAL_COVID_AMT"     ,DecimalType (28,2), True)
,StructField("MFP_AMT"               ,DecimalType (28,2), True)
,StructField("STATE_SHARE_AMT"       ,DecimalType (28,2), True)
,StructField("ENRL_RATE_DESC"        ,StringType(), True)
,StructField("PMT_PAID_YEAR_MNTH"    ,DecimalType (10,0), True)
,StructField("CAP_YEAR_MNTH"         ,DecimalType (8,0), True)
,StructField("NOTWITHHOLD_CAP_TYP"   ,StringType(), True)
,StructField("WITHHOLD_CAP_TYP"      ,StringType(), True)
,StructField("CAP_PROC_DT"           ,TimestampType(), True)
,StructField("CAP_PROC_DT_NBR"       ,DecimalType (8,0), True)
,StructField("CAP_PROC_YEAR_MNTH"    ,DecimalType (8,0), True)
,StructField("CAPFCTR_EFF_DT"        ,TimestampType(), True)
,StructField("CAPFCTR_EFF_DT_NBR"    ,DecimalType (8,0), True)
,StructField("CAPFCTR_TERM_DT"       ,TimestampType(), True)
,StructField("CAPFCTR_TERM_DT_NBR"   ,DecimalType (8,0), True)
,StructField("CAPFCTR_RATE_AMT"      ,DecimalType (19,4), True)
])

table_name = f"{catalog}.{schema_name}.{VEN113FB}"
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
columnsInfo = spark.sql(f"DESCRIBE {catalog}.{schema_name}.{VEN113FB}")
  
# Show the schema including column names
# columnsInfo.show(truncate=False)

# Count the number of columns
numColumns = columnsInfo.count()
print(f"Number of columns in the table {catalog}.{schema_name}.{VEN113FB}: {numColumns}")

spark.sql(f"ALTER TABLE {catalog}.{schema_name}.{VEN113FB} SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '{numColumns}')")

# Manually trigger the recomputation of statistics for the Delta table
spark.sql(f"ANALYZE TABLE {catalog}.{schema_name}.{VEN113FB} COMPUTE STATISTICS")
print(f"Updated statistics for {catalog}.{schema_name}.{VEN113FB}")


# COMMAND ----------

# DBTITLE 1,Clustering and Optimize Table
# MAGIC %sql
# MAGIC -- Clustering and optimizing the  table
# MAGIC ALTER TABLE ${catalog}.${schema_name}.${VEN113FB}
# MAGIC CLUSTER BY (MEDICAID_ID,PROV_NAME,MCD_PROV_ID,CAP_DT);
# MAGIC --
# MAGIC -- Optimize Table
# MAGIC OPTIMIZE ${catalog}.${schema_name}.${VEN113FB};
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN113FB Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN113FB_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN113FB} )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN113FB Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN113FB_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,EDW_VEN16FB_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN16FB} (
# MAGIC  FI_MBR_CONDITION_EXT_SK	DECIMAL	(18,0)
# MAGIC ,MEMBER_SAK_ID              STRING
# MAGIC ,CONDITION_SAK_ID           STRING
# MAGIC ,EFFECTIVE_DT               TIMESTAMP	
# MAGIC ,EFFECTIVE_DT_NUM           DECIMAL (8,0)
# MAGIC ,TERMINATION_DT             TIMESTAMP	
# MAGIC ,TERMINATION_DT_NUM		    DECIMAL	(8,0)
# MAGIC ,PRE_EXISTING               STRING
# MAGIC ,RECONSIDER_DT              TIMESTAMP	
# MAGIC ,RECONSIDER_DT_NUM		    DECIMAL	(8,0)
# MAGIC ,CONDITION_TYP              STRING
# MAGIC ,CONDITION_DESCRIPTION      STRING
# MAGIC ,SAK_RECIPIENT_ID           DECIMAL (18,0)
# MAGIC ,MEDICAID_ID                STRING
# MAGIC ,LAST_NAME                  STRING
# MAGIC ,FIRST_NAME                 STRING
# MAGIC ,MIDDLE_NAME                STRING
# MAGIC ,SORT_ORDER                 DECIMAL (18,0)
# MAGIC ,REPORT_DTE                 TIMESTAMP	
# MAGIC ,REPORT_DTE_NBR             DECIMAL (8,0)
# MAGIC ,STATUS                     STRING
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
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN16FB Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN16FB_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC
