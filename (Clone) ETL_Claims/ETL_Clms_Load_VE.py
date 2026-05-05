# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     ETL_Clms_Load_VE.                                                                                                *
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
#* 05/29/2024 CCRB70930/CO#43342  Jaime Zavala        ChgFldLength.- VEN10001FA.DIAG_SAK_ID DECIMAL from (10,0) to (18,0).          *
#*                                                    NewExtract.- VEN10008FA- FI_Claim_Control_Num_Info Extract.                   *
#*                                                    NewExtract.- VEN10009FA- RX_Claim_Control_Num_Info Extract.                   *
#*                                                    NewFld.- VEN100FA.ALWD_QTY DECIMAL (19,2)                                     *
#*                                                    NewFld.- VEN100FA.THE_PAID_AMT DECIMAL (19,2)                                 *
#*                                                    NewFld.- VEN100FA.THE_DETAIL_PAID_AMT DECIMAL (19,2)                          *
#*                                                    NewFld.- VEN100FA_historic.ALWD_QTY DECIMAL (19,2)                            *
#*                                                    NewFld.- VEN100FA_historic.THE_PAID_AMT DECIMAL (19,2)                        *
#*                                                    NewFld.- VEN100FA_historic.THE_DETAIL_PAID_AMT DECIMAL (19,2)                 *
#* 06/19/2024 CCRB70930/CO#43342  Jaime Zavala        NewFld.- VEN100FA.HDR_DTL_PAID_IND STRING                                     *
#*                                                    NewFld.- VEN100FA_historic.HDR_DTL_PAID_IND STRING                            *
#*                                                    NewFld.- VEN12404FA.DRG_CD_DESC STRING                                        *
#*                                                    NewFld.- VEN12404FA_historic.DRG_CD_DESC STRING                               *
#* 08/15/2024 CCRB70930/CO#43342  Jaime Zavala        NewFld.- VEN10008FA.ICN_NBR_PLAN STRING                                       *
#*                                                    NewFld.- VEN10008FA_historic.ICN_NBR_PLAN STRING                              *
#*                                                    NewFld.- VEN10009FA.ICN_NBR_PLAN STRING                                       *
#*                                                    NewFld.- VEN10009FA_historic.ICN_NBR_PLAN STRING                              *
#* 01/13/2025 CCRB70930/CO#43342  Jaime Zavala        ---- Staging Table ----                                                       *
#*                                                    NewFld.- VEN100FA_staging.DKP_AFFILIATEID    STRING                           *
#*                                                    NewFld.- VEN100FA_staging.DKP_MEDICAID_ID    STRING                           *
#*                                                    NewFld.- VEN100FA_staging.DKP_PROV_NAME      STRING                           *
#*                                                    NewFld.- VEN100FA_staging.DKP_SAK_PROV_ID    DECIMAL (18,0)                   *
#*                                                    NewFld.- VEN100FA_staging.IS_DKP_IND         STRING                           *
#*                                                    NewFld.- VEN100FA_staging.ICN_MASK_CD        STRING                           *
#*                                                    NewFld.- VEN100FA_staging.CONTROL_NUM        STRING                           *
#*                                                    ---- Historical Table ----                                                    *
#*                                                    NewFld.- VEN100FA_historic.DKP_AFFILIATEID    STRING                          *
#*                                                    NewFld.- VEN100FA_historic.DKP_MEDICAID_ID    STRING                          *
#*                                                    NewFld.- VEN100FA_historic.DKP_PROV_NAME      STRING                          *
#*                                                    NewFld.- VEN100FA_historic.DKP_SAK_PROV_ID    DECIMAL (18,0)                  *
#*                                                    NewFld.- VEN100FA_historic.IS_DKP_IND         STRING                          *
#*                                                    NewFld.- VEN100FA_historic.ICN_MASK_CD        STRING                          *
#*                                                    NewFld.- VEN100FA_historic.CONTROL_NUM        STRING                          *
#* 04/10/2025 CCRB70930/CO#43342  Jaime Zavala        NewExtract.- VEN12501FA- Multi Provider Claim Extract.                        *
#* 05/13/2025 CCRB70930/CO#43342  Jaime Zavala        ---- Staging Table ----                                                       *
#*                                                    NewFld.- VEN12404FA_staging.DRG_SBMT_CD       STRING                          *
#*                                                    NewFld.- VEN12404FA_staging.DRG_MOD_CD        STRING                          *
#*                                                    DelFld.- VEN12404FA_staging.DRG_CD_DESC                                       *
#*                                                    ---- Historical Table ----                                                    *
#*                                                    NewFld.- VEN12404FA_historic.DRG_SBMT_CD      STRING                          *
#*                                                    NewFld.- VEN12404FA_historic.DRG_MOD_CD       STRING                          *
#*                                                    DelFld.- VEN12404FA_staging.DRG_CD_DESC                                       *
#************************************************************************************************************************************


# COMMAND ----------

# DBTITLE 1,Parameters
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('s3_location', 's3://gia-stg-oh-ue1-data-raw/haven/inbound/VE_EDW')
dbutils.widgets.text('received_date', '/process/ETL_Clm')
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('Clng_Month_Gap', '-120')
  
#-------------------
# EDW Staging Tables
#-------------------
  
dbutils.widgets.text('VEN100FA', 'EDW_VEN100FA_Staging')
dbutils.widgets.text('VEN10001FA', 'EDW_VEN10001FA_Staging')
dbutils.widgets.text('VEN10002FA', 'EDW_VEN10002FA_Staging')
dbutils.widgets.text('VEN10003FA', 'EDW_VEN10003FA_Staging')
dbutils.widgets.text('VEN10004FA', 'EDW_VEN10004FA_Staging')
dbutils.widgets.text('VEN10005FA', 'EDW_VEN10005FA_Staging')
dbutils.widgets.text('VEN10006FA', 'EDW_VEN10006FA_Staging')
dbutils.widgets.text('VEN10007FA', 'EDW_VEN10007FA_Staging')
dbutils.widgets.text('VEN10008FA', 'EDW_VEN10008FA_Staging')
dbutils.widgets.text('VEN10009FA', 'EDW_VEN10009FA_Staging')
dbutils.widgets.text('VEN12301FA', 'EDW_VEN12301FA_Staging')
dbutils.widgets.text('VEN12401FA', 'EDW_VEN12401FA_Staging')
dbutils.widgets.text('VEN12403FA', 'EDW_VEN12403FA_Staging')
dbutils.widgets.text('VEN12404FA', 'EDW_VEN12404FA_Staging')
dbutils.widgets.text('VEN12501FA', 'EDW_VEN12501FA_Staging')

#--------------------
# EDW Historic Tables
#--------------------

dbutils.widgets.text('VEN100FA_hist', 'EDW_VEN100FA_Historic')
dbutils.widgets.text('VEN10001FA_hist', 'EDW_VEN10001FA_Historic')
dbutils.widgets.text('VEN10002FA_hist', 'EDW_VEN10002FA_Historic')
dbutils.widgets.text('VEN10003FA_hist', 'EDW_VEN10003FA_Historic')
dbutils.widgets.text('VEN10004FA_hist', 'EDW_VEN10004FA_Historic')
dbutils.widgets.text('VEN10005FA_hist', 'EDW_VEN10005FA_Historic')
dbutils.widgets.text('VEN10006FA_hist', 'EDW_VEN10006FA_Historic')
dbutils.widgets.text('VEN10007FA_hist', 'EDW_VEN10007FA_Historic')
dbutils.widgets.text('VEN10008FA_hist', 'EDW_VEN10008FA_Historic')
dbutils.widgets.text('VEN10009FA_hist', 'EDW_VEN10009FA_Historic')
dbutils.widgets.text('VEN12301FA_hist', 'EDW_VEN12301FA_Historic')
dbutils.widgets.text('VEN12401FA_hist', 'EDW_VEN12401FA_Historic')
dbutils.widgets.text('VEN12403FA_hist', 'EDW_VEN12403FA_Historic')
dbutils.widgets.text('VEN12404FA_hist', 'EDW_VEN12404FA_Historic')
dbutils.widgets.text('VEN12501FA_hist', 'EDW_VEN12501FA_Historic')


# COMMAND ----------

# DBTITLE 1,Get Parameters Values
#-------------------
# EDW Staging Tables
#-------------------

VEN100FA = dbutils.widgets.get('VEN100FA')
VEN10001FA = dbutils.widgets.get('VEN10001FA')
VEN10002FA = dbutils.widgets.get('VEN10002FA')
VEN10003FA = dbutils.widgets.get('VEN10003FA')
VEN10004FA = dbutils.widgets.get('VEN10004FA')
VEN10005FA = dbutils.widgets.get('VEN10005FA')
VEN10006FA = dbutils.widgets.get('VEN10006FA')
VEN10007FA = dbutils.widgets.get('VEN10007FA')
VEN10008FA = dbutils.widgets.get('VEN10008FA')
VEN10009FA = dbutils.widgets.get('VEN10009FA')
VEN12301FA = dbutils.widgets.get('VEN12301FA')
VEN12401FA = dbutils.widgets.get('VEN12401FA')
VEN12403FA = dbutils.widgets.get('VEN12403FA')
VEN12404FA = dbutils.widgets.get('VEN12404FA')
VEN12501FA = dbutils.widgets.get('VEN12501FA')

#--------------------
# EDW Historic Tables
#--------------------

VEN100FA_hist = dbutils.widgets.get('VEN100FA_hist')
VEN10001FA_hist = dbutils.widgets.get('VEN10001FA_hist')
VEN10002FA_hist = dbutils.widgets.get('VEN10002FA_hist')
VEN10003FA_hist = dbutils.widgets.get('VEN10003FA_hist')
VEN10004FA_hist = dbutils.widgets.get('VEN10004FA_hist')
VEN10005FA_hist = dbutils.widgets.get('VEN10005FA_hist')
VEN10006FA_hist = dbutils.widgets.get('VEN10006FA_hist')
VEN10007FA_hist = dbutils.widgets.get('VEN10007FA_hist')
VEN10008FA_hist = dbutils.widgets.get('VEN10008FA_hist')
VEN10009FA_hist = dbutils.widgets.get('VEN10009FA_hist')
VEN12301FA_hist = dbutils.widgets.get('VEN12301FA_hist')
VEN12401FA_hist = dbutils.widgets.get('VEN12401FA_hist')
VEN12403FA_hist = dbutils.widgets.get('VEN12403FA_hist')
VEN12404FA_hist = dbutils.widgets.get('VEN12404FA_hist')
VEN12501FA_hist = dbutils.widgets.get('VEN12501FA_hist')

#-----------
# DBX Parms
#-----------
Clng_Month_Gap = dbutils.widgets.get('Clng_Month_Gap')
s3_location = dbutils.widgets.get('s3_location')
received_date = dbutils.widgets.get('received_date')
catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')


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
print(VEN100FA)
print(VEN10001FA)
print(VEN10002FA)
print(VEN10003FA)
print(VEN10004FA)
print(VEN10005FA)
print(VEN10006FA)
print(VEN10007FA)
print(VEN10008FA)
print(VEN10009FA)
print(VEN12301FA)
print(VEN12401FA)
print(VEN12403FA)
print(VEN12404FA)
print(VEN12501FA)

print()
print("EDW Historic Tables")
print("-------------------")
print(VEN100FA_hist)
print(VEN10001FA_hist)
print(VEN10002FA_hist)
print(VEN10003FA_hist)
print(VEN10004FA_hist)
print(VEN10005FA_hist)
print(VEN10006FA_hist)
print(VEN10007FA_hist)
print(VEN10008FA_hist)
print(VEN10009FA_hist)
print(VEN12301FA_hist)
print(VEN12401FA_hist)
print(VEN12403FA_hist)
print(VEN12404FA_hist)
print(VEN12501FA_hist)


# COMMAND ----------

# MAGIC %md
# MAGIC ### VE Load Stage

# COMMAND ----------

# DBTITLE 1,EDW_VEN100FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN100FA} (
# MAGIC  CLAIM_EXT_SK                DECIMAL (10,0)
# MAGIC ,SAK_CLAIM                   DECIMAL (18,0)
# MAGIC ,ICN_NBR                     STRING
# MAGIC ,DTL_NBR                     DECIMAL (4,0)
# MAGIC ,MEDICAID_ID                 STRING
# MAGIC ,CLAIM_IND                   STRING
# MAGIC ,SOI_CD                      STRING
# MAGIC ,ROM_CD                      STRING
# MAGIC ,DRG_CD                      STRING
# MAGIC ,PD_DT                       TIMESTAMP
# MAGIC ,PD_DT_NBR                   DECIMAL (8,0)
# MAGIC ,CLM_TYP_CD                  STRING
# MAGIC ,HDR_STS_CD                  STRING
# MAGIC ,HDR_DTL_IND                 STRING
# MAGIC ,ADMIT_SRC_CD                STRING
# MAGIC ,PAY_ARR_CD                  STRING
# MAGIC ,BILL_DT                     TIMESTAMP
# MAGIC ,BILL_DT_NBR                 DECIMAL (8,0)
# MAGIC ,HDR_FIRST_SVC_DT            TIMESTAMP
# MAGIC ,HDR_FIRST_SVC_DT_NBR        DECIMAL (8,0)
# MAGIC ,HDR_LAST_SVC_DT             TIMESTAMP
# MAGIC ,HDR_LAST_SVC_DT_NBR         DECIMAL (8,0)
# MAGIC ,SEX_CD                      STRING
# MAGIC ,ADJ_ICN_NBR                 STRING
# MAGIC ,RX_ID_NBR                   STRING
# MAGIC ,PD_AMT                      DECIMAL (19,2)
# MAGIC ,DTL_STS_CD                  STRING
# MAGIC ,DTL_ALWD_AMT                DECIMAL (19,2)
# MAGIC ,DTL_CO_PAY_AMT              DECIMAL (18,2)
# MAGIC ,DTL_FIRST_SVC_DT            TIMESTAMP
# MAGIC ,DTL_FIRST_SVC_DT_NBR        DECIMAL (8,0)
# MAGIC ,DTL_LAST_SVC_DT             TIMESTAMP
# MAGIC ,DTL_LAST_SVC_DT_NBR         DECIMAL (8,0)
# MAGIC ,UNT_BILL_QTY                DECIMAL (19,3)
# MAGIC ,DTL_BILL_AMT                DECIMAL (19,2)
# MAGIC ,PD_MCO_AMT                  DECIMAL (19,2)
# MAGIC ,NDC_CD                      STRING
# MAGIC ,BASIS_OF_CST                DECIMAL (10,0)
# MAGIC ,PROC_PRIM_CD                STRING
# MAGIC ,PROV_MCAID_ID               STRING
# MAGIC ,PROV_NPI_ID                 STRING
# MAGIC ,PROV_PRIM_CD_TYP            STRING
# MAGIC ,ICD_VERS_CD                 STRING
# MAGIC ,COST_INGRD_AMT              DECIMAL (18,2)
# MAGIC ,DSPN_DT                     TIMESTAMP
# MAGIC ,DSPN_DT_NBR                 DECIMAL (8,0)
# MAGIC ,DAY_SPLY_NBR                DECIMAL (18,0)
# MAGIC ,COINSR_AMT                  DECIMAL (19,2)
# MAGIC ,PD_MCARE_AMT                DECIMAL (19,2)
# MAGIC ,DEDUCT_AMT                  DECIMAL (19,2)
# MAGIC ,CLAIM_TYP_CD_DESC           STRING
# MAGIC ,CLAIM_TYP_CD_LONG_DESC      STRING
# MAGIC ,ENCTR_OR_FFS_DESC           STRING
# MAGIC ,DRG_CD_DESC                 STRING
# MAGIC ,DTL_CLAIM_STS_CD_DESC       STRING
# MAGIC ,ADJ_CLAIM_SAK_ID            DECIMAL (10,0)
# MAGIC ,FUND_CD_SHORT_DESC          STRING
# MAGIC ,THRPTC_CLS_SPLTY_CD         STRING
# MAGIC ,THRPTC_CLS_SPLTY_CD_DESC    STRING
# MAGIC ,DRUG_PACKAGE_SIZE_QTY       DECIMAL (11,3)
# MAGIC ,PROC_CD_DESC                STRING
# MAGIC ,REVENUE_CD_DESC             STRING
# MAGIC ,BILL_TYPE_CD_DESC           STRING
# MAGIC ,SUBMITTER_CD                STRING
# MAGIC ,PLAN_NAME                   STRING
# MAGIC ,PGM_NM                      STRING
# MAGIC ,SAK_RECIP                   DECIMAL (18,0)
# MAGIC ,BIRTH_DT                    TIMESTAMP
# MAGIC ,BIRTH_DT_NBR                DECIMAL (8,0)
# MAGIC ,DEATH_DT                    TIMESTAMP
# MAGIC ,DEATH_DT_NBR                DECIMAL (8,0)
# MAGIC ,SAK_PROV                    DECIMAL (18,0)
# MAGIC ,PROV_NAME                   STRING
# MAGIC ,TPL_AMT                     DECIMAL (18,2)
# MAGIC ,DAYS_CVRD                   DECIMAL (8,0)
# MAGIC ,DAYS_NCOVD                  DECIMAL (8,0)
# MAGIC ,ADMIT_SOURCE_CD_DESC        STRING
# MAGIC ,RPA_PROV_SAK_ID             DECIMAL (18,0)
# MAGIC ,OPERATING_PROV_SAK_ID       DECIMAL (18,0)
# MAGIC ,ORP_PROV_SAK_ID             DECIMAL (18,0)
# MAGIC ,OPERATING_PROV_SUBMITTED_ID STRING
# MAGIC ,ORP_PROV_SUBMITTED_ID       STRING
# MAGIC ,PATIENT_STS_CD              STRING
# MAGIC ,ADMIT_HOUR_CD               STRING
# MAGIC ,DTL_DISPENSE_QTY            DECIMAL (19,3)
# MAGIC ,MODIFIER_1_CD               STRING
# MAGIC ,MODIFIER_2_CD               STRING
# MAGIC ,MODIFIER_3_CD               STRING
# MAGIC ,MODIFIER_4_CD               STRING
# MAGIC ,HIC_SUB_NBR                 STRING
# MAGIC ,TIME_DISCHARGE              DECIMAL (4,0)
# MAGIC ,PRIOR_AUTH_NBR              DECIMAL (11,0)
# MAGIC ,COPAY_REASON_CD             STRING
# MAGIC ,IS_NON_DUPLICATE_IND        STRING
# MAGIC ,CLAIM_ACTIVE_IND            STRING
# MAGIC ,LAST_CLAIM_IND              STRING
# MAGIC ,TOOTH_CD_NBR                STRING
# MAGIC ,ADMISSION_DT                TIMESTAMP
# MAGIC ,ADMISSION_DT_NBR            DECIMAL (8,0)
# MAGIC ,DISCHARGE_DT                TIMESTAMP
# MAGIC ,DISCHARGE_DT_NBR            DECIMAL (8,0)
# MAGIC ,ENTERED_SYS_DT              TIMESTAMP
# MAGIC ,ENTERED_SYS_DT_NBR          DECIMAL (8,0)
# MAGIC ,BILL_CD_TYP                 STRING
# MAGIC ,NON_CVRD_AMT                DECIMAL (19,2)
# MAGIC ,POS_CD                      STRING
# MAGIC ,THERA_CLS_SPEC_CD           STRING
# MAGIC ,REVENUE_CD                  STRING
# MAGIC ,NDC_PROFEE_AMT              DECIMAL (18,2)
# MAGIC ,TCN_NBR                     STRING
# MAGIC ,FUND_CD                     STRING
# MAGIC ,EMERGENCY_CD                STRING
# MAGIC ,MCO_ADJUD_DT                TIMESTAMP
# MAGIC ,MCO_ADJUD_DT_NBR            DECIMAL (8,0)
# MAGIC ,CLARIFICATION1_CD           STRING
# MAGIC ,CLARIFICATION2_CD           STRING
# MAGIC ,CLARIFICATION3_CD           STRING
# MAGIC ,AID_CTG_CD                  STRING
# MAGIC ,ENC_TYP_CD                  STRING
# MAGIC ,COS_ST_CD                   STRING
# MAGIC ,PARNT_ICN_NBR               STRING
# MAGIC ,DML_TYPE                    STRING
# MAGIC ,IND_BRAND_MED_NEC           STRING
# MAGIC ,REPORT_DTE                  TIMESTAMP
# MAGIC ,REPORT_DTE_NBR              DECIMAL (8,0)
# MAGIC ,ALWD_QTY                    DECIMAL (19,2)
# MAGIC ,THE_PAID_AMT                DECIMAL (19,2)
# MAGIC ,THE_DETAIL_PAID_AMT         DECIMAL (19,2)
# MAGIC ,HDR_DTL_PAID_IND            STRING
# MAGIC ,DKP_AFFILIATEID             STRING
# MAGIC ,DKP_MEDICAID_ID             STRING
# MAGIC ,DKP_PROV_NAME               STRING
# MAGIC ,DKP_SAK_PROV_ID             DECIMAL (18,0)
# MAGIC ,IS_DKP_IND                  STRING
# MAGIC ,ICN_MASK_CD                 STRING
# MAGIC ,CONTROL_NUM                 STRING
# MAGIC ) 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN100FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*100FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("CLAIM_EXT_SK"                ,DecimalType (10,0), True)
,StructField("SAK_CLAIM"                   ,DecimalType (18,0), True)
,StructField("ICN_NBR"                     ,StringType(), True)
,StructField("DTL_NBR"                     ,DecimalType (4,0), True)
,StructField("MEDICAID_ID"                 ,StringType(), True)
,StructField("CLAIM_IND"                   ,StringType(), True)
,StructField("SOI_CD"                      ,StringType(), True)
,StructField("ROM_CD"                      ,StringType(), True)
,StructField("DRG_CD"                      ,StringType(), True)
,StructField("PD_DT"                       ,TimestampType(), True)
,StructField("PD_DT_NBR"                   ,DecimalType (8,0), True)
,StructField("CLM_TYP_CD"                  ,StringType(), True)
,StructField("HDR_STS_CD"                  ,StringType(), True)
,StructField("HDR_DTL_IND"                 ,StringType(), True)
,StructField("ADMIT_SRC_CD"                ,StringType(), True)
,StructField("PAY_ARR_CD"                  ,StringType(), True)
,StructField("BILL_DT"                     ,TimestampType(), True)
,StructField("BILL_DT_NBR"                 ,DecimalType (8,0), True)
,StructField("HDR_FIRST_SVC_DT"            ,TimestampType(), True)
,StructField("HDR_FIRST_SVC_DT_NBR"        ,DecimalType (8,0), True)
,StructField("HDR_LAST_SVC_DT"             ,TimestampType(), True)
,StructField("HDR_LAST_SVC_DT_NBR"         ,DecimalType (8,0), True)
,StructField("SEX_CD"                      ,StringType(), True)
,StructField("ADJ_ICN_NBR"                 ,StringType(), True)
,StructField("RX_ID_NBR"                   ,StringType(), True)
,StructField("PD_AMT"                      ,DecimalType (19,2), True)
,StructField("DTL_STS_CD"                  ,StringType(), True)
,StructField("DTL_ALWD_AMT"                ,DecimalType (19,2), True)
,StructField("DTL_CO_PAY_AMT"              ,DecimalType (18,2), True)
,StructField("DTL_FIRST_SVC_DT"            ,TimestampType(), True)
,StructField("DTL_FIRST_SVC_DT_NBR"        ,DecimalType (8,0), True)
,StructField("DTL_LAST_SVC_DT"             ,TimestampType(), True)
,StructField("DTL_LAST_SVC_DT_NBR"         ,DecimalType (8,0), True)
,StructField("UNT_BILL_QTY"                ,DecimalType (19,3), True)
,StructField("DTL_BILL_AMT"                ,DecimalType (19,2), True)
,StructField("PD_MCO_AMT"                  ,DecimalType (19,2), True)
,StructField("NDC_CD"                      ,StringType(), True)
,StructField("BASIS_OF_CST"                ,DecimalType (10,0), True)
,StructField("PROC_PRIM_CD"                ,StringType(), True)
,StructField("PROV_MCAID_ID"               ,StringType(), True)
,StructField("PROV_NPI_ID"                 ,StringType(), True)
,StructField("PROV_PRIM_CD_TYP"            ,StringType(), True)
,StructField("ICD_VERS_CD"                 ,StringType(), True)
,StructField("COST_INGRD_AMT"              ,DecimalType (18,2), True)
,StructField("DSPN_DT"                     ,TimestampType(), True)
,StructField("DSPN_DT_NBR"                 ,DecimalType (8,0), True)
,StructField("DAY_SPLY_NBR"                ,DecimalType (18,0), True)
,StructField("COINSR_AMT"                  ,DecimalType (19,2), True)
,StructField("PD_MCARE_AMT"                ,DecimalType (19,2), True)
,StructField("DEDUCT_AMT"                  ,DecimalType (19,2), True)
,StructField("CLAIM_TYP_CD_DESC"           ,StringType(), True)
,StructField("CLAIM_TYP_CD_LONG_DESC"      ,StringType(), True)
,StructField("ENCTR_OR_FFS_DESC"           ,StringType(), True)
,StructField("DRG_CD_DESC"                 ,StringType(), True)
,StructField("DTL_CLAIM_STS_CD_DESC"       ,StringType(), True)
,StructField("ADJ_CLAIM_SAK_ID"            ,DecimalType (10,0), True)
,StructField("FUND_CD_SHORT_DESC"          ,StringType(), True)
,StructField("THRPTC_CLS_SPLTY_CD"         ,StringType(), True)
,StructField("THRPTC_CLS_SPLTY_CD_DESC"    ,StringType(), True)
,StructField("DRUG_PACKAGE_SIZE_QTY"       ,DecimalType (11,3), True)
,StructField("PROC_CD_DESC"                ,StringType(), True)
,StructField("REVENUE_CD_DESC"             ,StringType(), True)
,StructField("BILL_TYPE_CD_DESC"           ,StringType(), True)
,StructField("SUBMITTER_CD"                ,StringType(), True)
,StructField("PLAN_NAME"                   ,StringType(), True)
,StructField("PGM_NM"                      ,StringType(), True)
,StructField("SAK_RECIP"                   ,DecimalType (18,0), True)
,StructField("BIRTH_DT"                    ,TimestampType(), True)
,StructField("BIRTH_DT_NBR"                ,DecimalType (8,0), True)
,StructField("DEATH_DT"                    ,TimestampType(), True)
,StructField("DEATH_DT_NBR"                ,DecimalType (8,0), True)
,StructField("SAK_PROV"                    ,DecimalType (18,0), True)
,StructField("PROV_NAME"                   ,StringType(), True)
,StructField("TPL_AMT"                     ,DecimalType (18,2), True)
,StructField("DAYS_CVRD"                   ,DecimalType (8,0), True)
,StructField("DAYS_NCOVD"                  ,DecimalType (8,0), True)
,StructField("ADMIT_SOURCE_CD_DESC"        ,StringType(), True)
,StructField("RPA_PROV_SAK_ID"             ,DecimalType (18,0), True)
,StructField("OPERATING_PROV_SAK_ID"       ,DecimalType (18,0), True)
,StructField("ORP_PROV_SAK_ID"             ,DecimalType (18,0), True)
,StructField("OPERATING_PROV_SUBMITTED_ID" ,StringType(), True)
,StructField("ORP_PROV_SUBMITTED_ID"       ,StringType(), True)
,StructField("PATIENT_STS_CD"              ,StringType(), True)
,StructField("ADMIT_HOUR_CD"               ,StringType(), True)
,StructField("DTL_DISPENSE_QTY"            ,DecimalType (19,3), True)
,StructField("MODIFIER_1_CD"               ,StringType(), True)
,StructField("MODIFIER_2_CD"               ,StringType(), True)
,StructField("MODIFIER_3_CD"               ,StringType(), True)
,StructField("MODIFIER_4_CD"               ,StringType(), True)
,StructField("HIC_SUB_NBR"                 ,StringType(), True)
,StructField("TIME_DISCHARGE"              ,DecimalType (4,0), True)
,StructField("PRIOR_AUTH_NBR"              ,DecimalType (11,0), True)
,StructField("COPAY_REASON_CD"             ,StringType(), True)
,StructField("IS_NON_DUPLICATE_IND"        ,StringType(), True)
,StructField("CLAIM_ACTIVE_IND"            ,StringType(), True)
,StructField("LAST_CLAIM_IND"              ,StringType(), True)
,StructField("TOOTH_CD_NBR"                ,StringType(), True)
,StructField("ADMISSION_DT"                ,TimestampType(), True)
,StructField("ADMISSION_DT_NBR"            ,DecimalType (8,0), True)
,StructField("DISCHARGE_DT"                ,TimestampType(), True)
,StructField("DISCHARGE_DT_NBR"            ,DecimalType (8,0), True)
,StructField("ENTERED_SYS_DT"              ,TimestampType(), True)
,StructField("ENTERED_SYS_DT_NBR"          ,DecimalType (8,0), True)
,StructField("BILL_CD_TYP"                 ,StringType(), True)
,StructField("NON_CVRD_AMT"                ,DecimalType (19,2), True)
,StructField("POS_CD"                      ,StringType(), True)
,StructField("THERA_CLS_SPEC_CD"           ,StringType(), True)
,StructField("REVENUE_CD"                  ,StringType(), True)
,StructField("NDC_PROFEE_AMT"              ,DecimalType (18,2), True)
,StructField("TCN_NBR"                     ,StringType(), True)
,StructField("FUND_CD"                     ,StringType(), True)
,StructField("EMERGENCY_CD"                ,StringType(), True)
,StructField("MCO_ADJUD_DT"                ,TimestampType(), True)
,StructField("MCO_ADJUD_DT_NBR"            ,DecimalType (8,0), True)
,StructField("CLARIFICATION1_CD"           ,StringType(), True)
,StructField("CLARIFICATION2_CD"           ,StringType(), True)
,StructField("CLARIFICATION3_CD"           ,StringType(), True)
,StructField("AID_CTG_CD"                  ,StringType(), True)
,StructField("ENC_TYP_CD"                  ,StringType(), True)
,StructField("COS_ST_CD"                   ,StringType(), True)
,StructField("PARNT_ICN_NBR"               ,StringType(), True)
,StructField("DML_TYPE"                    ,StringType(), True)
,StructField("IND_BRAND_MED_NEC"           ,StringType(), True)
,StructField("REPORT_DTE"                  ,TimestampType(), True)
,StructField("REPORT_DTE_NBR"              ,DecimalType (8,0), True)
,StructField("ALWD_QTY"                    ,DecimalType (19,2), True)
,StructField("THE_PAID_AMT"                ,DecimalType (19,2), True)
,StructField("THE_DETAIL_PAID_AMT"         ,DecimalType (19,2), True)
,StructField("HDR_DTL_PAID_IND"            ,StringType(), True)
,StructField("DKP_AFFILIATEID"             ,StringType(), True)
,StructField("DKP_MEDICAID_ID"             ,StringType(), True)
,StructField("DKP_PROV_NAME"               ,StringType(), True)
,StructField("DKP_SAK_PROV_ID"             ,DecimalType (18,0), True)
,StructField("IS_DKP_IND"                  ,StringType(), True)
,StructField("ICN_MASK_CD"                 ,StringType(), True)
,StructField("CONTROL_NUM"                 ,StringType(), True)
])

table_name = f"{catalog}.{schema_name}.{VEN100FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)



# COMMAND ----------

# DBTITLE 1,EDW_VEN10001FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN10001FA} (
# MAGIC  CLAIM_DIAG_SK  DECIMAL (10,0)
# MAGIC ,SAK_CLAIM      DECIMAL (18,0)
# MAGIC ,ICN_NBR        STRING
# MAGIC ,CLM_TYP_CD     STRING
# MAGIC ,DIAG_SEQ_CD    STRING
# MAGIC ,DIAG_CD        STRING
# MAGIC ,DIAG_DESC_CD   STRING
# MAGIC ,DIAG_SAK_ID    DECIMAL (18,0)
# MAGIC ,POA_CD         STRING
# MAGIC ,REPORT_DTE     TIMESTAMP
# MAGIC ,REPORT_DTE_NBR DECIMAL (8,0)
# MAGIC ,DIAG_TYP       STRING
# MAGIC ) 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10001FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*10001FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("CLAIM_DIAG_SK"  ,DecimalType (10,0), True)
,StructField("SAK_CLAIM"      ,DecimalType (18,0), True)
,StructField("ICN_NBR"        ,StringType(), True)
,StructField("CLM_TYP_CD"     ,StringType(), True)
,StructField("DIAG_SEQ_CD"    ,StringType(), True)
,StructField("DIAG_CD"        ,StringType(), True)
,StructField("DIAG_DESC_CD"   ,StringType(), True)
,StructField("DIAG_SAK_ID"    ,DecimalType (18,0), True)
,StructField("POA_CD"         ,StringType(), True)
,StructField("REPORT_DTE"     ,TimestampType(), True)
,StructField("REPORT_DTE_NBR" ,DecimalType (8,0), True)
,StructField("DIAG_TYP"       ,StringType(), True)
])

table_name = f"{catalog}.{schema_name}.{VEN10001FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)



# COMMAND ----------

# DBTITLE 1,EDW_VEN10002FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN10002FA} (
# MAGIC  CLAIM_OCCRNC_SK  DECIMAL (18,0)
# MAGIC ,SAK_CLAIM        DECIMAL (18,0)
# MAGIC ,ICN_NBR          STRING
# MAGIC ,CLM_TYP_CD       STRING
# MAGIC ,OCCUR_CD         STRING
# MAGIC ,OCCUR_DT         TIMESTAMP
# MAGIC ,OCCUR_DT_NBR     DECIMAL (8,0)
# MAGIC ,OCRNC_TO_DT      TIMESTAMP
# MAGIC ,OCRNC_TO_DT_NBR  DECIMAL (8,0)
# MAGIC ,QLFY_LIST_CD_TYP STRING
# MAGIC ,REPORT_DTE       TIMESTAMP
# MAGIC ,REPORT_DTE_NBR   DECIMAL (8,0)
# MAGIC ) 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10002FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*10002FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("CLAIM_OCCRNC_SK"  ,DecimalType (18,0), True)
,StructField("SAK_CLAIM"        ,DecimalType (18,0), True)
,StructField("ICN_NBR"          ,StringType(), True)
,StructField("CLM_TYP_CD"       ,StringType(), True)
,StructField("OCCUR_CD"         ,StringType(), True)
,StructField("OCCUR_DT"         ,TimestampType(), True)
,StructField("OCCUR_DT_NBR"     ,DecimalType (8,0), True)
,StructField("OCRNC_TO_DT"      ,TimestampType(), True)
,StructField("OCRNC_TO_DT_NBR"  ,DecimalType (8,0), True)
,StructField("QLFY_LIST_CD_TYP" ,StringType(), True)
,StructField("REPORT_DTE"       ,TimestampType(), True)
,StructField("REPORT_DTE_NBR"   ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN10002FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN10003FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN10003FA} (
# MAGIC  CLAIM_CONDITION_SK DECIMAL (18,0)
# MAGIC ,SAK_CLAIM          DECIMAL (18,0)
# MAGIC ,ICN_NBR            STRING
# MAGIC ,CLM_TYP_CD         STRING
# MAGIC ,COND_CD            STRING
# MAGIC ,COND_TYP           STRING
# MAGIC ,EFF_DT             TIMESTAMP
# MAGIC ,EFF_DT_NBR         DECIMAL (8,0)
# MAGIC ,TERM_DT            TIMESTAMP
# MAGIC ,TERM_DT_NBR        DECIMAL (8,0)
# MAGIC ,REPORT_DTE         TIMESTAMP 
# MAGIC ,REPORT_DTE_NBR     DECIMAL (8,0)
# MAGIC ) 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10003FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*10003FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("CLAIM_CONDITION_SK" ,DecimalType (18,0), True)
,StructField("SAK_CLAIM"          ,DecimalType (18,0), True)
,StructField("ICN_NBR"            ,StringType(), True)
,StructField("CLM_TYP_CD"         ,StringType(), True)
,StructField("COND_CD"            ,StringType(), True)
,StructField("COND_TYP"           ,StringType(), True)
,StructField("EFF_DT"             ,TimestampType(), True)
,StructField("EFF_DT_NBR"         ,DecimalType (8,0), True)
,StructField("TERM_DT"            ,TimestampType(), True)
,StructField("TERM_DT_NBR"        ,DecimalType (8,0), True)
,StructField("REPORT_DTE"         ,TimestampType(), True)
,StructField("REPORT_DTE_NBR"     ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN10003FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)



# COMMAND ----------

# DBTITLE 1,EDW_VEN10004FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN10004FA} (
# MAGIC  DENTAL_CLAIM_DETAILS_SK DECIMAL (18,0)
# MAGIC ,SAK_CLAIM               DECIMAL (18,0)
# MAGIC ,ICN_NBR                 STRING
# MAGIC ,CLM_TYP_CD              STRING
# MAGIC ,DTL_NBR                 DECIMAL (4,0)
# MAGIC ,TTH_SRFC_SEQ_NBR        DECIMAL (18,0)
# MAGIC ,TTH_SRFC_CD             STRING
# MAGIC ,REPORT_DTE              TIMESTAMP
# MAGIC ,REPORT_DTE_NBR          DECIMAL (8,0)
# MAGIC ) 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10004FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*10004FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("DENTAL_CLAIM_DETAILS_SK" ,DecimalType (18,0), True)
,StructField("SAK_CLAIM"               ,DecimalType (18,0), True)
,StructField("ICN_NBR"                 ,StringType(), True)
,StructField("CLM_TYP_CD"              ,StringType(), True)
,StructField("DTL_NBR"                 ,DecimalType (4,0), True)
,StructField("TTH_SRFC_SEQ_NBR"        ,DecimalType (18,0), True)
,StructField("TTH_SRFC_CD"             ,StringType(), True)
,StructField("REPORT_DTE"              ,TimestampType(), True)
,StructField("REPORT_DTE_NBR"          ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN10004FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN10005FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN10005FA} (
# MAGIC  MEDICAL_NDC_DETAIL_SK DECIMAL (18,0)
# MAGIC ,SAK_CLAIM             DECIMAL (18,0)
# MAGIC ,ICN_NBR               STRING
# MAGIC ,CLM_TYP_CD            STRING
# MAGIC ,DTL_NBR               DECIMAL (4,0)
# MAGIC ,RX_ID_NBR             STRING
# MAGIC ,NDC_CD                STRING
# MAGIC ,REPORT_DTE            TIMESTAMP
# MAGIC ,REPORT_DTE_NBR        DECIMAL (8,0)
# MAGIC ) 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10005FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*10005FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("MEDICAL_NDC_DETAIL_SK" ,DecimalType (18,0), True)
,StructField("SAK_CLAIM"             ,DecimalType (18,0), True)
,StructField("ICN_NBR"               ,StringType(), True)
,StructField("CLM_TYP_CD"            ,StringType(), True)
,StructField("DTL_NBR"               ,DecimalType (4,0), True)
,StructField("RX_ID_NBR"             ,StringType(), True)
,StructField("NDC_CD"                ,StringType(), True)
,StructField("REPORT_DTE"            ,TimestampType(), True)
,StructField("REPORT_DTE_NBR"        ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN10005FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN10006FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN10006FA} (
# MAGIC  HDR_ALWD_AMT_SK DECIMAL (18,0)
# MAGIC ,SAK_CLAIM       DECIMAL (18,0)
# MAGIC ,ICN_NBR         STRING
# MAGIC ,CLM_TYP_CD      STRING
# MAGIC ,DTL_NBR         DECIMAL (4,0)
# MAGIC ,HDR_ALWD_AMT    DECIMAL (19,2)
# MAGIC ,REPORT_DTE      TIMESTAMP
# MAGIC ,REPORT_DTE_NBR  DECIMAL (8,0)
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10006FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*10006FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("HDR_ALWD_AMT_SK" ,DecimalType (18,0), True)
,StructField("SAK_CLAIM"       ,DecimalType (18,0), True)
,StructField("ICN_NBR"         ,StringType(), True)
,StructField("CLM_TYP_CD"      ,StringType(), True)
,StructField("DTL_NBR"         ,DecimalType (4,0), True)
,StructField("HDR_ALWD_AMT"    ,DecimalType (19,2), True)
,StructField("REPORT_DTE"      ,TimestampType(), True)
,StructField("REPORT_DTE_NBR"  ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN10006FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN10007FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN10007FA} (
# MAGIC  CLAIM_COST_DEBT_SK DECIMAL (18,0)
# MAGIC ,SAK_CLAIM          DECIMAL (18,0)
# MAGIC ,ICN_NBR            STRING
# MAGIC ,CLM_TYP_CD         STRING
# MAGIC ,COST_DET_BASIS     STRING
# MAGIC ,DRUG_QTY           STRING
# MAGIC ,ING_COST           DECIMAL (19,4)
# MAGIC ,ING_IDX            DECIMAL (8,3)
# MAGIC ,WEIGHT_COST        DECIMAL (18,4)
# MAGIC ,REPORT_DTE         TIMESTAMP
# MAGIC ,REPORT_DTE_NBR     DECIMAL (8,0)
# MAGIC ) 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10007FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*10007FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("CLAIM_COST_DEBT_SK" ,DecimalType (18,0), True)
,StructField("SAK_CLAIM"          ,DecimalType (18,0), True)
,StructField("ICN_NBR"            ,StringType(), True)
,StructField("CLM_TYP_CD"         ,StringType(), True)
,StructField("COST_DET_BASIS"     ,StringType(), True)
,StructField("DRUG_QTY"           ,StringType(), True)
,StructField("ING_COST"           ,DecimalType (19,4), True)
,StructField("ING_IDX"            ,DecimalType (8,3), True)
,StructField("WEIGHT_COST"        ,DecimalType (18,4), True)
,StructField("REPORT_DTE"         ,TimestampType(), True)
,StructField("REPORT_DTE_NBR"     ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN10007FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN10008FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN10008FA} (
# MAGIC  FI_CONTROL_NUM_INFO_EXT_SK DECIMAL (18,0)
# MAGIC ,ICN_NBR                    STRING
# MAGIC ,CONTROL_NUM                STRING
# MAGIC ,ICN_NBR_PLAN               STRING
# MAGIC ) 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10008FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*10008FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("FI_CONTROL_NUM_INFO_EXT_SK" ,DecimalType (18,0), True)
,StructField("ICN_NBR"                    ,StringType(), True)
,StructField("CONTROL_NUM"                ,StringType(), True)
,StructField("ICN_NBR_PLAN"               ,StringType(), True)
])

table_name = f"{catalog}.{schema_name}.{VEN10008FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN10009FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN10009FA} (
# MAGIC  RX_CONTROL_NUM_INFO_EXT_SK DECIMAL (18,0)
# MAGIC ,ICN_NBR                    STRING
# MAGIC ,CONTROL_NUM                STRING
# MAGIC ,ICN_NBR_PLAN               STRING
# MAGIC ) 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10009FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*10009FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("RX_CONTROL_NUM_INFO_EXT_SK" ,DecimalType (18,0), True)
,StructField("ICN_NBR"                    ,StringType(), True)
,StructField("CONTROL_NUM"                ,StringType(), True)
,StructField("ICN_NBR_PLAN"               ,StringType(), True)
])

table_name = f"{catalog}.{schema_name}.{VEN10009FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN12301FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN12301FA} (
# MAGIC  CLAIM_PROC_SK        DECIMAL (18,0)
# MAGIC ,SAK_CLAIM            DECIMAL (18,0)
# MAGIC ,ICN_NBR              STRING
# MAGIC ,CLM_TYP_CD           STRING
# MAGIC ,SEQ_NBR              DECIMAL (18,0)
# MAGIC ,ICD_9_CM_PROC_CD     STRING
# MAGIC ,ICD_9_CM_PROC_DT     TIMESTAMP
# MAGIC ,ICD_9_CM_PROC_DT_NBR DECIMAL (8,0)
# MAGIC ,ICD_INP_PROC_CD_VERS STRING
# MAGIC ,REPORT_DTE           TIMESTAMP
# MAGIC ,REPORT_DTE_NBR       DECIMAL (8,0)
# MAGIC ) 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN12301FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*12301FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("CLAIM_PROC_SK"        ,DecimalType (18,0), True)
,StructField("SAK_CLAIM"            ,DecimalType (18,0), True)
,StructField("ICN_NBR"              ,StringType(), True)
,StructField("CLM_TYP_CD"           ,StringType(), True)
,StructField("SEQ_NBR"              ,DecimalType (18,0), True)
,StructField("ICD_9_CM_PROC_CD"     ,StringType(), True)
,StructField("ICD_9_CM_PROC_DT"     ,TimestampType(), True)
,StructField("ICD_9_CM_PROC_DT_NBR" ,DecimalType (8,0), True)
,StructField("ICD_INP_PROC_CD_VERS" ,StringType(), True)
,StructField("REPORT_DTE"           ,TimestampType(), True)
,StructField("REPORT_DTE_NBR"       ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN12301FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN12401FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN12401FA} (
# MAGIC  CLAIM_ADTL_EOB_SK DECIMAL (10,0)
# MAGIC ,SAK_CLAIM         DECIMAL (18,0)
# MAGIC ,ICN_NBR           STRING
# MAGIC ,DTL_NBR           DECIMAL (4,0)
# MAGIC ,PAID_DT           TIMESTAMP
# MAGIC ,PAID_DT_NBR       DECIMAL (8,0)
# MAGIC ,HDR_STATUS_CD     STRING
# MAGIC ,DTL_STATUS_CD     STRING
# MAGIC ,CLAIM_IND         STRING
# MAGIC ,EOB_AMT           DECIMAL (19,4)
# MAGIC ,EOB_ELGB_AMT      DECIMAL (19,4)
# MAGIC ,REPORT_DTE        TIMESTAMP
# MAGIC ,REPORT_DTE_NBR    DECIMAL (8,0)
# MAGIC ) 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN12401FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*12401FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("CLAIM_ADTL_EOB_SK" ,DecimalType (10,0), True)
,StructField("SAK_CLAIM"         ,DecimalType (18,0), True)
,StructField("ICN_NBR"           ,StringType(), True)
,StructField("DTL_NBR"           ,DecimalType (4,0), True)
,StructField("PAID_DT"           ,TimestampType(), True)
,StructField("PAID_DT_NBR"       ,DecimalType (8,0), True)
,StructField("HDR_STATUS_CD"     ,StringType(), True)
,StructField("DTL_STATUS_CD"     ,StringType(), True)
,StructField("CLAIM_IND"         ,StringType(), True)
,StructField("EOB_AMT"           ,DecimalType (19,4), True)
,StructField("EOB_ELGB_AMT"      ,DecimalType (19,4), True)
,StructField("REPORT_DTE"        ,TimestampType(), True)
,StructField("REPORT_DTE_NBR"    ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN12401FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN12403FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN12403FA} (
# MAGIC  CLAIM_ADTL_OTHERPAYER_SK DECIMAL (10,0)
# MAGIC ,SAK_CLAIM                DECIMAL (18,0)
# MAGIC ,ICN_NBR                  STRING
# MAGIC ,DTL_NBR                  DECIMAL (4,0)
# MAGIC ,PAID_DT                  TIMESTAMP
# MAGIC ,PAID_DT_NBR              DECIMAL (8,0)
# MAGIC ,HDR_STATUS_CD            STRING
# MAGIC ,DTL_STATUS_CD            STRING
# MAGIC ,CLAIM_IND                STRING
# MAGIC ,SAK_PAYER                DECIMAL (18,0)
# MAGIC ,CLAIM_FILING_IND_CD      STRING
# MAGIC ,FQHC_IND                 STRING
# MAGIC ,NAM_INSURED_GRP          STRING
# MAGIC ,OTHER_PYR_PARTY_ID       STRING
# MAGIC ,ALWD_OTH_PYR_AMT         DECIMAL (19,2)
# MAGIC ,CONTRACT_SUB_ID          STRING
# MAGIC ,MCO_ADJUD_DTL_DT         TIMESTAMP
# MAGIC ,MCO_ADJUD_DTL_DT_NBR     DECIMAL (8,0)
# MAGIC ,REPORT_DTE               TIMESTAMP
# MAGIC ,REPORT_DTE_NBR           DECIMAL (8,0)
# MAGIC ) 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN12403FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*12403FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("CLAIM_ADTL_OTHERPAYER_SK" ,DecimalType (10,0), True)
,StructField("SAK_CLAIM"                ,DecimalType (18,0), True)
,StructField("ICN_NBR"                  ,StringType(), True)
,StructField("DTL_NBR"                  ,DecimalType (4,0), True)
,StructField("PAID_DT"                  ,TimestampType(), True)
,StructField("PAID_DT_NBR"              ,DecimalType (8,0), True)
,StructField("HDR_STATUS_CD"            ,StringType(), True)
,StructField("DTL_STATUS_CD"            ,StringType(), True)
,StructField("CLAIM_IND"                ,StringType(), True)
,StructField("SAK_PAYER"                ,DecimalType (18,0), True)
,StructField("CLAIM_FILING_IND_CD"      ,StringType(), True)
,StructField("FQHC_IND"                 ,StringType(), True)
,StructField("NAM_INSURED_GRP"          ,StringType(), True)
,StructField("OTHER_PYR_PARTY_ID"       ,StringType(), True)
,StructField("ALWD_OTH_PYR_AMT"         ,DecimalType (19,2), True)
,StructField("CONTRACT_SUB_ID"          ,StringType(), True)
,StructField("MCO_ADJUD_DTL_DT"         ,TimestampType(), True)
,StructField("MCO_ADJUD_DTL_DT_NBR"     ,DecimalType (8,0), True)
,StructField("REPORT_DTE"               ,TimestampType(), True)
,StructField("REPORT_DTE_NBR"           ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN12403FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN12404FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN12404FA} (
# MAGIC  CLAIM_ADTL_DRG_SK DECIMAL (10,0)
# MAGIC ,SAK_CLAIM         DECIMAL (18,0)
# MAGIC ,ICN_NBR           STRING
# MAGIC ,DTL_NBR           DECIMAL (4,0)
# MAGIC ,PAID_DT           TIMESTAMP
# MAGIC ,PAID_DT_NBR       DECIMAL (8,0)
# MAGIC ,HDR_STATUS_CD     STRING
# MAGIC ,DTL_STATUS_CD     STRING
# MAGIC ,CLAIM_IND         STRING
# MAGIC ,CAPITAL_AMT       DECIMAL (9,2)
# MAGIC ,COST_OUTLIER_AMT  DECIMAL (9,2)
# MAGIC ,DAY_OUTLIER_AMT   DECIMAL (9,2)
# MAGIC ,DRG_ADMIT_CD      STRING
# MAGIC ,DRG_CD            STRING
# MAGIC ,DRG_VERS_NBR      STRING
# MAGIC ,ROM_CD            STRING
# MAGIC ,SOI_CD            STRING
# MAGIC ,ICD_VERS_CD       STRING
# MAGIC ,REPORT_DTE        TIMESTAMP
# MAGIC ,REPORT_DTE_NBR    DECIMAL (8,0)
# MAGIC ,DRG_SBMT_CD       STRING
# MAGIC ,DRG_MOD_CD        STRING
# MAGIC ) 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN12404FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*12404FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("CLAIM_ADTL_DRG_SK" ,DecimalType (10,0), True)
,StructField("SAK_CLAIM"         ,DecimalType (18,0), True)
,StructField("ICN_NBR"           ,StringType(), True)
,StructField("DTL_NBR"           ,DecimalType (4,0), True)
,StructField("PAID_DT"           ,TimestampType(), True)
,StructField("PAID_DT_NBR"       ,DecimalType (8,0), True)
,StructField("HDR_STATUS_CD"     ,StringType(), True)
,StructField("DTL_STATUS_CD"     ,StringType(), True)
,StructField("CLAIM_IND"         ,StringType(), True)
,StructField("CAPITAL_AMT"       ,DecimalType (9,2), True)
,StructField("COST_OUTLIER_AMT"  ,DecimalType (9,2), True)
,StructField("DAY_OUTLIER_AMT"   ,DecimalType (9,2), True)
,StructField("DRG_ADMIT_CD"      ,StringType(), True)
,StructField("DRG_CD"            ,StringType(), True)
,StructField("DRG_VERS_NBR"      ,StringType(), True)
,StructField("ROM_CD"            ,StringType(), True)
,StructField("SOI_CD"            ,StringType(), True)
,StructField("ICD_VERS_CD"       ,StringType(), True)
,StructField("REPORT_DTE"        ,TimestampType(), True)
,StructField("REPORT_DTE_NBR"    ,DecimalType (8,0), True)
,StructField("DRG_SBMT_CD"       ,StringType(), True)
,StructField("DRG_MOD_CD"        ,StringType(), True)
])

table_name = f"{catalog}.{schema_name}.{VEN12404FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# DBTITLE 1,EDW_VEN12501FA_Staging DDL
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${VEN12501FA} (
# MAGIC  CLM_SVCDTL_PROV_EXT_SK              DECIMAL (18,0)
# MAGIC ,ICN_NBR                             STRING
# MAGIC ,ATTENDING_EXTERNALPROVID            STRING
# MAGIC ,ATTENDING_EXTERNALPROVIDQUALIFIER	 STRING
# MAGIC ,ATTENDING_PROV_NAME                 STRING
# MAGIC ,ATTENDING_PROVID                    STRING
# MAGIC ,ATTENDING_SAK_PROV_ID               DECIMAL (18,0)
# MAGIC ,BILLING_EXTERNALPROVID         	   STRING
# MAGIC ,BILLING_EXTERNALPROVIDQUALIFIER	   STRING
# MAGIC ,BILLING_MEDICAID_ID		             STRING
# MAGIC ,BILLING_PROV_NAME                   STRING
# MAGIC ,BILLING_PROVID                      STRING
# MAGIC ,BILLING_SAK_PROV_ID                 DECIMAL (18,0)
# MAGIC ,BILLING_MMIS_PROV_TYP               STRING
# MAGIC ,BILLING_PROV_TYPE_NM                STRING
# MAGIC ,OPERATING_EXTERNALPROVID            STRING
# MAGIC ,OPERATING_EXTERNALPROVIDQUALIFIER	 STRING
# MAGIC ,OPERATING_PROV_NAME                 STRING
# MAGIC ,OPERATING_PROVID                    STRING
# MAGIC ,OPERATING_SAK_PROV_ID               DECIMAL (18,0)
# MAGIC ,ORDERING_EXTERNALPROVID             STRING
# MAGIC ,ORDERING_EXTERNALPROVIDQUALIFIER	   STRING
# MAGIC ,ORDERING_PROV_NAME		               STRING
# MAGIC ,ORDERING_PROVID					           STRING
# MAGIC ,ORDERING_SAK_PROV_ID				         DECIMAL (18,0)
# MAGIC ,PRESCRIBING_PROV_NAME				       STRING
# MAGIC ,PRESCRIBING_EXTERNALPROVID			     STRING
# MAGIC ,PRESCRIBING_EXTERNALPROVIDQUALIFIER STRING
# MAGIC ,PRESCRIBING_PROVID					         STRING
# MAGIC ,PRESCRIBING_SAK_PROV_ID			       DECIMAL (18,0)
# MAGIC ,REFERRING_EXTERNALPROVID			       STRING
# MAGIC ,REFERRING_EXTERNALPROVIDQUALIFIER	 STRING
# MAGIC ,REFERRING_PROV_NAME				         STRING
# MAGIC ,REFERRING_PROVID					           STRING
# MAGIC ,REFERRING_SAK_PROV_ID				       DECIMAL (18,0)
# MAGIC ,RENDERING_EXTERNALPROVID			       STRING
# MAGIC ,RENDERING_EXTERNALPROVIDQUALIFIER	 STRING
# MAGIC ,RENDERING_PROV_NAME				         STRING
# MAGIC ,RENDERING_PROVID					           STRING
# MAGIC ,RENDERING_SAK_PROV_ID				       DECIMAL (18,0)
# MAGIC ,SERVICE_EXTERNALPROVIDID			       STRING
# MAGIC ,SERVICE_EXTERNALPROVIDQUALIFIER	   STRING
# MAGIC ,SERVICE_PROV_NAME					         STRING
# MAGIC ,SERVICE_PROVID						           STRING
# MAGIC ,SERVICE_SAK_PROV					           DECIMAL (18,0)
# MAGIC ,SERVICE_MMIS_PROV_TYP				       STRING
# MAGIC ,SERVICE_PROV_TYPE_NM				         STRING
# MAGIC ,REPORT_DTE							             TIMESTAMP	
# MAGIC ,REPORT_DTE_NBR						           DECIMAL (8,0)
# MAGIC ) 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load EDW VEN12501FA Staging Table
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
s3_location_final = f"{s3_location}{received_date}/*12501FA*" 
print(s3_location_final)
# Define the schema
schema = StructType([
 StructField("CLM_SVCDTL_PROV_EXT_SK"              ,DecimalType (18,0), True)
,StructField("ICN_NBR"                             ,StringType(), True)
,StructField("ATTENDING_EXTERNALPROVID"            ,StringType(), True)
,StructField("ATTENDING_EXTERNALPROVIDQUALIFIER"   ,StringType(), True)
,StructField("ATTENDING_PROV_NAME"                 ,StringType(), True)
,StructField("ATTENDING_PROVID"                    ,StringType(), True)
,StructField("ATTENDING_SAK_PROV_ID"               ,DecimalType (18,0), True)
,StructField("BILLING_EXTERNALPROVID"              ,StringType(), True)
,StructField("BILLING_EXTERNALPROVIDQUALIFIER"     ,StringType(), True)
,StructField("BILLING_MEDICAID_ID"                 ,StringType(), True)
,StructField("BILLING_PROV_NAME"                   ,StringType(), True)
,StructField("BILLING_PROVID"                      ,StringType(), True)
,StructField("BILLING_SAK_PROV_ID"                 ,DecimalType (18,0), True)
,StructField("BILLING_MMIS_PROV_TYP"               ,StringType(), True)
,StructField("BILLING_PROV_TYPE_NM"                ,StringType(), True)
,StructField("OPERATING_EXTERNALPROVID"            ,StringType(), True)
,StructField("OPERATING_EXTERNALPROVIDQUALIFIER"   ,StringType(), True)
,StructField("OPERATING_PROV_NAME"                 ,StringType(), True)
,StructField("OPERATING_PROVID"                    ,StringType(), True)
,StructField("OPERATING_SAK_PROV_ID"               ,DecimalType (18,0), True)
,StructField("ORDERING_EXTERNALPROVID"             ,StringType(), True)
,StructField("ORDERING_EXTERNALPROVIDQUALIFIER"    ,StringType(), True)
,StructField("ORDERING_PROV_NAME"                  ,StringType(), True)
,StructField("ORDERING_PROVID"                     ,StringType(), True)
,StructField("ORDERING_SAK_PROV_ID"                ,DecimalType (18,0), True)
,StructField("PRESCRIBING_PROV_NAME"               ,StringType(), True)
,StructField("PRESCRIBING_EXTERNALPROVID"          ,StringType(), True)
,StructField("PRESCRIBING_EXTERNALPROVIDQUALIFIER" ,StringType(), True)
,StructField("PRESCRIBING_PROVID"                  ,StringType(), True)
,StructField("PRESCRIBING_SAK_PROV_ID"             ,DecimalType (18,0), True)
,StructField("REFERRING_EXTERNALPROVID"            ,StringType(), True)
,StructField("REFERRING_EXTERNALPROVIDQUALIFIER"   ,StringType(), True)
,StructField("REFERRING_PROV_NAME"                 ,StringType(), True)
,StructField("REFERRING_PROVID"                    ,StringType(), True)
,StructField("REFERRING_SAK_PROV_ID"               ,DecimalType (18,0), True)
,StructField("RENDERING_EXTERNALPROVID"            ,StringType(), True)
,StructField("RENDERING_EXTERNALPROVIDQUALIFIER"   ,StringType(), True)
,StructField("RENDERING_PROV_NAME"                 ,StringType(), True)
,StructField("RENDERING_PROVID"                    ,StringType(), True)
,StructField("RENDERING_SAK_PROV_ID"               ,DecimalType (18,0), True)
,StructField("SERVICE_EXTERNALPROVIDID"            ,StringType(), True)
,StructField("SERVICE_EXTERNALPROVIDQUALIFIER"     ,StringType(), True)
,StructField("SERVICE_PROV_NAME"                   ,StringType(), True)
,StructField("SERVICE_PROVID"                      ,StringType(), True)
,StructField("SERVICE_SAK_PROV"                    ,DecimalType (18,0), True)
,StructField("SERVICE_MMIS_PROV_TYP"               ,StringType(), True)
,StructField("SERVICE_PROV_TYPE_NM"                ,StringType(), True)
,StructField("REPORT_DTE"                          ,TimestampType(), True)
,StructField("REPORT_DTE_NBR"                      ,DecimalType (8,0), True)
])

table_name = f"{catalog}.{schema_name}.{VEN12501FA}"
# Read the CSV files into a DataFrame using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load(s3_location_final)

df.write.mode("overwrite").saveAsTable(table_name)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Load into Historic Stage

# COMMAND ----------

# DBTITLE 1,Load EDW VEN100FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN100FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN100FA} )

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10001FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN10001FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * from ${catalog}.${schema_name}.${VEN10001FA} )

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10002FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN10002FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * from ${catalog}.${schema_name}.${VEN10002FA} )

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10003FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN10003FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * from ${catalog}.${schema_name}.${VEN10003FA} )

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10004FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN10004FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * from ${catalog}.${schema_name}.${VEN10004FA} )

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10005FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN10005FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * from ${catalog}.${schema_name}.${VEN10005FA} )

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10006FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN10006FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * from ${catalog}.${schema_name}.${VEN10006FA} )

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10007FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN10007FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * from ${catalog}.${schema_name}.${VEN10007FA} )

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10008FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN10008FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * from ${catalog}.${schema_name}.${VEN10008FA} )
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Load EDW VEN10009FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN10009FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * from ${catalog}.${schema_name}.${VEN10009FA} )
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Load EDW VEN12301FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN12301FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * from ${catalog}.${schema_name}.${VEN12301FA} )

# COMMAND ----------

# DBTITLE 1,Load EDW VEN12401FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN12401FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * from ${catalog}.${schema_name}.${VEN12401FA} )

# COMMAND ----------

# DBTITLE 1,Load EDW VEN12403FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN12403FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * from ${catalog}.${schema_name}.${VEN12403FA} )

# COMMAND ----------

# DBTITLE 1,Load EDW VEN12404FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN12404FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * from ${catalog}.${schema_name}.${VEN12404FA} )

# COMMAND ----------

# DBTITLE 1,Load EDW VEN12501FA Historic Table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${VEN12501FA_hist} (
# MAGIC SELECT current_timestamp() AS EXTRACTION_DATE, * FROM ${catalog}.${schema_name}.${VEN12501FA} )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning Stage

# COMMAND ----------

# DBTITLE 1,Cleaning VEN100FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN100FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN10001FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN10001FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN10002FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN10002FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN10003FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN10003FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN10004FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN10004FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN10005FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN10005FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN10006FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN10006FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN10007FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN10007FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN10008FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN10008FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN10009FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN10009FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN12301FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN12301FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN12401FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN12401FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN12403FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN12403FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN12404FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN12404FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleaning VEN12501FA Historic Table
# MAGIC %sql
# MAGIC DELETE FROM ${catalog}.${schema_name}.${VEN12501FA_hist}
# MAGIC WHERE EXTRACTION_DATE < ADD_MONTHS(CURRENT_TIMESTAMP(),${Clng_Month_Gap})
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking Stage

# COMMAND ----------

# DBTITLE 1,Check Extracts' Paid Date
# MAGIC %sql
# MAGIC select distinct PD_DT AS PD_DT_All_Desc
# MAGIC from ${catalog}.${schema_name}.${VEN100FA}
# MAGIC order by 1 desc
# MAGIC limit 20
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct PD_DT AS PD_DT_All_Asc
# MAGIC from ${catalog}.${schema_name}.${VEN100FA}
# MAGIC order by 1
# MAGIC limit 20
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Check Extracts' DML Type
# MAGIC %sql
# MAGIC select distinct DML_TYPE, count(*)
# MAGIC from ${catalog}.${schema_name}.${VEN100FA}
# MAGIC group by 1
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct PD_DT AS PD_DT_Inserted_Desc
# MAGIC from ${catalog}.${schema_name}.${VEN100FA}
# MAGIC where DML_TYPE = 'I'
# MAGIC order by 1 Desc
# MAGIC limit 20
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct PD_DT AS PD_DT_Inserted_Asc
# MAGIC from ${catalog}.${schema_name}.${VEN100FA}
# MAGIC where DML_TYPE = 'I'
# MAGIC order by 1
# MAGIC limit 20
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct PD_DT AS PD_DT_Updated_Desc
# MAGIC from ${catalog}.${schema_name}.${VEN100FA}
# MAGIC where DML_TYPE = 'U'
# MAGIC order by 1 Desc
# MAGIC limit 20
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct PD_DT AS PD_DT_Updated_Asc
# MAGIC from ${catalog}.${schema_name}.${VEN100FA}
# MAGIC where DML_TYPE = 'U'
# MAGIC order by 1
# MAGIC limit 20
# MAGIC ;
