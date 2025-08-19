# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     ETL_Clms_Inst_Analytics.                                                                                         *
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
#* 03/27/2024 CCRB70930/CO#43342  Jaime Zavala        DTE_PAID1     Modified the CAST Date in the Load Phar_Analytics_Staging table.*
#* 03/28/2024 CCRB70930/CO#43342  Jaime Zavala        Several Fields, Modified to use SUBSTRING instead of CAST with varchar.       *
#* 05/29/2024 CCRB70930/CO#43342  Jaime Zavala        QTY_UNITS_ALWD Mapped to field added/VEN100FA.ALWD_QTY.                       *
#*                                                    AMT_PAID_MCO SrcChng FieldNm- VEN12403FA.ALWD_OTH_PYR_AMT to VEN100FA.        *
#*                                                    THE_PAID_AMT. Removed the by 100.                                             *
#*                                                    AMT_PAID_MCO_2 SrcChng FieldNm- VEN12403FA.ALWD_OTH_PYR_AMT to VEN100FA.      *
#*                                                    THE_PAID_AMT. Removed the by 100.                                             *
#*                                                    AMT_PAID Chng FieldNm- PD_AMT to THE_DETAIL_PAID_AMT.                         *
#*                                                    AMT_PAID_2 Chng FieldNm- PD_AMT to THE_DETAIL_PAID_AMT.Apply to I,L and O CT. *
#*                                                    CDE_ENC_TYPE modified to expect one character (N,Y,C,D,E)                     *
#* 06/03/2024 CCRB70930/CO#43342  Jaime Zavala        QTY_UNITS_ALWD_2 Mapped to field added/VEN100FA.ALWD_QTY.                     *
#*                                                    AMT_CO_PAY_2 Fixed issue to apply for I, L and O Claim types.                 *
#* 06/20/2024 CCRB70930/CO#43342  Jaime Zavala        IND_HDR_DTL FieldAdded.- VEN100FA.HDR_DTL_PAID_IND.                           *
#*                                                    Added CleanDstntTblsFlag Defaul-F, T when using EDW_temp_ in table's names    *
#*                                                    and VE are Full Refresh.                                                      *
#* 07/17/2024 CCRB70930/CO#43342  Jaime Zavala        CDE_REVENUE set to '0000' when value is 'XXXX'.                               *
#* 07/19/2024 CCRB70930/CO#43342  Jaime Zavala        Fixed issue with the CleanDstntTblsFlag.                                      *
#* 07/24/2024 CCRB70930/CO#43342  Jaime Zavala        Added logic to use CleanDstntTblsFlag for Delta VE source type.               *
#* 07/30/2024 CCRB70930/CO#43342  Jaime Zavala        Fixed issue with Sum Over Partition on Sak_Claim.                             *
#* 08/01/2024 CCRB70930/CO#43342  Jaime Zavala        CODE_CLM_TYPE applied the following Mapping:                                  *
#*                                                    'PART B INPATIENT'  THEN 'B'                                                  *
#*                                                    'PART C LTC'        THEN 'C'                                                  *
#*                                                    'PART A OUTPATIENT' THEN 'A'                                                  *
#*                                                    'PART B OUTPATIENT' THEN 'B'                                                  *
#*                                                    'PART B LTC'        THEN 'B'                                                  *
#*                                                    'PART C INPATIENT'  THEN 'C'                                                  *
#* 09/25/2024 CCRB70930/CO#43342  Jaime Zavala        NA83 Mapped to VEN100FA.THE_PAID_AMT.                                         *
#* 09/26/2024 CCRB70930/CO#43342  Jaime Zavala        Removed Round for AMT_ALWD_2,AMT_BILLED_2,AMT_NON_COVERED,AMT_PAID_MCO_2,     *
#*                                                    AMT_PAID_2, AMT_TPL_APPLD_2,AMT_DAY_OUTLIER,AMT_COST_OUTLIER,AMT_TPL_APPLD,   *
#*                                                    AMT_PAID_MCO.                                                                 *
#*                                                    Add a criteria for CDE_PROC_PRIM,CDE_MODIFIER_1,CDE_MODIFIER_2,CDE_MODIFIER_3,*
#*                                                    CDE_MODIFIER_4.                                                               *
#* 01/16/2024 CCRB70930/CO#43342  Jaime Zavala        CDE_FUND_CODE set to '#########' when Null, to met BIAR expected value.       *
#* 01/27/2024  CCRB70930/CO#43342  Jaime Zavala       DTL_STS_CD added "PAID" as expected value.                                    *
#* 01/28/2024  CCRB70930/CO#43342  Jaime Zavala       DTL_STS_CD added "DENIED", and  "REVERSED" as expected value.                 *
#* 03/03/2025  CCRB70930/CO#43342  Jaime Zavala        CDE_FUND_CODE set to '#########' when '' or '0000000'.                       *
#* 04/10/2025 CCRB70930/CO#43342  Jaime Zavala        NewExtract.- VEN12501FA- Multi Provider Claim Extract.                        *
#*                                                    FldChng NA4  TO ATTENDING_EXTERNALPROVID. Mapped to VEN12501 extract.         *
#*                                                    FldChng NA5  TO ATTENDING_EXTERNALPROVIDQUALIFIER. Mapped to VEN12501 extract.*
#*                                                    FldChng NA6  TO ATTENDING_PROV_NAME. Mapped to VEN12501 extract.              *
#*                                                    FldChng NA7  TO ATTENDING_PROVID. Mapped to VEN12501 extract.                 *
#*                                                    FldChng NA8  TO ATTENDING_SAK_PROV_ID. Mapped to VEN12501 extract.            *
#*                                                    FldChng NA9  TO BILLING_EXTERNALPROVID. Mapped to VEN12501 extract.           *
#*                                                    FldChng NA10 TO BILLING_EXTERNALPROVIDQUALIFIER. Mapped to VEN12501 extract.  *
#*                                                    FldChng NA11 TO BILLING_MEDICAID_ID. Mapped to VEN12501 extract.              *
#*                                                    FldChng NA12 TO BILLING_PROV_NAME. Mapped to VEN12501 extract.                *
#*                                                    FldChng NA13 TO BILLING_PROVID. Mapped to VEN12501 extract.                   *
#*                                                    FldChng NA14 TO BILLING_SAK_PROV_ID. Mapped to VEN12501 extract.              *
#*                                                    FldChng NA16 TO BILLING_MMIS_PROV_TYP. Mapped to VEN12501 extract.            *
#*                                                    FldChng NA17 TO BILLING_PROV_TYPE_NM. Mapped to VEN12501 extract.             *
#*                                                    FldChng NA18 TO OPERATING_EXTERNALPROVID. Mapped to VEN12501 extract.         *
#*                                                    FldChng NA20 TO OPERATING_EXTERNALPROVIDQUALIFIER. Mapped to VEN12501 extract.*
#*                                                    FldChng NA21 TO OPERATING_PROV_NAME. Mapped to VEN12501 extract.              *
#*                                                    FldChng NA22 TO OPERATING_PROVID. Mapped to VEN12501 extract.                 *
#*                                                    FldChng NA23 TO OPERATING_SAK_PROV_ID. Mapped to VEN12501 extract.            *
#*                                                    FldChng NA24 TO ORDERING_EXTERNALPROVID. Mapped to VEN12501 extract.          *
#*                                                    FldChng NA25 TO ORDERING_EXTERNALPROVIDQUALIFIER. Mapped to VEN12501 extract. *
#*                                                    FldChng NA26 TO ORDERING_PROV_NAME. Mapped to VEN12501 extract.               *
#*                                                    FldChng NA28 TO ORDERING_PROVID. Mapped to VEN12501 extract.                  *
#*                                                    FldChng NA30 TO ORDERING_SAK_PROV_ID. Mapped to VEN12501 extract.             *
#*                                                    FldChng NA31 TO PRESCRIBING_PROV_NAME. Mapped to VEN12501 extract.            *
#*                                                    FldChng NA33 TO PRESCRIBING_EXTERNALPROVID. Mapped to VEN12501 extract.       *
#*                                                    FldChng NA35 TO PRESCRIBING_EXTERNALPROVIDQUALIFIER. Mapped to VEN12501.      *
#*                                                    FldChng NA36 TO PRESCRIBING_PROVID. Mapped to VEN12501 extract.               *
#*                                                    FldChng NA37 TO PRESCRIBING_SAK_PROV_ID. Mapped to VEN12501 extract.          *
#*                                                    FldChng NA38 TO REFERRING_EXTERNALPROVID. Mapped to VEN12501 extract.         *
#*                                                    FldChng NA39 TO REFERRING_EXTERNALPROVIDQUALIFIER. Mapped to VEN12501 extract.*
#*                                                    FldChng NA40 TO REFERRING_PROV_NAME. Mapped to VEN12501 extract.              *
#*                                                    FldChng NA41 TO REFERRING_PROVID. Mapped to VEN12501 extract.                 *
#*                                                    FldChng NA42 TO REFERRING_SAK_PROV_ID. Mapped to VEN12501 extract.            *
#*                                                    FldChng NA43 TO RENDERING_EXTERNALPROVID. Mapped to VEN12501 extract.         *
#*                                                    FldChng NA44 TO RENDERING_EXTERNALPROVIDQUALIFIER. Mapped to VEN12501 extract.*
#*                                                    FldChng NA45 TO RENDERING_PROV_NAME. Mapped to VEN12501 extract.              *
#*                                                    FldChng NA46 TO RENDERING_PROVID. Mapped to VEN12501 extract.                 *
#*                                                    FldChng NA47 TO RENDERING_SAK_PROV_ID. Mapped to VEN12501 extract.            *
#*                                                    FldChng NA48 TO SERVICE_EXTERNALPROVIDID. Mapped to VEN12501 extract.         *
#*                                                    FldChng NA49 TO SERVICE_EXTERNALPROVIDQUALIFIER. Mapped to VEN12501 extract.  *
#*                                                    FldChng NA50 TO SERVICE_PROV_NAME. Mapped to VEN12501 extract.                *
#*                                                    FldChng NA51 TO SERVICE_PROVID. Mapped to VEN12501 extract.                   *
#*                                                    FldChng NA52 TO SERVICE_SAK_PROV. Mapped to VEN12501 extract.                 *
#*                                                    FldChng NA53 TO SERVICE_MMIS_PROV_TYP. Mapped to VEN12501 extract.            *
#*                                                    FldChng NA54 TO SERVICE_PROV_TYPE_NM. Mapped to VEN12501 extract.             *
#*                                                    FldChng NA78 TO IS_NON_DUPLICATE_IND. Mapped to VEN100FA.                     *
#*                                                    FldChng NA79 TO CLAIM_ACTIVE_IND. Mapped to VEN100FA.                         *
#*                                                    FldChng NA80 TO LAST_CLAIM_IND. Mapped to VEN100FA.                           *
#*                                                    FldChng NA81 TO IS_DKP_IND. Mapped to VEN100FA.                               *
#* 08/19/2025 CCRB70930/CO#43342  Jaime Zavala        CODE_CLM_TYPE applied the following Mapping:                                  *
#*                                                    'PART A INSTITUTIONAL' THEN 'A'                                               *
#*                                                    'PART B INSTITUTIONAL' THEN 'B'                                               *
#*                                                    'PART C INSTITUTIONAL' THEN 'C'                                               *
#************************************************************************************************************************************
#

# COMMAND ----------

# DBTITLE 1,Parameters
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('schema_name_mc', 'managed_care')
dbutils.widgets.dropdown('CleanDstntTblsFlag', 'F',{'F','T'})
  
#-------------------
# EDW Staging Tables
#-------------------
# Claims Extracts  
dbutils.widgets.text('VEN100FA', 'EDW_VEN100FA_Staging')
dbutils.widgets.text('VEN10002FA', 'EDW_VEN10002FA_Staging')
dbutils.widgets.text('VEN10003FA', 'EDW_VEN10003FA_Staging')
dbutils.widgets.text('VEN10005FA', 'EDW_VEN10005FA_Staging')
dbutils.widgets.text('VEN10006FA', 'EDW_VEN10006FA_Staging')
dbutils.widgets.text('VEN10007FA', 'EDW_VEN10007FA_Staging')
dbutils.widgets.text('VEN12401FA', 'EDW_VEN12401FA_Staging')
dbutils.widgets.text('VEN12403FA', 'EDW_VEN12403FA_Staging')
dbutils.widgets.text('VEN12404FA', 'EDW_VEN12404FA_Staging')
dbutils.widgets.text('VEN12501FA', 'EDW_VEN12501FA_Staging')

# Provider Extracts
dbutils.widgets.text('VEN117FA1', 'EDW_VEN117FA1_Staging')
dbutils.widgets.text('VEN117FA4', 'EDW_VEN117FA4_Staging')
dbutils.widgets.text('VEN117FA11', 'EDW_VEN117FA11_Staging')

#-------------------
# Manage Care Tables
#-------------------
dbutils.widgets.text('hipaa_820_raw', 'hipaa_820_raw')

#-----------------
# Analytics Tables
#-----------------
dbutils.widgets.text('cl_inst_stg', 'EDW_temp_cl_inst_staging')
dbutils.widgets.text('cl_inst', 'EDW_temp_cl_inst')
dbutils.widgets.text('Inst_Analytics_stg', 'EDW_temp_Inst_Analytics_staging')
dbutils.widgets.text('Inst_Analytics', 'EDW_temp_Inst_Analytics')


# COMMAND ----------

# DBTITLE 1,Get Parameters Values
#-------------------
# EDW Staging Tables
#-------------------
# Claims Extracts
VEN100FA = dbutils.widgets.get('VEN100FA')
VEN10002FA = dbutils.widgets.get('VEN10002FA')
VEN10003FA = dbutils.widgets.get('VEN10003FA')
VEN10005FA = dbutils.widgets.get('VEN10005FA')
VEN10006FA = dbutils.widgets.get('VEN10006FA')
VEN10007FA = dbutils.widgets.get('VEN10007FA')
VEN12401FA = dbutils.widgets.get('VEN12401FA')
VEN12403FA = dbutils.widgets.get('VEN12403FA')
VEN12404FA = dbutils.widgets.get('VEN12404FA')
VEN12501FA = dbutils.widgets.get('VEN12501FA')

# Provider Extracts  
VEN117FA1 = dbutils.widgets.get('VEN117FA1')
VEN117FA4 = dbutils.widgets.get('VEN117FA4')
VEN117FA11 = dbutils.widgets.get('VEN117FA11')

#-------------------
# Manage Care Tables
#-------------------
hipaa_820_raw = dbutils.widgets.get('hipaa_820_raw')

#-----------------
# Analytics Tables
#-----------------

cl_inst_stg = dbutils.widgets.get('cl_inst_stg')
cl_inst = dbutils.widgets.get('cl_inst')
Inst_Analytics_stg = dbutils.widgets.get('Inst_Analytics_stg')
Inst_Analytics = dbutils.widgets.get('Inst_Analytics')

#-----------
# DBX Parms
#-----------

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
schema_name_mc = dbutils.widgets.get('schema_name_mc')
CleanDstntTblsFlag = dbutils.widgets.get('CleanDstntTblsFlag')


# COMMAND ----------

# DBTITLE 1,Show Parms
print("catalog:", catalog)
print("schema:", schema_name)
print("schema_mc:", schema_name_mc)
print("Clean Destination's Tables Flag:", CleanDstntTblsFlag)

print()
print("EDW Staging Tables")
print("------------------")
print(VEN100FA)
print(VEN10002FA)
print(VEN10003FA)
print(VEN10005FA)
print(VEN10006FA)
print(VEN10007FA)
print(VEN12401FA)
print(VEN12403FA)
print(VEN12404FA)
print(VEN12501FA)

print()
print("Provider Extracts")
print("------------------")
print('VEN117FA1')
print('VEN117FA4')
print('VEN117FA11')

print()
print("Manage Care Tables")
print("------------------")
print('hipaa_820_raw')


print()
print("Analytics Tables")
print("---------------")
print(cl_inst_stg)
print(cl_inst)
print(Inst_Analytics_stg)
print(Inst_Analytics)


# COMMAND ----------

# DBTITLE 1,Truncate cl_inst_staging table
# MAGIC %sql
# MAGIC truncate table ${catalog}.${schema_name}.${cl_inst_stg}
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load cl_inst_staging table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${cl_inst_stg}
# MAGIC SELECT DISTINCT
# MAGIC     --CODE WITH COLUMNS MAPPING GOES HERE
# MAGIC      SUBSTRING(TRIM(clm.ICN_NBR),1,15)                                                          AS NUM_ICN
# MAGIC     ,SUBSTRING(TRIM(INDICATOR_CLAIM),1,1)                                                       AS IND_CLAIM
# MAGIC     ,SUBSTRING(TRIM(CODE_CLM_TYPE),1,3)                                                         AS CDE_CLM_TYPE
# MAGIC     ,CASE UPPER(TRIM(clm.HDR_STS_CD)) WHEN 'DENIED'   THEN 'D'                                  
# MAGIC                                       WHEN 'PAID'     THEN 'P'                                  
# MAGIC                                       WHEN 'REVERSED' THEN 'P'                                  
# MAGIC                                       ELSE NULL                                                 
# MAGIC                                       END                                                       AS CDE_HDR_STATUS
# MAGIC     ,NULL                                                                                       AS CDE_PGM_HEALTH
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.AID_CTG_CD),1,7), '')                                          AS CDE_AID_CATEGORY
# MAGIC     ,NULL                                                                                       AS ID_CLERK                     --Removed Field
# MAGIC     ,NULL                                                                                       AS CDE_PRESCRIPTION_ORIG        --INST NOT USE
# MAGIC     ,'C'                                                                                        AS DERIVED
# MAGIC     ,UPPER(TRIM(clm.ENC_TYP_CD))                                                                AS CDE_ENC_TYPE
# MAGIC     ,NULL                                                                                       AS NA                           --EMPTY
# MAGIC     ,LPAD(TRIM(clm.MEDICAID_ID),12,'0')                                                         AS ID_MEDICAID
# MAGIC     ,CAST(clm.BIRTH_DT AS DATE)                                                                 AS DTE_BIRTH
# MAGIC     ,NULL                                                                                       AS IND_BRAND_MED_NEC            --INST NOT USE
# MAGIC     ,NULL                                                                                       AS AMT_VACC_INCENTIVE           --INST NOT USE
# MAGIC     ,NULL                                                                                       AS NUM_PRIOR_AUTH               --INST NOT USE
# MAGIC     ,NULL                                                                                       AS NA2                          --EMPTY
# MAGIC     ,0                                                                                          AS NA3                          --EMPTY
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmAdtlExtOthrPyr_CONTRACT_SUB_ID),1,5),'')                        AS ID_CONTRACT_SUB
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.HIC_SUB_NBR),1,12),'')                                         AS NUM_HIC_SUB
# MAGIC     ,NULL                                                                                       AS NUM_CMS_ICN
# MAGIC     ,TRIM(ATTENDING_EXTERNALPROVID)                                                             AS ATTENDING_EXTERNALPROVID
# MAGIC     ,TRIM(ATTENDING_EXTERNALPROVIDQUALIFIER)                                                    AS ATTENDING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(ATTENDING_PROV_NAME)                                                                  AS ATTENDING_PROV_NAME  
# MAGIC     ,TRIM(ATTENDING_PROVID)                                                                     AS ATTENDING_PROVID     
# MAGIC     ,TRIM(CAST(ATTENDING_SAK_PROV_ID AS STRING))                                                AS ATTENDING_SAK_PROV_ID
# MAGIC     ,TRIM(BILLING_EXTERNALPROVID)                                                               AS BILLING_EXTERNALPROVID
# MAGIC     ,TRIM(BILLING_EXTERNALPROVIDQUALIFIER)                                                      AS BILLING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(BILLING_MEDICAID_ID)                                                                  AS BILLING_MEDICAID_ID
# MAGIC     ,TRIM(BILLING_PROV_NAME)                                                                    AS BILLING_PROV_NAME  
# MAGIC     ,TRIM(BILLING_PROVID)                                                                       AS BILLING_PROVID     
# MAGIC     ,TRIM(CAST(BILLING_SAK_PROV_ID AS STRING))                                                  AS BILLING_SAK_PROV_ID 
# MAGIC     ,NULL                                                                                       AS NA15                         --EMPTY
# MAGIC     ,TRIM(BILLING_MMIS_PROV_TYP)                                                                AS BILLING_MMIS_PROV_TYP
# MAGIC     ,TRIM(BILLING_PROV_TYPE_NM)                                                                 AS BILLING_PROV_TYPE_NM
# MAGIC     ,TRIM(OPERATING_EXTERNALPROVID)                                                             AS OPERATING_EXTERNALPROVID
# MAGIC     ,NULL                                                                                       AS NA19                         --EMPTY
# MAGIC     ,TRIM(OPERATING_EXTERNALPROVIDQUALIFIER)                                                    AS OPERATING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmAdtlExtDRG_SOI_CD),1,1),'')                                     AS CDE_SOI
# MAGIC     ,NULL                                                                                       AS CDE_LEVEL_OF_CARE            --Removed Field 
# MAGIC     ,CASE TRIM(INDICATOR_CLAIM) WHEN 'F' THEN 61                                                
# MAGIC                                 WHEN 'E' THEN 71                                                
# MAGIC                                 ELSE NULL END                                                   AS DERIVED_2
# MAGIC     ,NULL                                                                                       AS CDE_MDC                      --Removed Field 
# MAGIC     ,TRIM(OPERATING_PROV_NAME)                                                                  AS OPERATING_PROV_NAME
# MAGIC     ,TRIM(OPERATING_PROVID)                                                                     AS OPERATING_PROVID
# MAGIC     ,COALESCE(LPAD(TRIM(clm.PATIENT_STS_CD),2,'0'),'')                                          AS CDE_PATIENT_STATUS           --Added Field
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.EMERGENCY_CD),1,2),'')                                         AS CDE_EMERGENCY                --Added Field
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.ADMIT_SRC_CD),1,1),'')                                         AS CDE_ADMIT_SOURCE             
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmAdtlExtDRG_ROM_CD),1,1),'')                                     AS CDE_ROM
# MAGIC     ,CASE CAST(clm.ADMISSION_DT AS DATE)                                                        
# MAGIC           WHEN CAST('1799-12-31' AS DATE) THEN CAST('0101-01-01' AS DATE)                       
# MAGIC           WHEN CAST('1800-01-01' AS DATE) THEN CAST('0101-01-01' AS DATE)                       
# MAGIC 	      ELSE COALESCE(CAST(clm.ADMISSION_DT AS DATE),CAST('0101-01-01' AS DATE))              
# MAGIC      END                                                                                        AS DTE_ADMISSION           --Added Field
# MAGIC     ,COALESCE(CAST(clm.ADMIT_HOUR_CD AS NUMERIC), 0)                                            AS CDE_ADMIT_HOUR               
# MAGIC     ,CASE TRIM(ClmAdtlExtDRG_DRG_CD)                                                            
# MAGIC           WHEN '' THEN '####'                                                                   
# MAGIC           ELSE COALESCE(LPAD(TRIM(ClmAdtlExtDRG_DRG_CD),4,'0'),'####') END                      AS CDE_DRG
# MAGIC     ,NULL                                                                                       AS AMT_BASE_DRG            --Removed Field/GWT It's Planning to change DEDUP Logic to eliminate this field.
# MAGIC     ,CASE CAST(clm.DISCHARGE_DT AS DATE)                                                        
# MAGIC           WHEN CAST('1799-12-31' AS DATE) THEN CAST('2299-12-31' AS DATE)                       
# MAGIC           WHEN CAST('1800-01-01' AS DATE) THEN CAST('2299-12-31' AS DATE)                       
# MAGIC           ELSE CAST(clm.DISCHARGE_DT AS DATE) END                                               AS DTE_DISCHARGE                --Added Field
# MAGIC     ,COALESCE(CAST(clm.TIME_DISCHARGE AS NUMERIC),0)                                            AS TIME_DISCHARGE
# MAGIC     ,NULL                                                                                       AS CDE_COMPOUND_DOSAGE          --INST NOT USE
# MAGIC     ,COALESCE(CAST(ClmAdtlExtDRG_DAY_OUTLIER_AMT AS NUMERIC),0)                                 AS AMT_DAY_OUTLIER
# MAGIC     ,COALESCE(CAST(ClmAdtlExtDRG_COST_OUTLIER_AMT AS NUMERIC),0)                                AS AMT_COST_OUTLIER
# MAGIC     ,NULL                                                                                       AS CDE_MED_REC_NUM              --Removed Field
# MAGIC     ,NULL                                                                                       AS CDE_PEER_GROUP               --Removed Field
# MAGIC     ,0                                                                                          AS AMT_COST_INGREDIENT          --INST NOT USE
# MAGIC     ,TRIM(CAST(OPERATING_SAK_PROV_ID AS STRING))                                                AS OPERATING_SAK_PROV_ID
# MAGIC     ,NULL                                                                                       AS CDE_SOI_DISC                 --Removed Field
# MAGIC     ,NULL                                                                                       AS CDE_ROM_DISC                 --Removed Field
# MAGIC     ,COALESCE(SUBSTRING(clm.PRIOR_AUTH_NBR,1,30),'0')                                           AS NUM_PA_REF
# MAGIC     ,NULL                                                                                       AS AMT_TPL_SUBM                 --Removed Field
# MAGIC     ,CASE WHEN IND_HDR_DTL='H' THEN COALESCE(CAST(clm.TPL_AMT AS NUMERIC),0)                    
# MAGIC 	                           ELSE 0 END                                                     AS AMT_TPL_APPLD
# MAGIC     ,CASE WHEN IND_HDR_DTL='H' THEN CAST(SumOverSakClm_THE_PAID_AMT AS NUMERIC(11,2))
# MAGIC 	                         ELSE 0 END                                                       AS AMT_PAID_MCO                 --Removed the by 100
# MAGIC     ,TRIM(ORDERING_EXTERNALPROVID)                                                              AS ORDERING_EXTERNALPROVID
# MAGIC     ,IND_HDR_DTL                                                                                AS IND_HDR_DTL                  --Change to take it from Extract/'D' all INST claims are Detail paid
# MAGIC     ,TRIM(ORDERING_EXTERNALPROVIDQUALIFIER)                                                     AS ORDERING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmCndtn_COND_CD1),1,2),'')                                        AS CDE_COND_1                   
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmCndtn_COND_CD2),1,2),'')                                        AS CDE_COND_2                   
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmCndtn_COND_CD3),1,2),'')                                        AS CDE_COND_3                   
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmCndtn_COND_CD4),1,2),'')                                        AS CDE_COND_4                   
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmCndtn_COND_CD5),1,2),'')                                        AS CDE_COND_5                   
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmCndtn_COND_CD6),1,2),'')                                        AS CDE_COND_6                   
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmCndtn_COND_CD7),1,2),'')                                        AS CDE_COND_7                   
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmCndtn_COND_CD8),1,2),'')                                        AS CDE_COND_8                   
# MAGIC     ,TRIM(ORDERING_PROV_NAME)                                                                   AS ORDERING_PROV_NAME
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.PAY_ARR_CD),1,2),'')                                           AS CDE_PAY_ARR
# MAGIC     ,NULL                                                                                       AS NA27                         --EMPTY
# MAGIC     ,NULL                                                                                       AS CDE_DRG_DISC
# MAGIC     ,NULL                                                                                       AS CDE_CLARIFICATION1           --INST NOT USE
# MAGIC     ,NULL                                                                                       AS CDE_CLARIFICATION2           --INST NOT USE
# MAGIC     ,NULL                                                                                       AS CDE_CLARIFICATION3           --INST NOT USE
# MAGIC     ,TRIM(ORDERING_PROVID)                                                                      AS ORDERING_PROVID
# MAGIC     ,NULL                                                                                       AS NA29                         --EMPTY
# MAGIC     ,COALESCE(CAST(clm.BILL_DT AS DATE),CAST('0101-01-01' AS DATE))                             AS DTE_BILLED
# MAGIC     ,NULL                                                                                       AS CDE_RECIP_COUNTY
# MAGIC     ,NULL                                                                                       AS AMT_REIMBURSED               --Removed Field
# MAGIC     ,CAST(clm.PD_DT AS DATE)                                                                    AS DTE_PAID
# MAGIC     ,NULL                                                                                       AS CDE_CLM_REGION               --Removed Field
# MAGIC     ,CAST(clm.HDR_FIRST_SVC_DT AS DATE)                                                         AS DTE_FIRST_SVC
# MAGIC     ,CAST(clm.HDR_LAST_SVC_DT AS DATE)                                                          AS DTE_LAST_SVC
# MAGIC     ,CAST(clm.ENTERED_SYS_DT AS DATE)                                                           AS BATCH_DATE                   --Added Field / same as DTE_ENTERED_SYS (added on EDW extraction)
# MAGIC     ,TRIM(CAST(ORDERING_SAK_PROV_ID AS STRING))                                                 AS ORDERING_SAK_PROV_ID
# MAGIC     ,TRIM(PRESCRIBING_PROV_NAME)                                                                AS PRESCRIBING_PROV_NAME
# MAGIC     ,COALESCE(CAST(clm.DTL_BILL_AMT AS NUMERIC),0)                                              AS AMT_BILLED                   --INST NOT USE
# MAGIC     ,CASE WHEN IND_HDR_DTL='H' THEN CASE WHEN SumOverSakClm_ALWD_QTY < 0                                                            
# MAGIC 	                                   THEN '-' || LPAD(CAST(SumOverSakClm_ALWD_QTY * -1 AS varchar(8)), 8, '0')
# MAGIC                                          ELSE LPAD( CAST(SumOverSakClm_ALWD_QTY AS varchar(9)) ,9,'0')
# MAGIC                                          END
# MAGIC 	                         ELSE '000000000' END                                             AS QTY_UNITS_ALWD               --Added Field
# MAGIC     ,0                                                                                          AS NA32                         --EMPTY
# MAGIC     ,NULL                                                                                       AS NUM_RECIP_AGE                 --Removed Field
# MAGIC     ,TRIM(PRESCRIBING_EXTERNALPROVID)                                                           AS PRESCRIBING_EXTERNALPROVID
# MAGIC     ,0                                                                                          AS NA34                         --EMPTY
# MAGIC     ,NULL                                                                                       AS AMT_INTEREST                 --Removed Field     
# MAGIC     ,CASE CAST(clm.MCO_ADJUD_DT AS DATE)                                                        
# MAGIC           WHEN CAST('1799-12-31' AS DATE) THEN NULL                                             
# MAGIC 	      ELSE CAST(clm.MCO_ADJUD_DT AS DATE) END                                             AS DTE_MCO_ADJUD               --Added Field
# MAGIC     ,NULL                                                                                       AS ADR_ZIP_CODE
# MAGIC     ,NULL                                                                                       AS ADR_ZIP_CODE_4
# MAGIC     ,TRIM(PRESCRIBING_EXTERNALPROVIDQUALIFIER)                                                  AS PRESCRIBING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(PRESCRIBING_PROVID)                                                                   AS PRESCRIBING_PROVID     
# MAGIC     ,TRIM(CAST(PRESCRIBING_SAK_PROV_ID AS STRING))                                              AS PRESCRIBING_SAK_PROV_ID
# MAGIC     ,TRIM(REFERRING_EXTERNALPROVID)                                                             AS REFERRING_EXTERNALPROVID
# MAGIC     ,TRIM(REFERRING_EXTERNALPROVIDQUALIFIER)                                                    AS REFERRING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,COALESCE(SUBSTRING(clm.SEX_CD,1,1),'')                                                     AS CDE_SEX                      --CRG required Field.
# MAGIC     ,NULL                                                                                       AS CDE_RACE
# MAGIC     ,TRIM(REFERRING_PROV_NAME)                                                                  AS REFERRING_PROV_NAME  
# MAGIC     ,TRIM(REFERRING_PROVID)                                                                     AS REFERRING_PROVID  
# MAGIC     ,TRIM(CAST(REFERRING_SAK_PROV_ID AS STRING))                                                AS REFERRING_SAK_PROV_ID
# MAGIC     ,NULL                                                                                       AS NUM_WEIGHT                   --INST NOT USE
# MAGIC     ,NULL                                                                                       AS NUM_PAT_ACCT                 --Removed Field
# MAGIC     ,TRIM(RENDERING_EXTERNALPROVID)                                                             AS RENDERING_EXTERNALPROVID
# MAGIC     ,TRIM(RENDERING_EXTERNALPROVIDQUALIFIER)                                                    AS RENDERING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,0                                                                                          AS AMT_SPENDDOWN                --INST NOT USE
# MAGIC     ,TRIM(RENDERING_PROV_NAME)                                                                  AS RENDERING_PROV_NAME
# MAGIC     ,TRIM(RENDERING_PROVID)                                                                     AS RENDERING_PROVID
# MAGIC     ,COALESCE(CAST(clm.COINSR_AMT AS varchar(10)),'0')                                          AS AMT_COINSURANCE 
# MAGIC     ,NULL                                                                                       AS CDE_LIV_ARNG                 --Removed Field
# MAGIC     ,TRIM(CAST(RENDERING_SAK_PROV_ID AS STRING))                                                AS RENDERING_SAK_PROV_ID
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.ADJ_ICN_NBR),1,20),NULL)                                       AS NUM_ADJ_ICN
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmAdtlExtDRG_DRG_VERS_NBR),1,4),'')                               AS NUM_VERSION_DRG
# MAGIC     ,TRIM(SERVICE_EXTERNALPROVIDID)                                                             AS SERVICE_EXTERNALPROVIDID
# MAGIC     ,NULL                                                                                       AS NUM_RA
# MAGIC     ,NULL                                                                                       AS ID_VENDOR
# MAGIC     ,TRIM(SERVICE_EXTERNALPROVIDQUALIFIER)                                                      AS SERVICE_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(SERVICE_PROV_NAME)                                                                    AS SERVICE_PROV_NAME
# MAGIC     ,NULL                                                                                       AS NUM_WARRANT
# MAGIC     ,TRIM(SERVICE_PROVID)                                                                       AS SERVICE_PROVID
# MAGIC     ,CAST(clm.ENTERED_SYS_DT AS DATE)                                                           AS DTE_ENTERED_SYS              --Added Field
# MAGIC     ,NULL                                                                                       AS AMT_PAT_LIAB                 --Removed Field
# MAGIC     ,NULL                                                                                       AS AMT_APL_PAT_LIAB             --INST NOT USE
# MAGIC     ,CAST(clm.REPORT_DTE AS DATE)                                                               AS DERIVED_3
# MAGIC     ,NULL                                                                                       AS DTE_GENERIC
# MAGIC     ,TRIM(CAST(SERVICE_SAK_PROV AS STRING))                                                     AS SERVICE_SAK_PROV
# MAGIC     ,COALESCE(CAST(clm.PD_MCARE_AMT AS NUMERIC),0)                                              AS AMT_PAID_MCARE 
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmOccrnc_OCCUR_CD1),1,2),'')                                      AS CDE_OCCUR_1
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmOccrnc_OCCUR_CD2),1,2),'')                                      AS CDE_OCCUR_2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmOccrnc_OCCUR_CD3),1,2),'')                                      AS CDE_OCCUR_3
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmOccrnc_OCCUR_CD4),1,2),'')                                      AS CDE_OCCUR_4
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmOccrnc_OCCUR_CD5),1,2),'')                                      AS CDE_OCCUR_5
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmOccrnc_OCCUR_CD6),1,2),'')                                      AS CDE_OCCUR_6
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmOccrnc_OCCUR_CD7),1,2),'')                                      AS CDE_OCCUR_7
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmOccrnc_OCCUR_CD8),1,2),'')                                      AS CDE_OCCUR_8
# MAGIC     ,TRIM(SERVICE_MMIS_PROV_TYP)                                                                AS SERVICE_MMIS_PROV_TYP
# MAGIC     ,CAST(ClmOccrnc_OCCUR_DT1 AS DATE)                                                          AS DTE_OCCUR_1
# MAGIC     ,CAST(ClmOccrnc_OCCUR_DT2 AS DATE)                                                          AS DTE_OCCUR_2
# MAGIC     ,CAST(ClmOccrnc_OCCUR_DT3 AS DATE)                                                          AS DTE_OCCUR_3
# MAGIC     ,CAST(ClmOccrnc_OCCUR_DT4 AS DATE)                                                          AS DTE_OCCUR_4
# MAGIC     ,CAST(ClmOccrnc_OCCUR_DT5 AS DATE)                                                          AS DTE_OCCUR_5
# MAGIC     ,CAST(ClmOccrnc_OCCUR_DT6 AS DATE)                                                          AS DTE_OCCUR_6
# MAGIC     ,CAST(ClmOccrnc_OCCUR_DT7 AS DATE)                                                          AS DTE_OCCUR_7
# MAGIC     ,CAST(ClmOccrnc_OCCUR_DT8 AS DATE)                                                          AS DTE_OCCUR_8
# MAGIC     ,NULL                                                                                       AS CDE_EPSDT_FP                 --INST NOT USE
# MAGIC     ,NULL                                                                                       AS IND_HYST                     --INST NOT USE
# MAGIC     ,TRIM(SERVICE_PROV_TYPE_NM)                                                                 AS SERVICE_PROV_TYPE_NM
# MAGIC     ,NULL                                                                                       AS IND_STERILIZATION            --INST NOT USE
# MAGIC     ,NULL                                                                                       AS NA55                         --EMPTY
# MAGIC     ,NULL                                                                                       AS IND_ABORTION                 --INST NOT USE
# MAGIC     ,NULL                                                                                       AS NA56                         --EMPTY
# MAGIC     ,NULL                                                                                       AS NA57                         --EMPTY
# MAGIC     ,NULL                                                                                       AS NA58                         --EMPTY
# MAGIC     ,NULL                                                                                       AS NA59                         --EMPTY
# MAGIC     ,COALESCE(CAST(clm.DEDUCT_AMT AS NUMERIC),0)                                                AS AMT_DEDUCT 
# MAGIC     ,0                                                                                          AS AMT_MCARE_PAID               --INST NOT USE
# MAGIC     ,NULL                                                                                       AS NA60                         --EMPTY
# MAGIC     ,NULL                                                                                       AS NA61                         --EMPTY
# MAGIC     ,COALESCE(SUBSTRING(TRIM(MedClmNDCDtl_RX_ID_NBR),1,12),'')                                  AS NUM_PRESCRIPTION_ID          --PULLED FROM 10005FA supplemental extraction
# MAGIC     ,NULL                                                                                       AS DTE_PRESCRIB                 --INST NOT USE
# MAGIC     ,NULL                                                                                       AS NA62                         --EMPTY
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.TCN_NBR),1,18),'')                                             AS NUM_TCN                      --Added Field
# MAGIC     ,CASE WHEN TRIM(INDICATOR_CLAIM) = 'F' AND (TRIM(CODE_CLM_TYPE)='A' or                      
# MAGIC                                                 TRIM(CODE_CLM_TYPE)='C' or                      
# MAGIC                                                 TRIM(CODE_CLM_TYPE)='I')                        
# MAGIC           THEN COALESCE(CAST(HdrAlwdAmtClm_HDR_ALWD_AMT AS NUMERIC),0)                          
# MAGIC           ELSE 0 END                                                                            AS AMT_ALWD
# MAGIC     ,NULL                                                                                       AS NA63                         --EMPTY
# MAGIC     ,NULL                                                                                       AS NA64                         --EMPTY
# MAGIC     ,0                                                                                          AS AMT_NDC_PROFEE               --INST NOT USE
# MAGIC     ,NULL                                                                                       AS NA65                         --EMPTY
# MAGIC     ,0                                                                                          AS NA66                         --EMPTY
# MAGIC     ,NULL                                                                                       AS NA67                         --EMPTY
# MAGIC     ,NULL                                                                                       AS NA68                         --EMPTY
# MAGIC     ,CAST('0101-01-01' AS DATE)                                                                 AS NA69                         --EMPTY
# MAGIC     ,COALESCE(CAST(clm.DTL_CO_PAY_AMT AS NUMERIC),0)                                            AS AMT_CO_PAY                   --INST NOT USE
# MAGIC     ,CASE WHEN IND_HDR_DTL='H' THEN SumOverSakClm_THE_DETAIL_PAID_AMT
# MAGIC 	                         ELSE 0 END                                                       AS AMT_PAID
# MAGIC     ,NULL                                                                                       AS NA70                         --EMPTY
# MAGIC     ,0                                                                                          AS NA71                         --EMPTY
# MAGIC     ,'0'                                                                                        AS NA72                         --EMPTY
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.COS_ST_CD),1,2),'')                                           AS CDE_COS_ST                   --Added Field
# MAGIC     ,NULL                                                                                       AS CDE_COS_SUB
# MAGIC     ,NULL                                                                                       AS NA73                         --EMPTY
# MAGIC     ,NULL                                                                                       AS NA74                         --EMPTY
# MAGIC     ,NULL                                                                                       AS ID_VOUCHER_RELATED
# MAGIC     ,NULL                                                                                       AS NA75                         --EMPTY
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.BILL_CD_TYP), 2, 1),'')                                        AS CDE_TYPE_OF_BILL             --Added Field
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.BILL_CD_TYP), 4, 1),'')                                        AS CDE_TYPE_OF_BILL_2           --Added Field
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.BILL_CD_TYP), 3, 1),'')                                        AS CDE_TYPE_OF_BILL_3           --Added Field
# MAGIC     ,NULL                                                                                       AS QTY_REFILL                   --INST NOT USE
# MAGIC     ,NULL                                                                                       AS NA76                         --EMPTY
# MAGIC     ,CAST('0101-01-01' AS DATE)                                                                 AS DTE_DISPENSE                 --INST NOT USE
# MAGIC     ,0                                                                                          AS QTY_DISPENSE                 --INST NOT USE
# MAGIC     ,0                                                                                          AS NUM_DAY_SUPPLY               --INST NOT USE
# MAGIC     ,CASE UPPER(TRIM(clm.DTL_STS_CD)) WHEN 'DENY' THEN 'D'                                      
# MAGIC                                       WHEN 'DENIED' THEN 'D'                                      
# MAGIC                                       WHEN 'OKAY' THEN 'P'                                      
# MAGIC                                       WHEN 'PAID' THEN 'P'                                      
# MAGIC                                       WHEN 'PEND' THEN 'S'                                      
# MAGIC                                       WHEN 'VOID' THEN 'V'                                      
# MAGIC                                       WHEN 'WARN' THEN 'W'                                      
# MAGIC                                       WHEN 'REVERSED' THEN 'R'                                      
# MAGIC                                       ELSE NULL                                                 
# MAGIC                                       END                                                       AS CDE_DTL_STATUS
# MAGIC     ,NULL                                                                                       AS CDE_PAY_ARR_2
# MAGIC     ,CASE WHEN clm.ALWD_QTY < 0                                                            
# MAGIC 	    THEN '-' || LPAD(CAST(COALESCE(clm.ALWD_QTY, 0) * -1 AS varchar(8)), 8, '0')          
# MAGIC           ELSE LPAD( CAST(COALESCE(clm.ALWD_QTY,0) AS varchar(9)) ,9,'0')               
# MAGIC           END                                                                                   AS QTY_UNITS_ALWD_2             --Added Field
# MAGIC     ,0                                                                                          AS QTY_DISPENSE_2                --INST NOT USE
# MAGIC     ,CAST(clm.REPORT_DTE AS DATE)                                                               AS DERIVED_4
# MAGIC     ,NULL                                                                                       AS NA77                         --EMPTY
# MAGIC     ,0                                                                                          AS AMT_AWP                      --INST NOT USE
# MAGIC     ,NULL                                                                                       AS CDE_MCAR_COVRG               --Removed Field
# MAGIC     ,NULL                                                                                       AS IND_PHARMACY_FAMILY_PLAN     --INST NOT USE
# MAGIC     ,NULL                                                                                       AS IND_REBATE_ELIG              --Removed Field
# MAGIC     ,NULL                                                                                       AS CDE_DISP_STATUS
# MAGIC     ,TRIM(clm.IS_NON_DUPLICATE_IND)                                                             AS IS_NON_DUPLICATE_IND
# MAGIC     ,COALESCE(SUBSTRING(ClmAdtlExtEOB_CLAIM_ADTL_EOB_SK1,1,5),'')                               AS CDE_EOB_1
# MAGIC     ,COALESCE(SUBSTRING(ClmAdtlExtEOB_CLAIM_ADTL_EOB_SK2,1,5),'')                               AS CDE_EOB_2
# MAGIC     ,NULL                                                                                       AS IND_PRICING
# MAGIC     ,CASE WHEN TRIM(INDICATOR_CLAIM) = 'F'                                                      
# MAGIC           THEN (COALESCE(CAST(clm.DTL_ALWD_AMT AS NUMERIC),0)*100)                              
# MAGIC           ELSE 0 END                                                                            AS AMT_ALWD_2
# MAGIC     ,NULL                                                                                       AS IND_STERILIZATION_2
# MAGIC     ,TRIM(clm.CLAIM_ACTIVE_IND)                                                                 AS CLAIM_ACTIVE_IND
# MAGIC     ,NULL                                                                                       AS IND_HYST_2
# MAGIC     ,TRIM(clm.LAST_CLAIM_IND)                                                                   AS LAST_CLAIM_IND
# MAGIC     ,NULL                                                                                       AS IND_ABORTION_2
# MAGIC     ,TRIM(clm.IS_DKP_IND)                                                                       AS IS_DKP_IND
# MAGIC     ,COALESCE(CAST(clm.DAYS_CVRD AS NUMERIC),0)                                                 AS NUM_DAYS_COVD                --INST NOT USE/Added Field
# MAGIC     ,COALESCE(CAST(clm.DAYS_NCOVD AS NUMERIC),0)                                                AS NUM_DAYS_NCOVD               --INST NOT USE/Added Field
# MAGIC     ,NULL                                                                                       AS NUM_LEAVE_DAYS               --INST NOT USE/Removed Field
# MAGIC     ,COALESCE(CAST(ClmCostDebtAnlys_ING_COST AS NUMERIC),0)                                     AS AMT_DRUG_UNIT_PRICE
# MAGIC     ,CASE WHEN (TRIM(CODE_CLM_TYPE)='I' or                                                       
# MAGIC                 TRIM(CODE_CLM_TYPE)='L' or                                                       
# MAGIC                 TRIM(CODE_CLM_TYPE)='O')                                                         
# MAGIC           THEN (COALESCE(CAST(clm.DTL_CO_PAY_AMT AS NUMERIC),0) *100)                           
# MAGIC           ELSE 0 END                                                                            AS AMT_CO_PAY_2                 --Fixed Issue#39
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.COPAY_REASON_CD),1,4),'')                                      AS CDE_COPAY_REASON
# MAGIC     ,COALESCE(CAST(clm.DTL_NBR AS NUMERIC),0)                                                   AS NUM_DTL
# MAGIC     ,CAST(clm.DTL_FIRST_SVC_DT AS DATE)                                                         AS DTE_FIRST_SVC_2
# MAGIC     ,CAST(clm.DTL_LAST_SVC_DT AS DATE)                                                          AS DTE_LAST_SVC_2
# MAGIC     ,CASE WHEN clm.UNT_BILL_QTY < 0                                                            
# MAGIC 	      THEN '-' || LPAD(CAST(COALESCE(clm.UNT_BILL_QTY, 0) * -100 AS varchar(8)), 8, '0')          
# MAGIC           ELSE LPAD( CAST((COALESCE(clm.UNT_BILL_QTY,0) * 100) AS varchar(9)) ,9,'0')               
# MAGIC           END                                                                                   AS QTY_UNITS_BILLED             -- UNT_BILL_QTY's Logic for negative values e.g. Reversed Hdr Status Code or negative qty.
# MAGIC     ,CASE WHEN TRIM(clm.REVENUE_CD) = 'XXXX' THEN '0000'
# MAGIC           ELSE COALESCE(LPAD(TRIM(clm.REVENUE_CD),4,'0'),'') END                                AS CDE_REVENUE                  --INST NOT USE/Added Field
# MAGIC     ,CAST((COALESCE(clm.DTL_BILL_AMT,0) * 100) AS NUMERIC)                                      AS AMT_BILLED_2
# MAGIC     ,CAST((COALESCE(clm.NON_CVRD_AMT,0) * 100) AS NUMERIC)                                      AS AMT_NON_COVERED              --INST NOT USE
# MAGIC     ,CAST((COALESCE(clm.THE_PAID_AMT,0) ) AS NUMERIC(11,2))                                     AS AMT_PAID_MCO_2               --Removed the by 100
# MAGIC     ,0                                                                                          AS NA82                         --EMPTY
# MAGIC     ,CASE WHEN (TRIM(CODE_CLM_TYPE)='I' or                                                       
# MAGIC                 TRIM(CODE_CLM_TYPE)='L' or                                                       
# MAGIC                 TRIM(CODE_CLM_TYPE)='O')                                                         
# MAGIC           THEN CAST((clm.THE_DETAIL_PAID_AMT *100) AS INT)                                                   
# MAGIC           ELSE 0 END                                                                            AS AMT_PAID_2                   --Fixed Issue#37
# MAGIC     ,CAST(clm.PD_DT AS DATE)                                                                    AS DTE_PAID_2
# MAGIC     ,NULL                                                                                       AS AMT_PAT_LIAB_2               --Removed Field
# MAGIC     ,CAST((COALESCE(clm.TPL_AMT,0)*100) AS NUMERIC)                                             AS AMT_TPL_APPLD_2
# MAGIC     ,CAST(clm.THE_PAID_AMT AS STRING)                                                           AS NA83                         --New Mapping Sep/2024
# MAGIC     ,NULL                                                                                       AS NA84                         --EMPTY
# MAGIC     ,0                                                                                          AS AMT_APL_PAT_LIAB_2
# MAGIC     ,NULL                                                                                       AS NA85                         --EMPTY
# MAGIC     ,NULL                                                                                       AS AMT_TPL_SUBM_2               --Removed Field
# MAGIC     ,NULL                                                                                       AS CDE_TOOTH_NBR                --INST NOT USE
# MAGIC     ,NULL                                                                                       AS CDE_TOOTH_SURFACE_1          --INST NOT USE
# MAGIC     ,NULL                                                                                       AS CDE_TOOTH_SURFACE_2          --INST NOT USE
# MAGIC     ,NULL                                                                                       AS CDE_TOOTH_SURFACE_3          --INST NOT USE
# MAGIC     ,NULL                                                                                       AS CDE_TOOTH_SURFACE_4          --INST NOT USE
# MAGIC     ,NULL                                                                                       AS CDE_TOOTH_SURFACE_5          --INST NOT USE
# MAGIC     ,NULL                                                                                       AS CDE_TOOTH_SURFACE_6          --INST NOT USE
# MAGIC     ,NULL                                                                                       AS NA86                         --EMPTY
# MAGIC     ,CAST(DATE_FORMAT(CAST(ClmAdtlExtOthrPyr_MCO_ADJUD_DTL_DT AS DATE),'MM/dd/y') AS varchar(10))      AS DTE_MCO_ADJUD_2
# MAGIC     ,0                                                                                          AS AMT_SPENDDOWN_2
# MAGIC     ,NULL                                                                                       AS IND_EPSDT                    --Removed Field
# MAGIC     ,NULL                                                                                       AS NA87                         --EMPTY
# MAGIC     ,NULL                                                                                       AS NA88                         --EMPTY
# MAGIC     ,CASE UPPER(TRIM(CODE_CLM_TYPE)) WHEN 'A' THEN '21'                                         
# MAGIC                                      WHEN 'I' THEN '21'                                         
# MAGIC                                      WHEN 'C' THEN '22'                                         
# MAGIC                                      WHEN 'O' THEN '22'                                         
# MAGIC                                      WHEN 'L' THEN '31'                                         
# MAGIC                                      ELSE ''  END                                               AS CDE_POS
# MAGIC     ,NULL                                                                                       AS AMT_REIMBURSED_2             --Removed Field
# MAGIC     ,CAST(COALESCE(clm.PD_MCARE_AMT,0) AS NUMERIC)                                              AS AMT_PAID_MCARE_2
# MAGIC     ,CAST(COALESCE(clm.COINSR_AMT,0) AS NUMERIC)                                                AS AMT_COINSURANCE_2
# MAGIC     ,NULL                                                                                       AS QTY_DAYS_COINSURANCE         --Removed Field
# MAGIC     ,COALESCE(SUBSTRING(TRIM(MedClmNDCDtl_NDC_CD),1,11),'')                                     AS CDE_NDC                      --PULLED FROM 10005FA supplemental extraction
# MAGIC     ,CAST(COALESCE(clm.DEDUCT_AMT,0) AS NUMERIC)                                                AS AMT_DEDUCT_2
# MAGIC     ,NULL                                                                                       AS CDE_THERA_CLS_AHFS           --INST NOT USE
# MAGIC     ,NULL                                                                                       AS CDE_THERA_CLS_SPEC           --INST NOT USE
# MAGIC     ,NULL                                                                                       AS NA89                         --EMPTY
# MAGIC     ,NULL                                                                                       AS NA90                         --EMPTY
# MAGIC     ,CASE WHEN clm.PROC_PRIM_CD='-1' THEN NULL                                                  
# MAGIC           WHEN TRIM(clm.PROC_PRIM_CD) = '' THEN '######'                                        
# MAGIC           ELSE COALESCE(SUBSTRING(TRIM(clm.PROC_PRIM_CD),1,6),'######')                        
# MAGIC           END                                                                                   AS CDE_PROC_PRIM
# MAGIC     ,CASE WHEN TRIM(clm.MODIFIER_1_CD) = '' THEN '##'                                
# MAGIC           ELSE COALESCE(SUBSTRING(TRIM(clm.MODIFIER_1_CD),1,2),'##') END                        AS CDE_MODIFIER_1
# MAGIC     ,CASE WHEN TRIM(clm.MODIFIER_2_CD) = '' THEN '##'                                
# MAGIC           ELSE COALESCE(SUBSTRING(TRIM(clm.MODIFIER_2_CD),1,2),'##') END                        AS CDE_MODIFIER_2
# MAGIC     ,CASE WHEN TRIM(clm.MODIFIER_3_CD) = '' THEN '##'                                
# MAGIC           ELSE COALESCE(SUBSTRING(TRIM(clm.MODIFIER_3_CD),1,2),'##') END                        AS CDE_MODIFIER_3
# MAGIC     ,CASE WHEN TRIM(clm.MODIFIER_4_CD) = '' THEN '##'                                
# MAGIC           ELSE COALESCE(SUBSTRING(TRIM(clm.MODIFIER_4_CD),1,2),'##') END                        AS CDE_MODIFIER_4
# MAGIC     ,0                                                                                          AS NA91                         --EMPTY
# MAGIC     ,CASE WHEN TRIM(clm.FUND_CD) = '' or TRIM(clm.FUND_CD) = '0000000' THEN '#########'
# MAGIC           ELSE COALESCE(SUBSTRING(clm.FUND_CD,1,9),'#########') END                             AS CDE_FUND_CODE                --Added Field
# MAGIC     ,NULL                                                                                       AS CDE_RATE_TYPE
# MAGIC     ,SUBSTRING(TRIM(clm.ICN_NBR),1,15)                                                          AS NUM_ICN_2
# MAGIC 	,CASE WHEN TRIM(clm.ADJ_ICN_NBR) = '' THEN NULL                                             
# MAGIC                ELSE	COALESCE(SUBSTRING(TRIM(clm.ADJ_ICN_NBR),1,20),NULL)                        
# MAGIC                END                                                                              AS NUM_ADJ_ICN_2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.TCN_NBR),1,18),'')                                             AS NUM_TCN_2                    --Added Field
# MAGIC     ,SUBSTRING(TRIM(clm.MEDICAID_ID),1,12)                                                      AS ID_MEDICAID_2                --INST NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(AttndProv_MEDICAID_ID),1,10),'')                                   AS ID_PROVIDER_MCAID            --INST NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(AttndProv_NPI),1,10),'')                                           AS ID_PROVIDER_NPI              --INST NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(AttndProv_MMIS_PROV_TYP_ID),1,2),'')                               AS ATTENDING_CDE_PROV_TYPE      --INST NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(AttndProv_SL_CNTY),1,10),'')                                       AS CDE_SVC_COUNTY               --INST NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(AttndProv_TXNMY_CD),1,10),'')                                      AS CDE_TAXONOMY                 --INST NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(AttndProv_PRIMARY_SPCLTY_CD),1,3),'')                              AS CDE_PROV_SPEC_PRIM           --INST NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_MEDICAID_ID),1,10),'')                                    AS ID_PROVIDER_MCAID_2 
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_NPI),1,10),'')                                            AS ID_PROVIDER_NPI_2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_MMIS_PROV_TYP_ID),1,2),'')                                AS BILLING_CDE_PROV_TYPE_PRIM
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_PROGRAM_ID),1,5),'')                                      AS CDE_PROV_PGM                 --INST NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_PT_CNTY),1,10),'')                                        AS CDE_SVC_COUNTY_2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_TXNMY_CD),1,10),'')                                       AS CDE_TAXONOMY_2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_PRIMARY_SPCLTY_CD),1,3),'')                               AS CDE_PROV_SPEC_PRIM_2
# MAGIC     ,NULL                                                                                       AS ID_PROVIDER_MCAID_3          --Fixed ISSUE#60
# MAGIC     ,NULL                                                                                       AS ID_PROVIDER_NPI_3            --Fixed ISSUE#60
# MAGIC     ,NULL                                                                                       AS CDE_PROV_TYPE_PRIM_BLANK_1   --INST NOT USE
# MAGIC     ,NULL                                                                                       AS CDE_PROV_PGM_2
# MAGIC     ,NULL                                                                                       AS CDE_SVC_COUNTY_3
# MAGIC     ,NULL                                                                                       AS CDE_TAXONOMY_3               --Fixed ISSUE#60
# MAGIC     ,NULL                                                                                       AS CDE_PROV_SPEC_PRIM_3
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RfrngProv_MEDICAID_ID),1,10),'')                                   AS ID_PROVIDER_MCAID_4
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RfrngProv_NPI),1,10),'')                                           AS ID_PROVIDER_NPI_4
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RfrngProv_MMIS_PROV_TYP_ID),1,2),'')                               AS REFERRING_CDE_PROV_TYPE_PRIM
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RfrngProv_PRIMARY_SPCLTY_CD),1,3),'')                              AS CDE_PROV_SPEC_PRIM_4    
# MAGIC     ,NULL                                                                                       AS ID_PROVIDER_MCAID_5          --INST NOT USE
# MAGIC     ,NULL                                                                                       AS ID_PROVIDER_NPI_5            --INST NOT USE
# MAGIC     ,NULL                                                                                       AS SURGICAL_CDE_PROV_TYPE_PRIM  --INST NOT USE
# MAGIC     ,NULL                                                                                       AS CDE_PROV_SPEC_PRIM_5         --INST NOT USE
# MAGIC     ,NULL                                                                                       AS ID_PROVIDER_MCAID_6          --INST NOT USE
# MAGIC     ,NULL                                                                                       AS ID_PROVIDER_NPI_6            --INST NOT USE
# MAGIC     ,NULL                                                                                       AS FACILTIY_CDE_PROV_TYPE_PRIM  --INST NOT USE
# MAGIC     ,NULL                                                                                       AS CDE_PROV_SPEC_PRIM_6         --INST NOT USE
# MAGIC     ,NULL                                                                                       AS ID_PROVIDER_MCAID_7          --INST NOT USE
# MAGIC     ,NULL                                                                                       AS ID_PROVIDER_NPI_7            --INST NOT USE
# MAGIC     ,NULL                                                                                       AS CDE_PROV_TYPE_PRIM_BLANK_2   --INST NOT USE
# MAGIC     ,NULL                                                                                       AS CDE_PROV_SPEC_PRIM_7         --INST NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(MCP_MEDICAID_ID),1,10),'')                                         AS ID_PROVIDER_MCAID_8          --New mapped field/ Fixed ISSUE#60
# MAGIC     ,COALESCE(SUBSTRING(TRIM(MCP_MMIS_PROV_TYP_ID),1,2),'')                                     AS MCP_CDE_PROV_TYPE_PRIM
# MAGIC     ,COALESCE(SUBSTRING(TRIM(MCP_PRIMARY_SPCLTY_CD),1,3),'')                                    AS CDE_PROV_SPEC_PRIM_8
# MAGIC
# MAGIC from
# MAGIC (
# MAGIC         select ClmMain.*
# MAGIC         ,CASE UPPER(TRIM(ClmMain.CLM_TYP_CD)) WHEN 'LTC'               THEN 'L'
# MAGIC                                               WHEN 'INPATIENT'         THEN 'I'
# MAGIC                                               WHEN 'PART B INPATIENT'  THEN 'B'
# MAGIC                                               WHEN 'PART C LTC'        THEN 'C'
# MAGIC                                               WHEN 'PART A OUTPATIENT' THEN 'A'
# MAGIC                                               WHEN 'INSTITUTIONAL'     THEN 'I'
# MAGIC                                               WHEN 'PART C OUTPATIENT' THEN 'C'
# MAGIC                                               WHEN 'PART A LTC'        THEN 'A'
# MAGIC                                               WHEN 'PART B OUTPATIENT' THEN 'B'
# MAGIC                                               WHEN 'PART B LTC'        THEN 'B'
# MAGIC                                               WHEN 'PART A INPATIENT'  THEN 'A'
# MAGIC                                               WHEN 'PART C INPATIENT'  THEN 'C'
# MAGIC                                               WHEN 'OUTPATIENT'        THEN 'O'
# MAGIC                                               WHEN 'PART A INSTITUTIONAL' THEN 'A'
# MAGIC                                               WHEN 'PART B INSTITUTIONAL' THEN 'B'
# MAGIC                                               WHEN 'PART C INSTITUTIONAL' THEN 'C'
# MAGIC                                               ELSE ''
# MAGIC                                               END  AS CODE_CLM_TYPE
# MAGIC         ,CASE UPPER(TRIM(ClmMain.CLAIM_IND)) WHEN 'N' THEN 'F'
# MAGIC                                              WHEN 'Y' THEN 'E'
# MAGIC                                              ELSE SUBSTRING(TRIM(ClmMain.CLAIM_IND),1,1)
# MAGIC                                              END  AS INDICATOR_CLAIM
# MAGIC         ,ClmMain.HDR_DTL_PAID_IND                                                                AS IND_HDR_DTL --NewFld from Extract
# MAGIC         ,HdrAlwdAmtClm_HDR_ALWD_AMT                                                      -- Fixed Issue#66
# MAGIC         ,BillProv_PROGRAM_ID
# MAGIC         ,BillProv_PT_CNTY
# MAGIC         ,AttndProv_SL_CNTY
# MAGIC         ,AttndProv_MEDICAID_ID
# MAGIC         ,AttndProv_MMIS_PROV_TYP_ID
# MAGIC         ,AttndProv_PRIMARY_SPCLTY_CD
# MAGIC         ,RfrngProv_MEDICAID_ID
# MAGIC         ,RfrngProv_MMIS_PROV_TYP_ID
# MAGIC         ,RfrngProv_PRIMARY_SPCLTY_CD
# MAGIC         ,BillProv_TXNMY_CD
# MAGIC         ,AttndProv_NPI
# MAGIC         ,AttndProv_TXNMY_CD
# MAGIC         ,RfrngProv_NPI
# MAGIC         ,ClmOccrnc_OCCUR_CD1
# MAGIC         ,ClmOccrnc_OCCUR_CD2
# MAGIC         ,ClmOccrnc_OCCUR_CD3
# MAGIC         ,ClmOccrnc_OCCUR_CD4
# MAGIC         ,ClmOccrnc_OCCUR_CD5
# MAGIC         ,ClmOccrnc_OCCUR_CD6
# MAGIC         ,ClmOccrnc_OCCUR_CD7
# MAGIC         ,ClmOccrnc_OCCUR_CD8
# MAGIC         ,ClmOccrnc_OCCUR_DT1
# MAGIC         ,ClmOccrnc_OCCUR_DT2
# MAGIC         ,ClmOccrnc_OCCUR_DT3
# MAGIC         ,ClmOccrnc_OCCUR_DT4
# MAGIC         ,ClmOccrnc_OCCUR_DT5
# MAGIC         ,ClmOccrnc_OCCUR_DT6
# MAGIC         ,ClmOccrnc_OCCUR_DT7
# MAGIC         ,ClmOccrnc_OCCUR_DT8
# MAGIC         ,ClmCostDebtAnlys_ING_COST
# MAGIC         ,ClmAdtlExtEOB_CLAIM_ADTL_EOB_SK1
# MAGIC         ,ClmAdtlExtEOB_CLAIM_ADTL_EOB_SK2
# MAGIC         ,ClmAdtlExtOthrPyr_CONTRACT_SUB_ID
# MAGIC         ,ClmAdtlExtOthrPyr_MCO_ADJUD_DTL_DT
# MAGIC         ,ClmAdtlExtOthrPyr_ALWD_OTH_PYR_AMT
# MAGIC         ,ClmAdtlExtOthrPyr_ALWD_OTH_PYR_AMT_2
# MAGIC         ,ClmCndtn_COND_CD1
# MAGIC         ,ClmCndtn_COND_CD2
# MAGIC         ,ClmCndtn_COND_CD3
# MAGIC         ,ClmCndtn_COND_CD4
# MAGIC         ,ClmCndtn_COND_CD5
# MAGIC         ,ClmCndtn_COND_CD6
# MAGIC         ,ClmCndtn_COND_CD7
# MAGIC         ,ClmCndtn_COND_CD8
# MAGIC         ,MedClmNDCDtl_RX_ID_NBR
# MAGIC         ,MedClmNDCDtl_NDC_CD
# MAGIC         ,ClmAdtlExtDRG_SOI_CD
# MAGIC         ,ClmAdtlExtDRG_ROM_CD
# MAGIC         ,ClmAdtlExtDRG_DRG_CD
# MAGIC         ,ClmAdtlExtDRG_DAY_OUTLIER_AMT
# MAGIC         ,ClmAdtlExtDRG_COST_OUTLIER_AMT
# MAGIC         ,ClmAdtlExtDRG_DRG_VERS_NBR
# MAGIC         ,BillProv_PRIMARY_SPCLTY_CD
# MAGIC         ,BillProv_MEDICAID_ID
# MAGIC         ,BillProv_NPI
# MAGIC         ,BillProv_MMIS_PROV_TYP_ID
# MAGIC         ,MCP_MEDICAID_ID
# MAGIC         ,MCP_MMIS_PROV_TYP_ID
# MAGIC         ,MCP_PRIMARY_SPCLTY_CD
# MAGIC         ,SumOverSakClm_THE_PAID_AMT
# MAGIC         ,SumOverSakClm_ALWD_QTY
# MAGIC         ,SumOverSakClm_THE_DETAIL_PAID_AMT
# MAGIC         ,ATTENDING_EXTERNALPROVID
# MAGIC         ,ATTENDING_EXTERNALPROVIDQUALIFIER
# MAGIC         ,ATTENDING_PROV_NAME
# MAGIC         ,ATTENDING_PROVID
# MAGIC         ,ATTENDING_SAK_PROV_ID
# MAGIC         ,BILLING_EXTERNALPROVID
# MAGIC         ,BILLING_EXTERNALPROVIDQUALIFIER
# MAGIC         ,BILLING_MEDICAID_ID
# MAGIC         ,BILLING_PROV_NAME
# MAGIC         ,BILLING_PROVID
# MAGIC         ,BILLING_SAK_PROV_ID
# MAGIC         ,BILLING_MMIS_PROV_TYP
# MAGIC         ,BILLING_PROV_TYPE_NM
# MAGIC         ,OPERATING_EXTERNALPROVID
# MAGIC         ,OPERATING_EXTERNALPROVIDQUALIFIER
# MAGIC         ,OPERATING_PROV_NAME
# MAGIC         ,OPERATING_PROVID
# MAGIC         ,OPERATING_SAK_PROV_ID
# MAGIC         ,ORDERING_EXTERNALPROVID
# MAGIC         ,ORDERING_EXTERNALPROVIDQUALIFIER
# MAGIC         ,ORDERING_PROV_NAME
# MAGIC         ,ORDERING_PROVID
# MAGIC         ,ORDERING_SAK_PROV_ID
# MAGIC         ,PRESCRIBING_PROV_NAME
# MAGIC         ,PRESCRIBING_EXTERNALPROVID
# MAGIC         ,PRESCRIBING_EXTERNALPROVIDQUALIFIER
# MAGIC         ,PRESCRIBING_PROVID
# MAGIC         ,PRESCRIBING_SAK_PROV_ID
# MAGIC         ,REFERRING_EXTERNALPROVID
# MAGIC         ,REFERRING_EXTERNALPROVIDQUALIFIER
# MAGIC         ,REFERRING_PROV_NAME
# MAGIC         ,REFERRING_PROVID
# MAGIC         ,REFERRING_SAK_PROV_ID
# MAGIC         ,RENDERING_EXTERNALPROVID
# MAGIC         ,RENDERING_EXTERNALPROVIDQUALIFIER
# MAGIC         ,RENDERING_PROV_NAME
# MAGIC         ,RENDERING_PROVID
# MAGIC         ,RENDERING_SAK_PROV_ID
# MAGIC         ,SERVICE_EXTERNALPROVIDID
# MAGIC         ,SERVICE_EXTERNALPROVIDQUALIFIER
# MAGIC         ,SERVICE_PROV_NAME
# MAGIC         ,SERVICE_PROVID
# MAGIC         ,SERVICE_SAK_PROV
# MAGIC         ,SERVICE_MMIS_PROV_TYP
# MAGIC         ,SERVICE_PROV_TYPE_NM
# MAGIC         from ${catalog}.${schema_name}.${VEN100FA} ClmMain
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_CLAIM
# MAGIC                   ,SUM(THE_PAID_AMT)                    AS SumOverSakClm_THE_PAID_AMT
# MAGIC                   ,SUM(COALESCE(ALWD_QTY,0))            AS SumOverSakClm_ALWD_QTY
# MAGIC                   ,SUM(COALESCE(THE_DETAIL_PAID_AMT,0)) AS SumOverSakClm_THE_DETAIL_PAID_AMT
# MAGIC              from ${catalog}.${schema_name}.${VEN100FA}
# MAGIC           group by 1
# MAGIC         ) ClmTmp
# MAGIC         on clmMain.SAK_CLAIM = ClmTmp.SAK_CLAIM
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_CLAIM, DTL_NBR
# MAGIC                   ,MAX(RX_ID_NBR) AS MedClmNDCDtl_RX_ID_NBR
# MAGIC                   ,MAX(NDC_CD)  AS MedClmNDCDtl_NDC_CD
# MAGIC               from ${catalog}.${schema_name}.${VEN10005FA}
# MAGIC              group by 1,2
# MAGIC         ) MedClmNDCDtl
# MAGIC         on ClmMain.SAK_CLAIM = MedClmNDCDtl.SAK_CLAIM and ClmMain.DTL_NBR = MedClmNDCDtl.DTL_NBR
# MAGIC         left join                                                               -- Fixed Issue#66
# MAGIC         (
# MAGIC                 select SAK_CLAIM
# MAGIC                       ,MAX(HDR_ALWD_AMT) AS HdrAlwdAmtClm_HDR_ALWD_AMT
# MAGIC                  from ${catalog}.${schema_name}.${VEN10006FA}
# MAGIC                 group by 1
# MAGIC         ) HdrAlwdAmtClm
# MAGIC         on ClmMain.SAK_CLAIM = HdrAlwdAmtClm.SAK_CLAIM
# MAGIC         left join
# MAGIC         (
# MAGIC           select distinct SAK_CLAIM
# MAGIC                          ,MAX(CASE WHEN rnum=1 THEN OCCUR_CD ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_CD1
# MAGIC                          ,MAX(CASE WHEN rnum=1 THEN OCCUR_DT ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_DT1
# MAGIC                          ,MAX(CASE WHEN rnum=2 THEN OCCUR_CD ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_CD2
# MAGIC                          ,MAX(CASE WHEN rnum=2 THEN OCCUR_DT ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_DT2
# MAGIC                          ,MAX(CASE WHEN rnum=3 THEN OCCUR_CD ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_CD3
# MAGIC                          ,MAX(CASE WHEN rnum=3 THEN OCCUR_DT ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_DT3
# MAGIC                          ,MAX(CASE WHEN rnum=4 THEN OCCUR_CD ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_CD4
# MAGIC                          ,MAX(CASE WHEN rnum=4 THEN OCCUR_DT ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_DT4
# MAGIC                          ,MAX(CASE WHEN rnum=5 THEN OCCUR_CD ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_CD5
# MAGIC                          ,MAX(CASE WHEN rnum=5 THEN OCCUR_DT ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_DT5
# MAGIC                          ,MAX(CASE WHEN rnum=6 THEN OCCUR_CD ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_CD6
# MAGIC                          ,MAX(CASE WHEN rnum=6 THEN OCCUR_DT ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_DT6
# MAGIC                          ,MAX(CASE WHEN rnum=7 THEN OCCUR_CD ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_CD7
# MAGIC                          ,MAX(CASE WHEN rnum=7 THEN OCCUR_DT ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_DT7
# MAGIC                          ,MAX(CASE WHEN rnum=8 THEN OCCUR_CD ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_CD8
# MAGIC                          ,MAX(CASE WHEN rnum=8 THEN OCCUR_DT ELSE NULL END) over(partition by SAK_CLAIM) AS ClmOccrnc_OCCUR_DT8
# MAGIC                   from
# MAGIC                   (
# MAGIC                      select *, row_number() over (partition by SAK_CLAIM order by OCCUR_DT desc) rnum
# MAGIC                      from ${catalog}.${schema_name}.${VEN10002FA}
# MAGIC                   )t 
# MAGIC         ) ClmOccrnc
# MAGIC         on ClmMain.SAK_CLAIM = ClmOccrnc.SAK_CLAIM
# MAGIC         left join
# MAGIC         (
# MAGIC           select distinct SAK_CLAIM
# MAGIC                           ,MAX(CASE WHEN rnum=1 THEN COND_CD ELSE '' END) over(partition by SAK_CLAIM) AS ClmCndtn_COND_CD1
# MAGIC                           ,MAX(CASE WHEN rnum=2 THEN COND_CD ELSE '' END) over(partition by SAK_CLAIM) AS ClmCndtn_COND_CD2
# MAGIC                           ,MAX(CASE WHEN rnum=3 THEN COND_CD ELSE '' END) over(partition by SAK_CLAIM) AS ClmCndtn_COND_CD3
# MAGIC                           ,MAX(CASE WHEN rnum=4 THEN COND_CD ELSE '' END) over(partition by SAK_CLAIM) AS ClmCndtn_COND_CD4
# MAGIC                           ,MAX(CASE WHEN rnum=5 THEN COND_CD ELSE '' END) over(partition by SAK_CLAIM) AS ClmCndtn_COND_CD5
# MAGIC                           ,MAX(CASE WHEN rnum=6 THEN COND_CD ELSE '' END) over(partition by SAK_CLAIM) AS ClmCndtn_COND_CD6
# MAGIC                           ,MAX(CASE WHEN rnum=7 THEN COND_CD ELSE '' END) over(partition by SAK_CLAIM) AS ClmCndtn_COND_CD7
# MAGIC                           ,MAX(CASE WHEN rnum=8 THEN COND_CD ELSE '' END) over(partition by SAK_CLAIM) AS ClmCndtn_COND_CD8
# MAGIC                   from
# MAGIC                   (
# MAGIC                      SELECT *, ROW_NUMBER() OVER(PARTITION BY SAK_CLAIM order by EFF_DT desc) rnum -- COND_SEQ_CD Deleted Field; Use PARTITION BY SAK_CLAIM , EFF_DT instead.
# MAGIC                        FROM ${catalog}.${schema_name}.${VEN10003FA}
# MAGIC                   )t 
# MAGIC         ) ClmCndtn
# MAGIC         on ClmMain.SAK_CLAIM = ClmCndtn.SAK_CLAIM
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_CLAIM
# MAGIC                    ,MAX(ING_COST) AS ClmCostDebtAnlys_ING_COST
# MAGIC               from ${catalog}.${schema_name}.${VEN10007FA}
# MAGIC              group by 1
# MAGIC         ) ClmCostDebtAnlys
# MAGIC         on ClmMain.SAK_CLAIM = ClmCostDebtAnlys.SAK_CLAIM
# MAGIC         left join
# MAGIC         (
# MAGIC              select distinct SAK_CLAIM, DTL_NBR
# MAGIC 			 -- CDE_EOB Removed Field
# MAGIC              ,MAX(CASE WHEN rnum=1 THEN CLAIM_ADTL_EOB_SK ELSE NULL END) over(partition by SAK_CLAIM) AS ClmAdtlExtEOB_CLAIM_ADTL_EOB_SK1
# MAGIC              ,MAX(CASE WHEN rnum=2 THEN CLAIM_ADTL_EOB_SK ELSE NULL END) over(partition by SAK_CLAIM) AS ClmAdtlExtEOB_CLAIM_ADTL_EOB_SK2
# MAGIC              from
# MAGIC              (
# MAGIC                 select *, row_number() over(partition by SAK_CLAIM, DTL_NBR order by DTL_NBR) rnum
# MAGIC                 from ${catalog}.${schema_name}.${VEN12401FA}
# MAGIC              )t
# MAGIC         ) ClmAdtlExtEOB
# MAGIC         on ClmMain.SAK_CLAIM = ClmAdtlExtEOB.SAK_CLAIM and ClmMain.DTL_NBR = ClmAdtlExtEOB.DTL_NBR
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_CLAIM, DTL_NBR
# MAGIC                   ,MAX(CONTRACT_SUB_ID)  AS ClmAdtlExtOthrPyr_CONTRACT_SUB_ID
# MAGIC                   ,MAX(MCO_ADJUD_DTL_DT) AS ClmAdtlExtOthrPyr_MCO_ADJUD_DTL_DT
# MAGIC 				  ,MAX(ALWD_OTH_PYR_AMT) AS ClmAdtlExtOthrPyr_ALWD_OTH_PYR_AMT_2
# MAGIC               from ${catalog}.${schema_name}.${VEN12403FA}
# MAGIC              group by 1,2
# MAGIC         ) ClmAdtlExtOthrPyr
# MAGIC         on ClmMain.SAK_CLAIM = ClmAdtlExtOthrPyr.SAK_CLAIM and ClmMain.DTL_NBR = ClmAdtlExtOthrPyr.DTL_NBR
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_CLAIM
# MAGIC                   ,SUM(ALWD_OTH_PYR_AMT)  AS ClmAdtlExtOthrPyr_ALWD_OTH_PYR_AMT
# MAGIC               from ${catalog}.${schema_name}.${VEN12403FA}
# MAGIC              group by 1
# MAGIC         ) ClmAdtlExtOthrPyr2
# MAGIC         on clmMain.SAK_CLAIM = ClmAdtlExtOthrPyr2.SAK_CLAIM
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_CLAIM, DTL_NBR
# MAGIC                   ,SOI_CD           AS ClmAdtlExtDRG_SOI_CD
# MAGIC                   ,ROM_CD           AS ClmAdtlExtDRG_ROM_CD
# MAGIC                   ,DRG_CD           AS ClmAdtlExtDRG_DRG_CD
# MAGIC                   ,DAY_OUTLIER_AMT  AS ClmAdtlExtDRG_DAY_OUTLIER_AMT
# MAGIC                   ,COST_OUTLIER_AMT AS ClmAdtlExtDRG_COST_OUTLIER_AMT
# MAGIC                   ,DRG_VERS_NBR     AS ClmAdtlExtDRG_DRG_VERS_NBR
# MAGIC               from ${catalog}.${schema_name}.${VEN12404FA}
# MAGIC         ) ClmAdtlExtDRG
# MAGIC         on ClmMain.SAK_CLAIM = ClmAdtlExtDRG.SAK_CLAIM and ClmMain.DTL_NBR = ClmAdtlExtDRG.DTL_NBR
# MAGIC         left join
# MAGIC         (
# MAGIC             select ICN_NBR
# MAGIC                   ,ATTENDING_EXTERNALPROVID
# MAGIC                   ,ATTENDING_EXTERNALPROVIDQUALIFIER
# MAGIC                   ,ATTENDING_PROV_NAME
# MAGIC                   ,ATTENDING_PROVID
# MAGIC                   ,ATTENDING_SAK_PROV_ID
# MAGIC                   ,BILLING_EXTERNALPROVID
# MAGIC                   ,BILLING_EXTERNALPROVIDQUALIFIER
# MAGIC                   ,BILLING_MEDICAID_ID
# MAGIC                   ,BILLING_PROV_NAME
# MAGIC                   ,BILLING_PROVID
# MAGIC                   ,BILLING_SAK_PROV_ID
# MAGIC                   ,BILLING_MMIS_PROV_TYP
# MAGIC                   ,BILLING_PROV_TYPE_NM
# MAGIC                   ,OPERATING_EXTERNALPROVID
# MAGIC                   ,OPERATING_EXTERNALPROVIDQUALIFIER
# MAGIC                   ,OPERATING_PROV_NAME
# MAGIC                   ,OPERATING_PROVID
# MAGIC                   ,OPERATING_SAK_PROV_ID
# MAGIC                   ,ORDERING_EXTERNALPROVID
# MAGIC                   ,ORDERING_EXTERNALPROVIDQUALIFIER
# MAGIC                   ,ORDERING_PROV_NAME
# MAGIC                   ,ORDERING_PROVID
# MAGIC                   ,ORDERING_SAK_PROV_ID
# MAGIC                   ,PRESCRIBING_PROV_NAME
# MAGIC                   ,PRESCRIBING_EXTERNALPROVID
# MAGIC                   ,PRESCRIBING_EXTERNALPROVIDQUALIFIER
# MAGIC                   ,PRESCRIBING_PROVID
# MAGIC                   ,PRESCRIBING_SAK_PROV_ID
# MAGIC                   ,REFERRING_EXTERNALPROVID
# MAGIC                   ,REFERRING_EXTERNALPROVIDQUALIFIER
# MAGIC                   ,REFERRING_PROV_NAME
# MAGIC                   ,REFERRING_PROVID
# MAGIC                   ,REFERRING_SAK_PROV_ID
# MAGIC                   ,RENDERING_EXTERNALPROVID
# MAGIC                   ,RENDERING_EXTERNALPROVIDQUALIFIER
# MAGIC                   ,RENDERING_PROV_NAME
# MAGIC                   ,RENDERING_PROVID
# MAGIC                   ,RENDERING_SAK_PROV_ID
# MAGIC                   ,SERVICE_EXTERNALPROVIDID
# MAGIC                   ,SERVICE_EXTERNALPROVIDQUALIFIER
# MAGIC                   ,SERVICE_PROV_NAME
# MAGIC                   ,SERVICE_PROVID
# MAGIC                   ,SERVICE_SAK_PROV
# MAGIC                   ,SERVICE_MMIS_PROV_TYP
# MAGIC                   ,SERVICE_PROV_TYPE_NM
# MAGIC               from ${catalog}.${schema_name}.${VEN12501FA}
# MAGIC         ) ClmMultiProv
# MAGIC         on TRIM(ClmMain.ICN_NBR) = TRIM(ClmMultiProv.ICN_NBR)
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_PROV
# MAGIC                    ,PROGRAM_ID AS BillProv_PROGRAM_ID
# MAGIC               from ${catalog}.${schema_name}.${VEN117FA11}
# MAGIC         ) ProvCntrctExtrct
# MAGIC         on ClmMain.SAK_PROV = ProvCntrctExtrct.SAK_PROV
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_PROV
# MAGIC                   ,PT_CNTY           AS BillProv_PT_CNTY
# MAGIC 				  ,PRIMARY_SPCLTY_CD AS BillProv_PRIMARY_SPCLTY_CD
# MAGIC 				  ,MEDICAID_ID       AS BillProv_MEDICAID_ID
# MAGIC 				  ,NPI               AS BillProv_NPI
# MAGIC 				  ,MMIS_PROV_TYP_ID  AS BillProv_MMIS_PROV_TYP_ID
# MAGIC               from ${catalog}.${schema_name}.${VEN117FA1}
# MAGIC         ) ProvExtrct
# MAGIC         on ClmMain.SAK_PROV = ProvExtrct.SAK_PROV
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_PROV
# MAGIC                   ,SL_CNTY           AS AttndProv_SL_CNTY
# MAGIC                   ,MEDICAID_ID       AS AttndProv_MEDICAID_ID
# MAGIC 				  ,NPI               AS AttndProv_NPI
# MAGIC 				  ,MMIS_PROV_TYP_ID  AS AttndProv_MMIS_PROV_TYP_ID
# MAGIC 				  ,PRIMARY_SPCLTY_CD AS AttndProv_PRIMARY_SPCLTY_CD
# MAGIC               from ${catalog}.${schema_name}.${VEN117FA1}
# MAGIC         ) ProvExtrct2
# MAGIC         on ClmMain.RPA_PROV_SAK_ID = ProvExtrct2.SAK_PROV
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_PROV
# MAGIC                   ,MEDICAID_ID       AS RfrngProv_MEDICAID_ID
# MAGIC 				  ,NPI               AS RfrngProv_NPI
# MAGIC 				  ,MMIS_PROV_TYP_ID  AS RfrngProv_MMIS_PROV_TYP_ID
# MAGIC 				  ,PRIMARY_SPCLTY_CD AS RfrngProv_PRIMARY_SPCLTY_CD
# MAGIC               from ${catalog}.${schema_name}.${VEN117FA1}
# MAGIC         ) ProvExtrct3
# MAGIC         on ClmMain.ORP_PROV_SAK_ID = ProvExtrct3.SAK_PROV		
# MAGIC         left join
# MAGIC         (
# MAGIC             select distinct tp_prov_num
# MAGIC                            ,MEDICAID_ID       AS MCP_MEDICAID_ID
# MAGIC                            ,MMIS_PROV_TYP_ID  AS MCP_MMIS_PROV_TYP_ID
# MAGIC                            ,PRIMARY_SPCLTY_CD AS MCP_PRIMARY_SPCLTY_CD
# MAGIC             from (select hippa_raw_820.*
# MAGIC 			        from (select raw_820.*, row_number() over(partition by tp_prov_num order by tp_prov_num) rnum
# MAGIC 			     	        from (select distinct tp_prov_num,
# MAGIC                                                   prov_num 
# MAGIC                                     from ${catalog}.${schema_name_mc}.${hipaa_820_raw}
# MAGIC                                   )raw_820
# MAGIC                          ) hippa_raw_820
# MAGIC 			       where rnum = 1
# MAGIC 			     ) MC_hippa_raw_820
# MAGIC                  left join
# MAGIC                  (
# MAGIC                    select MEDICAID_ID
# MAGIC                          ,NPI
# MAGIC                          ,MMIS_PROV_TYP_ID
# MAGIC                          ,PRIMARY_SPCLTY_CD
# MAGIC                     from ${catalog}.${schema_name}.${VEN117FA1}
# MAGIC                  ) ProvExtrct4
# MAGIC                  on TRIM(MC_hippa_raw_820.prov_num) = ProvExtrct4.MEDICAID_ID
# MAGIC         ) MCPHippa820
# MAGIC         on SUBSTRING(TRIM(ClmMain.SUBMITTER_CD),1,7) = TRIM(MCPHippa820.tp_prov_num)
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_PROV
# MAGIC                   ,TXNMY_CD AS BillProv_TXNMY_CD
# MAGIC               from ${catalog}.${schema_name}.${VEN117FA4}
# MAGIC         ) ProvTaxnmyExtrct
# MAGIC         on ClmMain.SAK_PROV = ProvTaxnmyExtrct.SAK_PROV
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_PROV
# MAGIC                   ,TXNMY_CD AS AttndProv_TXNMY_CD
# MAGIC               from ${catalog}.${schema_name}.${VEN117FA4}
# MAGIC         ) ProvTaxnmyExtrct2
# MAGIC         on ClmMain.RPA_PROV_SAK_ID = ProvTaxnmyExtrct2.SAK_PROV
# MAGIC         --where ClmMain.CLM_TYP_CD in('A','C','I','L','O')
# MAGIC         where UPPER(TRIM(ClmMain.CLM_TYP_CD)) in ('LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
# MAGIC                                                  ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
# MAGIC                                                  ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
# MAGIC                                                  ,'PART A INSTITUTIONAL','PART B INSTITUTIONAL','PART C INSTITUTIONAL')
# MAGIC )clm
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Full Refresh Or Delta Step.- Truncate/Delete Main cl_inst and Inst_Analytics Tables
if CleanDstntTblsFlag == 'T':
    # Logic to process Full Refresh VE Source Type
    #
    sql_out = spark.sql(f"""
                        TRUNCATE TABLE {catalog}.{schema_name}.{cl_inst}
                        ;
                        """)
    display(sql_out)
    sql_out = spark.sql(f"""
                        TRUNCATE TABLE {catalog}.{schema_name}.{Inst_Analytics}
                        ;
                        """)
    display(sql_out)
else:
    # Logic to process Delta VE source type
    #
    sql_out = spark.sql(f"""
                        DELETE 
                         FROM {catalog}.{schema_name}.{cl_inst} t1
                        WHERE TRUE
                          AND EXISTS
                              ( SELECT 1
                                  FROM {catalog}.{schema_name}.{cl_inst_stg} t2
                                 WHERE TRUE
                                   AND t2.NUM_ICN = t1.NUM_ICN
                                   AND t2.NUM_DTL  = t1.NUM_DTL
                              )
                        ;
                        """)
    display(sql_out)
    sql_out = spark.sql(f"""
                        DELETE 
                         FROM {catalog}.{schema_name}.{Inst_Analytics} t1
                        WHERE TRUE
                          AND EXISTS
                              ( SELECT 1
                                  FROM {catalog}.{schema_name}.{cl_inst_stg} t2
                                 WHERE TRUE
                                   AND t2.NUM_ICN = t1.NUM_ICN
                                   AND t2.NUM_DTL  = t1.NUM_DTL
                              )
                        ;
                        """)
    display(sql_out)


# COMMAND ----------

# DBTITLE 1,Load cl_inst table
# MAGIC %sql 
# MAGIC INSERT INTO TABLE ${catalog}.${schema_name}.${cl_inst}
# MAGIC SELECT * FROM ${catalog}.${schema_name}.${cl_inst_stg}
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Truncate Inst_Analytics_Staging table
# MAGIC %sql
# MAGIC TRUNCATE TABLE ${catalog}.${schema_name}.${Inst_Analytics_stg}
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Load Inst_Analytics_Staging table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${Inst_Analytics_stg}
# MAGIC SELECT DISTINCT
# MAGIC    --CODE WITH COLUMNS MAPPING GOES HERE
# MAGIC      TRIM(NUM_ICN) AS NUM_ICN
# MAGIC     ,TRIM(IND_CLAIM) AS IND_CLAIM
# MAGIC     ,TRIM(CDE_CLM_TYPE) AS CDE_CLM_TYPE
# MAGIC     ,TRIM(CDE_HDR_STATUS) AS CDE_HDR_STATUS
# MAGIC     ,TRIM(CDE_PGM_HEALTH) AS CDE_PGM_HEALTH
# MAGIC     ,TRIM(CDE_AID_CATEGORY) AS CDE_AID_CATEGORY
# MAGIC     ,TRIM(ID_CLERK) AS ID_CLERK
# MAGIC     ,TRIM(CDE_PRESCRIPTION_ORIG) AS CDE_PRESCRIPTION_ORIG
# MAGIC     ,TRIM(DERIVED) AS DERIVED
# MAGIC     ,TRIM(CDE_ENC_TYPE) AS CDE_ENC_TYPE
# MAGIC     ,TRIM(NA) AS NA
# MAGIC     ,TRIM(ID_MEDICAID) AS ID_MEDICAID
# MAGIC     ,DTE_BIRTH
# MAGIC     ,TRIM(IND_BRAND_MED_NEC) AS IND_BRAND_MED_NEC
# MAGIC     ,CASE WHEN ((substring(CAST(AMT_VACC_INCENTIVE AS STRING), 1, 1) = '-' OR substring(CAST(AMT_VACC_INCENTIVE AS STRING), 1, 1) = ' ') and LENGTH(CAST(AMT_VACC_INCENTIVE AS STRING)) > 1) THEN ROUND(AMT_VACC_INCENTIVE/100,2.0) ELSE 0 END AS AMT_VACC_INCENTIVE
# MAGIC     ,TRIM(NUM_PRIOR_AUTH) AS NUM_PRIOR_AUTH
# MAGIC     ,TRIM(NA2) AS NA2
# MAGIC     ,NA3
# MAGIC     ,TRIM(ID_CONTRACT_SUB) AS ID_CONTRACT_SUB
# MAGIC     ,TRIM(NUM_HIC_SUB) AS NUM_HIC_SUB
# MAGIC     ,TRIM(NUM_CMS_ICN) AS NUM_CMS_ICN
# MAGIC     ,TRIM(ATTENDING_EXTERNALPROVID) AS ATTENDING_EXTERNALPROVID
# MAGIC     ,TRIM(ATTENDING_EXTERNALPROVIDQUALIFIER) AS ATTENDING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(ATTENDING_PROV_NAME) AS ATTENDING_PROV_NAME
# MAGIC     ,TRIM(ATTENDING_PROVID) AS ATTENDING_PROVID
# MAGIC     ,TRIM(ATTENDING_SAK_PROV_ID) AS ATTENDING_SAK_PROV_ID
# MAGIC     ,TRIM(BILLING_EXTERNALPROVID) AS BILLING_EXTERNALPROVID
# MAGIC     ,TRIM(BILLING_EXTERNALPROVIDQUALIFIER) AS BILLING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(BILLING_MEDICAID_ID) AS BILLING_MEDICAID_ID
# MAGIC     ,TRIM(BILLING_PROV_NAME) AS BILLING_PROV_NAME
# MAGIC     ,TRIM(BILLING_PROVID) AS BILLING_PROVID
# MAGIC     ,TRIM(BILLING_SAK_PROV_ID) AS BILLING_SAK_PROV_ID
# MAGIC     ,TRIM(NA15) AS NA15
# MAGIC     ,TRIM(BILLING_MMIS_PROV_TYP) AS BILLING_MMIS_PROV_TYP
# MAGIC     ,TRIM(BILLING_PROV_TYPE_NM) AS BILLING_PROV_TYPE_NM
# MAGIC     ,TRIM(OPERATING_EXTERNALPROVID) AS OPERATING_EXTERNALPROVID
# MAGIC     ,NA19
# MAGIC     ,TRIM(OPERATING_EXTERNALPROVIDQUALIFIER) AS OPERATING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(CDE_SOI) AS CDE_SOI
# MAGIC     ,TRIM(CDE_LEVEL_OF_CARE) AS CDE_LEVEL_OF_CARE
# MAGIC     ,TRIM(DERIVED_2) AS DERIVED_2
# MAGIC     ,TRIM(CDE_MDC) AS CDE_MDC
# MAGIC     ,TRIM(OPERATING_PROV_NAME) AS OPERATING_PROV_NAME
# MAGIC     ,TRIM(OPERATING_PROVID) AS OPERATING_PROVID
# MAGIC     ,TRIM(CDE_PATIENT_STATUS) AS CDE_PATIENT_STATUS
# MAGIC     ,TRIM(CDE_EMERGENCY) AS CDE_EMERGENCY
# MAGIC     ,TRIM(CDE_ADMIT_SOURCE) AS CDE_ADMIT_SOURCE
# MAGIC     ,TRIM(CDE_ROM) AS CDE_ROM
# MAGIC     ,DTE_ADMISSION
# MAGIC     ,CDE_ADMIT_HOUR
# MAGIC     ,TRIM(CDE_DRG) AS CDE_DRG
# MAGIC     ,ROUND(AMT_BASE_DRG/100,2.0) as AMT_BASE_DRG
# MAGIC     ,DTE_DISCHARGE
# MAGIC     ,TIME_DISCHARGE
# MAGIC     ,TRIM(CDE_COMPOUND_DOSAGE) AS CDE_COMPOUND_DOSAGE
# MAGIC     ,AMT_DAY_OUTLIER/100 AS AMT_DAY_OUTLIER -- Removed Round
# MAGIC     ,AMT_COST_OUTLIER/100 AS AMT_COST_OUTLIER -- Removed Round
# MAGIC     ,TRIM(CDE_MED_REC_NUM) AS CDE_MED_REC_NUM
# MAGIC     ,TRIM(CDE_PEER_GROUP) AS CDE_PEER_GROUP
# MAGIC     ,ROUND(AMT_COST_INGREDIENT/100,2.0) as AMT_COST_INGREDIENT
# MAGIC     ,TRIM(OPERATING_SAK_PROV_ID) AS OPERATING_SAK_PROV_ID
# MAGIC     ,TRIM(CDE_SOI_DISC) AS CDE_SOI_DISC
# MAGIC     ,TRIM(CDE_ROM_DISC) AS CDE_ROM_DISC
# MAGIC     ,TRIM(NUM_PA_REF) AS NUM_PA_REF
# MAGIC     ,ROUND(AMT_TPL_SUBM/100,2.0) as AMT_TPL_SUBM
# MAGIC     ,AMT_TPL_APPLD/100 AS AMT_TPL_APPLD -- Removed Round
# MAGIC     ,AMT_PAID_MCO -- Removed the by 100 and Removed Round
# MAGIC     ,TRIM(ORDERING_EXTERNALPROVID) AS ORDERING_EXTERNALPROVID
# MAGIC     ,TRIM(IND_HDR_DTL) AS IND_HDR_DTL
# MAGIC     ,TRIM(ORDERING_EXTERNALPROVIDQUALIFIER) AS ORDERING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(CDE_COND_1) AS CDE_COND_1
# MAGIC     ,TRIM(CDE_COND_2) AS CDE_COND_2
# MAGIC     ,TRIM(CDE_COND_3) AS CDE_COND_3
# MAGIC     ,TRIM(CDE_COND_4) AS CDE_COND_4
# MAGIC     ,TRIM(CDE_COND_5) AS CDE_COND_5
# MAGIC     ,TRIM(CDE_COND_6) AS CDE_COND_6
# MAGIC     ,TRIM(CDE_COND_7) AS CDE_COND_7
# MAGIC     ,TRIM(CDE_COND_8) AS CDE_COND_8
# MAGIC     ,TRIM(ORDERING_PROV_NAME) AS ORDERING_PROV_NAME
# MAGIC     ,TRIM(CDE_PAY_ARR) AS CDE_PAY_ARR
# MAGIC     ,TRIM(NA27) AS NA27
# MAGIC     ,TRIM(CDE_DRG_DISC) AS CDE_DRG_DISC
# MAGIC     ,TRIM(CDE_CLARIFICATION1) AS CDE_CLARIFICATION1
# MAGIC     ,TRIM(CDE_CLARIFICATION2) AS CDE_CLARIFICATION2
# MAGIC     ,TRIM(CDE_CLARIFICATION3) AS CDE_CLARIFICATION3
# MAGIC     ,TRIM(ORDERING_PROVID) AS ORDERING_PROVID
# MAGIC     ,TRIM(NA29) AS NA29
# MAGIC     ,DTE_BILLED
# MAGIC     ,TRIM(CDE_RECIP_COUNTY) AS CDE_RECIP_COUNTY
# MAGIC     ,ROUND(AMT_REIMBURSED/100,2.0) as AMT_REIMBURSED
# MAGIC     ,DTE_PAID
# MAGIC     ,TRIM(CDE_CLM_REGION) AS CDE_CLM_REGION
# MAGIC     ,DTE_FIRST_SVC
# MAGIC     ,DTE_LAST_SVC
# MAGIC     ,BATCH_DATE
# MAGIC     ,TRIM(ORDERING_SAK_PROV_ID) AS ORDERING_SAK_PROV_ID
# MAGIC     ,TRIM(PRESCRIBING_PROV_NAME) AS PRESCRIBING_PROV_NAME
# MAGIC     ,AMT_BILLED                                -- Fixed issue/removed by 100
# MAGIC     ,TRIM(QTY_UNITS_ALWD) AS QTY_UNITS_ALWD
# MAGIC     ,NA32
# MAGIC     ,NUM_RECIP_AGE
# MAGIC     ,TRIM(PRESCRIBING_EXTERNALPROVID) AS PRESCRIBING_EXTERNALPROVID
# MAGIC     ,NA34
# MAGIC     ,ROUND(AMT_INTEREST/100,2.0) as AMT_INTEREST
# MAGIC     ,DTE_MCO_ADJUD
# MAGIC     ,TRIM(ADR_ZIP_CODE) AS ADR_ZIP_CODE
# MAGIC     ,TRIM(ADR_ZIP_CODE_4) AS ADR_ZIP_CODE_4
# MAGIC     ,TRIM(PRESCRIBING_EXTERNALPROVIDQUALIFIER) AS PRESCRIBING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(PRESCRIBING_PROVID) AS PRESCRIBING_PROVID
# MAGIC     ,TRIM(PRESCRIBING_SAK_PROV_ID) AS PRESCRIBING_SAK_PROV_ID
# MAGIC     ,TRIM(REFERRING_EXTERNALPROVID) AS REFERRING_EXTERNALPROVID
# MAGIC     ,TRIM(REFERRING_EXTERNALPROVIDQUALIFIER) AS REFERRING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(CDE_SEX) AS CDE_SEX
# MAGIC     ,TRIM(CDE_RACE) AS CDE_RACE
# MAGIC     ,TRIM(REFERRING_PROV_NAME) AS REFERRING_PROV_NAME
# MAGIC     ,TRIM(REFERRING_PROVID) AS REFERRING_PROVID
# MAGIC     ,TRIM(REFERRING_SAK_PROV_ID) AS REFERRING_SAK_PROV_ID
# MAGIC     ,TRIM(NUM_WEIGHT) AS NUM_WEIGHT
# MAGIC     ,TRIM(NUM_PAT_ACCT) AS NUM_PAT_ACCT
# MAGIC     ,TRIM(RENDERING_EXTERNALPROVID) AS RENDERING_EXTERNALPROVID
# MAGIC     ,TRIM(RENDERING_EXTERNALPROVIDQUALIFIER) AS RENDERING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,ROUND(AMT_SPENDDOWN/100,2.0) as AMT_SPENDDOWN
# MAGIC     ,TRIM(RENDERING_PROV_NAME) AS RENDERING_PROV_NAME
# MAGIC     ,TRIM(RENDERING_PROVID) AS RENDERING_PROVID
# MAGIC     ,ROUND(CAST(CASE WHEN TRIM(BOTH '#' FROM TRIM(LEADING '-' FROM amt_coinsurance)) = '' THEN '0' ELSE amt_coinsurance END AS NUMERIC)/100,2.0) AS AMT_COINSURANCE
# MAGIC     ,TRIM(CDE_LIV_ARNG) AS CDE_LIV_ARNG
# MAGIC     ,TRIM(RENDERING_SAK_PROV_ID) AS RENDERING_SAK_PROV_ID
# MAGIC     ,TRIM(NUM_ADJ_ICN) AS NUM_ADJ_ICN
# MAGIC     ,TRIM(NUM_VERSION_DRG) AS NUM_VERSION_DRG
# MAGIC     ,TRIM(SERVICE_EXTERNALPROVIDID) AS SERVICE_EXTERNALPROVIDID
# MAGIC     ,TRIM(NUM_RA) AS NUM_RA
# MAGIC     ,TRIM(ID_VENDOR) AS ID_VENDOR
# MAGIC     ,TRIM(SERVICE_EXTERNALPROVIDQUALIFIER) AS SERVICE_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(SERVICE_PROV_NAME) AS SERVICE_PROV_NAME
# MAGIC     ,TRIM(NUM_WARRANT) AS NUM_WARRANT
# MAGIC     ,TRIM(SERVICE_PROVID) AS SERVICE_PROVID
# MAGIC     ,DTE_ENTERED_SYS
# MAGIC     ,ROUND(AMT_PAT_LIAB/100,2.0) as AMT_PAT_LIAB
# MAGIC     ,ROUND(AMT_APL_PAT_LIAB/100,2.0) as AMT_APL_PAT_LIAB
# MAGIC     ,DERIVED_3
# MAGIC     ,DTE_GENERIC
# MAGIC     ,TRIM(SERVICE_SAK_PROV) AS SERVICE_SAK_PROV
# MAGIC     ,ROUND(CAST(AMT_PAID_MCARE AS NUMERIC(12,2))/100,2.0) as AMT_PAID_MCARE
# MAGIC     ,TRIM(CDE_OCCUR_1) AS CDE_OCCUR_1
# MAGIC     ,TRIM(CDE_OCCUR_2) AS CDE_OCCUR_2
# MAGIC     ,TRIM(CDE_OCCUR_3) AS CDE_OCCUR_3
# MAGIC     ,TRIM(CDE_OCCUR_4) AS CDE_OCCUR_4
# MAGIC     ,TRIM(CDE_OCCUR_5) AS CDE_OCCUR_5
# MAGIC     ,TRIM(CDE_OCCUR_6) AS CDE_OCCUR_6
# MAGIC     ,TRIM(CDE_OCCUR_7) AS CDE_OCCUR_7
# MAGIC     ,TRIM(CDE_OCCUR_8) AS CDE_OCCUR_8
# MAGIC     ,TRIM(SERVICE_MMIS_PROV_TYP) AS SERVICE_MMIS_PROV_TYP
# MAGIC     ,DTE_OCCUR_1
# MAGIC     ,DTE_OCCUR_2
# MAGIC     ,DTE_OCCUR_3
# MAGIC     ,DTE_OCCUR_4
# MAGIC     ,DTE_OCCUR_5
# MAGIC     ,DTE_OCCUR_6
# MAGIC     ,DTE_OCCUR_7
# MAGIC     ,DTE_OCCUR_8
# MAGIC     ,TRIM(CDE_EPSDT_FP) AS CDE_EPSDT_FP
# MAGIC     ,TRIM(IND_HYST) AS IND_HYST
# MAGIC     ,TRIM(SERVICE_PROV_TYPE_NM) AS SERVICE_PROV_TYPE_NM
# MAGIC     ,TRIM(IND_STERILIZATION) AS IND_STERILIZATION
# MAGIC     ,TRIM(NA55) AS NA55
# MAGIC     ,TRIM(IND_ABORTION) AS IND_ABORTION
# MAGIC     ,TRIM(NA56) AS NA56
# MAGIC     ,TRIM(NA57) AS NA57
# MAGIC     ,TRIM(NA58) AS NA58
# MAGIC     ,TRIM(NA59) AS NA59
# MAGIC     ,ROUND(AMT_DEDUCT/100,2.0) as AMT_DEDUCT
# MAGIC     ,ROUND(AMT_MCARE_PAID/100,2.0) as AMT_MCARE_PAID
# MAGIC     ,TRIM(NA60) AS NA60
# MAGIC     ,TRIM(NA61) AS NA61
# MAGIC     ,TRIM(NUM_PRESCRIPTION_ID) AS NUM_PRESCRIPTION_ID
# MAGIC     ,DTE_PRESCRIB
# MAGIC     ,TRIM(NA62) AS NA62
# MAGIC     ,TRIM(NUM_TCN) AS NUM_TCN
# MAGIC     ,AMT_ALWD                                 -- Fixed issue/removed by 100
# MAGIC     ,TRIM(NA63) AS NA63
# MAGIC     ,TRIM(NA64) AS NA64
# MAGIC     ,ROUND(AMT_NDC_PROFEE/100,2.0) as AMT_NDC_PROFEE
# MAGIC     ,TRIM(NA65) AS NA65
# MAGIC     ,NA66
# MAGIC     ,TRIM(NA67) AS NA67
# MAGIC     ,TRIM(NA68) AS NA68
# MAGIC     ,NA69
# MAGIC     ,ROUND(AMT_CO_PAY/100,2.0) as AMT_CO_PAY
# MAGIC     ,AMT_PAID                                 -- Fixed issue/removed by 100
# MAGIC     ,TRIM(NA70) AS NA70
# MAGIC     ,NA71
# MAGIC     ,TRIM(NA72) AS NA72
# MAGIC     ,TRIM(CDE_COS_ST) AS CDE_COS_ST
# MAGIC     ,TRIM(CDE_COS_SUB) AS CDE_COS_SUB
# MAGIC     ,TRIM(NA73) AS NA73
# MAGIC     ,TRIM(NA74) AS NA74
# MAGIC     ,TRIM(ID_VOUCHER_RELATED) AS ID_VOUCHER_RELATED
# MAGIC     ,TRIM(NA75) AS NA75
# MAGIC     ,TRIM(CDE_TYPE_OF_BILL) AS CDE_TYPE_OF_BILL
# MAGIC     ,TRIM(CDE_TYPE_OF_BILL_2) AS CDE_TYPE_OF_BILL_2
# MAGIC     ,TRIM(CDE_TYPE_OF_BILL_3) AS CDE_TYPE_OF_BILL_3
# MAGIC     ,TRIM(QTY_REFILL) AS QTY_REFILL
# MAGIC     ,TRIM(NA76) AS NA76
# MAGIC     ,DTE_DISPENSE
# MAGIC     ,QTY_DISPENSE
# MAGIC     ,NUM_DAY_SUPPLY
# MAGIC     ,TRIM(CDE_DTL_STATUS) AS CDE_DTL_STATUS
# MAGIC     ,TRIM(CDE_PAY_ARR_2) AS CDE_PAY_ARR_2
# MAGIC     ,TRIM(QTY_UNITS_ALWD_2) AS QTY_UNITS_ALWD_2
# MAGIC     ,QTY_DISPENSE_2
# MAGIC     ,DERIVED_4
# MAGIC     ,TRIM(NA77) AS NA77
# MAGIC     ,ROUND(AMT_AWP/10000000,7.0) as AMT_AWP
# MAGIC     ,TRIM(CDE_MCAR_COVRG) AS CDE_MCAR_COVRG
# MAGIC     ,TRIM(IND_PHARMACY_FAMILY_PLAN) AS IND_PHARMACY_FAMILY_PLAN
# MAGIC     ,TRIM(IND_REBATE_ELIG) AS IND_REBATE_ELIG
# MAGIC     ,TRIM(CDE_DISP_STATUS) AS CDE_DISP_STATUS
# MAGIC     ,TRIM(IS_NON_DUPLICATE_IND) AS IS_NON_DUPLICATE_IND
# MAGIC     ,TRIM(CDE_EOB_1) AS CDE_EOB_1
# MAGIC     ,TRIM(CDE_EOB_2) AS CDE_EOB_2
# MAGIC     ,TRIM(IND_PRICING) AS IND_PRICING
# MAGIC     ,CAST(AMT_ALWD_2 AS NUMERIC(15,2))/100 as AMT_ALWD_2 -- Removed Round
# MAGIC     ,TRIM(IND_STERILIZATION_2) AS IND_STERILIZATION_2
# MAGIC     ,TRIM(CLAIM_ACTIVE_IND) AS CLAIM_ACTIVE_IND
# MAGIC     ,TRIM(IND_HYST_2) AS IND_HYST_2
# MAGIC     ,TRIM(LAST_CLAIM_IND) AS LAST_CLAIM_IND
# MAGIC     ,TRIM(IND_ABORTION_2) AS IND_ABORTION_2
# MAGIC     ,TRIM(IS_DKP_IND) AS IS_DKP_IND
# MAGIC     ,NUM_DAYS_COVD
# MAGIC     ,NUM_DAYS_NCOVD
# MAGIC     ,NUM_LEAVE_DAYS
# MAGIC     ,ROUND(AMT_DRUG_UNIT_PRICE/10000000,7.0) as AMT_DRUG_UNIT_PRICE
# MAGIC     ,ROUND(AMT_CO_PAY_2/100,2.0) as AMT_CO_PAY_2
# MAGIC     ,TRIM(CDE_COPAY_REASON) AS CDE_COPAY_REASON
# MAGIC     ,NUM_DTL
# MAGIC     ,DTE_FIRST_SVC_2
# MAGIC     ,DTE_LAST_SVC_2
# MAGIC     ,TRIM(QTY_UNITS_BILLED) AS QTY_UNITS_BILLED
# MAGIC     ,TRIM(CDE_REVENUE) AS CDE_REVENUE
# MAGIC     ,AMT_BILLED_2/100 as AMT_BILLED_2 -- Removed Round
# MAGIC     ,AMT_NON_COVERED/100 as AMT_NON_COVERED -- Removed Round
# MAGIC     ,AMT_PAID_MCO_2  --Removed the by 100 & Round
# MAGIC     ,NA82
# MAGIC     ,AMT_PAID_2/100 as AMT_PAID_2 -- Removed Round
# MAGIC     ,DTE_PAID_2
# MAGIC     ,ROUND(AMT_PAT_LIAB_2/100,2.0) as AMT_PAT_LIAB_2
# MAGIC     ,AMT_TPL_APPLD_2/100 as AMT_TPL_APPLD_2 -- Removed Round
# MAGIC     ,TRIM(NA83) AS NA83
# MAGIC     ,TRIM(NA84) AS NA84
# MAGIC     ,ROUND(AMT_APL_PAT_LIAB_2/100,2.0) as AMT_APL_PAT_LIAB_2
# MAGIC     ,TRIM(NA85) AS NA85
# MAGIC     ,ROUND(AMT_TPL_SUBM_2/100,2.0) as AMT_TPL_SUBM_2
# MAGIC     ,TRIM(CDE_TOOTH_NBR) AS CDE_TOOTH_NBR
# MAGIC     ,TRIM(CDE_TOOTH_SURFACE_1) AS CDE_TOOTH_SURFACE_1
# MAGIC     ,TRIM(CDE_TOOTH_SURFACE_2) AS CDE_TOOTH_SURFACE_2
# MAGIC     ,TRIM(CDE_TOOTH_SURFACE_3) AS CDE_TOOTH_SURFACE_3
# MAGIC     ,TRIM(CDE_TOOTH_SURFACE_4) AS CDE_TOOTH_SURFACE_4
# MAGIC     ,TRIM(CDE_TOOTH_SURFACE_5) AS CDE_TOOTH_SURFACE_5
# MAGIC     ,TRIM(CDE_TOOTH_SURFACE_6) AS CDE_TOOTH_SURFACE_6
# MAGIC     ,TRIM(NA86) AS NA86
# MAGIC     ,COALESCE(TRIM(DTE_MCO_ADJUD_2),'0000000000') AS DTE_MCO_ADJUD_2                      --Per BIAR's Criteria, set value when NULL.
# MAGIC     ,ROUND(AMT_SPENDDOWN_2/100,2.0) as AMT_SPENDDOWN_2
# MAGIC     ,TRIM(IND_EPSDT) AS IND_EPSDT
# MAGIC     ,TRIM(NA87) AS NA87
# MAGIC     ,TRIM(NA88) AS NA88
# MAGIC     ,TRIM(CDE_POS) AS CDE_POS
# MAGIC     ,ROUND(AMT_REIMBURSED_2/100,2.0) as AMT_REIMBURSED_2
# MAGIC     ,ROUND(AMT_PAID_MCARE_2/100,2.0) as AMT_PAID_MCARE_2
# MAGIC     ,ROUND(AMT_COINSURANCE_2/100,2.0) as AMT_COINSURANCE_2
# MAGIC     ,QTY_DAYS_COINSURANCE
# MAGIC     ,TRIM(CDE_NDC) AS CDE_NDC
# MAGIC     ,ROUND(AMT_DEDUCT_2/100,2.0) as AMT_DEDUCT_2
# MAGIC     ,TRIM(CDE_THERA_CLS_AHFS) AS CDE_THERA_CLS_AHFS
# MAGIC     ,TRIM(CDE_THERA_CLS_SPEC) AS CDE_THERA_CLS_SPEC
# MAGIC     ,TRIM(NA89) AS NA89
# MAGIC     ,TRIM(NA90) AS NA90
# MAGIC     ,TRIM(CDE_PROC_PRIM) AS CDE_PROC_PRIM
# MAGIC     ,TRIM(CDE_MODIFIER_1) AS CDE_MODIFIER_1
# MAGIC     ,TRIM(CDE_MODIFIER_2) AS CDE_MODIFIER_2
# MAGIC     ,TRIM(CDE_MODIFIER_3) AS CDE_MODIFIER_3
# MAGIC     ,TRIM(CDE_MODIFIER_4) AS CDE_MODIFIER_4
# MAGIC     ,NA91
# MAGIC     ,TRIM(CDE_FUND_CODE) AS CDE_FUND_CODE
# MAGIC     ,TRIM(CDE_RATE_TYPE) AS CDE_RATE_TYPE
# MAGIC     ,TRIM(NUM_ICN_2) AS NUM_ICN_2
# MAGIC     ,TRIM(NUM_ADJ_ICN_2) AS NUM_ADJ_ICN_2
# MAGIC     ,TRIM(NUM_TCN_2) AS NUM_TCN_2
# MAGIC     ,TRIM(ID_MEDICAID_2) AS ID_MEDICAID_2
# MAGIC     ,TRIM(ID_PROVIDER_MCAID) AS ID_PROVIDER_MCAID
# MAGIC     ,TRIM(ID_PROVIDER_NPI) AS ID_PROVIDER_NPI
# MAGIC     ,TRIM(ATTENDING_CDE_PROV_TYPE) AS ATTENDING_CDE_PROV_TYPE
# MAGIC     ,TRIM(CDE_SVC_COUNTY) AS CDE_SVC_COUNTY
# MAGIC     ,TRIM(CDE_TAXONOMY) AS CDE_TAXONOMY
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM) AS CDE_PROV_SPEC_PRIM
# MAGIC     ,TRIM(ID_PROVIDER_MCAID_2) AS ID_PROVIDER_MCAID_2
# MAGIC     ,TRIM(ID_PROVIDER_NPI_2) AS ID_PROVIDER_NPI_2
# MAGIC     ,TRIM(BILLING_CDE_PROV_TYPE_PRIM) AS BILLING_CDE_PROV_TYPE_PRIM
# MAGIC     ,TRIM(CDE_PROV_PGM) AS CDE_PROV_PGM
# MAGIC     ,TRIM(CDE_SVC_COUNTY_2) AS CDE_SVC_COUNTY_2
# MAGIC     ,TRIM(CDE_TAXONOMY_2) AS CDE_TAXONOMY_2
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM_2) AS CDE_PROV_SPEC_PRIM_2
# MAGIC     ,TRIM(ID_PROVIDER_MCAID_3) AS ID_PROVIDER_MCAID_3
# MAGIC     ,TRIM(ID_PROVIDER_NPI_3) AS ID_PROVIDER_NPI_3
# MAGIC     ,TRIM(CDE_PROV_TYPE_PRIM_BLANK_1) AS CDE_PROV_TYPE_PRIM_BLANK_1
# MAGIC     ,TRIM(CDE_PROV_PGM_2) AS CDE_PROV_PGM_2
# MAGIC     ,TRIM(CDE_SVC_COUNTY_3) AS CDE_SVC_COUNTY_3
# MAGIC     ,TRIM(CDE_TAXONOMY_3) AS CDE_TAXONOMY_3
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM_3) AS CDE_PROV_SPEC_PRIM_3
# MAGIC     ,TRIM(ID_PROVIDER_MCAID_4) AS ID_PROVIDER_MCAID_4
# MAGIC     ,TRIM(ID_PROVIDER_NPI_4) AS ID_PROVIDER_NPI_4
# MAGIC     ,TRIM(REFERRING_CDE_PROV_TYPE_PRIM) AS REFERRING_CDE_PROV_TYPE_PRIM
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM_4) AS CDE_PROV_SPEC_PRIM_4
# MAGIC     ,TRIM(ID_PROVIDER_MCAID_5) AS ID_PROVIDER_MCAID_5
# MAGIC     ,TRIM(ID_PROVIDER_NPI_5) AS ID_PROVIDER_NPI_5
# MAGIC     ,TRIM(SURGICAL_CDE_PROV_TYPE_PRIM) AS SURGICAL_CDE_PROV_TYPE_PRIM
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM_5) AS CDE_PROV_SPEC_PRIM_5
# MAGIC     ,TRIM(ID_PROVIDER_MCAID_6) AS ID_PROVIDER_MCAID_6
# MAGIC     ,TRIM(ID_PROVIDER_NPI_6) AS ID_PROVIDER_NPI_6
# MAGIC     ,TRIM(FACILTIY_CDE_PROV_TYPE_PRIM) AS FACILTIY_CDE_PROV_TYPE_PRIM
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM_6) AS CDE_PROV_SPEC_PRIM_6
# MAGIC     ,TRIM(ID_PROVIDER_MCAID_7) AS ID_PROVIDER_MCAID_7
# MAGIC     ,TRIM(ID_PROVIDER_NPI_7) AS ID_PROVIDER_NPI_7
# MAGIC     ,TRIM(CDE_PROV_TYPE_PRIM_BLANK_2) AS CDE_PROV_TYPE_PRIM_BLANK_2
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM_7) AS CDE_PROV_SPEC_PRIM_7
# MAGIC     ,TRIM(ID_PROVIDER_MCAID_8) AS ID_PROVIDER_MCAID_8
# MAGIC     ,TRIM(MCP_CDE_PROV_TYPE_PRIM) AS MCP_CDE_PROV_TYPE_PRIM
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM_8) AS CDE_PROV_SPEC_PRIM_8
# MAGIC     ,year(coalesce(dte_paid, current_date)) * 100 + month(coalesce(dte_paid,current_date)) as partition_col
# MAGIC
# MAGIC from ${catalog}.${schema_name}.${cl_inst_stg} a 
# MAGIC where 1 = 1 
# MAGIC and a.NUM_ICN is not null  
# MAGIC and a.ID_MEDICAID is not null 
# MAGIC and DERIVED not in ( 'T')
# MAGIC and not exists 
# MAGIC (
# MAGIC     select 1 
# MAGIC     from  ${catalog}.${schema_name}.${Inst_Analytics} tmp
# MAGIC     where a.NUM_ICN = tmp.NUM_ICN
# MAGIC       and a.NUM_DTL = tmp.NUM_DTL
# MAGIC       and a.ID_MEDICAID = tmp.ID_MEDICAID
# MAGIC )
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load Inst_Analytics table
# MAGIC %sql
# MAGIC INSERT INTO TABLE ${catalog}.${schema_name}.${Inst_Analytics}
# MAGIC SELECT * FROM ${catalog}.${schema_name}.${Inst_Analytics_stg}
# MAGIC ;
# MAGIC
