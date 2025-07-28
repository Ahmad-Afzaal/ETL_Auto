# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     ETL_Clms_Med_Analytics.                                                                                         *
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
#* 03/28/2024 CCRB70930/CO#43342  Jaime Zavala        Several Fields, Modified to use SUBSTRING instead of CAST with varchar.       *
#* 05/29/2024 CCRB70930/CO#43342  Jaime Zavala        QTY_UNITS_ALWD2 Mapped to field added/VEN100FA.ALWD_QTY.                      *
#*                                                    AMT_PAID_MCO2 SrcChng FieldNm- VEN12403FA.ALWD_OTH_PYR_AMT to VEN100FA.       *
#*                                                    THE_PAID_AMT.                                                                 *
#*                                                    AMT_PAID2 Chng FieldNm- PD_AMT to THE_DETAIL_PAID_AMT.                        *
#*                                                    CDE_ENC_TYPE modified to expect one character (N,Y,C,D,E)                     *
#* 06/05/2024 CCRB70930/CO#43342  Jaime Zavala        AMT_PAID_MCO2 Removed the SUM over sak_claim.                                 *
#* 06/20/2024 CCRB70930/CO#43342  Jaime Zavala        IND_HDR_DTL FieldAdded.- VEN100FA.HDR_DTL_PAID_IND.                           *
#*                                                    Added CleanDstntTblsFlag Defaul-F, T when using EDW_temp_ in table's names    *
#*                                                    and VE are Full Refresh.                                                      *
#* 07/19/2024 CCRB70930/CO#43342  Jaime Zavala        Fixed issue with the CleanDstntTblsFlag.                                      *
#* 07/24/2024 CCRB70930/CO#43342  Jaime Zavala        Added logic to use CleanDstntTblsFlag for Delta VE source type.               *
#* 08/01/2024 CCRB70930/CO#43342  Jaime Zavala        CODE_CLM_TYPE applied the following Mapping:                                  *
#*                                                    'PART C PROFESSIONAL' THEN 'C'                                                *
#*                                                    'PART A PROFESSIONAL' THEN 'A'                                                *
#* 09/25/2024 CCRB70930/CO#43342  Jaime Zavala        NA83 Mapped to VEN100FA.THE_PAID_AMT.                                         *
#* 09/25/2024 CCRB70930/CO#43342  Jaime Zavala        Removed Round fnc. to AMT_PAID_MCO2,AMT_PAID2,AMT_TPL_APPLD2.                 *
#*                                                    Add a criteria for CDE_PROC_PRIM,CDE_MODIFIER_1,CDE_MODIFIER_2,CDE_MODIFIER_3,*
#*                                                    CDE_MODIFIER_4,CDE_TOOTH_NBR.                                                 *
#* 01/16/2024 CCRB70930/CO#43342  Jaime Zavala        CDE_FUND_CODE set to '#########' when Null, to met BIAR expected value.       *
#* 01/27/2024  CCRB70930/CO#43342  Jaime Zavala       DTL_STS_CD added "PAID" as expected value.                                    *
#* 01/28/2024  CCRB70930/CO#43342  Jaime Zavala       DTL_STS_CD added "DENIED", and "REVERSED" as expected value.                  *
#* 03/03/2025  CCRB70930/CO#43342  Jaime Zavala       CDE_FUND_CODE set to '#########' when NULL, '0000000' or '000000'.            *
#* 04/15/2025 CCRB70930/CO#43342  Jaime Zavala        NewExtract.- VEN12501FA- Multi Provider Claim Extract.                        *
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
#************************************************************************************************************************************


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
  
dbutils.widgets.text('VEN100FA', 'EDW_VEN100FA_Staging')
dbutils.widgets.text('VEN10001FA', 'EDW_VEN10001FA_Staging')
dbutils.widgets.text('VEN10004FA', 'EDW_VEN10004FA_Staging')
dbutils.widgets.text('VEN10005FA', 'EDW_VEN10005FA_Staging')
dbutils.widgets.text('VEN10007FA', 'EDW_VEN10007FA_Staging')
dbutils.widgets.text('VEN12401FA', 'EDW_VEN12401FA_Staging')
dbutils.widgets.text('VEN12403FA', 'EDW_VEN12403FA_Staging')
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

dbutils.widgets.text('cl_med_stg', 'EDW_temp_cl_med_staging')
dbutils.widgets.text('cl_med', 'EDW_temp_cl_med')
dbutils.widgets.text('Med_Analytics_stg', 'EDW_temp_Med_Analytics_staging')
dbutils.widgets.text('Med_Analytics', 'EDW_temp_Med_Analytics')


# COMMAND ----------

# DBTITLE 1,Get Parameters Values
#-------------------
# EDW Staging Tables
#-------------------

VEN100FA = dbutils.widgets.get('VEN100FA')
VEN10001FA = dbutils.widgets.get('VEN10001FA')
VEN10004FA = dbutils.widgets.get('VEN10004FA')
VEN10005FA = dbutils.widgets.get('VEN10005FA')
VEN10007FA = dbutils.widgets.get('VEN10007FA')
VEN12401FA = dbutils.widgets.get('VEN12401FA')
VEN12403FA = dbutils.widgets.get('VEN12403FA')
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

cl_med_stg = dbutils.widgets.get('cl_med_stg')
cl_med = dbutils.widgets.get('cl_med')
Med_Analytics_stg = dbutils.widgets.get('Med_Analytics_stg')
Med_Analytics = dbutils.widgets.get('Med_Analytics')


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
print("schema mc:", schema_name_mc)
print("Clean Destination's Tables Flag:", CleanDstntTblsFlag)

print()
print("EDW Staging Tables")
print("------------------")
print(VEN100FA)
print(VEN10001FA)
print(VEN10004FA)
print(VEN10005FA)
print(VEN10007FA)
print(VEN12401FA)
print(VEN12403FA)
print(VEN12501FA)

print()
print("Provider Extracts Tables")
print("------------------------")
print(VEN117FA1)
print(VEN117FA4)
print(VEN117FA11)

print()
print("Manage Care Tables")
print("------------------")
print(hipaa_820_raw)

print()
print("Analytics Tables")
print("---------------")
print(cl_med_stg)
print(cl_med)
print(Med_Analytics_stg) 
print(Med_Analytics)

# COMMAND ----------

# DBTITLE 1,Truncate cl_med_staging table
# MAGIC %sql
# MAGIC TRUNCATE TABLE ${catalog}.${schema_name}.${cl_med_stg}
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load cl_med_staging table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${cl_med_stg}
# MAGIC SELECT DISTINCT
# MAGIC    --CODE WITH COLUMNS MAPPING GOES HERE
# MAGIC      SUBSTRING(TRIM(clm.ICN_NBR),1,15)                                                                        AS NUM_ICN1
# MAGIC     ,SUBSTRING(TRIM(INDICATOR_CLAIM),1,1)                                                                     AS IND_CLAIM
# MAGIC     ,SUBSTRING(TRIM(CODE_CLM_TYPE),1,3)                                                                       AS CDE_CLM_TYPE
# MAGIC     ,CASE UPPER(TRIM(clm.HDR_STS_CD)) WHEN 'DENIED'   THEN 'D'                                                
# MAGIC                                       WHEN 'PAID'     THEN 'P'                                                
# MAGIC                                       WHEN 'REVERSED' THEN 'P'                                                
# MAGIC                                       ELSE NULL                                                               
# MAGIC                                       END                                                                     AS CDE_HDR_STATUS
# MAGIC     ,NULL                                                                                                     AS CDE_PGM_HEALTH
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.AID_CTG_CD),1,7),'')                                                         AS CDE_AID_CATEGORY
# MAGIC     ,NULL                                                                                                     AS ID_CLERK                     --Removed Field
# MAGIC     ,NULL                                                                                                     AS CDE_PRESCRIPTION_ORIG        --MED NOT USE
# MAGIC     ,'C'                                                                                                      AS DERIVED1
# MAGIC     ,UPPER(TRIM(clm.ENC_TYP_CD))                                                                              AS CDE_ENC_TYPE
# MAGIC     ,NULL                                                                                                     AS NA1                          --EMPTY
# MAGIC     ,LPAD(TRIM(clm.MEDICAID_ID),12,'0')                                                                       AS ID_MEDICAID1
# MAGIC     ,CAST(clm.BIRTH_DT AS DATE)                                                                               AS DTE_BIRTH
# MAGIC     ,NULL                                                                                                     AS IND_BRAND_MED_NEC            --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS AMT_VACC_INCENTIVE           --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS NUM_PRIOR_AUTH               --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS NA2                          --EMPTY
# MAGIC     ,0                                                                                                        AS NA3                          --EMPTY
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmAdtlExtOthrPyr_CONTRACT_SUB_ID),1,5),'')                                      AS ID_CONTRACT_SUB
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.HIC_SUB_NBR),1,12),'')                                                       AS NUM_HIC_SUB
# MAGIC     ,NULL                                                                                                     AS NUM_CMS_ICN
# MAGIC     ,TRIM(ATTENDING_EXTERNALPROVID)                                                                           AS ATTENDING_EXTERNALPROVID     
# MAGIC     ,TRIM(ATTENDING_EXTERNALPROVIDQUALIFIER)                                                                  AS ATTENDING_EXTERNALPROVIDQUALIFIER     
# MAGIC     ,TRIM(ATTENDING_PROV_NAME)                                                                                AS ATTENDING_PROV_NAME     
# MAGIC     ,TRIM(ATTENDING_PROVID)                                                                                   AS ATTENDING_PROVID     
# MAGIC     ,TRIM(CAST(ATTENDING_SAK_PROV_ID AS STRING))                                                              AS ATTENDING_SAK_PROV_ID
# MAGIC     ,TRIM(BILLING_EXTERNALPROVID)                                                                             AS BILLING_EXTERNALPROVID
# MAGIC     ,TRIM(BILLING_EXTERNALPROVIDQUALIFIER)                                                                    AS BILLING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(BILLING_MEDICAID_ID)                                                                                AS BILLING_MEDICAID_ID
# MAGIC     ,TRIM(BILLING_PROV_NAME)                                                                                  AS BILLING_PROV_NAME
# MAGIC     ,COALESCE(SUBSTRING(TRIM(regexp_replace(ClmDiagExt_DIAG_CD,'[.]','')),1,7),'#######')                     AS CDE_DIAG_PRIM                --CRG Required Field. Pulled from 10001FA supplemental extraction
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmDiagExt_DIAG_CD2),1,7),'')                                                    AS CDE_DIAG_2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmDiagExt_DIAG_CD3),1,7),'')                                                    AS CDE_DIAG_3
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmDiagExt_DIAG_CD4),1,7),'')                                                    AS CDE_DIAG_4
# MAGIC     ,TRIM(BILLING_PROVID)                                                                                     AS BILLING_PROVID
# MAGIC     ,TRIM(CAST(BILLING_SAK_PROV_ID AS STRING))                                                                AS BILLING_SAK_PROV_ID
# MAGIC     ,NULL                                                                                                     AS NA15                         --EMPTY
# MAGIC     ,TRIM(BILLING_MMIS_PROV_TYP)                                                                              AS BILLING_MMIS_PROV_TYP
# MAGIC     ,NULL                                                                                                     AS CDE_SOI                      --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_LEVEL_OF_CARE            --Removed Field
# MAGIC     ,CASE TRIM(INDICATOR_CLAIM) WHEN 'F' THEN 60                                                              
# MAGIC 	                            WHEN 'E' THEN 70                                                              
# MAGIC 								ELSE NULL                                                                     
# MAGIC 	                            END                                                                         AS DERIVED2
# MAGIC     ,NULL                                                                                                     AS CDE_MDC                      --MED NOT USE
# MAGIC     ,CASE WHEN TRIM(clm.ICD_VERS_CD)='9' THEN '09'                                                            
# MAGIC           WHEN TRIM(clm.ICD_VERS_CD)='0' THEN '10'                                                            
# MAGIC           WHEN clm.ICD_VERS_CD IS NULL   THEN '##'                                                            
# MAGIC 		  ELSE TRIM(clm.ICD_VERS_CD)                                                                          
# MAGIC           END                                                                                                 AS CDE_ICD_VERSION              --CRG Required Field.
# MAGIC     ,TRIM(BILLING_PROV_TYPE_NM)                                                                               AS BILLING_PROV_TYPE_NM
# MAGIC     ,NULL                                                                                                     AS CDE_PATIENT_STATUS           --MED NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.EMERGENCY_CD),1,1),'')                                                       AS CDE_EMERGENCY                --Added Field
# MAGIC     ,NULL                                                                                                     AS CDE_ADMIT_SOURCE             --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_ROM                      --MED NOT USE
# MAGIC     ,CASE CAST(clm.ADMISSION_DT AS DATE)                                                                      
# MAGIC           WHEN CAST('1799-12-31' AS DATE) THEN CAST('0101-01-01' AS DATE)                                     
# MAGIC           WHEN CAST('1800-01-01' AS DATE) THEN CAST('0101-01-01' AS DATE)                                     
# MAGIC 	      ELSE COALESCE(CAST(clm.ADMISSION_DT AS DATE),CAST('0101-01-01' AS DATE)) END                      AS DTE_ADMISSION                --Added Field
# MAGIC     ,0                                                                                                        AS CDE_ADMIT_HOUR               --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_DRG                      --MED NOT USE
# MAGIC     ,0.00                                                                                                     AS AMT_BASE_DRG                 --MED NOT USE
# MAGIC     ,CASE CAST(clm.DISCHARGE_DT AS DATE)                                                                      
# MAGIC           WHEN CAST('1799-12-31' AS DATE) THEN CAST('2299-12-31' AS DATE)                                     
# MAGIC           WHEN CAST('1800-01-01' AS DATE) THEN CAST('2299-12-31' AS DATE)                                     
# MAGIC           ELSE CAST(clm.DISCHARGE_DT AS DATE) END                                                             AS DTE_DISCHARGE                --Added Field
# MAGIC     ,0                                                                                                        AS TIME_DISCHARGE               --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_COMPOUND_DOSAGE          --MED NOT USE
# MAGIC     ,0.00                                                                                                     AS AMT_DAY_OUTLIER              --MED NOT USE
# MAGIC     ,0.00                                                                                                     AS AMT_COST_OUTLIER             --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_MED_REC_NUM              --Removed Field
# MAGIC     ,NULL                                                                                                     AS CDE_PEER_GROUP               --MED NOT USE
# MAGIC     ,0.00                                                                                                     AS AMT_COST_INGREDIENT          --MED NOT USE
# MAGIC     ,TRIM(OPERATING_EXTERNALPROVID)                                                                           AS OPERATING_EXTERNALPROVID
# MAGIC     ,NULL                                                                                                     AS CDE_SOI_DISC                 --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_ROM_DISC                 --MED NOT USE
# MAGIC     ,COALESCE(SUBSTRING(clm.PRIOR_AUTH_NBR,1,30),'0')                                                         AS NUM_PA_REF
# MAGIC     ,0.00                                                                                                     AS AMT_TPL_SUBM1                --MED NOT USE
# MAGIC     ,0.00                                                                                                     AS AMT_TPL_APPLD1               --MED NOT USE
# MAGIC     ,0.00                                                                                                     AS AMT_PAID_MCO1                --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS NA19                         --EMPTY
# MAGIC     ,clm.HDR_DTL_PAID_IND                                                                                     AS IND_HDR_DTL                  --NewFld from Extract
# MAGIC     ,TRIM(OPERATING_EXTERNALPROVIDQUALIFIER)                                                                  AS OPERATING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,NULL                                                                                                     AS CDE_COND_1                   --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_COND_2                   --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_COND_3                   --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_COND_4                   --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_COND_5                   --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_COND_6                   --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_COND_7                   --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_COND_8                   --MED NOT USE
# MAGIC     ,TRIM(OPERATING_PROV_NAME)                                                                                AS OPERATING_PROV_NAME
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.PAY_ARR_CD),1,2),'')                                                         AS CDE_PAY_ARR1
# MAGIC     ,TRIM(OPERATING_PROVID)                                                                                   AS OPERATING_PROVID
# MAGIC     ,NULL                                                                                                     AS CDE_DRG_DISC                 --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_CLARIFICATION1           --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_CLARIFICATION2           --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_CLARIFICATION3           --MED NOT USE
# MAGIC     ,TRIM(CAST(OPERATING_SAK_PROV_ID AS STRING))                                                              AS OPERATING_SAK_PROV_ID
# MAGIC     ,TRIM(ORDERING_EXTERNALPROVID)                                                                            AS ORDERING_EXTERNALPROVID
# MAGIC     ,COALESCE(CAST(clm.BILL_DT AS DATE),CAST('0101-01-01' AS DATE))                                           AS DTE_BILLED
# MAGIC     ,NULL                                                                                                     AS CDE_RECIP_COUNTY
# MAGIC     ,0.0                                                                                                      AS AMT_REIMBURSED1              --MED NOT USE
# MAGIC     ,CAST(clm.PD_DT AS DATE)                                                                                  AS DTE_PAID1
# MAGIC     ,NULL                                                                                                     AS CDE_CLM_REGION               --Removed Field
# MAGIC     ,CAST(clm.HDR_FIRST_SVC_DT AS DATE)                                                                       AS DTE_FIRST_SVC1
# MAGIC     ,CAST(clm.HDR_LAST_SVC_DT AS DATE)                                                                        AS DTE_LAST_SVC1
# MAGIC     ,CAST(clm.ENTERED_SYS_DT AS DATE)                                                                         AS BATCH_DATE                   --Added Field / same as DTE_ENTERED_SYS (added on EDW extraction)
# MAGIC     ,TRIM(ORDERING_EXTERNALPROVIDQUALIFIER)                                                                   AS ORDERING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(ORDERING_PROV_NAME)                                                                                 AS ORDERING_PROV_NAME
# MAGIC     ,0.00                                                                                                     AS AMT_BILLED1                  --MED NOT USE
# MAGIC     ,0.00                                                                                                     AS QTY_UNITS_ALWD1              --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS NA27                         --EMPTY
# MAGIC     ,NULL                                                                                                     AS NUM_RECIP_AGE                --Removed Field
# MAGIC     ,TRIM(ORDERING_PROVID)                                                                                    AS ORDERING_PROVID
# MAGIC     ,0                                                                                                        AS NA29                         --EMPTY
# MAGIC     ,NULL                                                                                                     AS AMT_INTEREST                 --Removed Field
# MAGIC     ,CASE CAST(clm.MCO_ADJUD_DT AS DATE)                                                                      
# MAGIC           WHEN CAST('1799-12-31' AS DATE) THEN NULL                                                           
# MAGIC 	      ELSE CAST(clm.MCO_ADJUD_DT AS DATE) END                                                           AS DTE_MCO_ADJUD1               --Added Field
# MAGIC     ,NULL                                                                                                     AS ADR_ZIP_CODE
# MAGIC     ,NULL                                                                                                     AS ADR_ZIP_CODE_4
# MAGIC     ,TRIM(CAST(ORDERING_SAK_PROV_ID AS STRING))                                                               AS ORDERING_SAK_PROV_ID
# MAGIC     ,TRIM(PRESCRIBING_PROV_NAME)                                                                              AS PRESCRIBING_PROV_NAME
# MAGIC     ,NULL                                                                                                     AS NA32                         --EMPTY
# MAGIC     ,TRIM(PRESCRIBING_EXTERNALPROVID)                                                                         AS PRESCRIBING_EXTERNALPROVID
# MAGIC     ,NULL                                                                                                     AS NA34                         --EMPTY
# MAGIC     ,SUBSTRING(clm.SEX_CD,1,1)                                                                                AS CDE_SEX                      --CRG required Field.
# MAGIC     ,NULL                                                                                                     AS CDE_RACE
# MAGIC     ,TRIM(PRESCRIBING_EXTERNALPROVIDQUALIFIER)                                                                AS PRESCRIBING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(PRESCRIBING_PROVID)                                                                                 AS PRESCRIBING_PROVID     
# MAGIC     ,TRIM(CAST(PRESCRIBING_SAK_PROV_ID AS STRING))                                                            AS PRESCRIBING_SAK_PROV_ID
# MAGIC     ,0.00                                                                                                     AS NUM_WEIGHT                   --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS NUM_PAT_ACCT                 --Removed Field
# MAGIC     ,TRIM(REFERRING_EXTERNALPROVID)                                                                           AS REFERRING_EXTERNALPROVID
# MAGIC     ,TRIM(REFERRING_EXTERNALPROVIDQUALIFIER)                                                                  AS REFERRING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,0.00                                                                                                     AS AMT_SPENDDOWN1               --MED NOT USE
# MAGIC     ,TRIM(REFERRING_PROV_NAME)                                                                                AS REFERRING_PROV_NAME
# MAGIC     ,TRIM(REFERRING_PROVID)                                                                                   AS REFERRING_PROVID
# MAGIC     ,CAST(clm.COINSR_AMT AS NUMERIC)                                                                          AS AMT_COINSURANCE1
# MAGIC     ,NULL                                                                                                     AS CDE_LIV_ARNG                 --Removed Field
# MAGIC     ,TRIM(CAST(REFERRING_SAK_PROV_ID AS STRING))                                                              AS REFERRING_SAK_PROV_ID
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.ADJ_ICN_NBR),1,20),NULL)                                                     AS NUM_ADJ_ICN1
# MAGIC     ,NULL                                                                                                     AS NUM_VERSION_DRG              --MED NOT USE
# MAGIC     ,TRIM(RENDERING_EXTERNALPROVID)                                                                           AS RENDERING_EXTERNALPROVID
# MAGIC     ,NULL                                                                                                     AS NUM_RA
# MAGIC     ,NULL                                                                                                     AS ID_VENDOR
# MAGIC     ,TRIM(RENDERING_EXTERNALPROVIDQUALIFIER)                                                                  AS RENDERING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(RENDERING_PROV_NAME)                                                                                AS RENDERING_PROV_NAME
# MAGIC     ,NULL                                                                                                     AS NUM_WARRANT
# MAGIC     ,TRIM(RENDERING_PROVID)                                                                                   AS RENDERING_PROVID
# MAGIC     ,CAST(clm.ENTERED_SYS_DT AS DATE)                                                                         AS DTE_ENTERED_SYS              --Added Field
# MAGIC     ,0.00                                                                                                     AS AMT_PAT_LIAB1                --MED NOT USE
# MAGIC     ,0.00                                                                                                     AS AMT_APL_PAT_LIAB1            --MED NOT USE
# MAGIC     ,CAST(clm.REPORT_DTE AS DATE)                                                                             AS DERIVED3
# MAGIC     ,NULL                                                                                                     AS DTE_GENERIC
# MAGIC     ,TRIM(CAST(RENDERING_SAK_PROV_ID AS STRING))                                                              AS RENDERING_SAK_PROV_ID
# MAGIC     ,CAST(clm.PD_MCARE_AMT AS NUMERIC)                                                                        AS AMT_PAID_MCARE1
# MAGIC     ,NULL                                                                                                     AS CDE_OCCUR_1                  --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_OCCUR_2                  --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_OCCUR_3                  --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_OCCUR_4                  --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_OCCUR_5                  --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_OCCUR_6                  --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_OCCUR_7                  --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_OCCUR_8                  --MED NOT USE
# MAGIC     ,TRIM(SERVICE_EXTERNALPROVIDID)                                                                           AS SERVICE_EXTERNALPROVIDID
# MAGIC     ,NULL                                                                                                     AS DTE_OCCUR_1                  --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS DTE_OCCUR_2                  --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS DTE_OCCUR_3                  --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS DTE_OCCUR_4                  --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS DTE_OCCUR_5                  --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS DTE_OCCUR_6                  --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS DTE_OCCUR_7                  --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS DTE_OCCUR_8                  --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_EPSDT_FP                 --Removed Field
# MAGIC     ,NULL                                                                                                     AS IND_HYST1                    --MED NOT USE
# MAGIC     ,TRIM(SERVICE_EXTERNALPROVIDQUALIFIER)                                                                    AS SERVICE_EXTERNALPROVIDQUALIFIER
# MAGIC     ,NULL                                                                                                     AS IND_STERILIZATION1           --MED NOT USE
# MAGIC     ,TRIM(SERVICE_PROV_NAME)                                                                                  AS SERVICE_PROV_NAME
# MAGIC     ,NULL                                                                                                     AS IND_ABORTION1                --MED NOT USE
# MAGIC     ,TRIM(SERVICE_PROVID)                                                                                     AS SERVICE_PROVID
# MAGIC     ,TRIM(CAST(SERVICE_SAK_PROV AS STRING))                                                                   AS SERVICE_SAK_PROV
# MAGIC     ,TRIM(SERVICE_MMIS_PROV_TYP)                                                                              AS SERVICE_MMIS_PROV_TYP
# MAGIC     ,TRIM(SERVICE_PROV_TYPE_NM)                                                                               AS SERVICE_PROV_TYPE_NM
# MAGIC     ,CAST(clm.DEDUCT_AMT AS NUMERIC)                                                                          AS AMT_DEDUCT1
# MAGIC     ,0.00                                                                                                     AS AMT_MCARE_PAID               --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS NA55                         --EMPTY
# MAGIC     ,NULL                                                                                                     AS NA56                         --EMPTY
# MAGIC     ,SUBSTRING(TRIM(MedClmNDCDtl_RX_ID_NBR),1,12)                                                             AS NUM_PRESCRIPTION_ID          --PULLED FROM 10005FA supplemental extraction
# MAGIC     ,NULL                                                                                                     AS DTE_PRESCRIB                 --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS NA57                         --EMPTY
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.TCN_NBR),1,18),'')                                                           AS NUM_TCN1                     --Added Field
# MAGIC     ,0.00                                                                                                     AS AMT_ALWD1                    --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS NA58                         --EMPTY
# MAGIC     ,NULL                                                                                                     AS NA59                         --EMPTY
# MAGIC     ,0.00                                                                                                     AS AMT_NDC_PROFEE               --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS NA60                         --EMPTY
# MAGIC     ,0                                                                                                        AS NA61                         --EMPTY
# MAGIC     ,NULL                                                                                                     AS NA62                         --EMPTY
# MAGIC     ,NULL                                                                                                     AS NA63                         --EMPTY
# MAGIC     ,NULL                                                                                                     AS NA64                         --EMPTY
# MAGIC     ,0.00                                                                                                     AS AMT_CO_PAY1                  --MED NOT USE
# MAGIC     ,0.00                                                                                                     AS AMT_PAID1                    --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS NA65                         --EMPTY
# MAGIC     ,0                                                                                                        AS NA66                         --EMPTY
# MAGIC     ,'0'                                                                                                      AS NA67                         --EMPTY
# MAGIC     ,SUBSTRING(TRIM(clm.COS_ST_CD),1,2)                                                                       AS CDE_COS_ST
# MAGIC     ,NULL                                                                                                     AS CDE_COS_SUB
# MAGIC     ,NULL                                                                                                     AS NA68                         --EMPTY
# MAGIC     ,NULL                                                                                                     AS NA69                         --EMPTY
# MAGIC     ,NULL                                                                                                     AS ID_VOUCHER_RELATED
# MAGIC     ,NULL                                                                                                     AS NA70                         --EMPTY
# MAGIC     ,NULL                                                                                                     AS CDE_TYPE_OF_BILL1            --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_TYPE_OF_BILL2            --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_TYPE_OF_BILL3            --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS QTY_REFILL                   --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS NA71                         --EMPTY
# MAGIC     ,NULL                                                                                                     AS DTE_DISPENSE                 --MED NOT USE
# MAGIC     ,0.000                                                                                                    AS QTY_DISPENSE1                --MED NOT USE
# MAGIC     ,0                                                                                                        AS NUM_DAY_SUPPLY               --MED NOT USE
# MAGIC     ,CASE UPPER(TRIM(clm.DTL_STS_CD)) WHEN 'DENY' THEN 'D'                                      
# MAGIC                                       WHEN 'DENIED' THEN 'D'                                      
# MAGIC                                       WHEN 'OKAY' THEN 'P'                                      
# MAGIC                                       WHEN 'PAID' THEN 'P'                                      
# MAGIC                                       WHEN 'PEND' THEN 'S'                                      
# MAGIC                                       WHEN 'VOID' THEN 'V'                                      
# MAGIC                                       WHEN 'WARN' THEN 'W'                                      
# MAGIC                                       WHEN 'REVERSED' THEN 'R'                                      
# MAGIC                                       ELSE NULL                                                 
# MAGIC                                       END                                                                     AS CDE_DTL_STATUS
# MAGIC     ,SUBSTRING(TRIM(clm.PAY_ARR_CD),1,2)                                                                      AS CDE_PAY_ARR2
# MAGIC     ,COALESCE(clm.ALWD_QTY,0)                                                                                 AS QTY_UNITS_ALWD2       --Added Field
# MAGIC     ,0.000                                                                                                    AS QTY_DISPENSE2                --MED NOT USE
# MAGIC     ,CAST(clm.REPORT_DTE AS DATE)                                                                             AS DERIVED4
# MAGIC     ,NULL                                                                                                     AS NA72                         --EMPTY
# MAGIC     ,NULL                                                                                                     AS AMT_AWP                      --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_MCAR_COVRG               --Removed Field
# MAGIC     ,NULL                                                                                                     AS IND_PHARMACY_FAMILY_PLAN     --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS IND_REBATE_ELIG              --Removed Field
# MAGIC     ,NULL                                                                                                     AS CDE_DISP_STATUS
# MAGIC     ,NULL                                                                                                     AS NA73                         --EMPTY
# MAGIC     ,COALESCE(SUBSTRING(ClmAdtlExtEOB_CLAIM_ADTL_EOB_SK1,1,5),'')                                             AS CDE_EOB_1                    --Removed Field
# MAGIC     ,COALESCE(SUBSTRING(ClmAdtlExtEOB_CLAIM_ADTL_EOB_SK2,1,5),'')                                             AS CDE_EOB_2                    --Removed Field
# MAGIC     ,NULL                                                                                                     AS IND_PRICING
# MAGIC     ,COALESCE(CAST(clm.DTL_ALWD_AMT AS NUMERIC),0)                                                            AS AMT_ALWD2
# MAGIC     ,NULL                                                                                                     AS IND_STERILIZATION2
# MAGIC     ,NULL                                                                                                     AS NA74                         --EMPTY
# MAGIC     ,NULL                                                                                                     AS IND_HYST2
# MAGIC     ,NULL                                                                                                     AS NA75                         --EMPTY
# MAGIC     ,NULL                                                                                                     AS IND_ABORTION2
# MAGIC     ,NULL                                                                                                     AS NA76                         --EMPTY
# MAGIC     ,0                                                                                                        AS NUM_DAYS_COVD                --MED NOT USE
# MAGIC     ,0                                                                                                        AS NUM_DAYS_NCOVD               --MED NOT USE
# MAGIC     ,0                                                                                                        AS NUM_LEAVE_DAYS               --MED NOT USE
# MAGIC     ,CAST(ClmCostDebtAnlys_ING_COST AS NUMERIC)                                                               AS AMT_DRUG_UNIT_PRICE
# MAGIC     ,CASE WHEN (TRIM(CODE_CLM_TYPE)='B' or                                                                    
# MAGIC 	            TRIM(CODE_CLM_TYPE)='D' or                                                                    
# MAGIC 				TRIM(CODE_CLM_TYPE)='M')                                                                      
# MAGIC           THEN CAST((clm.DTL_CO_PAY_AMT *100) AS INT)                                                         
# MAGIC           ELSE 0                                                                                              
# MAGIC           END                                                                                                 AS AMT_CO_PAY2                  --Fixed Issue#39
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.COPAY_REASON_CD),1,4),'')                                                    AS CDE_COPAY_REASON
# MAGIC     ,CAST(clm.DTL_NBR AS NUMERIC)                                                                             AS NUM_DTL
# MAGIC     ,CAST(clm.DTL_FIRST_SVC_DT AS DATE)                                                                       AS DTE_FIRST_SVC2
# MAGIC     ,CAST(clm.DTL_LAST_SVC_DT AS DATE)                                                                        AS DTE_LAST_SVC2
# MAGIC     ,CASE WHEN clm.UNT_BILL_QTY < 0                                                                           
# MAGIC           THEN '-' || LPAD(CAST(COALESCE(clm.UNT_BILL_QTY,0) * -100 AS varchar(8)), 8,'0')                        
# MAGIC           ELSE LPAD( CAST(COALESCE(clm.UNT_BILL_QTY,0) * 100 AS varchar(9)), 9,'0') END                       AS QTY_UNITS_BILLED             -- UNT_BILL_QTY's Logic for negative values e.g. Reversed Hdr Status Code or negative qty.
# MAGIC     ,NULL                                                                                                     AS CDE_REVENUE                  --MED NOT USE
# MAGIC     ,CASE WHEN clm.DTL_BILL_AMT < 0                                                                           
# MAGIC           THEN '-'||LPAD(CAST(COALESCE(clm.DTL_BILL_AMT * -1,0) AS varchar(14)), 14,'0')                           
# MAGIC           ELSE LPAD(CAST(COALESCE(clm.DTL_BILL_AMT,0) AS varchar(15)), 15,'0') END                            AS AMT_BILLED2                  -- DTL_BILL_AMT's Logic for negative values e.g. Reversed Hdr Status Code or negative amt.
# MAGIC     ,0.00                                                                                                     AS AMT_NON_COVERED              --MED NOT USE
# MAGIC     ,COALESCE(clm.THE_PAID_AMT, 0)                                                                            AS AMT_PAID_MCO2
# MAGIC     ,NULL                                                                                                     AS NA77                         --EMPTY
# MAGIC     ,CASE WHEN (TRIM(CODE_CLM_TYPE)='B' or                                                                    
# MAGIC 	          TRIM(CODE_CLM_TYPE)='D' or                                                                    
# MAGIC 		    TRIM(CODE_CLM_TYPE)='M')                                                                      
# MAGIC           THEN CAST((clm.THE_DETAIL_PAID_AMT *100) AS INT)                                                                 
# MAGIC           ELSE 0                                                                                              
# MAGIC           END                                                                                                 AS AMT_PAID2                    --Fixed Issue#37
# MAGIC     ,CAST(clm.PD_DT AS DATE)                                                                                  AS DTE_PAID2
# MAGIC     ,NULL                                                                                                     AS AMT_PAT_LIAB2               --Removed Field
# MAGIC     ,CAST((COALESCE(clm.TPL_AMT,0)*100) AS NUMERIC)                                                           AS AMT_TPL_APPLD2
# MAGIC     ,TRIM(clm.IS_NON_DUPLICATE_IND)                                                                           AS IS_NON_DUPLICATE_IND
# MAGIC     ,TRIM(clm.CLAIM_ACTIVE_IND)                                                                               AS CLAIM_ACTIVE_IND
# MAGIC     ,NULL                                                                                                     AS AMT_APL_PAT_LIAB2
# MAGIC     ,TRIM(clm.LAST_CLAIM_IND)                                                                                 AS LAST_CLAIM_IND
# MAGIC     ,NULL                                                                                                     AS AMT_TPL_SUBM2                --Removed Field
# MAGIC     ,CASE WHEN TRIM(clm.TOOTH_CD_NBR) NOT regexp '^[0-9]+$'
# MAGIC               THEN TRIM(clm.TOOTH_CD_NBR)
# MAGIC           WHEN TRIM(clm.TOOTH_CD_NBR) = ''
# MAGIC               THEN '##'             
# MAGIC           ELSE COALESCE(LPAD(TRIM(clm.TOOTH_CD_NBR),2,'0'),'##') END                                          AS CDE_TOOTH_NBR                --Added Field
# MAGIC     ,COALESCE(CASE WHEN TRIM(DntlClmDtl_TTH_SRFC_CD1)='' THEN '#' ELSE TRIM(DntlClmDtl_TTH_SRFC_CD1) END,'#') AS CDE_TOOTH_SURFACE_1
# MAGIC     ,COALESCE(CASE WHEN TRIM(DntlClmDtl_TTH_SRFC_CD2)='' THEN '#' ELSE TRIM(DntlClmDtl_TTH_SRFC_CD2) END,'#') AS CDE_TOOTH_SURFACE_2
# MAGIC     ,COALESCE(CASE WHEN TRIM(DntlClmDtl_TTH_SRFC_CD3)='' THEN '#' ELSE TRIM(DntlClmDtl_TTH_SRFC_CD3) END,'#') AS CDE_TOOTH_SURFACE_3
# MAGIC     ,COALESCE(CASE WHEN TRIM(DntlClmDtl_TTH_SRFC_CD4)='' THEN '#' ELSE TRIM(DntlClmDtl_TTH_SRFC_CD4) END,'#') AS CDE_TOOTH_SURFACE_4
# MAGIC     ,COALESCE(CASE WHEN TRIM(DntlClmDtl_TTH_SRFC_CD5)='' THEN '#' ELSE TRIM(DntlClmDtl_TTH_SRFC_CD5) END,'#') AS CDE_TOOTH_SURFACE_5
# MAGIC     ,COALESCE(CASE WHEN TRIM(DntlClmDtl_TTH_SRFC_CD6)='' THEN '#' ELSE TRIM(DntlClmDtl_TTH_SRFC_CD6) END,'#') AS CDE_TOOTH_SURFACE_6
# MAGIC     ,TRIM(clm.IS_DKP_IND)                                                                                     AS IS_DKP_IND
# MAGIC     ,CAST(DATE_FORMAT(CAST(ClmAdtlExtOthrPyr_MCO_ADJUD_DTL_DT AS DATE),'MM/dd/y') AS varchar(10))             AS DTE_MCO_ADJUD2
# MAGIC     ,NULL                                                                                                     AS AMT_SPENDDOWN2
# MAGIC     ,NULL                                                                                                     AS IND_EPSDT                    --Removed Field
# MAGIC     ,NULL                                                                                                     AS NA82                         --EMPTY
# MAGIC     ,CAST(clm.THE_PAID_AMT AS STRING)                                                                         AS NA83                         --New Mapping Sep/2024
# MAGIC     ,SUBSTRING(clm.POS_CD,1,2)                                                                                AS CDE_POS                      --Added Field
# MAGIC     ,NULL                                                                                                     AS AMT_REIMBURSED2              --Removed Field
# MAGIC     ,CAST(clm.PD_MCARE_AMT AS NUMERIC)                                                                        AS AMT_PAID_MCARE2
# MAGIC     ,CAST(clm.COINSR_AMT AS varchar(14))                                                                      AS AMT_COINSURANCE2
# MAGIC     ,NULL                                                                                                     AS QTY_DAYS_COINSURANCE         --Removed Field
# MAGIC     ,SUBSTRING(TRIM(MedClmNDCDtl_NDC_CD),1,11)                                                                AS CDE_NDC                      --PULLED FROM 10005FA supplemental extraction
# MAGIC     ,CAST(clm.DEDUCT_AMT AS NUMERIC)                                                                          AS AMT_DEDUCT2
# MAGIC     ,NULL                                                                                                     AS CDE_THERA_CLS_AHFS           --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_THERA_CLS_SPEC           --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS NA84                         --EMPTY
# MAGIC     ,NULL                                                                                                     AS NA85                         --EMPTY
# MAGIC     ,CASE WHEN clm.PROC_PRIM_CD='-1' THEN NULL                                                                
# MAGIC           WHEN TRIM(clm.PROC_PRIM_CD) = '' THEN '######'                                                     
# MAGIC           ELSE SUBSTRING(COALESCE(TRIM(clm.PROC_PRIM_CD),'######'),1,6)                                       
# MAGIC           END                                                                                                 AS CDE_PROC_PRIM
# MAGIC     ,CASE WHEN TRIM(clm.MODIFIER_1_CD) = '' THEN '##'                                                         
# MAGIC          ELSE COALESCE(SUBSTRING(TRIM(clm.MODIFIER_1_CD),1,2),'##') END                                       AS CDE_MODIFIER_1
# MAGIC     ,CASE WHEN TRIM(clm.MODIFIER_2_CD) = '' THEN '##'                                                         
# MAGIC          ELSE COALESCE(SUBSTRING(TRIM(clm.MODIFIER_2_CD),1,2),'##') END                                       AS CDE_MODIFIER_2
# MAGIC     ,CASE WHEN TRIM(clm.MODIFIER_3_CD) = '' THEN '##'                                                        
# MAGIC          ELSE COALESCE(SUBSTRING(TRIM(clm.MODIFIER_3_CD),1,2),'##') END                                       AS CDE_MODIFIER_3
# MAGIC     ,CASE WHEN TRIM(clm.MODIFIER_4_CD) = '' THEN '##'                                                        
# MAGIC          ELSE COALESCE(SUBSTRING(TRIM(clm.MODIFIER_4_CD),1,2),'##') END                                       AS CDE_MODIFIER_4
# MAGIC     ,0.00                                                                                                     AS NA86                         --EMPTY
# MAGIC     ,CASE WHEN TRIM(clm.FUND_CD) = '000000' OR TRIM(clm.FUND_CD) = '0000000' OR 
# MAGIC                clm.FUND_CD IS NULL                                              THEN '#########'
# MAGIC           ELSE COALESCE(SUBSTRING(clm.FUND_CD,1,9),'#########') END                                           AS CDE_FUND_CODE    --Added Field
# MAGIC     ,NULL                                                                                                     AS CDE_RATE_TYPE
# MAGIC     ,SUBSTRING(TRIM(clm.ICN_NBR),1,15)                                                                        AS NUM_ICN2
# MAGIC     ,CASE WHEN TRIM(clm.ADJ_ICN_NBR) = '' THEN NULL                                                           
# MAGIC           ELSE COALESCE(SUBSTRING(TRIM(clm.ADJ_ICN_NBR),1,20),NULL)                                           
# MAGIC           END                                                                                                 AS NUM_ADJ_ICN2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.TCN_NBR),1,18),'')                                                           AS NUM_TCN2                     --Added Field
# MAGIC     ,SUBSTRING(TRIM(clm.MEDICAID_ID),1,12)                                                                    AS ID_MEDICAID2
# MAGIC     ,NULL                                                                                                     AS ID_PROVIDER_MCAID1           --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS ID_PROVIDER_NPI1             --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_PROV_TYPE_PRIM_BLANK_1   --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_SVC_COUNTY1              --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_TAXONOMY1                --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_PROV_SPEC_PRIM1          --MED NOT USE
# MAGIC     ,SUBSTRING(TRIM(BillProv_MEDICAID_ID),1,10)                                                               AS ID_PROVIDER_MCAID2
# MAGIC     ,SUBSTRING(TRIM(BillProv_NPI),1,10)                                                                       AS ID_PROVIDER_NPI2
# MAGIC     ,SUBSTRING(TRIM(BillProv_MMIS_PROV_TYP_ID),1,2)                                                           AS BILLING_CDE_PROV_TYPE_PRIM
# MAGIC     ,NULL                                                                                                     AS CDE_PROV_PGM1                --MED NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_PT_CNTY),1,10),'')                                                      AS CDE_SVC_COUNTY2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_TXNMY_CD),1,10),'')                                                     AS CDE_TAXONOMY2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_PRIMARY_SPCLTY_CD),1,3),'')                                             AS CDE_PROV_SPEC_PRIM2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RndngProv_MEDICAID_ID),1,10),'')                                                 AS ID_PROVIDER_MCAID3           --Fixed ISSUE#60
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RndngProv_NPI),1,10),'')                                                         AS ID_PROVIDER_NPI3             --Fixed ISSUE#60
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RndngProv_MMIS_PROV_TYP_ID),1,2),'')                                             AS RENDERING_CDE_PROV_TYPE_PRIM
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RndngProv_PROGRAM_ID),1,5),'')                                                   AS CDE_PROV_PGM2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RndngProv_SL_CNTY),1,10),'')                                                     AS CDE_SVC_COUNTY3
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RndngProv_TXNMY_CD),1,10),'')                                                    AS CDE_TAXONOMY3                --Fixed ISSUE#60
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RndngProv_PRIMARY_SPCLTY_CD),1,3),'')                                            AS CDE_PROV_SPEC_PRIM3
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RfrngProv_MEDICAID_ID),1,10),'')                                                 AS ID_PROVIDER_MCAID4
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RfrngProv_NPI),1,10),'')                                                         AS ID_PROVIDER_NPI4
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RfrngProv_MMIS_PROV_TYP_ID),1,2),'##')                                           AS REFERRING_CDE_PROV_TYPE_PRIM
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RfrngProv_PRIMARY_SPCLTY_CD),1,3),'')                                            AS CDE_PROV_SPEC_PRIM4
# MAGIC     ,NULL                                                                                                     AS ID_PROVIDER_MCAID5           --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS ID_PROVIDER_NPI5             --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_PROV_TYPE_PRIM_BLANK_2   --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_PROV_SPEC_PRIM5          --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS ID_PROVIDER_MCAID6           --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS ID_PROVIDER_NPI6             --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_PROV_TYPE_PRIM_BLANK_3   --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_PROV_SPEC_PRIM6          --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS ID_PROVIDER_MCAID7           --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS ID_PROVIDER_NPI7             --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_PROV_TYPE_PRIM_BLANK_4   --MED NOT USE
# MAGIC     ,NULL                                                                                                     AS CDE_PROV_SPEC_PRIM7          --MED NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(MCP_MEDICAID_ID),1,10),'')                                                       AS ID_PROVIDER_MCAID8           --New mapped field / Fixed ISSUE#60
# MAGIC     ,COALESCE(SUBSTRING(TRIM(MCP_MMIS_PROV_TYP_ID),1,2),'')                                                   AS MCP_CDE_PROV_TYPE_PRIM
# MAGIC     ,COALESCE(SUBSTRING(TRIM(MCP_PRIMARY_SPCLTY_CD),1,3),'')                                                  AS CDE_PROV_SPEC_PRIM8
# MAGIC     ,NULL                                                                                                     AS VISIT_START                  --Removed Field
# MAGIC     ,NULL                                                                                                     AS VISIT_END                    --Removed Field
# MAGIC     ,NULL                                                                                                     AS IND_LESS                     --Removed Field
# MAGIC     
# MAGIC from
# MAGIC (
# MAGIC         select ClmMain.*
# MAGIC 		,CASE UPPER(TRIM(ClmMain.CLM_TYP_CD)) WHEN 'PART C PROFESSIONAL' THEN 'C'
# MAGIC                                                   WHEN 'DENTAL'              THEN 'D'
# MAGIC                                                   WHEN 'PART B PROFESSIONAL' THEN 'B'
# MAGIC                                                   WHEN 'PROFESSIONAL'        THEN 'M'
# MAGIC                                                   WHEN 'PART A PROFESSIONAL' THEN 'A'
# MAGIC                                                   ELSE ''
# MAGIC                                                   END  AS CODE_CLM_TYPE
# MAGIC         ,CASE UPPER(TRIM(ClmMain.CLAIM_IND)) WHEN 'N' THEN 'F'
# MAGIC                                              WHEN 'Y' THEN 'E'
# MAGIC                                              ELSE SUBSTRING(TRIM(ClmMain.CLAIM_IND),1,1)
# MAGIC                                              END  AS INDICATOR_CLAIM
# MAGIC         ,BillProv_PT_CNTY
# MAGIC         ,RndngProv_SL_CNTY
# MAGIC         ,RndngProv_MEDICAID_ID
# MAGIC         ,RndngProv_MMIS_PROV_TYP_ID
# MAGIC         ,RndngProv_PRIMARY_SPCLTY_CD
# MAGIC         ,RfrngProv_MEDICAID_ID
# MAGIC         ,RfrngProv_MMIS_PROV_TYP_ID
# MAGIC         ,RfrngProv_PRIMARY_SPCLTY_CD
# MAGIC         ,BillProv_TXNMY_CD
# MAGIC         ,RndngProv_NPI
# MAGIC         ,RndngProv_TXNMY_CD
# MAGIC         ,RfrngProv_NPI
# MAGIC         ,RndngProv_PROGRAM_ID
# MAGIC 	  ,BillProv_MEDICAID_ID
# MAGIC 	  ,BillProv_NPI
# MAGIC 	  ,BillProv_MMIS_PROV_TYP_ID
# MAGIC         ,ClmAdtlExtOthrPyr_CONTRACT_SUB_ID
# MAGIC         ,ClmDiagExt_DIAG_CD
# MAGIC         ,ClmDiagExt_DIAG_CD2
# MAGIC         ,ClmDiagExt_DIAG_CD3
# MAGIC         ,ClmDiagExt_DIAG_CD4
# MAGIC         ,MedClmNDCDtl_RX_ID_NBR
# MAGIC         ,MedClmNDCDtl_NDC_CD
# MAGIC         ,ClmCostDebtAnlys_ING_COST
# MAGIC         ,DntlClmDtl_TTH_SRFC_CD1
# MAGIC         ,DntlClmDtl_TTH_SRFC_CD2
# MAGIC         ,DntlClmDtl_TTH_SRFC_CD3
# MAGIC         ,DntlClmDtl_TTH_SRFC_CD4
# MAGIC         ,DntlClmDtl_TTH_SRFC_CD5
# MAGIC         ,DntlClmDtl_TTH_SRFC_CD6
# MAGIC         ,ClmAdtlExtOthrPyr_MCO_ADJUD_DTL_DT
# MAGIC 	  ,BillProv_PRIMARY_SPCLTY_CD
# MAGIC 	  ,ClmAdtlExtEOB_CLAIM_ADTL_EOB_SK1
# MAGIC 	  ,ClmAdtlExtEOB_CLAIM_ADTL_EOB_SK2
# MAGIC 	  ,ClmAdtlExtOthrPyr_ALWD_OTH_PYR_AMT
# MAGIC         ,MCP_MEDICAID_ID
# MAGIC         ,MCP_MMIS_PROV_TYP_ID
# MAGIC         ,MCP_PRIMARY_SPCLTY_CD
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
# MAGIC         left join                                                               --CRG required Field.
# MAGIC         (
# MAGIC             select distinct SAK_CLAIM
# MAGIC                   ,MAX(CASE WHEN TRIM(DIAG_SEQ_CD) = '1' THEN DIAG_CD ELSE '' END) over(partition by SAK_CLAIM) AS ClmDiagExt_DIAG_CD
# MAGIC                   ,MAX(CASE WHEN TRIM(DIAG_SEQ_CD) = '2' THEN DIAG_CD ELSE '' END) over(partition by SAK_CLAIM) AS ClmDiagExt_DIAG_CD2
# MAGIC                   ,MAX(CASE WHEN TRIM(DIAG_SEQ_CD) = '3' THEN DIAG_CD ELSE '' END) over(partition by SAK_CLAIM) AS ClmDiagExt_DIAG_CD3
# MAGIC                   ,MAX(CASE WHEN TRIM(DIAG_SEQ_CD) = '4' THEN DIAG_CD ELSE '' END) over(partition by SAK_CLAIM) AS ClmDiagExt_DIAG_CD4
# MAGIC                   from
# MAGIC                   (
# MAGIC                      select *, row_number() over(partition by SAK_CLAIM order by TRIM(DIAG_SEQ_CD) ) rnum
# MAGIC                      from ${catalog}.${schema_name}.${VEN10001FA}
# MAGIC                   )t
# MAGIC         ) ClmDiagExt
# MAGIC         on ClmMain.SAK_CLAIM = ClmDiagExt.SAK_CLAIM
# MAGIC         left join
# MAGIC         (
# MAGIC             select distinct SAK_CLAIM
# MAGIC                   ,MAX(CASE WHEN TTH_SRFC_SEQ_NBR = 1 THEN TTH_SRFC_CD ELSE '' END) over(partition by SAK_CLAIM) AS DntlClmDtl_TTH_SRFC_CD1
# MAGIC                   ,MAX(CASE WHEN TTH_SRFC_SEQ_NBR = 2 THEN TTH_SRFC_CD ELSE '' END) over(partition by SAK_CLAIM) AS DntlClmDtl_TTH_SRFC_CD2
# MAGIC                   ,MAX(CASE WHEN TTH_SRFC_SEQ_NBR = 3 THEN TTH_SRFC_CD ELSE '' END) over(partition by SAK_CLAIM) AS DntlClmDtl_TTH_SRFC_CD3
# MAGIC                   ,MAX(CASE WHEN TTH_SRFC_SEQ_NBR = 4 THEN TTH_SRFC_CD ELSE '' END) over(partition by SAK_CLAIM) AS DntlClmDtl_TTH_SRFC_CD4
# MAGIC                   ,MAX(CASE WHEN TTH_SRFC_SEQ_NBR = 5 THEN TTH_SRFC_CD ELSE '' END) over(partition by SAK_CLAIM) AS DntlClmDtl_TTH_SRFC_CD5
# MAGIC                   ,MAX(CASE WHEN TTH_SRFC_SEQ_NBR = 6 THEN TTH_SRFC_CD ELSE '' END) over(partition by SAK_CLAIM) AS DntlClmDtl_TTH_SRFC_CD6
# MAGIC                   from
# MAGIC                   (
# MAGIC                      select *, row_number() over(partition by SAK_CLAIM order by TTH_SRFC_SEQ_NBR ) rnum
# MAGIC                      from ${catalog}.${schema_name}.${VEN10004FA}
# MAGIC                   )t
# MAGIC         ) DntlClmDtl
# MAGIC         on ClmMain.SAK_CLAIM = DntlClmDtl.SAK_CLAIM
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_CLAIM, DTL_NBR
# MAGIC                   ,MAX(RX_ID_NBR) AS MedClmNDCDtl_RX_ID_NBR
# MAGIC                   ,MAX(NDC_CD)    AS MedClmNDCDtl_NDC_CD
# MAGIC               from ${catalog}.${schema_name}.${VEN10005FA}
# MAGIC              group by 1,2
# MAGIC         ) MedClmNDCDtl
# MAGIC         on ClmMain.SAK_CLAIM = MedClmNDCDtl.SAK_CLAIM and ClmMain.DTL_NBR = MedClmNDCDtl.DTL_NBR
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_CLAIM
# MAGIC                   ,ING_COST AS ClmCostDebtAnlys_ING_COST
# MAGIC               from ${catalog}.${schema_name}.${VEN10007FA}
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
# MAGIC                   ,SL_CNTY           AS RndngProv_SL_CNTY
# MAGIC                   ,MEDICAID_ID       AS RndngProv_MEDICAID_ID
# MAGIC 				  ,NPI               AS RndngProv_NPI
# MAGIC                   ,MMIS_PROV_TYP_ID  AS RndngProv_MMIS_PROV_TYP_ID
# MAGIC                   ,PRIMARY_SPCLTY_CD AS RndngProv_PRIMARY_SPCLTY_CD
# MAGIC               from ${catalog}.${schema_name}.${VEN117FA1}
# MAGIC         ) ProvExtrct2
# MAGIC         on ClmMain.RPA_PROV_SAK_ID = ProvExtrct2.SAK_PROV
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_PROV
# MAGIC                   ,MEDICAID_ID       AS RfrngProv_MEDICAID_ID
# MAGIC                   ,NPI               AS RfrngProv_NPI
# MAGIC                   ,MMIS_PROV_TYP_ID  AS RfrngProv_MMIS_PROV_TYP_ID
# MAGIC                   ,PRIMARY_SPCLTY_CD AS RfrngProv_PRIMARY_SPCLTY_CD
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
# MAGIC         on ClmMain.sak_prov = ProvTaxnmyExtrct.sak_prov
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_PROV
# MAGIC                   ,TXNMY_CD AS RndngProv_TXNMY_CD
# MAGIC               from ${catalog}.${schema_name}.${VEN117FA4}
# MAGIC         ) ProvTaxnmyExtrct2
# MAGIC         on ClmMain.RPA_PROV_SAK_ID = ProvTaxnmyExtrct2.sak_prov
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_PROV
# MAGIC                   ,PROGRAM_ID        AS RndngProv_PROGRAM_ID
# MAGIC               from ${catalog}.${schema_name}.${VEN117FA11}
# MAGIC         ) ProvCntrctExtrct
# MAGIC         on ClmMain.RPA_PROV_SAK_ID = ProvCntrctExtrct.sak_prov
# MAGIC         --where ClmMain.CLM_TYP_CD in('B','D','M')
# MAGIC 		where UPPER(TRIM(ClmMain.CLM_TYP_CD)) in ('PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL')
# MAGIC )clm
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Full Refresh Or Delta Step.- Truncate/Delete Main cl_med and Med_Analytics Tables
if CleanDstntTblsFlag == 'T':
    # Logic to process Full Refresh VE Source Type
    #
    sql_out = spark.sql(f"""
                        TRUNCATE TABLE {catalog}.{schema_name}.{cl_med}
                        ;
                        """)
    display(sql_out)
    sql_out = spark.sql(f"""
                        TRUNCATE TABLE {catalog}.{schema_name}.{Med_Analytics}
                        ;
                        """)
    display(sql_out)
else:
    # Logic to process Delta VE source type
    #
    sql_out = spark.sql(f"""
                        DELETE 
                         FROM {catalog}.{schema_name}.{cl_med} t1
                        WHERE TRUE
                          AND EXISTS
                              ( SELECT 1
                                  FROM {catalog}.{schema_name}.{cl_med_stg} t2
                                 WHERE TRUE
                                   AND t2.NUM_ICN1 = t1.NUM_ICN1
                                   AND t2.NUM_DTL  = t1.NUM_DTL
                              )
                        ;
                        """)
    display(sql_out)
    sql_out = spark.sql(f"""
                        DELETE 
                         FROM {catalog}.{schema_name}.{Med_Analytics} t1
                        WHERE TRUE
                          AND EXISTS
                              ( SELECT 1
                                  FROM {catalog}.{schema_name}.{cl_med_stg} t2
                                 WHERE TRUE
                                   AND t2.NUM_ICN1 = t1.NUM_ICN1
                                   AND t2.NUM_DTL  = t1.NUM_DTL
                              )
                        ;
                        """)
    display(sql_out)


# COMMAND ----------

# DBTITLE 1,Load cl_med table
# MAGIC %sql 
# MAGIC INSERT INTO TABLE ${catalog}.${schema_name}.${cl_med}
# MAGIC SELECT * FROM ${catalog}.${schema_name}.${cl_med_stg}
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Truncate Med_Analytics_Staging table
# MAGIC %sql
# MAGIC TRUNCATE TABLE ${catalog}.${schema_name}.${Med_Analytics_stg}
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Load Med_Analytics_Staging table
# MAGIC %sql 
# MAGIC INSERT INTO ${catalog}.${schema_name}.${Med_Analytics_stg}
# MAGIC SELECT DISTINCT
# MAGIC    --CODE WITH COLUMNS MAPPING GOES HERE
# MAGIC      TRIM(NUM_ICN1) AS NUM_ICN1
# MAGIC     ,TRIM(IND_CLAIM) AS IND_CLAIM
# MAGIC     ,TRIM(CDE_CLM_TYPE) AS CDE_CLM_TYPE
# MAGIC     ,TRIM(CDE_HDR_STATUS) AS CDE_HDR_STATUS
# MAGIC     ,TRIM(CDE_PGM_HEALTH) AS CDE_PGM_HEALTH
# MAGIC     ,TRIM(CDE_AID_CATEGORY) AS CDE_AID_CATEGORY
# MAGIC     ,TRIM(ID_CLERK) AS ID_CLERK
# MAGIC     ,TRIM(CDE_PRESCRIPTION_ORIG) AS CDE_PRESCRIPTION_ORIG
# MAGIC     ,TRIM(DERIVED1) AS DERIVED1
# MAGIC     ,TRIM(CDE_ENC_TYPE) AS CDE_ENC_TYPE
# MAGIC     ,TRIM(NA1) AS NA1
# MAGIC     ,TRIM(ID_MEDICAID1) AS ID_MEDICAID1
# MAGIC     ,DTE_BIRTH
# MAGIC     ,TRIM(IND_BRAND_MED_NEC) AS IND_BRAND_MED_NEC
# MAGIC     ,ROUND(AMT_VACC_INCENTIVE/100,2.0) as AMT_VACC_INCENTIVE
# MAGIC     ,TRIM(NUM_PRIOR_AUTH) AS NUM_PRIOR_AUTH
# MAGIC     ,TRIM(NA2) AS NA2
# MAGIC     ,NA3
# MAGIC     ,TRIM(ID_CONTRACT_SUB) AS ID_CONTRACT_SUB
# MAGIC     ,TRIM(NUM_HIC_SUB) AS NUM_HIC_SUB
# MAGIC     ,TRIM(NUM_CMS_ICN) AS NUM_CMS_ICN
# MAGIC     ,TRIM(ATTENDING_EXTERNALPROVID) AS ATTENDING_EXTERNALPROVID
# MAGIC     ,TRIM(ATTENDING_EXTERNALPROVIDQUALIFIER) AS ATTENDING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(BILLING_MMIS_PROV_TYP) AS BILLING_MMIS_PROV_TYP
# MAGIC     ,TRIM(ATTENDING_PROVID) AS ATTENDING_PROVID
# MAGIC     ,TRIM(ATTENDING_SAK_PROV_ID) AS ATTENDING_SAK_PROV_ID
# MAGIC     ,TRIM(BILLING_EXTERNALPROVID) AS BILLING_EXTERNALPROVID
# MAGIC     ,TRIM(BILLING_EXTERNALPROVIDQUALIFIER) AS BILLING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(BILLING_MEDICAID_ID) AS BILLING_MEDICAID_ID
# MAGIC     ,TRIM(BILLING_PROV_NAME) AS BILLING_PROV_NAME
# MAGIC     ,TRIM(CDE_DIAG_PRIM) AS CDE_DIAG_PRIM
# MAGIC     ,TRIM(CDE_DIAG_2) AS CDE_DIAG_2
# MAGIC     ,TRIM(CDE_DIAG_3) AS CDE_DIAG_3
# MAGIC     ,TRIM(CDE_DIAG_4) AS CDE_DIAG_4
# MAGIC     ,TRIM(BILLING_PROVID) AS BILLING_PROVID
# MAGIC     ,TRIM(BILLING_SAK_PROV_ID) AS BILLING_SAK_PROV_ID
# MAGIC     ,NA15
# MAGIC     ,TRIM(BILLING_MMIS_PROV_TYP) AS BILLING_MMIS_PROV_TYP
# MAGIC     ,TRIM(CDE_SOI) AS CDE_SOI
# MAGIC     ,TRIM(CDE_LEVEL_OF_CARE) AS CDE_LEVEL_OF_CARE
# MAGIC     ,TRIM(DERIVED2) AS DERIVED2
# MAGIC     ,TRIM(CDE_MDC) AS CDE_MDC
# MAGIC     ,TRIM(CDE_ICD_VERSION) AS CDE_ICD_VERSION
# MAGIC     ,TRIM(BILLING_PROV_TYPE_NM) AS BILLING_PROV_TYPE_NM
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
# MAGIC     ,ROUND(AMT_DAY_OUTLIER/100,2.0) as AMT_DAY_OUTLIER
# MAGIC     ,ROUND(AMT_COST_OUTLIER/100,2.0) as AMT_COST_OUTLIER
# MAGIC     ,TRIM(CDE_MED_REC_NUM) AS CDE_MED_REC_NUM
# MAGIC     ,TRIM(CDE_PEER_GROUP) AS CDE_PEER_GROUP
# MAGIC     ,ROUND(AMT_COST_INGREDIENT/100,2.0) as AMT_COST_INGREDIENT
# MAGIC     ,TRIM(OPERATING_EXTERNALPROVID) AS OPERATING_EXTERNALPROVID
# MAGIC     ,TRIM(CDE_SOI_DISC) AS CDE_SOI_DISC
# MAGIC     ,TRIM(CDE_ROM_DISC) AS CDE_ROM_DISC
# MAGIC     ,TRIM(NUM_PA_REF) AS NUM_PA_REF
# MAGIC     ,ROUND(AMT_TPL_SUBM1/100,2.0) as AMT_TPL_SUBM1
# MAGIC     ,ROUND(AMT_TPL_APPLD1/100,2.0) as AMT_TPL_APPLD1
# MAGIC     ,ROUND(AMT_PAID_MCO1/100,2.0) as AMT_PAID_MCO1
# MAGIC     ,TRIM(NA19) AS NA19
# MAGIC     ,TRIM(IND_HDR_DTL) AS IND_HDR_DTL
# MAGIC     ,TRIM(OPERATING_EXTERNALPROVIDQUALIFIER) AS OPERATING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(CDE_COND_1) AS CDE_COND_1
# MAGIC     ,TRIM(CDE_COND_2) AS CDE_COND_2
# MAGIC     ,TRIM(CDE_COND_3) AS CDE_COND_3
# MAGIC     ,TRIM(CDE_COND_4) AS CDE_COND_4
# MAGIC     ,TRIM(CDE_COND_5) AS CDE_COND_5
# MAGIC     ,TRIM(CDE_COND_6) AS CDE_COND_6
# MAGIC     ,TRIM(CDE_COND_7) AS CDE_COND_7
# MAGIC     ,TRIM(CDE_COND_8) AS CDE_COND_8
# MAGIC     ,TRIM(OPERATING_PROV_NAME) AS OPERATING_PROV_NAME
# MAGIC     ,TRIM(CDE_PAY_ARR1) AS CDE_PAY_ARR1
# MAGIC     ,TRIM(OPERATING_PROVID) AS OPERATING_PROVID
# MAGIC     ,TRIM(CDE_DRG_DISC) AS CDE_DRG_DISC
# MAGIC     ,TRIM(CDE_CLARIFICATION1) AS CDE_CLARIFICATION1
# MAGIC     ,TRIM(CDE_CLARIFICATION2) AS CDE_CLARIFICATION2
# MAGIC     ,TRIM(CDE_CLARIFICATION3) AS CDE_CLARIFICATION3
# MAGIC     ,TRIM(OPERATING_SAK_PROV_ID) AS OPERATING_SAK_PROV_ID
# MAGIC     ,TRIM(ORDERING_EXTERNALPROVID) AS ORDERING_EXTERNALPROVID
# MAGIC     ,DTE_BILLED
# MAGIC     ,TRIM(CDE_RECIP_COUNTY) AS CDE_RECIP_COUNTY
# MAGIC     ,ROUND(AMT_REIMBURSED1/100,2.0) as AMT_REIMBURSED1
# MAGIC     ,DTE_PAID1
# MAGIC     ,TRIM(CDE_CLM_REGION) AS CDE_CLM_REGION
# MAGIC     ,DTE_FIRST_SVC1
# MAGIC     ,DTE_LAST_SVC1
# MAGIC     ,BATCH_DATE
# MAGIC     ,TRIM(ORDERING_EXTERNALPROVIDQUALIFIER) AS ORDERING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(ORDERING_PROV_NAME) AS ORDERING_PROV_NAME
# MAGIC     ,ROUND(AMT_BILLED1/100,2.0) as AMT_BILLED1
# MAGIC     ,QTY_UNITS_ALWD1
# MAGIC     ,NA27
# MAGIC     ,NUM_RECIP_AGE
# MAGIC     ,TRIM(ORDERING_PROVID) AS ORDERING_PROVID
# MAGIC     ,NA29
# MAGIC     ,ROUND(AMT_INTEREST/100,2.0) as AMT_INTEREST
# MAGIC     ,DTE_MCO_ADJUD1
# MAGIC     ,TRIM(ADR_ZIP_CODE) AS ADR_ZIP_CODE
# MAGIC     ,TRIM(ADR_ZIP_CODE_4) AS ADR_ZIP_CODE_4
# MAGIC     ,TRIM(ORDERING_SAK_PROV_ID) AS ORDERING_SAK_PROV_ID
# MAGIC     ,TRIM(PRESCRIBING_PROV_NAME) AS PRESCRIBING_PROV_NAME
# MAGIC     ,TRIM(NA32) AS NA32
# MAGIC     ,TRIM(PRESCRIBING_EXTERNALPROVID) AS PRESCRIBING_EXTERNALPROVID
# MAGIC     ,TRIM(NA34) AS NA34
# MAGIC     ,TRIM(CDE_SEX) AS CDE_SEX
# MAGIC     ,TRIM(CDE_RACE) AS CDE_RACE
# MAGIC     ,TRIM(PRESCRIBING_EXTERNALPROVIDQUALIFIER) AS PRESCRIBING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(PRESCRIBING_PROVID) AS PRESCRIBING_PROVID
# MAGIC     ,TRIM(PRESCRIBING_SAK_PROV_ID) AS PRESCRIBING_SAK_PROV_ID
# MAGIC     ,NUM_WEIGHT
# MAGIC     ,TRIM(NUM_PAT_ACCT) AS NUM_PAT_ACCT
# MAGIC     ,TRIM(REFERRING_EXTERNALPROVID) AS REFERRING_EXTERNALPROVID
# MAGIC     ,TRIM(REFERRING_EXTERNALPROVIDQUALIFIER) AS REFERRING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,ROUND(AMT_SPENDDOWN1/100,2.0) as AMT_SPENDDOWN1
# MAGIC     ,TRIM(REFERRING_PROV_NAME) AS REFERRING_PROV_NAME
# MAGIC     ,TRIM(REFERRING_PROVID) AS REFERRING_PROVID
# MAGIC     ,ROUND(AMT_COINSURANCE1/100,2.0) as AMT_COINSURANCE1
# MAGIC     ,TRIM(CDE_LIV_ARNG) AS CDE_LIV_ARNG
# MAGIC     ,TRIM(REFERRING_SAK_PROV_ID) AS REFERRING_SAK_PROV_ID
# MAGIC     ,TRIM(NUM_ADJ_ICN1) AS NUM_ADJ_ICN1
# MAGIC     ,TRIM(NUM_VERSION_DRG) AS NUM_VERSION_DRG
# MAGIC     ,TRIM(RENDERING_EXTERNALPROVID) AS RENDERING_EXTERNALPROVID
# MAGIC     ,TRIM(NUM_RA) AS NUM_RA
# MAGIC     ,TRIM(ID_VENDOR) AS ID_VENDOR
# MAGIC     ,TRIM(RENDERING_EXTERNALPROVIDQUALIFIER) AS RENDERING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(RENDERING_PROV_NAME) AS RENDERING_PROV_NAME
# MAGIC     ,TRIM(NUM_WARRANT) AS NUM_WARRANT
# MAGIC     ,TRIM(RENDERING_PROVID) AS RENDERING_PROVID
# MAGIC     ,DTE_ENTERED_SYS
# MAGIC     ,ROUND(AMT_PAT_LIAB1/100,2.0) as AMT_PAT_LIAB1
# MAGIC     ,ROUND(AMT_APL_PAT_LIAB1/100,2.0) as AMT_APL_PAT_LIAB1
# MAGIC     ,DERIVED3
# MAGIC     ,DTE_GENERIC
# MAGIC     ,TRIM(RENDERING_SAK_PROV_ID) AS RENDERING_SAK_PROV_ID
# MAGIC     ,ROUND(AMT_PAID_MCARE1/100,2.0) as AMT_PAID_MCARE1
# MAGIC     ,TRIM(CDE_OCCUR_1) AS CDE_OCCUR_1
# MAGIC     ,TRIM(CDE_OCCUR_2) AS CDE_OCCUR_2
# MAGIC     ,TRIM(CDE_OCCUR_3) AS CDE_OCCUR_3
# MAGIC     ,TRIM(CDE_OCCUR_4) AS CDE_OCCUR_4
# MAGIC     ,TRIM(CDE_OCCUR_5) AS CDE_OCCUR_5
# MAGIC     ,TRIM(CDE_OCCUR_6) AS CDE_OCCUR_6
# MAGIC     ,TRIM(CDE_OCCUR_7) AS CDE_OCCUR_7
# MAGIC     ,TRIM(CDE_OCCUR_8) AS CDE_OCCUR_8
# MAGIC     ,TRIM(SERVICE_EXTERNALPROVIDID) AS SERVICE_EXTERNALPROVIDID
# MAGIC     ,DTE_OCCUR_1
# MAGIC     ,DTE_OCCUR_2
# MAGIC     ,DTE_OCCUR_3
# MAGIC     ,DTE_OCCUR_4
# MAGIC     ,DTE_OCCUR_5
# MAGIC     ,DTE_OCCUR_6
# MAGIC     ,DTE_OCCUR_7
# MAGIC     ,DTE_OCCUR_8
# MAGIC     ,TRIM(CDE_EPSDT_FP) AS CDE_EPSDT_FP
# MAGIC     ,TRIM(IND_HYST1) AS IND_HYST1
# MAGIC     ,TRIM(SERVICE_EXTERNALPROVIDQUALIFIER) AS SERVICE_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(IND_STERILIZATION1) AS IND_STERILIZATION1
# MAGIC     ,TRIM(SERVICE_PROV_NAME) AS SERVICE_PROV_NAME
# MAGIC     ,TRIM(IND_ABORTION1) AS IND_ABORTION1
# MAGIC     ,TRIM(SERVICE_PROVID) AS SERVICE_PROVID
# MAGIC     ,TRIM(SERVICE_SAK_PROV) AS SERVICE_SAK_PROV
# MAGIC     ,TRIM(SERVICE_MMIS_PROV_TYP) AS SERVICE_MMIS_PROV_TYP
# MAGIC     ,TRIM(SERVICE_PROV_TYPE_NM) AS SERVICE_PROV_TYPE_NM
# MAGIC     ,ROUND(AMT_DEDUCT1/100,2.0) as AMT_DEDUCT1
# MAGIC     ,ROUND(AMT_MCARE_PAID/100,2.0) as AMT_MCARE_PAID
# MAGIC     ,TRIM(NA55) AS NA55
# MAGIC     ,TRIM(NA56) AS NA56
# MAGIC     ,TRIM(NUM_PRESCRIPTION_ID) AS NUM_PRESCRIPTION_ID
# MAGIC     ,DTE_PRESCRIB
# MAGIC     ,TRIM(NA57) AS NA57
# MAGIC     ,TRIM(NUM_TCN1) AS NUM_TCN1
# MAGIC     ,ROUND(AMT_ALWD1/100,2.0) as AMT_ALWD1
# MAGIC     ,TRIM(NA58) AS NA58
# MAGIC     ,TRIM(NA59) AS NA59
# MAGIC     ,ROUND(AMT_NDC_PROFEE/100,2.0) as AMT_NDC_PROFEE
# MAGIC     ,TRIM(NA60) AS NA60
# MAGIC     ,NA61
# MAGIC     ,TRIM(NA62) AS NA62
# MAGIC     ,TRIM(NA63) AS NA63
# MAGIC     ,NA64
# MAGIC     ,ROUND(AMT_CO_PAY1/100,2.0) as AMT_CO_PAY1
# MAGIC     ,ROUND(AMT_PAID1/100,2.0) as AMT_PAID1
# MAGIC     ,TRIM(NA65) AS NA65
# MAGIC     ,NA66
# MAGIC     ,TRIM(NA67) AS NA67
# MAGIC     ,TRIM(CDE_COS_ST) AS CDE_COS_ST
# MAGIC     ,TRIM(CDE_COS_SUB) AS CDE_COS_SUB
# MAGIC     ,TRIM(NA68) AS NA68
# MAGIC     ,TRIM(NA69) AS NA69
# MAGIC     ,TRIM(ID_VOUCHER_RELATED) AS ID_VOUCHER_RELATED
# MAGIC     ,TRIM(NA70) AS NA70
# MAGIC     ,TRIM(CDE_TYPE_OF_BILL1) AS CDE_TYPE_OF_BILL1
# MAGIC     ,TRIM(CDE_TYPE_OF_BILL2) AS CDE_TYPE_OF_BILL2
# MAGIC     ,TRIM(CDE_TYPE_OF_BILL3) AS CDE_TYPE_OF_BILL3
# MAGIC     ,TRIM(QTY_REFILL) AS QTY_REFILL
# MAGIC     ,TRIM(NA71) AS NA71
# MAGIC     ,DTE_DISPENSE
# MAGIC     ,QTY_DISPENSE1
# MAGIC     ,NUM_DAY_SUPPLY
# MAGIC     ,TRIM(CDE_DTL_STATUS) AS CDE_DTL_STATUS
# MAGIC     ,TRIM(CDE_PAY_ARR2) AS CDE_PAY_ARR2
# MAGIC     ,QTY_UNITS_ALWD2
# MAGIC     ,QTY_DISPENSE2
# MAGIC     ,DERIVED4
# MAGIC     ,TRIM(NA72) AS NA72
# MAGIC     ,ROUND(AMT_AWP/10000000,7.0) as AMT_AWP
# MAGIC     ,TRIM(CDE_MCAR_COVRG) AS CDE_MCAR_COVRG
# MAGIC     ,TRIM(IND_PHARMACY_FAMILY_PLAN) AS IND_PHARMACY_FAMILY_PLAN
# MAGIC     ,TRIM(IND_REBATE_ELIG) AS IND_REBATE_ELIG
# MAGIC     ,TRIM(CDE_DISP_STATUS) AS CDE_DISP_STATUS
# MAGIC     ,TRIM(NA73) AS NA73
# MAGIC     ,TRIM(CDE_EOB_1) AS CDE_EOB_1
# MAGIC     ,TRIM(CDE_EOB_2) AS CDE_EOB_2
# MAGIC     ,TRIM(IND_PRICING) AS IND_PRICING
# MAGIC     ,AMT_ALWD2                                             -- Fixed issue/removed by 100
# MAGIC     ,TRIM(IND_STERILIZATION2) AS IND_STERILIZATION2
# MAGIC     ,TRIM(NA74) AS NA74
# MAGIC     ,TRIM(IND_HYST2) AS IND_HYST2
# MAGIC     ,TRIM(NA75) AS NA75
# MAGIC     ,TRIM(IND_ABORTION2) AS IND_ABORTION2
# MAGIC     ,TRIM(NA76) AS NA76
# MAGIC     ,NUM_DAYS_COVD
# MAGIC     ,NUM_DAYS_NCOVD
# MAGIC     ,NUM_LEAVE_DAYS
# MAGIC     ,ROUND(AMT_DRUG_UNIT_PRICE/10000000,7.0) as AMT_DRUG_UNIT_PRICE
# MAGIC     ,ROUND(AMT_CO_PAY2/100,2.0) as AMT_CO_PAY2
# MAGIC     ,TRIM(CDE_COPAY_REASON) AS CDE_COPAY_REASON
# MAGIC     ,NUM_DTL
# MAGIC     ,DTE_FIRST_SVC2
# MAGIC     ,DTE_LAST_SVC2
# MAGIC     ,ROUND(CAST(CASE WHEN TRIM(BOTH '#' FROM TRIM(LEADING '-' FROM qty_units_billed)) = '' THEN '0' ELSE qty_units_billed END AS INTEGER)/100,2.0) AS QTY_UNITS_BILLED
# MAGIC     ,TRIM(CDE_REVENUE) AS CDE_REVENUE
# MAGIC     ,CAST(AMT_BILLED2 AS NUMERIC(15,2))                    -- Fixed issue/removed by 100
# MAGIC     ,ROUND(AMT_NON_COVERED/100,2.0) as AMT_NON_COVERED
# MAGIC     ,AMT_PAID_MCO2 -- Removed Round
# MAGIC     ,NA77
# MAGIC     ,AMT_PAID2/100 as AMT_PAID2 -- Removed Round
# MAGIC     ,DTE_PAID2
# MAGIC     ,ROUND(AMT_PAT_LIAB2/100,2.0) as AMT_PAT_LIAB2
# MAGIC     ,AMT_TPL_APPLD2/100 as AMT_TPL_APPLD2 -- Removed Round
# MAGIC     ,TRIM(IS_NON_DUPLICATE_IND) AS IS_NON_DUPLICATE_IND
# MAGIC     ,TRIM(CLAIM_ACTIVE_IND) AS CLAIM_ACTIVE_IND
# MAGIC     ,ROUND(AMT_APL_PAT_LIAB2/100,2.0) as AMT_APL_PAT_LIAB2
# MAGIC     ,TRIM(LAST_CLAIM_IND) AS LAST_CLAIM_IND
# MAGIC     ,ROUND(AMT_TPL_SUBM2/100,2.0) as AMT_TPL_SUBM2
# MAGIC     ,TRIM(CDE_TOOTH_NBR) AS CDE_TOOTH_NBR
# MAGIC     ,TRIM(CDE_TOOTH_SURFACE_1) AS CDE_TOOTH_SURFACE_1
# MAGIC     ,TRIM(CDE_TOOTH_SURFACE_2) AS CDE_TOOTH_SURFACE_2
# MAGIC     ,TRIM(CDE_TOOTH_SURFACE_3) AS CDE_TOOTH_SURFACE_3
# MAGIC     ,TRIM(CDE_TOOTH_SURFACE_4) AS CDE_TOOTH_SURFACE_4
# MAGIC     ,TRIM(CDE_TOOTH_SURFACE_5) AS CDE_TOOTH_SURFACE_5
# MAGIC     ,TRIM(CDE_TOOTH_SURFACE_6) AS CDE_TOOTH_SURFACE_6
# MAGIC     ,TRIM(IS_DKP_IND) AS IS_DKP_IND
# MAGIC     ,COALESCE(TRIM(DTE_MCO_ADJUD2),'0000000000') AS DTE_MCO_ADJUD2     --Per BIAR's Criteria, set value when NULL.
# MAGIC     ,ROUND(AMT_SPENDDOWN2/100,2.0) as AMT_SPENDDOWN2
# MAGIC     ,TRIM(IND_EPSDT) AS IND_EPSDT
# MAGIC     ,TRIM(NA82) AS NA82
# MAGIC     ,TRIM(NA83) AS NA83
# MAGIC     ,TRIM(CDE_POS) AS CDE_POS
# MAGIC     ,ROUND(AMT_REIMBURSED2/100,2.0) as AMT_REIMBURSED2
# MAGIC     ,ROUND(AMT_PAID_MCARE2/100,2.0) as AMT_PAID_MCARE2
# MAGIC     ,CASE WHEN ((substring(AMT_COINSURANCE2, 1, 1) = '-' OR substring(AMT_COINSURANCE2, 1, 1) = ' ') and LENGTH(AMT_COINSURANCE2) > 1) THEN ROUND(AMT_COINSURANCE2/100,2.0) ELSE 0.00 END AS AMT_COINSURANCE2
# MAGIC     ,QTY_DAYS_COINSURANCE
# MAGIC     ,TRIM(CDE_NDC) AS CDE_NDC
# MAGIC     ,ROUND(AMT_DEDUCT2/100,2.0) as AMT_DEDUCT2
# MAGIC     ,TRIM(CDE_THERA_CLS_AHFS) AS CDE_THERA_CLS_AHFS
# MAGIC     ,TRIM(CDE_THERA_CLS_SPEC) AS CDE_THERA_CLS_SPEC
# MAGIC     ,TRIM(NA84) AS NA84
# MAGIC     ,TRIM(NA85) AS NA85
# MAGIC     ,TRIM(CDE_PROC_PRIM) AS CDE_PROC_PRIM
# MAGIC     ,TRIM(CDE_MODIFIER_1) AS CDE_MODIFIER_1
# MAGIC     ,TRIM(CDE_MODIFIER_2) AS CDE_MODIFIER_2
# MAGIC     ,TRIM(CDE_MODIFIER_3) AS CDE_MODIFIER_3
# MAGIC     ,TRIM(CDE_MODIFIER_4) AS CDE_MODIFIER_4
# MAGIC     ,NA86
# MAGIC     ,TRIM(CDE_FUND_CODE) AS CDE_FUND_CODE
# MAGIC     ,TRIM(CDE_RATE_TYPE) AS CDE_RATE_TYPE
# MAGIC     ,TRIM(NUM_ICN2) AS NUM_ICN2
# MAGIC     ,TRIM(NUM_ADJ_ICN2) AS NUM_ADJ_ICN2
# MAGIC     ,TRIM(NUM_TCN2) AS NUM_TCN2
# MAGIC     ,TRIM(ID_MEDICAID2) AS ID_MEDICAID2
# MAGIC     ,TRIM(ID_PROVIDER_MCAID1) AS ID_PROVIDER_MCAID1
# MAGIC     ,TRIM(ID_PROVIDER_NPI1) AS ID_PROVIDER_NPI1
# MAGIC     ,TRIM(CDE_PROV_TYPE_PRIM_BLANK_1) AS CDE_PROV_TYPE_PRIM_BLANK_1
# MAGIC     ,TRIM(CDE_SVC_COUNTY1) AS CDE_SVC_COUNTY1
# MAGIC     ,TRIM(CDE_TAXONOMY1) AS CDE_TAXONOMY1
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM1) AS CDE_PROV_SPEC_PRIM1
# MAGIC     ,TRIM(ID_PROVIDER_MCAID2) AS ID_PROVIDER_MCAID2
# MAGIC     ,TRIM(ID_PROVIDER_NPI2) AS ID_PROVIDER_NPI2
# MAGIC     ,TRIM(BILLING_CDE_PROV_TYPE_PRIM) AS BILLING_CDE_PROV_TYPE_PRIM
# MAGIC     ,TRIM(CDE_PROV_PGM1) AS CDE_PROV_PGM1
# MAGIC     ,TRIM(CDE_SVC_COUNTY2) AS CDE_SVC_COUNTY2
# MAGIC     ,TRIM(CDE_TAXONOMY2) AS CDE_TAXONOMY2
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM2) AS CDE_PROV_SPEC_PRIM2
# MAGIC     ,TRIM(ID_PROVIDER_MCAID3) AS ID_PROVIDER_MCAID3
# MAGIC     ,TRIM(ID_PROVIDER_NPI3) AS ID_PROVIDER_NPI3
# MAGIC     ,TRIM(RENDERING_CDE_PROV_TYPE_PRIM) AS RENDERING_CDE_PROV_TYPE_PRIM
# MAGIC     ,TRIM(CDE_PROV_PGM2) AS CDE_PROV_PGM2
# MAGIC     ,TRIM(CDE_SVC_COUNTY3) AS CDE_SVC_COUNTY3
# MAGIC     ,TRIM(CDE_TAXONOMY3) AS CDE_TAXONOMY3
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM3) AS CDE_PROV_SPEC_PRIM3
# MAGIC     ,TRIM(ID_PROVIDER_MCAID4) AS ID_PROVIDER_MCAID4
# MAGIC     ,TRIM(ID_PROVIDER_NPI4) AS ID_PROVIDER_NPI4
# MAGIC     ,TRIM(REFERRING_CDE_PROV_TYPE_PRIM) AS REFERRING_CDE_PROV_TYPE_PRIM
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM4) AS CDE_PROV_SPEC_PRIM4
# MAGIC     ,TRIM(ID_PROVIDER_MCAID5) AS ID_PROVIDER_MCAID5
# MAGIC     ,TRIM(ID_PROVIDER_NPI5) AS ID_PROVIDER_NPI5
# MAGIC     ,TRIM(CDE_PROV_TYPE_PRIM_BLANK_2) AS CDE_PROV_TYPE_PRIM_BLANK_2
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM5) AS CDE_PROV_SPEC_PRIM5
# MAGIC     ,TRIM(ID_PROVIDER_MCAID6) AS ID_PROVIDER_MCAID6
# MAGIC     ,TRIM(ID_PROVIDER_NPI6) AS ID_PROVIDER_NPI6
# MAGIC     ,TRIM(CDE_PROV_TYPE_PRIM_BLANK_3) AS CDE_PROV_TYPE_PRIM_BLANK_3
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM6) AS CDE_PROV_SPEC_PRIM6
# MAGIC     ,TRIM(ID_PROVIDER_MCAID7) AS ID_PROVIDER_MCAID7
# MAGIC     ,TRIM(ID_PROVIDER_NPI7) AS ID_PROVIDER_NPI7
# MAGIC     ,TRIM(CDE_PROV_TYPE_PRIM_BLANK_4) AS CDE_PROV_TYPE_PRIM_BLANK_4
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM7) AS CDE_PROV_SPEC_PRIM7
# MAGIC     ,TRIM(ID_PROVIDER_MCAID8) AS ID_PROVIDER_MCAID8
# MAGIC     ,TRIM(MCP_CDE_PROV_TYPE_PRIM) AS MCP_CDE_PROV_TYPE_PRIM
# MAGIC     ,TRIM(CDE_PROV_SPEC_PRIM8) AS CDE_PROV_SPEC_PRIM8
# MAGIC     ,year(coalesce(dte_paid1, current_date)) * 100 + month(coalesce(dte_paid1,current_date)) as partition_col
# MAGIC     ,TRIM(VISIT_START) as VISIT_START
# MAGIC     ,TRIM(VISIT_END) as VISIT_END
# MAGIC     ,TRIM(IND_LESS) as IND_LESS
# MAGIC
# MAGIC from ${catalog}.${schema_name}.${cl_med_stg}  a
# MAGIC where 1 = 1
# MAGIC and a.NUM_ICN1 is not null
# MAGIC and a.ID_MEDICAID1 is not  null
# MAGIC and DERIVED1 not in ( 'T')
# MAGIC and not exists
# MAGIC (
# MAGIC     select 1 
# MAGIC     from ${catalog}.${schema_name}.${Med_Analytics} tmp 
# MAGIC     where a.NUM_ICN1 = tmp.NUM_ICN1
# MAGIC     and a.NUM_DTL = tmp.NUM_DTL
# MAGIC     and a.ID_MEDICAID1 = tmp.ID_MEDICAID1
# MAGIC )
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Load Med_Analytics table
# MAGIC %sql
# MAGIC INSERT INTO TABLE ${catalog}.${schema_name}.${Med_Analytics}
# MAGIC SELECT * FROM ${catalog}.${schema_name}.${Med_Analytics_stg}
# MAGIC ;
