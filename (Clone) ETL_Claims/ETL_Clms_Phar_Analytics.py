# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     ETL_Clms_Phar_Analytics.                                                                                         *
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
#*                                                    AMT_PAID_MCO1 SrceFileChng: VEN12403FA.ALWD_OTH_PYR_AMT to VEN100FA.PD_MCO_AMT*
#*                                                    DTE_ENTERED_SYS Modified the CAST in the Load cl_phar_staging table.          *
#* 03/28/2024 CCRB70930/CO#43342  Jaime Zavala        Several Fields, Modified to use SUBSTRING instead of CAST with varchar.       *
#* 04/19/2024 CCRB70930/CO#43342  Jaime Zavala        QTY_DISPENSE1 Modified the CAST to avoid have decimal point in the value.     *
#* 05/29/2024 CCRB70930/CO#43342  Jaime Zavala        AMT_PAID_MCO1 Chng FieldNm- PD_MCO_AMT to THE_PAID_AMT. Removed the by 100.   *
#*                                                    AMT_PAID1 Chng FieldNm- PD_AMT to THE_DETAIL_PAID_AMT.                        *
#*                                                    CDE_ENC_TYPE modified to expect one character (N,Y,C,D,E)                     *
#* 06/03/2024 CCRB70930/CO#43342  Jaime Zavala        QTY_UNITS_ALWD1 FieldAdded.- VEN100FA.ALWD_QTY.                               *
#* 06/20/2024 CCRB70930/CO#43342  Jaime Zavala        IND_HDR_DTL FieldAdded.- VEN100FA.HDR_DTL_PAID_IND.                           *
#*                                                    Added CleanDstntTblsFlag Defaul-F, T when using EDW_temp_ in table's names    *
#*                                                    and VE are Full Refresh.                                                      *
#* 07/19/2024 CCRB70930/CO#43342  Jaime Zavala        Fixed issue with the CleanDstntTblsFlag.                                      *
#* 07/24/2024 CCRB70930/CO#43342  Jaime Zavala        Added logic to use CleanDstntTblsFlag for Delta VE source type.               *
#* 07/30/2024 CCRB70930/CO#43342  Jaime Zavala        Fixed issue with Sum Over Partition on Sak_Claim.                             *
#* 09/25/2024 CCRB70930/CO#43342  Jaime Zavala        NA83 Mapped to VEN100FA.THE_PAID_AMT.                                         *
#* 09/26/2024 CCRB70930/CO#43342  Jaime Zavala        Removed Round Fnc. to AMT_TPL_APPLD1,AMT_PAID_MCO1,AMT_ALWD2.                 *
#* 01/16/2024 CCRB70930/CO#43342  Jaime Zavala        CDE_FUND_CODE set to '#########' when Null, to met BIAR expected value.       *
#* 03/03/2025 CCRB70930/CO#43342  Jaime Zavala        CDE_FUND_CODE set to '#########' when ''.                                     *
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
#* 08/25/2025 CCRB70930/CO#43342  Jaime Zavala        Include Deny Claims in the cl_phar table.                                     *
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
# Claims Extracts  
dbutils.widgets.text('VEN100FA', 'EDW_VEN100FA_Staging')
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
dbutils.widgets.text('cl_phar_stg', 'EDW_temp_cl_phar_staging')
dbutils.widgets.text('cl_phar', 'EDW_temp_cl_phar')
dbutils.widgets.text('Phar_Analytics_stg', 'EDW_temp_Phar_Analytics_staging')
dbutils.widgets.text('Phar_Analytics', 'EDW_temp_Phar_Analytics')


# COMMAND ----------

# DBTITLE 1,Get Parameters Values
#-------------------
# EDW Staging Tables
#-------------------
# Claims Extracts
VEN100FA = dbutils.widgets.get('VEN100FA')
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
cl_phar_stg = dbutils.widgets.get('cl_phar_stg')
cl_phar = dbutils.widgets.get('cl_phar')
Phar_Analytics_stg = dbutils.widgets.get('Phar_Analytics_stg')
Phar_Analytics = dbutils.widgets.get('Phar_Analytics')

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
print(VEN12401FA)
print(VEN12403FA)
print(VEN12501FA)
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
print(cl_phar_stg)
print(cl_phar)
print(Phar_Analytics_stg)
print(Phar_Analytics)


# COMMAND ----------

# DBTITLE 1,Truncate cl_phar_staging table
# MAGIC %sql
# MAGIC TRUNCATE TABLE ${catalog}.${schema_name}.${cl_phar_stg}
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load cl_phar_staging table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${cl_phar_stg}
# MAGIC SELECT DISTINCT
# MAGIC     --CODE WITH COLUMNS MAPPING GOES HERE
# MAGIC      SUBSTRING(TRIM(clm.ICN_NBR),1,15)                                                                                      AS NUM_ICN1
# MAGIC     ,SUBSTRING(TRIM(INDICATOR_CLAIM),1,1)                                                                                   AS IND_CLAIM
# MAGIC     ,SUBSTRING(TRIM(CODE_CLM_TYPE),1,3)                                                                                     AS CDE_CLM_TYPE
# MAGIC     ,CASE UPPER(TRIM(clm.HDR_STS_CD)) WHEN 'DENIED'   THEN 'D'                                                              
# MAGIC                                       WHEN 'PAID'     THEN 'P'                                                              
# MAGIC                                       WHEN 'REVERSED' THEN 'P'                                                              
# MAGIC                                       ELSE NULL                                                                             
# MAGIC                                       END                                                                                   AS CDE_HDR_STATUS
# MAGIC     ,NULL                                                                                                                   AS CDE_PGM_HEALTH
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.AID_CTG_CD),1,7),'')                                                                       AS CDE_AID_CATEGORY
# MAGIC     ,NULL                                                                                                                   AS ID_CLERK                     --Removed Field
# MAGIC     ,NULL                                                                                                                   AS CDE_PRESCRIPTION_ORIG        --Removed Field
# MAGIC     ,'C'                                                                                                                    AS DERIVED1
# MAGIC     ,UPPER(TRIM(clm.ENC_TYP_CD))                                                                                            AS CDE_ENC_TYPE
# MAGIC     ,NULL                                                                                                                   AS NA1                          --EMPTY
# MAGIC     ,LPAD(TRIM(clm.MEDICAID_ID),12,'0')                                                                                     AS ID_MEDICAID1
# MAGIC     ,CAST(DATE_FORMAT(CAST(clm.BIRTH_DT AS DATE),'MM/dd/y') AS varchar(10))                                                 AS DTE_BIRTH
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.IND_BRAND_MED_NEC),1,1),'')                                                                AS IND_BRAND_MED_NEC            --Added Field
# MAGIC     ,NULL                                                                                                                   AS AMT_VACC_INCENTIVE           --Removed Field
# MAGIC     ,COALESCE(SUBSTRING(clm.PRIOR_AUTH_NBR,1,11),'0')                                                                       AS NUM_PRIOR_AUTH
# MAGIC     ,NULL                                                                                                                   AS NA2                          --EMPTY
# MAGIC     ,NULL                                                                                                                   AS NA3                          --EMPTY
# MAGIC     ,COALESCE(SUBSTRING(TRIM(ClmAdtlExtOthrPyr_CONTRACT_SUB_ID),1,5),'')                                                    AS ID_CONTRACT_SUB
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.HIC_SUB_NBR),1,12),'')                                                                     AS NUM_HIC_SUB
# MAGIC     ,NULL                                                                                                                   AS NUM_CMS_ICN
# MAGIC     ,TRIM(ATTENDING_EXTERNALPROVID)                                                                                         AS ATTENDING_EXTERNALPROVID     
# MAGIC     ,TRIM(ATTENDING_EXTERNALPROVIDQUALIFIER)                                                                                AS ATTENDING_EXTERNALPROVIDQUALIFIER     
# MAGIC     ,TRIM(ATTENDING_PROV_NAME)                                                                                              AS ATTENDING_PROV_NAME     
# MAGIC     ,TRIM(ATTENDING_PROVID)                                                                                                 AS ATTENDING_PROVID     
# MAGIC     ,TRIM(CAST(ATTENDING_SAK_PROV_ID AS STRING))                                                                            AS ATTENDING_SAK_PROV_ID
# MAGIC     ,TRIM(BILLING_EXTERNALPROVID)                                                                                           AS BILLING_EXTERNALPROVID
# MAGIC     ,TRIM(BILLING_EXTERNALPROVIDQUALIFIER)                                                                                  AS BILLING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(BILLING_MEDICAID_ID)                                                                                              AS BILLING_MEDICAID_ID
# MAGIC     ,TRIM(BILLING_PROV_NAME)                                                                                                AS BILLING_PROV_NAME
# MAGIC     ,TRIM(BILLING_PROVID)                                                                                                   AS BILLING_PROVID
# MAGIC     ,TRIM(CAST(BILLING_SAK_PROV_ID AS STRING))                                                                              AS BILLING_SAK_PROV_ID
# MAGIC     ,NULL
# MAGIC     ,TRIM(BILLING_MMIS_PROV_TYP)                                                                                            AS BILLING_MMIS_PROV_TYP
# MAGIC     ,TRIM(BILLING_PROV_TYPE_NM)                                                                                             AS BILLING_PROV_TYPE_NM
# MAGIC     ,TRIM(OPERATING_EXTERNALPROVID)                                                                                         AS OPERATING_EXTERNALPROVID
# MAGIC     ,NULL
# MAGIC     ,TRIM(OPERATING_EXTERNALPROVIDQUALIFIER)                                                                                AS OPERATING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,NULL                                                                                                                   AS CDE_SOI                      --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_LEVEL_OF_CARE            --Removed Field
# MAGIC     ,CASE TRIM(INDICATOR_CLAIM) WHEN 'F' THEN 62                                                                            
# MAGIC 	                            WHEN 'E' THEN 72                                                                            
# MAGIC 								ELSE NULL                                                                                   
# MAGIC 	                            END                                                                                       AS DERIVED2
# MAGIC     ,NULL                                                                                                                   AS CDE_MDC                      --PHAR NOT USE
# MAGIC     ,TRIM(OPERATING_PROV_NAME)                                                                                              AS OPERATING_PROV_NAME
# MAGIC     ,TRIM(OPERATING_PROVID)                                                                                                 AS OPERATING_PROVID
# MAGIC     ,NULL                                                                                                                   AS CDE_PATIENT_STATUS           --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_EMERGENCY                --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_ADMIT_SOURCE             --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_ROM                      --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS DTE_ADMISSION                --PHAR NOT USE
# MAGIC     ,0                                                                                                                      AS CDE_ADMIT_HOUR               --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_DRG                      --PHAR NOT USE
# MAGIC     ,'000000000000000'                                                                                                      AS AMT_BASE_DRG                 --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS DTE_DISCHARGE                --PHAR NOT USE
# MAGIC     ,0                                                                                                                      AS TIME_DISCHARGE               --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_COMPOUND_DOSAGE          --Removed Field
# MAGIC     ,0.0                                                                                                                    AS AMT_DAY_OUTLIER              --PHAR NOT USE
# MAGIC     ,'000000000000000'                                                                                                      AS AMT_COST_OUTLIER             --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_MED_REC_NUM              --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_PEER_GROUP               --PHAR NOT USE
# MAGIC     ,COALESCE(CAST(clm.COST_INGRD_AMT AS varchar(8)),'0.00')                                                                AS AMT_COST_INGREDIENT
# MAGIC     ,TRIM(CAST(OPERATING_SAK_PROV_ID AS STRING))                                                                            AS OPERATING_SAK_PROV_ID
# MAGIC     ,NULL                                                                                                                   AS CDE_SOI_DISC                 --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_ROM_DISC                 --PHAR NOT USE
# MAGIC     ,COALESCE(SUBSTRING(clm.PRIOR_AUTH_NBR,1,30),'0')                                                                       AS NUM_PA_REF
# MAGIC     ,NULL                                                                                                                   AS AMT_TPL_SUBM1                --Removed Field
# MAGIC     ,SumOverSakClm_TPL_AMT                                                                                                  AS AMT_TPL_APPLD1
# MAGIC     ,SumOverSakClm_THE_PAID_AMT                                                                                             AS AMT_PAID_MCO1                -- Logic for negative values e.g. Reversed Hdr Status Code or negative amt.
# MAGIC     ,TRIM(ORDERING_EXTERNALPROVID)                                                                                          AS ORDERING_EXTERNALPROVID
# MAGIC     ,clm.HDR_DTL_PAID_IND                                                                                                   AS IND_HDR_DTL                  --NewFld from Extract
# MAGIC     ,TRIM(ORDERING_EXTERNALPROVIDQUALIFIER)                                                                                 AS ORDERING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,NULL                                                                                                                   AS CDE_COND_1                   --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_COND_2                   --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_COND_3                   --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_COND_4                   --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_COND_5                   --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_COND_6                   --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_COND_7                   --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_COND_8                   --PHAR NOT USE
# MAGIC     ,TRIM(ORDERING_PROV_NAME)                                                                                               AS ORDERING_PROV_NAME
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.PAY_ARR_CD),1,2),'')                                                                       AS CDE_PAY_ARR1
# MAGIC     ,NULL                                                                                                                   AS NA27                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS CDE_DRG_DISC                 --PHAR NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.CLARIFICATION1_CD),1,2),'')                                                                AS CDE_CLARIFICATION1           --Added Field
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.CLARIFICATION2_CD),1,2),'')                                                                AS CDE_CLARIFICATION2           --Added Field
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.CLARIFICATION3_CD),1,2),'')                                                                AS CDE_CLARIFICATION3           --Added Field
# MAGIC     ,TRIM(ORDERING_PROVID)                                                                                                  AS ORDERING_PROVID
# MAGIC     ,NULL                                                                                                                   AS NA29                         --EMPTY
# MAGIC     ,CAST(DATE_FORMAT(CAST(COALESCE(clm.BILL_DT,'0101-01-01') AS DATE),'MM/dd/y') AS varchar(10))                           AS DTE_BILLED
# MAGIC     ,NULL                                                                                                                   AS CDE_RECIP_COUNTY
# MAGIC     ,NULL                                                                                                                   AS AMT_REIMBURSED1              --Removed Field
# MAGIC     ,CAST(DATE_FORMAT(CAST(clm.PD_DT AS DATE),'MM/dd/y') AS varchar(10))                                                    AS DTE_PAID1
# MAGIC     ,NULL                                                                                                                   AS CDE_CLM_REGION               --Removed Field
# MAGIC     ,CAST(DATE_FORMAT(CAST(clm.HDR_FIRST_SVC_DT AS DATE),'MM/dd/y') AS varchar(10))                                         AS DTE_FIRST_SVC1
# MAGIC     ,CAST(DATE_FORMAT(CAST(clm.HDR_LAST_SVC_DT  AS DATE),'MM/dd/y') AS varchar(10))                                         AS DTE_LAST_SVC1
# MAGIC     ,CAST(CAST(clm.ENTERED_SYS_DT AS DATE) AS varchar(10))                                                                  AS BATCH_date                   --Added Field/same as DTE_ENTERED_SYS (added on EDW extraction)
# MAGIC     ,TRIM(CAST(ORDERING_SAK_PROV_ID AS STRING))                                                                             AS ORDERING_SAK_PROV_ID
# MAGIC     ,TRIM(PRESCRIBING_PROV_NAME)                                                                                            AS PRESCRIBING_PROV_NAME
# MAGIC     ,SumOverSakClm_DTL_BILL_AMT                                                                                             AS AMT_BILLED1                  -- DTL_BILL_AMT's Logic for negative values e.g. Reversed Hdr Status Code or negative amt.
# MAGIC     ,clm.ALWD_QTY                                                                                                           AS QTY_UNITS_ALWD1              --Added Field
# MAGIC     ,NULL                                                                                                                   AS NA32                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS NUM_RECIP_AGE                --Removed Field
# MAGIC     ,TRIM(PRESCRIBING_EXTERNALPROVID)                                                                                       AS PRESCRIBING_EXTERNALPROVID
# MAGIC     ,NULL                                                                                                                   AS NA34                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS AMT_INTEREST                 --Removed Field
# MAGIC     ,CAST(CAST(clm.MCO_ADJUD_DT AS DATE) AS varchar(10))                                                                    AS DTE_MCO_ADJUD1               --Added Field
# MAGIC     ,NULL                                                                                                                   AS ADR_ZIP_CODE
# MAGIC     ,NULL                                                                                                                   AS ADR_ZIP_CODE_4
# MAGIC     ,TRIM(PRESCRIBING_EXTERNALPROVIDQUALIFIER)                                                                              AS PRESCRIBING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(PRESCRIBING_PROVID)                                                                                               AS PRESCRIBING_PROVID     
# MAGIC     ,TRIM(CAST(PRESCRIBING_SAK_PROV_ID AS STRING))                                                                          AS PRESCRIBING_SAK_PROV_ID
# MAGIC     ,TRIM(REFERRING_EXTERNALPROVID)                                                                                         AS REFERRING_EXTERNALPROVID
# MAGIC     ,TRIM(REFERRING_EXTERNALPROVIDQUALIFIER)                                                                                AS REFERRING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,COALESCE(SUBSTRING(clm.SEX_CD,1,1),'')                                                                                 AS CDE_SEX                      --CRG required Field.
# MAGIC     ,NULL                                                                                                                   AS CDE_RACE
# MAGIC     ,TRIM(REFERRING_PROV_NAME)                                                                                              AS REFERRING_PROV_NAME
# MAGIC     ,TRIM(REFERRING_PROVID)                                                                                                 AS REFERRING_PROVID
# MAGIC     ,TRIM(CAST(REFERRING_SAK_PROV_ID AS STRING))                                                                            AS REFERRING_SAK_PROV_ID
# MAGIC     ,NULL                                                                                                                   AS NUM_WEIGHT                   --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS NUM_PAT_ACCT                 --Removed Field
# MAGIC     ,TRIM(RENDERING_EXTERNALPROVID)                                                                                         AS RENDERING_EXTERNALPROVID
# MAGIC     ,TRIM(RENDERING_EXTERNALPROVIDQUALIFIER)                                                                                AS RENDERING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,NULL                                                                                                                   AS AMT_SPENDDOWN1
# MAGIC     ,TRIM(RENDERING_PROV_NAME)                                                                                              AS RENDERING_PROV_NAME
# MAGIC     ,TRIM(RENDERING_PROVID)                                                                                                 AS RENDERING_PROVID
# MAGIC     ,COALESCE(CAST(clm.COINSR_AMT AS varchar(8)),'0.00')                                                                    AS AMT_COINSURANCE1
# MAGIC     ,NULL                                                                                                                   AS CDE_LIV_ARNG                 --Removed Field
# MAGIC     ,TRIM(CAST(RENDERING_SAK_PROV_ID AS STRING))                                                                            AS RENDERING_SAK_PROV_ID
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.ADJ_ICN_NBR),1,20),NULL)                                                                   AS NUM_ADJ_ICN1
# MAGIC     ,NULL                                                                                                                   AS NUM_VERSION_DRG              --PHAR NOT USE
# MAGIC     ,TRIM(SERVICE_EXTERNALPROVIDID)                                                                                         AS SERVICE_EXTERNALPROVIDID
# MAGIC     ,NULL                                                                                                                   AS NUM_RA
# MAGIC     ,NULL                                                                                                                   AS ID_VENDOR
# MAGIC     ,TRIM(SERVICE_EXTERNALPROVIDQUALIFIER)                                                                                  AS SERVICE_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(SERVICE_PROV_NAME)                                                                                                AS SERVICE_PROV_NAME
# MAGIC     ,NULL                                                                                                                   AS NUM_WARRANT
# MAGIC     ,TRIM(SERVICE_PROVID)                                                                                                   AS SERVICE_PROVID
# MAGIC     ,CAST(DATE_FORMAT(CAST(clm.ENTERED_SYS_DT AS DATE),'MM/dd/y') AS varchar(10))                                           AS DTE_ENTERED_SYS              --Added Field/added on EDW extract
# MAGIC     ,NULL                                                                                                                   AS AMT_PAT_LIAB1                --Removed Field
# MAGIC     ,NULL                                                                                                                   AS AMT_APL_PAT_LIAB1
# MAGIC     ,CAST(DATE_FORMAT(CAST(clm.REPORT_DTE AS DATE),'MM/dd/y') AS varchar(10))                                               AS DERIVED3
# MAGIC     ,NULL                                                                                                                   AS DTE_GENERIC                  --PHAR NOT USE
# MAGIC     ,TRIM(CAST(SERVICE_SAK_PROV AS STRING))                                                                                 AS SERVICE_SAK_PROV
# MAGIC     ,COALESCE(CAST(clm.PD_MCARE_AMT AS NUMERIC),0.00)                                                                       AS AMT_PAID_MCARE1
# MAGIC     ,NULL                                                                                                                   AS CDE_OCCUR_1                  --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_OCCUR_2                  --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_OCCUR_3                  --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_OCCUR_4                  --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_OCCUR_5                  --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_OCCUR_6                  --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_OCCUR_7                  --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_OCCUR_8                  --PHAR NOT USE
# MAGIC     ,TRIM(SERVICE_MMIS_PROV_TYP)                                                                                            AS SERVICE_MMIS_PROV_TYP
# MAGIC     ,NULL                                                                                                                   AS DTE_OCCUR_1                  --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS DTE_OCCUR_2                  --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS DTE_OCCUR_3                  --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS DTE_OCCUR_4                  --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS DTE_OCCUR_5                  --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS DTE_OCCUR_6                  --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS DTE_OCCUR_7                  --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS DTE_OCCUR_8                  --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_EPSDT_FP                 --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS IND_HYST1                    --PHAR NOT USE
# MAGIC     ,TRIM(SERVICE_PROV_TYPE_NM)                                                                                             AS SERVICE_PROV_TYPE_NM
# MAGIC     ,NULL                                                                                                                   AS IND_STERILIZATION1           --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS NA55                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS IND_ABORTION1                --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS NA56                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS NA57                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS NA58                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS NA59                         --EMPTY
# MAGIC     ,COALESCE(CAST(clm.DEDUCT_AMT AS varchar(8)),'0.00')                                                                    AS AMT_DEDUCT1
# MAGIC     ,NULL                                                                                                                   AS AMT_MCARE_PAID               --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS NA60                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS NA61                         --EMPTY
# MAGIC     ,LPAD(TRIM(clm.RX_ID_NBR),12,'0')                                                                                       AS NUM_PRSCRIP
# MAGIC     ,NULL                                                                                                                   AS DTE_PRESCRIB                 --Removed Field
# MAGIC     ,NULL                                                                                                                   AS NA62                         --EMPTY
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.TCN_NBR),1,18),'')                                                                         AS NUM_TCN1                     --Added Field
# MAGIC     ,SumOverSakClm_DTL_ALWD_AMT                                                                                             AS AMT_ALWD1
# MAGIC     ,NULL                                                                                                                   AS NA63                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS NA64                         --EMPTY
# MAGIC     ,CAST(clm.NDC_PROFEE_AMT AS NUMERIC)                                                                                    AS AMT_NDC_PROFEE               --Removed Field
# MAGIC     ,NULL                                                                                                                   AS NA65                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS NA66                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS NA67                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS NA68                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS NA69                         --EMPTY
# MAGIC     ,SumOverSakClm_DTL_CO_PAY_AMT                                                                                           AS AMT_CO_PAY1                  --Fixed Issue#39
# MAGIC     ,SumOverSakClm_THE_DETAIL_PAID_AMT                                                                                      AS AMT_PAID1                    --Fixed Issue#37/AMT's Logic for negative values e.g. Reversed Hdr Status Code or amt < 0
# MAGIC     ,NULL                                                                                                                   AS NA70                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS QTY_PRESCRIBED
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.COS_ST_CD),1,2),'')                                                                        AS CDE_COS_ST                   --Added Field
# MAGIC     ,NULL                                                                                                                   AS CDE_COS_SUB
# MAGIC     ,NULL                                                                                                                   AS NA73                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS NA74                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS ID_VOUCHER_RELATED
# MAGIC     ,NULL                                                                                                                   AS NA75                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS CDE_TYPE_OF_BILL1            --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_TYPE_OF_BILL2            --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_TYPE_OF_BILL3            --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS QTY_REFILL                   --Removed Field
# MAGIC     ,NULL                                                                                                                   AS NA76                         --EMPTY
# MAGIC     ,CAST(DATE_FORMAT(CAST(clm.DSPN_DT AS DATE),'MM/dd/y')  AS varchar(10))                                                 AS DTE_DISPENSE
# MAGIC     ,CASE WHEN COALESCE(clm.DTL_DISPENSE_QTY,0) < 0                                                                         
# MAGIC 	      THEN '-'||LPAD( CAST(COALESCE(clm.DTL_DISPENSE_QTY,0) * -1000 AS DECIMAL(18,0)),9,'0')                                   
# MAGIC 	      ELSE LPAD( CAST(COALESCE(clm.DTL_DISPENSE_QTY,0) * 1000 AS DECIMAL(19,0)),10,'0') END                           AS QTY_DISPENSE1
# MAGIC     ,CAST(clm.DAY_SPLY_NBR AS NUMERIC)                                                                                      AS NUM_DAY_SUPPLY
# MAGIC     ,CASE UPPER(TRIM(clm.DTL_STS_CD)) WHEN 'DENY' THEN 'D'                                                                  
# MAGIC                                       WHEN 'OKAY' THEN 'P'                                                                  
# MAGIC                                       ELSE NULL                                                                             
# MAGIC                                       END                                                                                   AS CDE_DTL_STATUS
# MAGIC     ,NULL                                                                                                                   AS CDE_PAY_ARR2                 --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS QTY_UNITS_ALWD2              --PHAR NOT USE
# MAGIC     ,LPAD( CAST(COALESCE(clm.DTL_DISPENSE_QTY,0) * 1000 AS varchar(10)),10,'0')                                             AS QTY_DISPENSE2
# MAGIC     ,CAST(DATE_FORMAT(CAST(clm.REPORT_DTE AS DATE),'MM/dd/y') AS varchar(10))                                               AS DERIVED4
# MAGIC     ,NULL                                                                                                                   AS NA77                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS AMT_AWP                      --Removed Field
# MAGIC     ,NULL                                                                                                                   AS CDE_MCAR_COVRG               --Removed Field/NOT USED IN MITS
# MAGIC     ,NULL                                                                                                                   AS IND_PHARMACY_FAMILY_PLAN     --Removed Field
# MAGIC     ,NULL                                                                                                                   AS IND_REBATE_ELIG              --Removed Field
# MAGIC     ,NULL                                                                                                                   AS CDE_DISP_STATUS
# MAGIC     ,TRIM(clm.IS_NON_DUPLICATE_IND)                                                                                         AS IS_NON_DUPLICATE_IND
# MAGIC     ,COALESCE(SUBSTRING(ClmAdtlExtEOB_CLAIM_ADTL_EOB_SK1,1,5),'')                                                           AS CDE_EOB_1
# MAGIC     ,COALESCE(SUBSTRING(ClmAdtlExtEOB_CLAIM_ADTL_EOB_SK2,1,5),'')                                                           AS CDE_EOB_2
# MAGIC     ,NULL                                                                                                                   AS IND_PRICING
# MAGIC     ,CASE WHEN COALESCE(clm.DTL_ALWD_AMT,0) < 0                                                                             
# MAGIC 	      THEN '-'||LPAD( CAST(COALESCE(clm.DTL_ALWD_AMT,0) * -100 AS varchar(14)),14,'0')                                       
# MAGIC           ELSE LPAD( CAST(COALESCE(clm.DTL_ALWD_AMT,0) * 100 AS varchar(15)),15,'0')                                             
# MAGIC           END                                                                                                               AS AMT_ALWD2
# MAGIC     ,NULL                                                                                                                   AS IND_STERILIZATION2           --PHAR NOT USE
# MAGIC     ,TRIM(clm.CLAIM_ACTIVE_IND)                                                                                             AS CLAIM_ACTIVE_IND
# MAGIC     ,NULL                                                                                                                   AS IND_HYST2                    --PHAR NOT USE
# MAGIC     ,TRIM(clm.LAST_CLAIM_IND)                                                                                               AS LAST_CLAIM_IND
# MAGIC     ,NULL                                                                                                                   AS IND_ABORTION2                --PHAR NOT USE
# MAGIC     ,TRIM(clm.IS_DKP_IND)                                                                                                   AS IS_DKP_IND
# MAGIC     ,NULL                                                                                                                   AS NUM_DAYS_COVD                --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS NUM_DAYS_NCOVD               --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS NUM_LEAVE_DAYS               --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS AMT_MAC                      --Removed Field
# MAGIC     ,'000000000000000'                                                                                                      AS AMT_CO_PAY2                  --PHAR NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.COPAY_REASON_CD),1,4),'')                                                                  AS CDE_COPAY_REASON
# MAGIC     ,LPAD(CAST(clm.DTL_NBR AS varchar(4)),4,'0')                                                                            AS NUM_DTL
# MAGIC     ,CAST(DATE_FORMAT(CAST(clm.DTL_FIRST_SVC_DT AS DATE),'MM/dd/y') AS varchar(10))                                         AS DTE_FIRST_SVC2
# MAGIC     ,CAST(DATE_FORMAT(CAST(clm.DTL_LAST_SVC_DT  AS DATE),'MM/dd/y') AS varchar(10))                                         AS DTE_LAST_SVC2
# MAGIC     ,LPAD( CAST(COALESCE(clm.UNT_BILL_QTY,0) * 1000 AS varchar(9)),9,'0')                                                   AS QTY_UNITS_BILLED
# MAGIC     ,NULL                                                                                                                   AS CDE_REVENUE                  --PHAR NOT USE
# MAGIC     ,CASE WHEN clm.DTL_BILL_AMT < 0                                                                                         
# MAGIC           THEN '-'||LPAD(CAST(COALESCE((clm.DTL_BILL_AMT * -1),0) AS varchar(14)), 14,'0')                                       
# MAGIC           ELSE LPAD(CAST(COALESCE(clm.DTL_BILL_AMT,0) AS varchar(15)), 15,'0')                                                   
# MAGIC           END                                                                                                               AS AMT_BILLED2                  -- DTL_BILL_AMT's Logic for negative values e.g. Reversed Hdr Status Code or amt < 0.
# MAGIC     ,'000000000000000'                                                                                                      AS AMT_NON_COVERED              --PHAR NOT USE
# MAGIC     ,'00000000000'                                                                                                          AS AMT_PAID_MCO2                --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS NA82                         --EMPTY
# MAGIC     ,'000000000000000'                                                                                                      AS AMT_PAID2                    --PHAR USE AT HEADER LEVEL ONLY
# MAGIC     ,CAST(DATE_FORMAT(CAST(clm.PD_DT AS DATE),'MM/dd/y')  AS varchar(10))                                                   AS DTE_PAID2                    --PHAR USE SAME AS HEADER LEVEL
# MAGIC     ,'000000000000000'                                                                                                      AS AMT_PAT_LIAB2                --PHAR NOT USE
# MAGIC     ,'000000000000000'                                                                                                      AS AMT_TPL_APPLD2               --PHAR NOT USE
# MAGIC     ,CAST(clm.THE_PAID_AMT AS STRING)                                                                                       AS NA83                         --New Mapping Sep/2024
# MAGIC     ,NULL                                                                                                                   AS NA84                         --EMPTY
# MAGIC     ,'00000000000'                                                                                                          AS AMT_APL_PAT_LIAB2            --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS NA85                         --EMPTY
# MAGIC     ,'00000000000'                                                                                                          AS AMT_TPL_SUBM2                --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_TOOTH_NBR                --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_TOOTH_SURFACE_1          --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_TOOTH_SURFACE_2          --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_TOOTH_SURFACE_3          --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_TOOTH_SURFACE_4          --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_TOOTH_SURFACE_5          --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_TOOTH_SURFACE_6          --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS NA86                         --EMPTY
# MAGIC     ,CASE WHEN clm.MCO_ADJUD_DT::timestamp::date = '1800-01-01'::date                                                       
# MAGIC           THEN '0000000000'                                                                                                 
# MAGIC 		  ELSE CAST(DATE_FORMAT(CAST(clm.MCO_ADJUD_DT  AS DATE),'MM/dd/y') AS varchar(10)) END                          AS DTE_MCO_ADJUD2               --same as header value
# MAGIC     ,NULL                                                                                                                   AS AMT_SPENDDOWN2               --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS IND_EPSDT                    --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS NA87                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS NA88                         --EMPTY
# MAGIC     ,'01'                                                                                                                   AS CDE_POS                      --Added Field/Use Hardcode P=01, Q=01
# MAGIC     ,'000000000000000'                                                                                                      AS AMT_REIMBURSED2              --PHAR USE AT HEADER LEVEL ONLY
# MAGIC     ,COALESCE(CAST(clm.PD_MCARE_AMT AS varchar(15)),'0.00')                                                                 AS AMT_PAID_MCARE2
# MAGIC     ,COALESCE(CAST(clm.COINSR_AMT AS varchar(15)),'0.00')                                                                   AS AMT_COINSURANCE2
# MAGIC     ,0                                                                                                                      AS QTY_DAYS_COINSURANCE         --PHAR NOT USE
# MAGIC     ,CASE WHEN TRIM(clm.NDC_CD)='-1' THEN NULL                                                                              
# MAGIC 	      ELSE COALESCE(SUBSTRING(TRIM(clm.NDC_CD),1,11),'') END                                                          AS CDE_NDC
# MAGIC     ,COALESCE(CAST(clm.DEDUCT_AMT AS varchar(8)),'0.00')                                                                    AS AMT_DEDUCT2
# MAGIC     ,NULL                                                                                                                   AS CDE_THERA_CLS_AHFS           --Removed Field
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.THERA_CLS_SPEC_CD),1,3),'')                                                                AS CDE_THERA_CLS_SPEC           --Added Field
# MAGIC     ,NULL                                                                                                                   AS NA89                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS NA90                         --EMPTY
# MAGIC     ,NULL                                                                                                                   AS CDE_PROC_PRIM                --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_MODIFIER_1               --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_MODIFIER_2               --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_MODIFIER_3               --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_MODIFIER_4               --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS NA91                         --EMPTY
# MAGIC     ,CASE WHEN TRIM(clm.FUND_CD) = '' THEN '#########' 
# MAGIC           ELSE COALESCE(SUBSTRING(TRIM(clm.FUND_CD),1,9),'#########') END                                                   AS CDE_FUND_CODE                --Added Field
# MAGIC     ,NULL                                                                                                                   AS CDE_RATE_TYPE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.ICN_NBR),1,15),'')                                                                         AS NUM_ICN2
# MAGIC     ,CASE WHEN TRIM(clm.ADJ_ICN_NBR) = '' THEN NULL                                                                         
# MAGIC                ELSE	COALESCE(SUBSTRING(TRIM(clm.ADJ_ICN_NBR),1,20),NULL)                                                    
# MAGIC                END                                                                                                          AS NUM_ADJ_ICN2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.TCN_NBR),1,18),'')                                                                         AS NUM_TCN2                     --Added Field
# MAGIC     ,COALESCE(SUBSTRING(TRIM(clm.MEDICAID_ID),1,12),'')                                                                     AS ID_MEDICAID2                 --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS ID_PROVIDER_MCAID1           --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS ID_PROVIDER_NPI1             --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_PROV_TYPE_PRIM_BLANK_1   --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_SVC_COUNTY1              --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_TAXONOMY1                --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_PROV_SPEC_PRIM1          --PHAR NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_MEDICAID_ID),1,10),'')                                                                AS ID_PROVIDER_MCAID2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_NPI),1,10),'')                                                                        AS ID_PROVIDER_NPI2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_MMIS_PROV_TYP_ID),1,2),'')                                                            AS BILLING_CDE_PROV_TYPE_PRIM
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_PROGRAM_ID),1,5),'')                                                                  AS CDE_PROV_PGM1
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_SL_OGRIP_CNTY_CD),1,10),'')                                                           AS CDE_SVC_COUNTY2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_TXNMY_CD),1,10),'')                                                                   AS CDE_TAXONOMY2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(BillProv_PRIMARY_SPCLTY_CD),1,3),'')                                                           AS CDE_PROV_SPEC_PRIM2
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RndngProv_MEDICAID_ID),1,10),'')                                                               AS ID_PROVIDER_MCAID3           --Fixed ISSUE#60
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RndngProv_NPI),1,10),'')                                                                       AS ID_PROVIDER_NPI3             --Fixed ISSUE#60
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RndngProv_MMIS_PROV_TYP_ID),1,2),'')                                                           AS RENDERING_CDE_PROV_TYPE_PRIM
# MAGIC     ,NULL                                                                                                                   AS CDE_PROV_PGM2                --PHAR NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RndngProv_SL_OGRIP_CNTY_CD),1,10),'')                                                          AS CDE_SVC_COUNTY3
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RndngProv_TXNMY_CD),1,10),'')                                                                  AS CDE_TAXONOMY3                --Fixed ISSUE#60
# MAGIC     ,COALESCE(SUBSTRING(TRIM(RndngProv_PRIMARY_SPCLTY_CD),1,3),'')                                                          AS CDE_PROV_SPEC_PRIM3
# MAGIC     ,NULL                                                                                                                   AS ID_PROVIDER_MCAID4           --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS ID_PROVIDER_NPI4             --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_PROV_TYPE_PRIM_BLANK_2   --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_PROV_SPEC_PRIM4          --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS ID_PROVIDER_MCAID5           --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS ID_PROVIDER_NPI5             --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_PROV_TYPE_PRIM_BLANK_3   --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_PROV_SPEC_PRIM5          --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS ID_PROVIDER_MCAID6           --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS ID_PROVIDER_NPI6             --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_PROV_TYPE_PRIM_BLANK_4   --PHAR NOT USE
# MAGIC     ,NULL                                                                                                                   AS CDE_PROV_SPEC_PRIM6          --PHAR NOT USE
# MAGIC     ,COALESCE(SUBSTRING(TRIM(PrscbngProv_MEDICAID_ID),1,10),'')                                                             AS ID_PROVIDER_MCAID7
# MAGIC     ,COALESCE(SUBSTRING(TRIM(PrscbngProv_NPI),1,10),'')                                                                     AS ID_PROVIDER_NPI7
# MAGIC     ,COALESCE(SUBSTRING(TRIM(PrscbngProv_MMIS_PROV_TYP_ID),1,2),'')                                                         AS PRESCRIBING_CDE_PROV_TYPE_PRIM
# MAGIC     ,COALESCE(SUBSTRING(TRIM(PrscbngProv_PRIMARY_SPCLTY_CD),1,3),'')                                                        AS CDE_PROV_SPEC_PRIM7
# MAGIC     ,COALESCE(SUBSTRING(TRIM(MCP_MEDICAID_ID),1,10),'')                                                                     AS ID_PROVIDER_MCAID8           --New mapped field/Fixed ISSUE#60
# MAGIC     ,COALESCE(SUBSTRING(TRIM(MCP_MMIS_PROV_TYP_ID),1,2),'')                                                                 AS MCP_CDE_PROV_TYPE_PRIM
# MAGIC     ,COALESCE(SUBSTRING(TRIM(MCP_PRIMARY_SPCLTY_CD),1,3),'')                                                                AS CDE_PROV_SPEC_PRIM8
# MAGIC from 
# MAGIC (
# MAGIC         select clmMain.* 
# MAGIC               ,CASE UPPER(TRIM(ClmMain.CLM_TYP_CD)) WHEN 'P' THEN 'P'
# MAGIC                                                     WHEN 'Q' THEN 'Q'
# MAGIC                                                     ELSE ''
# MAGIC                                                     END  AS CODE_CLM_TYPE
# MAGIC               ,CASE UPPER(TRIM(ClmMain.ENCTR_OR_FFS_DESC)) WHEN 'FFS'       THEN 'F'
# MAGIC                                                            WHEN 'ENCOUNTER' THEN 'E'
# MAGIC                                                            ELSE SUBSTRING(TRIM(ClmMain.ENCTR_OR_FFS_DESC),1,1)
# MAGIC                                                            END  AS INDICATOR_CLAIM
# MAGIC               ,BillProv_PROGRAM_ID
# MAGIC               ,BillProv_SL_OGRIP_CNTY_CD
# MAGIC               ,BillProv_MEDICAID_ID
# MAGIC               ,BillProv_NPI
# MAGIC               ,BillProv_MMIS_PROV_TYP_ID			  
# MAGIC               ,RndngProv_SL_OGRIP_CNTY_CD
# MAGIC               ,RndngProv_MEDICAID_ID
# MAGIC               ,RndngProv_MMIS_PROV_TYP_ID
# MAGIC               ,RndngProv_PRIMARY_SPCLTY_CD
# MAGIC               ,PrscbngProv_MEDICAID_ID
# MAGIC               ,PrscbngProv_MMIS_PROV_TYP_ID
# MAGIC               ,PrscbngProv_PRIMARY_SPCLTY_CD
# MAGIC               ,BillProv_TXNMY_CD
# MAGIC               ,RndngProv_NPI
# MAGIC               ,RndngProv_TXNMY_CD
# MAGIC               ,PrscbngProv_NPI
# MAGIC               ,ClmAdtlExtOthrPyr_CONTRACT_SUB_ID
# MAGIC               ,ClmAdtlExtEOB_CLAIM_ADTL_EOB_SK1
# MAGIC               ,ClmAdtlExtEOB_CLAIM_ADTL_EOB_SK2
# MAGIC               --,ClmAdtlExtOthrPyr_ALWD_OTH_PYR_AMT
# MAGIC               ,BillProv_PRIMARY_SPCLTY_CD
# MAGIC               ,MCP_MEDICAID_ID
# MAGIC               ,MCP_MMIS_PROV_TYP_ID
# MAGIC               ,MCP_PRIMARY_SPCLTY_CD
# MAGIC               ,SumOverSakClm_TPL_AMT
# MAGIC               ,SumOverSakClm_THE_PAID_AMT
# MAGIC               ,SumOverSakClm_DTL_BILL_AMT
# MAGIC               ,SumOverSakClm_DTL_ALWD_AMT
# MAGIC               ,SumOverSakClm_DTL_CO_PAY_AMT
# MAGIC               ,SumOverSakClm_THE_DETAIL_PAID_AMT
# MAGIC               ,ATTENDING_EXTERNALPROVID
# MAGIC               ,ATTENDING_EXTERNALPROVIDQUALIFIER
# MAGIC               ,ATTENDING_PROV_NAME
# MAGIC               ,ATTENDING_PROVID
# MAGIC               ,ATTENDING_SAK_PROV_ID
# MAGIC               ,BILLING_EXTERNALPROVID
# MAGIC               ,BILLING_EXTERNALPROVIDQUALIFIER
# MAGIC               ,BILLING_MEDICAID_ID
# MAGIC               ,BILLING_PROV_NAME
# MAGIC               ,BILLING_PROVID
# MAGIC               ,BILLING_SAK_PROV_ID
# MAGIC               ,BILLING_MMIS_PROV_TYP
# MAGIC               ,BILLING_PROV_TYPE_NM
# MAGIC               ,OPERATING_EXTERNALPROVID
# MAGIC               ,OPERATING_EXTERNALPROVIDQUALIFIER
# MAGIC               ,OPERATING_PROV_NAME
# MAGIC               ,OPERATING_PROVID
# MAGIC               ,OPERATING_SAK_PROV_ID
# MAGIC               ,ORDERING_EXTERNALPROVID
# MAGIC               ,ORDERING_EXTERNALPROVIDQUALIFIER
# MAGIC               ,ORDERING_PROV_NAME
# MAGIC               ,ORDERING_PROVID
# MAGIC               ,ORDERING_SAK_PROV_ID
# MAGIC               ,PRESCRIBING_PROV_NAME
# MAGIC               ,PRESCRIBING_EXTERNALPROVID
# MAGIC               ,PRESCRIBING_EXTERNALPROVIDQUALIFIER
# MAGIC               ,PRESCRIBING_PROVID
# MAGIC               ,PRESCRIBING_SAK_PROV_ID
# MAGIC               ,REFERRING_EXTERNALPROVID
# MAGIC               ,REFERRING_EXTERNALPROVIDQUALIFIER
# MAGIC               ,REFERRING_PROV_NAME
# MAGIC               ,REFERRING_PROVID
# MAGIC               ,REFERRING_SAK_PROV_ID
# MAGIC               ,RENDERING_EXTERNALPROVID
# MAGIC               ,RENDERING_EXTERNALPROVIDQUALIFIER
# MAGIC               ,RENDERING_PROV_NAME
# MAGIC               ,RENDERING_PROVID
# MAGIC               ,RENDERING_SAK_PROV_ID
# MAGIC               ,SERVICE_EXTERNALPROVIDID
# MAGIC               ,SERVICE_EXTERNALPROVIDQUALIFIER
# MAGIC               ,SERVICE_PROV_NAME
# MAGIC               ,SERVICE_PROVID
# MAGIC               ,SERVICE_SAK_PROV
# MAGIC               ,SERVICE_MMIS_PROV_TYP
# MAGIC               ,SERVICE_PROV_TYPE_NM
# MAGIC         from ${catalog}.${schema_name}.${VEN100FA} clmMain
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_CLAIM
# MAGIC                   ,LPAD( CAST((SUM(COALESCE(TPL_AMT,0)) * 100) AS varchar(15)) ,15,'0')                   
# MAGIC                         AS SumOverSakClm_TPL_AMT
# MAGIC                   ,CASE WHEN SUM(THE_PAID_AMT) < 0                                                                       
# MAGIC 	                    THEN '-'||LPAD( CAST(SUM(COALESCE(THE_PAID_AMT,0)) * -1 AS varchar(10)) ,10,'0')
# MAGIC                         ELSE LPAD( CAST(SUM(COALESCE(THE_PAID_AMT,0)) AS varchar(11)) ,11,'0')
# MAGIC                         END AS SumOverSakClm_THE_PAID_AMT
# MAGIC                   ,CASE WHEN SUM(COALESCE(DTL_BILL_AMT,0)) < 0                                     
# MAGIC 	                    THEN '-'||LPAD( CAST(SUM(COALESCE(DTL_BILL_AMT,0)) * -100 AS varchar(14)) ,14,'0')
# MAGIC                         ELSE LPAD( CAST(SUM(COALESCE(DTL_BILL_AMT,0)) * 100 AS varchar(15)) ,15,'0')      
# MAGIC                          END AS SumOverSakClm_DTL_BILL_AMT
# MAGIC                   ,CASE WHEN SUM(COALESCE(DTL_ALWD_AMT,0)) < 0                                       
# MAGIC 	                    THEN '-'||LPAD( CAST(SUM(COALESCE(DTL_ALWD_AMT,0)) * -100 AS varchar(14)),14,'0') 
# MAGIC                         ELSE LPAD( CAST(SUM(COALESCE(DTL_ALWD_AMT,0)) * 100 AS varchar(15)),15,'0')       
# MAGIC                          END AS SumOverSakClm_DTL_ALWD_AMT
# MAGIC                   ,CASE WHEN SUM(COALESCE(DTL_CO_PAY_AMT,0)) < 0                                   
# MAGIC                         THEN '-'||LPAD(CAST(SUM(COALESCE(DTL_CO_PAY_AMT,0)) * -100 AS varchar(10)),10,'0')
# MAGIC                         ELSE LPAD( CAST(SUM(COALESCE(DTL_CO_PAY_AMT,0)) * 100 AS varchar(11)),11,'0')     
# MAGIC                          END AS SumOverSakClm_DTL_CO_PAY_AMT
# MAGIC                   ,CASE WHEN SUM(COALESCE(THE_DETAIL_PAID_AMT,0)) < 0                                           
# MAGIC 		                THEN '-'||LPAD( CAST(SUM(COALESCE(THE_DETAIL_PAID_AMT,0)) * -1 AS varchar(10)),10,'0')         
# MAGIC                         ELSE LPAD( CAST(SUM(COALESCE(THE_DETAIL_PAID_AMT,0)) AS varchar(11)),11,'0')                   
# MAGIC                          END  AS SumOverSakClm_THE_DETAIL_PAID_AMT
# MAGIC               from ${catalog}.${schema_name}.${VEN100FA}
# MAGIC           group by 1
# MAGIC         ) ClmTmp
# MAGIC         on clmMain.SAK_CLAIM = ClmTmp.SAK_CLAIM
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_PROV
# MAGIC                   ,PROGRAM_ID       AS BillProv_PROGRAM_ID
# MAGIC               from ${catalog}.${schema_name}.${VEN117FA11}
# MAGIC         ) ProvCntrctExtrct
# MAGIC         on clmMain.SAK_PROV = ProvCntrctExtrct.SAK_PROV
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_PROV
# MAGIC                   ,SL_OGRIP_CNTY_CD  AS BillProv_SL_OGRIP_CNTY_CD
# MAGIC 				  ,PRIMARY_SPCLTY_CD AS BillProv_PRIMARY_SPCLTY_CD
# MAGIC                   ,MEDICAID_ID       AS BillProv_MEDICAID_ID
# MAGIC                   ,NPI               AS BillProv_NPI
# MAGIC                   ,MMIS_PROV_TYP_ID  AS BillProv_MMIS_PROV_TYP_ID
# MAGIC               from ${catalog}.${schema_name}.${VEN117FA1}
# MAGIC         ) ProvExtrct
# MAGIC         on clmMain.SAK_PROV = ProvExtrct.SAK_PROV
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_PROV
# MAGIC                   ,SL_OGRIP_CNTY_CD  AS RndngProv_SL_OGRIP_CNTY_CD
# MAGIC                   ,MEDICAID_ID       AS RndngProv_MEDICAID_ID
# MAGIC                   ,NPI               AS RndngProv_NPI
# MAGIC                   ,MMIS_PROV_TYP_ID  AS RndngProv_MMIS_PROV_TYP_ID
# MAGIC                   ,PRIMARY_SPCLTY_CD AS RndngProv_PRIMARY_SPCLTY_CD
# MAGIC               from ${catalog}.${schema_name}.${VEN117FA1}
# MAGIC         ) ProvExtrct2
# MAGIC         on clmMain.SAK_PROV = ProvExtrct2.SAK_PROV       -- EDW staged that SAK_PROV will have Rendering Info. for Pharmacy CT.
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_PROV
# MAGIC                   ,MEDICAID_ID       AS PrscbngProv_MEDICAID_ID
# MAGIC                   ,NPI               AS PrscbngProv_NPI
# MAGIC                   ,MMIS_PROV_TYP_ID  AS PrscbngProv_MMIS_PROV_TYP_ID
# MAGIC                   ,PRIMARY_SPCLTY_CD AS PrscbngProv_PRIMARY_SPCLTY_CD
# MAGIC               from ${catalog}.${schema_name}.${VEN117FA1}
# MAGIC         ) ProvExtrct3
# MAGIC         on clmMain.ORP_PROV_SAK_ID = ProvExtrct3.SAK_PROV
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
# MAGIC         on clmMain.SAK_PROV = ProvTaxnmyExtrct.SAK_PROV
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_PROV
# MAGIC                   ,TXNMY_CD AS RndngProv_TXNMY_CD
# MAGIC               from ${catalog}.${schema_name}.${VEN117FA4}
# MAGIC         ) ProvTaxnmyExtrct2
# MAGIC         on clmMain.RPA_PROV_SAK_ID = ProvTaxnmyExtrct2.SAK_PROV
# MAGIC         left join
# MAGIC         (
# MAGIC             select SAK_CLAIM, DTL_NBR
# MAGIC                   ,MAX(CONTRACT_SUB_ID)  AS ClmAdtlExtOthrPyr_CONTRACT_SUB_ID
# MAGIC               from ${catalog}.${schema_name}.${VEN12403FA}
# MAGIC              group by 1,2
# MAGIC         ) ClmAdtlExtOthrPyr
# MAGIC         on clmMain.SAK_CLAIM = ClmAdtlExtOthrPyr.SAK_CLAIM and clmMain.DTL_NBR = ClmAdtlExtOthrPyr.DTL_NBR
# MAGIC /*        left join
# MAGIC         (
# MAGIC             select SAK_CLAIM
# MAGIC                   ,SUM(ALWD_OTH_PYR_AMT)  AS ClmAdtlExtOthrPyr_ALWD_OTH_PYR_AMT
# MAGIC               from ${catalog}.${schema_name}.${VEN12403FA}
# MAGIC              group by 1
# MAGIC         ) ClmAdtlExtOthrPyr2
# MAGIC         on clmMain.SAK_CLAIM = ClmAdtlExtOthrPyr2.SAK_CLAIM */
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
# MAGIC         on clmMain.SAK_CLAIM = ClmAdtlExtEOB.SAK_CLAIM and clmMain.DTL_NBR = ClmAdtlExtEOB.DTL_NBR
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
# MAGIC         where TRIM(clmMain.CLM_TYP_CD) in ('P','Q')
# MAGIC )clm
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Full Refresh Or Delta Step.- Truncate/Delete Main cl_phar and Phar_Analytics Tables
if CleanDstntTblsFlag == 'T':
    # Logic to process Full Refresh VE Source Type
    #
    sql_out = spark.sql(f"""
                        TRUNCATE TABLE {catalog}.{schema_name}.{cl_phar}
                        ;
                        """)
    display(sql_out)
    sql_out = spark.sql(f"""
                        TRUNCATE TABLE {catalog}.{schema_name}.{Phar_Analytics}
                        ;
                        """)
    display(sql_out)
else:
    # Logic to process Delta VE source type
    #
    sql_out = spark.sql(f"""
                        DELETE 
                         FROM {catalog}.{schema_name}.{cl_phar} t1
                        WHERE TRUE
                          AND EXISTS
                              ( SELECT 1
                                  FROM {catalog}.{schema_name}.{cl_phar_stg} t2
                                 WHERE TRUE
                                   AND t2.NUM_ICN1 = t1.NUM_ICN1
                                   AND t2.NUM_DTL  = t1.NUM_DTL
                              )
                        ;
                        """)
    display(sql_out)
    sql_out = spark.sql(f"""
                        DELETE 
                         FROM {catalog}.{schema_name}.{Phar_Analytics} t1
                        WHERE TRUE
                          AND EXISTS
                              ( SELECT 1
                                  FROM {catalog}.{schema_name}.{cl_phar_stg} t2
                                 WHERE TRUE
                                   AND t2.NUM_ICN1 = t1.NUM_ICN1
                                   AND t2.NUM_DTL  = t1.NUM_DTL
                              )
                        ;
                        """)
    display(sql_out)


# COMMAND ----------

# DBTITLE 1,Load cl_phar table
# MAGIC %sql
# MAGIC INSERT INTO TABLE ${catalog}.${schema_name}.${cl_phar}
# MAGIC SELECT * FROM ${catalog}.${schema_name}.${cl_phar_stg} a
# MAGIC WHERE a.num_dtl not in('#000','#001')
# MAGIC AND a.NUM_ICN1 is not null
# MAGIC AND a.ID_MEDICAID1 is not null
# MAGIC AND a.DERIVED1 NOT IN ('T')
# MAGIC -- AND a.CDE_HDR_STATUS = 'P' -- 8/25/2025 Include Deny Claims
# MAGIC AND NOT EXISTS
# MAGIC (
# MAGIC     SELECT 1 
# MAGIC     FROM ${catalog}.${schema_name}.${cl_phar} b
# MAGIC     WHERE true
# MAGIC     AND b.NUM_ICN1 = a.NUM_ICN1
# MAGIC     AND b.NUM_DTL  = a.NUM_DTL
# MAGIC     -- OLD LOGIC COMMENTED
# MAGIC     -- AND NVL(b.ID_MEDICAID1      ,'') = NVL(a.ID_MEDICAID1      ,'')
# MAGIC     -- AND NVL(b.DTE_FIRST_SVC1    ,'') = NVL(a.DTE_FIRST_SVC1    ,'')
# MAGIC     -- AND NVL(b.ID_PROVIDER_MCAID2,'') = NVL(a.ID_PROVIDER_MCAID2,'')
# MAGIC     -- AND NVL(b.CDE_NDC           ,'') = NVL(a.CDE_NDC           ,'')
# MAGIC     -- AND NVL(b.NUM_PRSCRIP       ,'') = NVL(a.NUM_PRSCRIP       ,'')
# MAGIC )
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Truncate Phar_Analytics_Staging table
# MAGIC %sql
# MAGIC TRUNCATE TABLE ${catalog}.${schema_name}.${Phar_Analytics_stg}
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Load Phar_Analytics_Staging table
# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${Phar_Analytics_stg}
# MAGIC SELECT DISTINCT
# MAGIC     --CODE WITH COLUMNS MAPPING GOES HERE
# MAGIC      TRIM(NUM_ICN1)
# MAGIC     ,TRIM(IND_CLAIM)
# MAGIC     ,TRIM(CDE_CLM_TYPE)
# MAGIC     ,TRIM(CDE_HDR_STATUS)
# MAGIC     ,TRIM(CDE_PGM_HEALTH)
# MAGIC     ,TRIM(CDE_AID_CATEGORY)
# MAGIC     ,TRIM(ID_CLERK)
# MAGIC     ,TRIM(CDE_PRESCRIPTION_ORIG)
# MAGIC     ,TRIM(DERIVED1)
# MAGIC     ,TRIM(CDE_ENC_TYPE)
# MAGIC     ,TRIM(NA1)
# MAGIC     ,TRIM(ID_MEDICAID1)
# MAGIC     ,TRIM(DTE_BIRTH)
# MAGIC     ,TRIM(IND_BRAND_MED_NEC)
# MAGIC     ,ROUND(CAST(AMT_VACC_INCENTIVE AS NUMERIC(10,2))/100,2.0) as AMT_VACC_INCENTIVE
# MAGIC     ,NUM_PRIOR_AUTH
# MAGIC     ,TRIM(NA2)
# MAGIC     ,TRIM(NA3)
# MAGIC     ,TRIM(ID_CONTRACT_SUB)
# MAGIC     ,NUM_HIC_SUB
# MAGIC     ,NUM_CMS_ICN
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
# MAGIC     ,TRIM(NA15)
# MAGIC     ,TRIM(BILLING_MMIS_PROV_TYP) AS BILLING_MMIS_PROV_TYP
# MAGIC     ,TRIM(BILLING_PROV_TYPE_NM) AS BILLING_PROV_TYPE_NM
# MAGIC     ,TRIM(OPERATING_EXTERNALPROVID) AS OPERATING_EXTERNALPROVID
# MAGIC     ,NA19
# MAGIC     ,TRIM(OPERATING_EXTERNALPROVIDQUALIFIER) AS OPERATING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(CDE_SOI)
# MAGIC     ,TRIM(CDE_LEVEL_OF_CARE)
# MAGIC     ,TRIM(DERIVED2)
# MAGIC     ,TRIM(CDE_MDC)
# MAGIC     ,TRIM(OPERATING_PROV_NAME) AS OPERATING_PROV_NAME
# MAGIC     ,TRIM(OPERATING_PROVID) AS OPERATING_PROVID
# MAGIC     ,TRIM(CDE_PATIENT_STATUS)
# MAGIC     ,TRIM(CDE_EMERGENCY)
# MAGIC     ,TRIM(CDE_ADMIT_SOURCE)
# MAGIC     ,TRIM(CDE_ROM)
# MAGIC     ,TRIM(DTE_ADMISSION)
# MAGIC     ,CDE_ADMIT_HOUR
# MAGIC     ,TRIM(CDE_DRG)
# MAGIC     ,ROUND(CAST(AMT_BASE_DRG AS NUMERIC(15,2))/100,2.0) as AMT_BASE_DRG
# MAGIC     ,TRIM(DTE_DISCHARGE)
# MAGIC     ,TIME_DISCHARGE
# MAGIC     ,CDE_COMPOUND_DOSAGE
# MAGIC     ,ROUND(CAST(AMT_DAY_OUTLIER AS NUMERIC(9,2))/100,2.0) as AMT_DAY_OUTLIER
# MAGIC     ,ROUND(CAST(AMT_COST_OUTLIER AS NUMERIC(15,2))/100,2.0) as AMT_COST_OUTLIER
# MAGIC     ,CDE_MED_REC_NUM
# MAGIC     ,CDE_PEER_GROUP
# MAGIC     ,ROUND(CAST(AMT_COST_INGREDIENT AS NUMERIC(8))/100,2.0) as AMT_COST_INGREDIENT
# MAGIC     ,TRIM(OPERATING_SAK_PROV_ID) AS OPERATING_SAK_PROV_ID
# MAGIC     ,CDE_SOI_DISC
# MAGIC     ,CDE_ROM_DISC
# MAGIC     ,NUM_PA_REF
# MAGIC     ,ROUND(CAST(AMT_TPL_SUBM1 AS NUMERIC(15,2))/100,2.0) as AMT_TPL_SUBM1
# MAGIC     ,CAST(AMT_TPL_APPLD1 AS NUMERIC(15,2))/100 as AMT_TPL_APPLD1 -- Removed Round
# MAGIC     ,CAST(AMT_PAID_MCO1 AS NUMERIC(11,2)) as AMT_PAID_MCO1  -- Removed the by 100 & Round
# MAGIC     ,TRIM(ORDERING_EXTERNALPROVID) AS ORDERING_EXTERNALPROVID
# MAGIC     ,TRIM(IND_HDR_DTL)
# MAGIC     ,TRIM(ORDERING_EXTERNALPROVIDQUALIFIER) AS ORDERING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,CDE_COND_1
# MAGIC     ,CDE_COND_2
# MAGIC     ,CDE_COND_3
# MAGIC     ,CDE_COND_4
# MAGIC     ,CDE_COND_5
# MAGIC     ,CDE_COND_6
# MAGIC     ,CDE_COND_7
# MAGIC     ,CDE_COND_8
# MAGIC     ,TRIM(ORDERING_PROV_NAME) AS ORDERING_PROV_NAME
# MAGIC     ,CDE_PAY_ARR1
# MAGIC     ,NA27
# MAGIC     ,CDE_DRG_DISC
# MAGIC     ,CDE_CLARIFICATION1
# MAGIC     ,CDE_CLARIFICATION2
# MAGIC     ,CDE_CLARIFICATION3
# MAGIC     ,TRIM(ORDERING_PROVID) AS ORDERING_PROVID
# MAGIC     ,NA29
# MAGIC     ,DTE_BILLED
# MAGIC     ,CDE_RECIP_COUNTY
# MAGIC     ,ROUND(CAST(AMT_REIMBURSED1 AS NUMERIC(15,2))/100,2.0) as AMT_REIMBURSED1
# MAGIC     ,CAST(split_part(DTE_PAID1,'/',3) ||'-'|| split_part(DTE_PAID1,'/',1) ||'-'|| split_part(DTE_PAID1,'/',2) AS DATE)  -- Fix: 3-27-2024 Modified the CAST for DATE
# MAGIC     ,CDE_CLM_REGION
# MAGIC     ,TRIM(DTE_FIRST_SVC1)
# MAGIC     ,TRIM(DTE_LAST_SVC1)
# MAGIC     ,BATCH_date
# MAGIC     ,TRIM(ORDERING_SAK_PROV_ID) AS ORDERING_SAK_PROV_ID
# MAGIC     ,TRIM(PRESCRIBING_PROV_NAME) AS PRESCRIBING_PROV_NAME
# MAGIC     ,ROUND(CAST(AMT_BILLED1 AS NUMERIC(15,2))/100,2.0) as AMT_BILLED1
# MAGIC     ,QTY_UNITS_ALWD1
# MAGIC     ,NA32
# MAGIC     ,NUM_RECIP_AGE
# MAGIC     ,TRIM(PRESCRIBING_EXTERNALPROVID) AS PRESCRIBING_EXTERNALPROVID
# MAGIC     ,NA34
# MAGIC     ,ROUND(CAST(AMT_INTEREST AS NUMERIC(15,2))/100,2.0) as AMT_INTEREST
# MAGIC     ,DTE_MCO_ADJUD1
# MAGIC     ,ADR_ZIP_CODE
# MAGIC     ,ADR_ZIP_CODE_4
# MAGIC     ,TRIM(PRESCRIBING_EXTERNALPROVIDQUALIFIER) AS PRESCRIBING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(PRESCRIBING_PROVID) AS PRESCRIBING_PROVID
# MAGIC     ,TRIM(PRESCRIBING_SAK_PROV_ID) AS PRESCRIBING_SAK_PROV_ID
# MAGIC     ,TRIM(REFERRING_EXTERNALPROVID) AS REFERRING_EXTERNALPROVID
# MAGIC     ,TRIM(REFERRING_EXTERNALPROVIDQUALIFIER) AS REFERRING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,CDE_SEX
# MAGIC     ,CDE_RACE
# MAGIC     ,TRIM(REFERRING_PROV_NAME) AS REFERRING_PROV_NAME
# MAGIC     ,TRIM(REFERRING_PROVID) AS REFERRING_PROVID
# MAGIC     ,TRIM(REFERRING_SAK_PROV_ID) AS REFERRING_SAK_PROV_ID
# MAGIC     ,NUM_WEIGHT
# MAGIC     ,NUM_PAT_ACCT
# MAGIC     ,TRIM(RENDERING_EXTERNALPROVID) AS RENDERING_EXTERNALPROVID
# MAGIC     ,TRIM(RENDERING_EXTERNALPROVIDQUALIFIER) AS RENDERING_EXTERNALPROVIDQUALIFIER
# MAGIC     ,ROUND(CAST(AMT_SPENDDOWN1 AS NUMERIC(11,2))/100,2.0) as AMT_SPENDDOWN1
# MAGIC     ,TRIM(RENDERING_PROV_NAME) AS RENDERING_PROV_NAME
# MAGIC     ,TRIM(RENDERING_PROVID) AS RENDERING_PROVID
# MAGIC     ,ROUND(CAST(AMT_COINSURANCE1 AS NUMERIC(8,2))/100,2.0) as AMT_COINSURANCE1
# MAGIC     ,CDE_LIV_ARNG
# MAGIC     ,TRIM(RENDERING_SAK_PROV_ID) AS RENDERING_SAK_PROV_ID
# MAGIC     ,NUM_ADJ_ICN1
# MAGIC     ,NUM_VERSION_DRG
# MAGIC     ,TRIM(SERVICE_EXTERNALPROVIDID) AS SERVICE_EXTERNALPROVIDID
# MAGIC     ,NUM_RA
# MAGIC     ,ID_VENDOR
# MAGIC     ,TRIM(SERVICE_EXTERNALPROVIDQUALIFIER) AS SERVICE_EXTERNALPROVIDQUALIFIER
# MAGIC     ,TRIM(SERVICE_PROV_NAME) AS SERVICE_PROV_NAME
# MAGIC     ,NUM_WARRANT
# MAGIC     ,TRIM(SERVICE_PROVID) AS SERVICE_PROVID
# MAGIC     ,DTE_ENTERED_SYS
# MAGIC     ,ROUND(CAST(AMT_PAT_LIAB1 AS NUMERIC(11,2))/100,2.0) as AMT_PAT_LIAB1
# MAGIC     ,ROUND(CAST(AMT_APL_PAT_LIAB1 AS NUMERIC(11,2))/100,2.0) as AMT_APL_PAT_LIAB1
# MAGIC     ,DERIVED3
# MAGIC     ,DTE_GENERIC
# MAGIC     ,TRIM(SERVICE_SAK_PROV) AS SERVICE_SAK_PROV
# MAGIC     ,ROUND(CAST(AMT_PAID_MCARE1 AS NUMERIC(10,2))/100,2.0) as AMT_PAID_MCARE1
# MAGIC     ,CDE_OCCUR_1
# MAGIC     ,CDE_OCCUR_2
# MAGIC     ,CDE_OCCUR_3
# MAGIC     ,CDE_OCCUR_4
# MAGIC     ,CDE_OCCUR_5
# MAGIC     ,CDE_OCCUR_6
# MAGIC     ,CDE_OCCUR_7
# MAGIC     ,CDE_OCCUR_8
# MAGIC     ,TRIM(SERVICE_MMIS_PROV_TYP) AS SERVICE_MMIS_PROV_TYP
# MAGIC     ,DTE_OCCUR_1
# MAGIC     ,DTE_OCCUR_2
# MAGIC     ,DTE_OCCUR_3
# MAGIC     ,DTE_OCCUR_4
# MAGIC     ,DTE_OCCUR_5
# MAGIC     ,DTE_OCCUR_6
# MAGIC     ,DTE_OCCUR_7
# MAGIC     ,DTE_OCCUR_8
# MAGIC     ,CDE_EPSDT_FP
# MAGIC     ,IND_HYST1
# MAGIC     ,TRIM(SERVICE_PROV_TYPE_NM) AS SERVICE_PROV_TYPE_NM
# MAGIC     ,IND_STERILIZATION1
# MAGIC     ,NA55
# MAGIC     ,IND_ABORTION1
# MAGIC     ,NA56
# MAGIC     ,NA57
# MAGIC     ,NA58
# MAGIC     ,NA59
# MAGIC     ,ROUND(CAST(AMT_DEDUCT1 AS NUMERIC(15,2))/100,2.0) as AMT_DEDUCT1
# MAGIC     ,ROUND(CAST(AMT_MCARE_PAID AS NUMERIC(15,2))/100,2.0) as AMT_MCARE_PAID
# MAGIC     ,NA60
# MAGIC     ,NA61
# MAGIC     ,NUM_PRSCRIP
# MAGIC     ,DTE_PRESCRIB
# MAGIC     ,NA62
# MAGIC     ,NUM_TCN1
# MAGIC     ,ROUND(CAST(AMT_ALWD1 AS NUMERIC(15,2))/100,2.0) as AMT_ALWD1
# MAGIC     ,NA63
# MAGIC     ,NA64
# MAGIC     ,ROUND(CAST(AMT_NDC_PROFEE AS NUMERIC(9,2))/100,2.0) as AMT_NDC_PROFEE
# MAGIC     ,NA65
# MAGIC     ,NA66
# MAGIC     ,NA67
# MAGIC     ,NA68
# MAGIC     ,NA69
# MAGIC     ,ROUND(CAST(AMT_CO_PAY1 AS NUMERIC(11,2))/100,2.0) as AMT_CO_PAY1
# MAGIC     ,ROUND(CAST(AMT_PAID1 AS NUMERIC(11,2)),2.0) as AMT_PAID1
# MAGIC     ,NA70
# MAGIC     ,ROUND(CAST(TRIM(QTY_PRESCRIBED) AS NUMERIC(20,3))/1000,3.0) as QTY_PRESCRIBED
# MAGIC     ,CDE_COS_ST
# MAGIC     ,CDE_COS_SUB
# MAGIC     ,NA73
# MAGIC     ,NA74
# MAGIC     ,ID_VOUCHER_RELATED
# MAGIC     ,NA75
# MAGIC     ,CDE_TYPE_OF_BILL1
# MAGIC     ,CDE_TYPE_OF_BILL2
# MAGIC     ,CDE_TYPE_OF_BILL3
# MAGIC     ,QTY_REFILL
# MAGIC     ,NA76
# MAGIC     ,DTE_DISPENSE
# MAGIC     ,QTY_DISPENSE1
# MAGIC     ,NUM_DAY_SUPPLY
# MAGIC     ,TRIM(CDE_DTL_STATUS)
# MAGIC     ,CDE_PAY_ARR2
# MAGIC     ,QTY_UNITS_ALWD2
# MAGIC     ,QTY_DISPENSE2
# MAGIC     ,DERIVED4
# MAGIC     ,NA77
# MAGIC     ,CASE WHEN AMT_AWP ilike '%#%' THEN null ELSE ROUND(CAST(AMT_AWP AS NUMERIC(30,7))/10000000,7.0) end as AMT_AWP
# MAGIC     ,CDE_MCAR_COVRG
# MAGIC     ,IND_PHARMACY_FAMILY_PLAN
# MAGIC     ,IND_REBATE_ELIG
# MAGIC     ,CDE_DISP_STATUS
# MAGIC     ,TRIM(IS_NON_DUPLICATE_IND) AS IS_NON_DUPLICATE_IND
# MAGIC     ,CDE_EOB_1
# MAGIC     ,CDE_EOB_2
# MAGIC     ,IND_PRICING
# MAGIC     ,CAST(AMT_ALWD2 AS NUMERIC(15,2))/100 as AMT_ALWD2 -- Removed Round
# MAGIC     ,IND_STERILIZATION2
# MAGIC     ,TRIM(CLAIM_ACTIVE_IND) AS CLAIM_ACTIVE_IND
# MAGIC     ,IND_HYST2
# MAGIC     ,TRIM(LAST_CLAIM_IND) AS LAST_CLAIM_IND
# MAGIC     ,IND_ABORTION2
# MAGIC     ,TRIM(IS_DKP_IND) AS IS_DKP_IND
# MAGIC     ,NUM_DAYS_COVD
# MAGIC     ,NUM_DAYS_NCOVD
# MAGIC     ,NUM_LEAVE_DAYS
# MAGIC     ,ROUND(CAST(AMT_MAC AS NUMERIC(30,7))/10000000,7.0) as AMT_MAC
# MAGIC     ,ROUND(CAST(AMT_CO_PAY2 AS NUMERIC(15,2))/100,2.0) as AMT_CO_PAY2
# MAGIC     ,CDE_COPAY_REASON
# MAGIC     ,CAST(NUM_DTL AS NUMERIC(4,0))
# MAGIC     ,DTE_FIRST_SVC2
# MAGIC     ,DTE_LAST_SVC2
# MAGIC     ,QTY_UNITS_BILLED
# MAGIC     ,CDE_REVENUE
# MAGIC     ,CAST(AMT_BILLED2 AS NUMERIC(15,2))          -- Fixed issue/removed by 100
# MAGIC     ,ROUND(CAST(AMT_NON_COVERED AS NUMERIC(15,2))/100,2.0) as AMT_NON_COVERED
# MAGIC     ,ROUND(CAST(AMT_PAID_MCO2 AS NUMERIC(11,2))/100,2.0) as AMT_PAID_MCO2
# MAGIC     ,NA82
# MAGIC     ,ROUND(CAST(AMT_PAID2 AS NUMERIC(11,2))/100,2.0) as AMT_PAID2
# MAGIC     ,DTE_PAID2
# MAGIC     ,ROUND(CAST(AMT_PAT_LIAB2 AS NUMERIC(15,2))/100,2.0) as AMT_PAT_LIAB2
# MAGIC     ,ROUND(CAST(AMT_TPL_APPLD2 AS NUMERIC(15,2))/100,2.0) as AMT_TPL_APPLD2
# MAGIC     ,TRIM(NA83)
# MAGIC     ,NA84
# MAGIC     ,ROUND(CAST(AMT_APL_PAT_LIAB2 AS NUMERIC(11,2))/100,2.0) as AMT_APL_PAT_LIAB2
# MAGIC     ,NA85
# MAGIC     ,ROUND(CAST(AMT_TPL_SUBM2 AS NUMERIC(15,2))/100,2.0) as AMT_TPL_SUBM2
# MAGIC     ,CDE_TOOTH_NBR
# MAGIC     ,CDE_TOOTH_SURFACE_1
# MAGIC     ,CDE_TOOTH_SURFACE_2
# MAGIC     ,CDE_TOOTH_SURFACE_3
# MAGIC     ,CDE_TOOTH_SURFACE_4
# MAGIC     ,CDE_TOOTH_SURFACE_5
# MAGIC     ,CDE_TOOTH_SURFACE_6
# MAGIC     ,NA86
# MAGIC     ,DTE_MCO_ADJUD2
# MAGIC     ,ROUND(CAST(AMT_SPENDDOWN2 AS NUMERIC(14,2))/100,2.0) as AMT_SPENDDOWN2
# MAGIC     ,IND_EPSDT
# MAGIC     ,NA87
# MAGIC     ,NA88
# MAGIC     ,TRIM(CDE_POS)
# MAGIC     ,ROUND(CAST(AMT_REIMBURSED2 AS NUMERIC(15,2))/100,2.0) as AMT_REIMBURSED2
# MAGIC     ,ROUND(CAST(AMT_PAID_MCARE2 AS NUMERIC(15,2))/100,2.0) as AMT_PAID_MCARE2
# MAGIC     ,ROUND(CAST(AMT_COINSURANCE2 AS NUMERIC(15,2))/100,2.0) as AMT_COINSURANCE2
# MAGIC     ,QTY_DAYS_COINSURANCE
# MAGIC     ,TRIM(CDE_NDC)
# MAGIC     ,ROUND(CAST(AMT_DEDUCT2 AS NUMERIC(8,2))/100,2.0) as AMT_DEDUCT2
# MAGIC     ,CDE_THERA_CLS_AHFS
# MAGIC     ,CDE_THERA_CLS_SPEC
# MAGIC     ,NA89
# MAGIC     ,NA90
# MAGIC     ,TRIM(CDE_PROC_PRIM)
# MAGIC     ,TRIM(CDE_MODIFIER_1)
# MAGIC     ,TRIM(CDE_MODIFIER_2)
# MAGIC     ,TRIM(CDE_MODIFIER_3)
# MAGIC     ,TRIM(CDE_MODIFIER_4)
# MAGIC     ,NA91
# MAGIC     ,CDE_FUND_CODE
# MAGIC     ,CDE_RATE_TYPE
# MAGIC     ,NUM_ICN2
# MAGIC     ,NUM_ADJ_ICN2
# MAGIC     ,NUM_TCN2
# MAGIC     ,TRIM(ID_MEDICAID2)
# MAGIC     ,TRIM(ID_PROVIDER_MCAID1)
# MAGIC     ,TRIM(ID_PROVIDER_NPI1)
# MAGIC     ,CDE_PROV_TYPE_PRIM_BLANK_1
# MAGIC     ,CDE_SVC_COUNTY1
# MAGIC     ,CDE_TAXONOMY1
# MAGIC     ,CDE_PROV_SPEC_PRIM1
# MAGIC     ,TRIM(ID_PROVIDER_MCAID2)
# MAGIC     ,TRIM(ID_PROVIDER_NPI2)
# MAGIC     ,BILLING_CDE_PROV_TYPE_PRIM
# MAGIC     ,CDE_PROV_PGM1
# MAGIC     ,CDE_SVC_COUNTY2
# MAGIC     ,CDE_TAXONOMY2
# MAGIC     ,CDE_PROV_SPEC_PRIM2
# MAGIC     ,TRIM(ID_PROVIDER_MCAID3)
# MAGIC     ,TRIM(ID_PROVIDER_NPI3)
# MAGIC     ,RENDERING_CDE_PROV_TYPE_PRIM
# MAGIC     ,CDE_PROV_PGM2
# MAGIC     ,CDE_SVC_COUNTY3
# MAGIC     ,CDE_TAXONOMY3
# MAGIC     ,CDE_PROV_SPEC_PRIM3
# MAGIC     ,TRIM(ID_PROVIDER_MCAID4)
# MAGIC     ,TRIM(ID_PROVIDER_NPI4)
# MAGIC     ,CDE_PROV_TYPE_PRIM_BLANK_2
# MAGIC     ,CDE_PROV_SPEC_PRIM4
# MAGIC     ,TRIM(ID_PROVIDER_MCAID5)
# MAGIC     ,TRIM(ID_PROVIDER_NPI5)
# MAGIC     ,CDE_PROV_TYPE_PRIM_BLANK_3
# MAGIC     ,CDE_PROV_SPEC_PRIM5
# MAGIC     ,TRIM(ID_PROVIDER_MCAID6)
# MAGIC     ,TRIM(ID_PROVIDER_NPI6)
# MAGIC     ,CDE_PROV_TYPE_PRIM_BLANK_4
# MAGIC     ,CDE_PROV_SPEC_PRIM6
# MAGIC     ,TRIM(ID_PROVIDER_MCAID7)
# MAGIC     ,TRIM(ID_PROVIDER_NPI7)
# MAGIC     ,PRESCRIBING_CDE_PROV_TYPE_PRIM
# MAGIC     ,CDE_PROV_SPEC_PRIM7
# MAGIC     ,TRIM(ID_PROVIDER_MCAID8)
# MAGIC     ,MCP_CDE_PROV_TYPE_PRIM
# MAGIC     ,CDE_PROV_SPEC_PRIM8
# MAGIC     ,year(COALESCE(CAST(dte_paid1 AS DATE), current_date)) * 100 + month(COALESCE(CAST(dte_paid1 AS DATE),current_date)) as partition_col
# MAGIC
# MAGIC FROM ${catalog}.${schema_name}.${cl_phar_stg} a
# MAGIC WHERE a.num_dtl not in('#000','#001')
# MAGIC AND a.NUM_ICN1 is not null
# MAGIC AND a.ID_MEDICAID1 is not null
# MAGIC AND a.DERIVED1 not in ('T')
# MAGIC AND a.CDE_HDR_STATUS = 'P'
# MAGIC AND NOT EXISTS
# MAGIC (
# MAGIC     SELECT 1 
# MAGIC     FROM ${catalog}.${schema_name}.${Phar_Analytics} b
# MAGIC     WHERE true
# MAGIC     AND b.NUM_ICN1 = a.NUM_ICN1
# MAGIC     AND b.NUM_DTL  = a.NUM_DTL
# MAGIC     -- COMMENTED OLD LOGIC
# MAGIC     -- AND NVL(b.ID_MEDICAID1      ,'') = NVL(a.ID_MEDICAID1      ,'')
# MAGIC     -- AND NVL(b.DTE_FIRST_SVC1    ,'') = NVL(a.DTE_FIRST_SVC1    ,'')
# MAGIC     -- AND NVL(b.ID_PROVIDER_MCAID2,'') = NVL(a.ID_PROVIDER_MCAID2,'')
# MAGIC     -- AND NVL(b.CDE_NDC           ,'') = NVL(a.CDE_NDC           ,'')
# MAGIC     -- AND NVL(b.NUM_PRSCRIP       ,'') = NVL(a.NUM_PRSCRIP       ,'')
# MAGIC )
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load Phar_Analytics table
# MAGIC %sql
# MAGIC INSERT INTO TABLE ${catalog}.${schema_name}.${Phar_Analytics}
# MAGIC SELECT * FROM ${catalog}.${schema_name}.${Phar_Analytics_stg};
