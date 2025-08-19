# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     ETL_Clms_DxPxCodes_Analytics.                                                                                    *
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
#* 06/20/2024 CCRB70930/CO#43342  Jaime Zavala        Added CleanDstntTblsFlag Defaul-F, T when using EDW_temp_ in table's names    *
#*                                                    and VE are Full Refresh.                                                      *
#* 07/24/2024 CCRB70930/CO#43342  Jaime Zavala        Added logic to use CleanDstntTblsFlag for Delta VE source type.               *
#* 08/01/2024 CCRB70930/CO#43342  Jaime Zavala        CDE_CLM_TYPE applied the following Mapping:                                   *
#*                                                    'PART B INPATIENT'  THEN 'B'                                                  *
#*                                                    'PART C LTC'        THEN 'C'                                                  *
#*                                                    'PART A OUTPATIENT' THEN 'A'                                                  *
#*                                                    'PART B OUTPATIENT' THEN 'B'                                                  *
#*                                                    'PART B LTC'        THEN 'B'                                                  *
#*                                                    'PART C INPATIENT'  THEN 'C'                                                  *
#*                                                    'PART C PROFESSIONAL' THEN 'C'                                                *
#*                                                    'PART A PROFESSIONAL' THEN 'A'                                                *
#* 08/19/2025 CCRB70930/CO#43342  Jaime Zavala        CDE_CLM_TYPE applied the following Mapping:                                   *
#*                                                    'PART A INSTITUTIONAL' THEN 'A'                                               *
#*                                                    'PART B INSTITUTIONAL' THEN 'B'                                               *
#*                                                    'PART C INSTITUTIONAL' THEN 'C'                                               *
#************************************************************************************************************************************


# COMMAND ----------

# DBTITLE 1,Parameters
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.dropdown('CleanDstntTblsFlag', 'F',{'F','T'})

#-------------------
# EDW Staging Tables
#-------------------
dbutils.widgets.text('VEN100FA', 'EDW_VEN100FA_Staging')
dbutils.widgets.text('VEN10001FA', 'EDW_VEN10001FA_Staging')
dbutils.widgets.text('VEN12301FA', 'EDW_VEN12301FA_Staging')

#-----------------
# Analytics Tables
#-----------------
dbutils.widgets.text('DxPx_Analytics', 'EDW_temp_DiagSurgProcValCodes_Analytics')


# COMMAND ----------

# DBTITLE 1,Get Parameters Values
#-----------
# DBX Parms
#-----------
catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
CleanDstntTblsFlag = dbutils.widgets.get('CleanDstntTblsFlag')

#-------------------
# EDW Staging Tables
#-------------------
VEN100FA = dbutils.widgets.get('VEN100FA')
VEN10001FA = dbutils.widgets.get('VEN10001FA')
VEN12301FA = dbutils.widgets.get('VEN12301FA')

#-----------------
# Analytics Tables
#-----------------
DxPx_Analytics = dbutils.widgets.get('DxPx_Analytics')


# COMMAND ----------

# DBTITLE 1,Show Parms
print("catalog:", catalog)
print("schema:", schema_name)
print("Clean Destination's Tables Flag:", CleanDstntTblsFlag)

print()
print("EDW Staging Tables")
print("------------------")
print(VEN100FA)
print(VEN10001FA)
print(VEN12301FA)

print()
print("Analytics Tables")
print("---------------")
print(DxPx_Analytics)


# COMMAND ----------

# DBTITLE 1,Full Refresh Or Delta Step.- Truncate Main DxPx_Analytics Table
if CleanDstntTblsFlag == 'T':
    # Logic to process Full Refresh VE Source Type
    #
    sql_out = spark.sql(f"""
                        TRUNCATE TABLE {catalog}.{schema_name}.{DxPx_Analytics}
                        ;
                        """)
    display(sql_out)
else:
    # Logic to process Delta VE source type
    #
    sql_out = spark.sql(f"""
                        DELETE 
                         FROM {catalog}.{schema_name}.{DxPx_Analytics} t1
                        WHERE TRUE
                          AND EXISTS
                              ( SELECT 1
                                  FROM {catalog}.{schema_name}.{VEN10001FA} t2
                                 WHERE TRUE
                                   AND TRIM(t2.ICN_NBR) = t1.NUM_ICN
                              )
                        ;
                        """)
    display(sql_out)


# COMMAND ----------

# DBTITLE 1,Load Dx-Diagnosis Codes
# MAGIC %sql 
# MAGIC INSERT INTO ${catalog}.${schema_name}.${DxPx_Analytics} 
# MAGIC (
# MAGIC                 --COMMON HEADER
# MAGIC      NUM_ICN
# MAGIC     ,DTE_PAID
# MAGIC     ,CDE_HDR_STATUS
# MAGIC     ,IND_CLAIM
# MAGIC     ,DERIVED
# MAGIC     ,ID_MEDICAID
# MAGIC     ,CDE_CLM_TYPE
# MAGIC     ,PARTITION_COL
# MAGIC                  --SPECIFIC FIELDS
# MAGIC     ,CDE_DIAG_SEQ
# MAGIC     ,CDE_DIAG
# MAGIC     ,CDE_POA
# MAGIC     ,CDE_VERSION_ICD_DIAG
# MAGIC                  --NA Fields for Dx
# MAGIC     ,NA1
# MAGIC     ,P_NUM_SEQ
# MAGIC     ,CDE_PROC_ICD9
# MAGIC     ,DTE_ICD_9_CM_PROC
# MAGIC     ,CDE_VERSION_ICD_INP_PROC
# MAGIC     ,NA2
# MAGIC     ,V_NUM_SEQ
# MAGIC     ,CDE_VALUE
# MAGIC     ,AMT_VALUE
# MAGIC     ,NA3
# MAGIC     ,Reporting_Month
# MAGIC     ,Total_Rows
# MAGIC )
# MAGIC select distinct
# MAGIC     --COMMON HEADER
# MAGIC      CAST(TRIM(a.ICN_NBR) AS varchar(15))  AS NUM_ICN               -- Meet v0.3 code changes
# MAGIC     ,CAST(c.PD_DT AS DATE)            AS DTE_PAID              -- Meet v0.3 code changes
# MAGIC     ,CASE UPPER(TRIM(c.HDR_STS_CD)) WHEN 'DENIED'   THEN 'D'   -- Meet v0.3 code changes                                                       
# MAGIC                                     WHEN 'PAID'     THEN 'P'                                                              
# MAGIC                                     WHEN 'REVERSED' THEN 'P'                                                              
# MAGIC                                     ELSE NULL                                                                             
# MAGIC                                     END AS CDE_HDR_STATUS
# MAGIC     ,CASE WHEN UPPER(TRIM(c.CLM_TYP_CD)) = 'P' or UPPER(TRIM(c.CLM_TYP_CD)) = 'Q' THEN (CASE UPPER(TRIM(c.ENCTR_OR_FFS_DESC)) WHEN 'FFS'       THEN 'F'
# MAGIC                                                                                                                               WHEN 'ENCOUNTER' THEN 'E'
# MAGIC                                                                                                                               ELSE CAST(TRIM(c.ENCTR_OR_FFS_DESC) AS varchar(1)) 
# MAGIC 																															  END
# MAGIC 	                                                                                    )
# MAGIC           ELSE (CASE UPPER(TRIM(c.CLAIM_IND)) WHEN 'N' THEN 'F'
# MAGIC                                               WHEN 'Y' THEN 'E'
# MAGIC                                               ELSE CAST(TRIM(c.CLAIM_IND) AS varchar(1))
# MAGIC                                               END
# MAGIC 		       )
# MAGIC           END AS IND_CLAIM                                     -- Meet v0.3 code changes
# MAGIC     ,'D' AS DERIVED    -- stands for diagnosis type
# MAGIC     ,LPAD(TRIM(c.MEDICAID_ID),12,'0') AS ID_MEDICAID           -- Meet v0.3 code changes
# MAGIC 	,CASE UPPER(TRIM(c.CLM_TYP_CD)) WHEN 'LTC'                 THEN 'L'
# MAGIC                                   WHEN 'INPATIENT'           THEN 'I'
# MAGIC                                   WHEN 'PART B INPATIENT'    THEN 'B'
# MAGIC                                   WHEN 'PART C LTC'          THEN 'C'
# MAGIC                                   WHEN 'PART A OUTPATIENT'   THEN 'A'
# MAGIC                                   WHEN 'INSTITUTIONAL'       THEN 'I'
# MAGIC                                   WHEN 'PART C OUTPATIENT'   THEN 'C'
# MAGIC                                   WHEN 'PART A LTC'          THEN 'A'
# MAGIC                                   WHEN 'PART B OUTPATIENT'   THEN 'B'
# MAGIC                                   WHEN 'PART B LTC'          THEN 'B'
# MAGIC                                   WHEN 'PART A INPATIENT'    THEN 'A'
# MAGIC                                   WHEN 'PART C INPATIENT'    THEN 'C'
# MAGIC                                   WHEN 'OUTPATIENT'          THEN 'O'
# MAGIC                                   WHEN 'PART C PROFESSIONAL' THEN 'C'
# MAGIC                                   WHEN 'DENTAL'              THEN 'D'
# MAGIC                                   WHEN 'PART B PROFESSIONAL' THEN 'B'
# MAGIC                                   WHEN 'PROFESSIONAL'        THEN 'M'
# MAGIC                                   WHEN 'PART A PROFESSIONAL' THEN 'A'
# MAGIC                                   WHEN 'P'                   THEN 'P'
# MAGIC                                   WHEN 'Q'                   THEN 'Q'
# MAGIC                                   WHEN 'PART A INSTITUTIONAL' THEN 'A'
# MAGIC                                   WHEN 'PART B INSTITUTIONAL' THEN 'B'
# MAGIC                                   WHEN 'PART C INSTITUTIONAL' THEN 'C'
# MAGIC                                   ELSE ''
# MAGIC                                   END AS CDE_CLM_TYPE                           -- Meet v0.3 code changes
# MAGIC     ,CAST(DATE_FORMAT(CAST(REPORT_DTE AS DATE),'yMM') AS INT)    AS PARTITION_COL  -- Meet v0.3 code changes
# MAGIC     --SPECIFIC FIELDS
# MAGIC     ,CAST(LPAD(CAST(DIAG_SEQ_CD AS STRING),2,'0') AS varchar(8)) AS CDE_DIAG_SEQ   -- Meet v0.3 code changes
# MAGIC     ,TRIM(regexp_replace(DIAG_CD,'[.]',''))         AS CDE_DIAG
# MAGIC     ,CAST(COALESCE(TRIM(POA_CD),'#') AS varchar(4)) AS CDE_POA     -- Meet v0.3 code changes & expected BIAR value when NULL
# MAGIC     ,NULL AS CDE_VERSION_ICD_DIAG                              -- EDW doesnt include in extraction but not require for EOC process
# MAGIC     --
# MAGIC     -- NA Fields for Dx
# MAGIC     --
# MAGIC     ,NULL AS NA1
# MAGIC     ,NULL AS P_NUM_SEQ
# MAGIC     ,NULL AS CDE_PROC_ICD9
# MAGIC     ,NULL AS DTE_ICD_9_CM_PROC
# MAGIC     ,NULL AS CDE_VERSION_ICD_INP_PROC
# MAGIC     ,NULL AS NA2
# MAGIC     ,NULL AS V_NUM_SEQ
# MAGIC     ,NULL AS CDE_VALUE
# MAGIC     ,NULL AS AMT_VALUE
# MAGIC     ,NULL AS NA3
# MAGIC     ,NULL AS Reporting_Month
# MAGIC     ,NULL AS Total_Rows
# MAGIC     
# MAGIC from ${catalog}.${schema_name}.${VEN10001FA} a
# MAGIC left join
# MAGIC (
# MAGIC         SELECT DISTINCT SAK_CLAIM
# MAGIC         ,PD_DT
# MAGIC         ,ICN_NBR
# MAGIC         ,CLAIM_IND
# MAGIC 		,CLM_TYP_CD
# MAGIC 		,ENCTR_OR_FFS_DESC
# MAGIC         ,HDR_STS_CD
# MAGIC         ,MEDICAID_ID
# MAGIC         from ${catalog}.${schema_name}.${VEN100FA}
# MAGIC ) c on a.SAK_CLAIM=c.SAK_CLAIM
# MAGIC where not exists
# MAGIC ( 
# MAGIC     select 1 
# MAGIC     from ${catalog}.${schema_name}.${DxPx_Analytics} b
# MAGIC     where DERIVED='D'
# MAGIC     and trim(a.ICN_NBR)      = b.NUM_ICN
# MAGIC     and trim(a.DIAG_SEQ_CD) = b.CDE_DIAG_SEQ
# MAGIC     and trim(a.DIAG_CD)     = b.CDE_DIAG
# MAGIC )
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load Px-Surgical Procedures Codes
# MAGIC %sql 
# MAGIC INSERT INTO ${catalog}.${schema_name}.${DxPx_Analytics}
# MAGIC (
# MAGIC             --COMMON HEADER
# MAGIC      NUM_ICN
# MAGIC     ,DTE_PAID
# MAGIC     ,CDE_HDR_STATUS
# MAGIC     ,IND_CLAIM
# MAGIC     ,DERIVED
# MAGIC     ,ID_MEDICAID
# MAGIC     ,CDE_CLM_TYPE
# MAGIC     ,PARTITION_COL
# MAGIC             --SPECIFIC FIELDS
# MAGIC     ,P_NUM_SEQ
# MAGIC     ,CDE_PROC_ICD9
# MAGIC     ,DTE_ICD_9_CM_PROC
# MAGIC     ,CDE_VERSION_ICD_INP_PROC
# MAGIC             --NA Fields for Px
# MAGIC     ,CDE_DIAG_SEQ
# MAGIC     ,CDE_DIAG
# MAGIC     ,CDE_POA
# MAGIC     ,NA1
# MAGIC     ,CDE_VERSION_ICD_DIAG
# MAGIC 	,NA2
# MAGIC     ,V_NUM_SEQ
# MAGIC     ,CDE_VALUE
# MAGIC     ,AMT_VALUE
# MAGIC     ,NA3
# MAGIC     ,Reporting_Month
# MAGIC     ,Total_Rows
# MAGIC ) 
# MAGIC SELECT DISTINCT
# MAGIC     --COMMON HEADER
# MAGIC      CAST(TRIM(c.ICN_NBR) AS varchar(15)) AS NUM_ICN                    -- Meet v0.3 code changes
# MAGIC     ,CAST(c.PD_DT AS DATE)           AS DTE_PAID                   -- Meet v0.3 code changes
# MAGIC     ,CASE UPPER(TRIM(c.HDR_STS_CD)) WHEN 'DENIED'   THEN 'D'       -- Meet v0.3 code changes                                                       
# MAGIC                                     WHEN 'PAID'     THEN 'P'                                                              
# MAGIC                                     WHEN 'REVERSED' THEN 'P'                                                              
# MAGIC                                     ELSE NULL                                                                             
# MAGIC                                     END AS CDE_HDR_STATUS
# MAGIC     ,CASE WHEN UPPER(TRIM(c.CLM_TYP_CD)) = 'P' or UPPER(TRIM(c.CLM_TYP_CD)) = 'Q' THEN (CASE UPPER(TRIM(c.ENCTR_OR_FFS_DESC)) WHEN 'FFS'       THEN 'F'
# MAGIC                                                                                                                               WHEN 'ENCOUNTER' THEN 'E'
# MAGIC                                                                                                                               ELSE CAST(TRIM(c.ENCTR_OR_FFS_DESC) AS varchar(1)) 
# MAGIC 																															  END
# MAGIC 	                                                                                    )
# MAGIC           ELSE (CASE UPPER(TRIM(c.CLAIM_IND)) WHEN 'N' THEN 'F'
# MAGIC                                               WHEN 'Y' THEN 'E'
# MAGIC                                               ELSE CAST(TRIM(c.CLAIM_IND) AS varchar(1))
# MAGIC                                               END
# MAGIC 		       )
# MAGIC           END AS IND_CLAIM                                         -- Meet v0.3 code changes
# MAGIC     ,'P' AS DERIVED       -- stands for surgical procedure type
# MAGIC     ,LPAD(TRIM(c.MEDICAID_ID),12,'0') AS ID_MEDICAID               -- Meet v0.3 code changes
# MAGIC 	,CASE UPPER(TRIM(c.CLM_TYP_CD)) WHEN 'LTC'                 THEN 'L'
# MAGIC                                   WHEN 'INPATIENT'           THEN 'I'
# MAGIC                                   WHEN 'PART B INPATIENT'    THEN 'B'
# MAGIC                                   WHEN 'PART C LTC'          THEN 'C'
# MAGIC                                   WHEN 'PART A OUTPATIENT'   THEN 'A'
# MAGIC                                   WHEN 'INSTITUTIONAL'       THEN 'I'
# MAGIC                                   WHEN 'PART C OUTPATIENT'   THEN 'C'
# MAGIC                                   WHEN 'PART A LTC'          THEN 'A'
# MAGIC                                   WHEN 'PART B OUTPATIENT'   THEN 'B'
# MAGIC                                   WHEN 'PART B LTC'          THEN 'B'
# MAGIC                                   WHEN 'PART A INPATIENT'    THEN 'A'
# MAGIC                                   WHEN 'PART C INPATIENT'    THEN 'C'
# MAGIC                                   WHEN 'OUTPATIENT'          THEN 'O'
# MAGIC                                   WHEN 'PART C PROFESSIONAL' THEN 'C'
# MAGIC                                   WHEN 'DENTAL'              THEN 'D'
# MAGIC                                   WHEN 'PART B PROFESSIONAL' THEN 'B'
# MAGIC                                   WHEN 'PROFESSIONAL'        THEN 'M'
# MAGIC                                   WHEN 'PART A PROFESSIONAL' THEN 'A'
# MAGIC                                   WHEN 'P'                   THEN 'P'
# MAGIC                                   WHEN 'Q'                   THEN 'Q'
# MAGIC                                   WHEN 'PART A INSTITUTIONAL' THEN 'A'
# MAGIC                                   WHEN 'PART B INSTITUTIONAL' THEN 'B'
# MAGIC                                   WHEN 'PART C INSTITUTIONAL' THEN 'C'
# MAGIC                                   ELSE ''
# MAGIC                                   END AS CDE_CLM_TYPE                            -- Meet v0.3 code changes
# MAGIC     ,CAST(DATE_FORMAT(CAST(REPORT_DTE AS DATE),'yMM') AS INT) AS PARTITION_COL   -- Meet v0.2 & v0.3 code changes
# MAGIC     --SPECIFIC FIELDS
# MAGIC     ,CAST(LPAD(CAST(SEQ_NBR AS STRING),4,'0') AS varchar(16)) AS P_NUM_SEQ       -- Meet v0.3 code changes
# MAGIC     ,CAST(TRIM(regexp_replace(ICD_9_CM_PROC_CD,'[.]','')) AS varchar(22)) AS CDE_PROC_ICD9                -- Meet v0.3 code changes
# MAGIC     ,CAST(DATE_FORMAT(CAST(ICD_9_CM_PROC_DT AS DATE),'MM/dd/y') AS varchar(10)) AS DTE_ICD_9_CM_PROC      -- Meet v0.2 & v0.3 code changes
# MAGIC     ,CAST(TRIM(ICD_INP_PROC_CD_VERS) AS varchar(5)) AS CDE_VERSION_ICD_INP_PROC                           -- Meet v0.3 code changes
# MAGIC     --
# MAGIC     -- NA Fields for Px
# MAGIC     --
# MAGIC     ,NULL AS CDE_DIAG_SEQ
# MAGIC     ,NULL AS CDE_DIAG
# MAGIC     ,NULL AS CDE_POA
# MAGIC     ,NULL AS NA1
# MAGIC     ,NULL AS CDE_VERSION_ICD_DIAG
# MAGIC 	,NULL AS NA2
# MAGIC     ,NULL AS V_NUM_SEQ
# MAGIC     ,NULL AS CDE_VALUE
# MAGIC     ,NULL AS AMT_VALUE
# MAGIC     ,NULL AS NA3
# MAGIC     ,NULL AS Reporting_Month
# MAGIC     ,NULL AS Total_Rows
# MAGIC
# MAGIC FROM ${catalog}.${schema_name}.${VEN12301FA} a
# MAGIC LEFT JOIN
# MAGIC (
# MAGIC         SELECT DISTINCT SAK_CLAIM
# MAGIC         ,PD_DT
# MAGIC         ,ICN_NBR
# MAGIC         ,CLAIM_IND
# MAGIC 		,CLM_TYP_CD
# MAGIC 		,ENCTR_OR_FFS_DESC
# MAGIC         ,HDR_STS_CD
# MAGIC         ,MEDICAID_ID
# MAGIC         FROM ${catalog}.${schema_name}.${VEN100FA}
# MAGIC ) c on a.SAK_CLAIM=c.SAK_CLAIM
# MAGIC WHERE NOT EXISTS
# MAGIC (
# MAGIC     SELECT 1 
# MAGIC     FROM ${catalog}.${schema_name}.${DxPx_Analytics} b 
# MAGIC     WHERE DERIVED='P'
# MAGIC     AND TRIM(c.ICN_NBR)            = b.NUM_ICN --use from claims extract because procedure extracts doenst have it
# MAGIC     AND LPAD(CAST(SEQ_NBR AS STRING),4,'0') = b.P_NUM_SEQ
# MAGIC     AND TRIM(ICD_9_CM_PROC_CD)    = b.CDE_PROC_ICD9 
# MAGIC )
# MAGIC ;
