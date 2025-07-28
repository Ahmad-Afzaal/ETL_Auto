# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_SumCnts_DxPx.                                                                                            *
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
#* 06/13/2024 CCRB70930/CO#43342  Jaime Zavala        Added Counts for Track_Missing_ICNs_EDWVsBIAR for Dx and Px.                  *
#* 06/14/2024 CCRB70930/CO#43342  Jaime Zavala        Dx's Issue# 110 Closed. Changed to Track counts.                              *
#* 08/01/2024 CCRB70930/CO#43342  Jaime Zavala        Added Variables to Table names.                                               *
#************************************************************************************************************************************


# COMMAND ----------

# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('VEN10001FA', 'EDW_VEN10001FA_Staging')
dbutils.widgets.text('EDW_TblNm', 'EDW_temp_DiagSurgProcValCodes_Analytics')
dbutils.widgets.text('BIAR_TblNm', 'DiagSurgProcValCodes_Analytics')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
VEN10001FA = dbutils.widgets.get('VEN10001FA')
EDW_TblNm = dbutils.widgets.get('EDW_TblNm')
BIAR_TblNm = dbutils.widgets.get('BIAR_TblNm')


print("catalog:", catalog)
print("schema:", schema_name)
print("VEN10001FA:", VEN10001FA)
print("EDW_TblNm:", EDW_TblNm)
print("BIAR_TblNm:", BIAR_TblNm)

# COMMAND ----------

# DBTITLE 1,Diagnosis
sql_out = spark.sql(f"""
select count(distinct SAK_CLAIM) AS Diagnosis_distinct_SAK_CLAIM
from {catalog}.{schema_name}.{VEN10001FA}
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ICN_NBR) AS Diagnosis_distinct_ICN_NBR
from {catalog}.{schema_name}.{VEN10001FA}
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Diagnosis_EDW_VEN10001FA_Staging_count
from {catalog}.{schema_name}.{VEN10001FA}
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Diagnosis_EDW_VEN10001FA_Staging_Missing_FI_ICNs_w_DxCds_Track_110_Expect_6_rows
from {catalog}.{schema_name}.{VEN10001FA} where ICN_NBR in (
 '2223293004653'
,'2023266168206'
-- ,'2223255004480' Do not expect a DxCde.
,'2223290004728'
);
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct NUM_ICN) AS Diagnosis_distinct_NUM_ICN
from {catalog}.{schema_name}.{EDW_TblNm}
where DERIVED = 'D'
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Diagnosis_distinct_ID_MEDICAID
from {catalog}.{schema_name}.{EDW_TblNm}
where DERIVED = 'D'
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Diagnosis_EDW_temp_DiagSurgProcValCodes_Analytics_count
from {catalog}.{schema_name}.{EDW_TblNm}
where DERIVED = 'D'
;
		""")
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct NUM_ICN)  AS Diagnosis_Track_Missing_ICNs_EDWVsBIAR
from {catalog}.{schema_name}.{EDW_TblNm} t1
where true
and NOT EXISTS (
 select 1
   from {catalog}.{schema_name}.{BIAR_TblNm} t2
  where TRUE
    AND t1.DERIVED      = 'D'
    AND t1.DERIVED      = t2.DERIVED
    AND t1.NUM_ICN      = t2.NUM_ICN
    AND t1.CDE_DIAG     = t2.CDE_DIAG
    AND t1.CDE_DIAG_SEQ = t2.CDE_DIAG_SEQ
)
;
		""")
display(sql_out)

# COMMAND ----------

# DBTITLE 1,Procedures
sql_out = spark.sql(f"""
select count(distinct SAK_CLAIM) AS Procedures_distinct_SAK_CLAIM
from {catalog}.{schema_name}.EDW_VEN12301FA_Staging
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ICN_NBR) AS Procedures_distinct_ICN_NBR
from {catalog}.{schema_name}.EDW_VEN12301FA_Staging
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Procedures_EDW_VEN10001FA_Staging_count
from {catalog}.{schema_name}.EDW_VEN12301FA_Staging
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct NUM_ICN) AS Procedures_distinct_NUM_ICN
from {catalog}.{schema_name}.{EDW_TblNm}
where DERIVED = 'P'
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Procedures_distinct_ID_MEDICAID
from {catalog}.{schema_name}.{EDW_TblNm}
where DERIVED = 'P'
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Procedures_EDW_temp_DiagSurgProcValCodes_Analytics_count
from {catalog}.{schema_name}.{EDW_TblNm}
where DERIVED = 'P'
;
		""")
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct NUM_ICN)  AS Procedures_Track_Missing_ICNs_EDWVsBIAR
from {catalog}.{schema_name}.{EDW_TblNm} t1
where true
and NOT EXISTS (
 select 1
   from {catalog}.{schema_name}.{BIAR_TblNm} t2
  where TRUE
    AND t1.DERIVED       = 'P'
    AND t1.DERIVED       = t2.DERIVED
    AND t1.NUM_ICN       = t2.NUM_ICN
    AND t1.CDE_PROC_ICD9 = t2.CDE_PROC_ICD9
    AND t1.P_NUM_SEQ     = t2.P_NUM_SEQ
)
;
		""")
display(sql_out)
