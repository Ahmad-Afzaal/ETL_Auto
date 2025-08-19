# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_SumCnts_Inst.                                                                                            *
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
#* 06/13/2024 CCRB70930/CO#43342  Jaime Zavala        Added Counts for Track_Missing_ICNs_EDWVsBIAR.                                *
#* 08/01/2024 CCRB70930/CO#43342  Jaime Zavala        Added Variables to Table names.                                               *
#* 10/08/2024 CCRB70930/CO#43342  Jaime Zavala        Added Logic to track Records w/Claim Type Code=NULL.                          *
#* 08/19/2025 CCRB70930/CO#43342  Jaime Zavala        CLM_TYP_CD applied the following Mapping:                                     *
#*                                                    'PART A INSTITUTIONAL' THEN 'A'                                               *
#*                                                    'PART B INSTITUTIONAL' THEN 'B'                                               *
#*                                                    'PART C INSTITUTIONAL' THEN 'C'                                               *
#************************************************************************************************************************************


# COMMAND ----------

# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('VEN100FA', 'EDW_VEN100FA_Staging')
dbutils.widgets.text('EDW_clStgng_TblNm', 'EDW_temp_cl_inst_staging')
dbutils.widgets.text('EDW_Anlytcs_TblNm', 'EDW_temp_Inst_Analytics')
dbutils.widgets.text('EDW_AnlytcsStgng_TblNm', 'EDW_temp_Inst_Analytics_staging')
dbutils.widgets.text('BIAR_TblNm', 'Inst_Analytics')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
VEN100FA = dbutils.widgets.get('VEN100FA')
EDW_clStgng_TblNm = dbutils.widgets.get('EDW_clStgng_TblNm')
EDW_Anlytcs_TblNm = dbutils.widgets.get('EDW_Anlytcs_TblNm')
EDW_AnlytcsStgng_TblNm = dbutils.widgets.get('EDW_AnlytcsStgng_TblNm')
BIAR_TblNm = dbutils.widgets.get('BIAR_TblNm')

print("catalog:", catalog)
print("schema:", schema_name)
print("VEN100FA:", VEN100FA)
print("EDW_clStgng_TblNm:", EDW_clStgng_TblNm)
print("EDW_Anlytcs_TblNm:", EDW_Anlytcs_TblNm)
print("EDW_AnlytcsStgng_TblNm:", EDW_AnlytcsStgng_TblNm)
print("BIAR_TblNm:", BIAR_TblNm)

# COMMAND ----------

# DBTITLE 1,Institutional
sql_out = spark.sql(f"""
select count(distinct SAK_CLAIM) AS Institutional_distinct_SAK_CLAIM
 from {catalog}.{schema_name}.{VEN100FA}
where UPPER(TRIM(CLM_TYP_CD)) in (
'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
,'PART A INSTITUTIONAL','PART B INSTITUTIONAL','PART C INSTITUTIONAL'
)
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ICN_NBR) AS Institutional_distinct_ICN_NBR
 from {catalog}.{schema_name}.{VEN100FA}
where UPPER(TRIM(CLM_TYP_CD)) in (
'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
,'PART A INSTITUTIONAL','PART B INSTITUTIONAL','PART C INSTITUTIONAL'
)
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ICN_NBR||DTL_NBR) AS Institutional_distinct_ICN_NBR_DTL_NBR
 from {catalog}.{schema_name}.{VEN100FA}
where UPPER(TRIM(CLM_TYP_CD)) in (
'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
,'PART A INSTITUTIONAL','PART B INSTITUTIONAL','PART C INSTITUTIONAL'
)
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Institutional_distinct_SAK_RECIP
 from {catalog}.{schema_name}.{VEN100FA}
where UPPER(TRIM(CLM_TYP_CD)) in (
'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
,'PART A INSTITUTIONAL','PART B INSTITUTIONAL','PART C INSTITUTIONAL'
)
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Institutional_VEN100FA_count
 from {catalog}.{schema_name}.{VEN100FA}
where UPPER(TRIM(CLM_TYP_CD)) in (
'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
,'PART A INSTITUTIONAL','PART B INSTITUTIONAL','PART C INSTITUTIONAL'
)
;
               """)
display(sql_out)
#sql_out = spark.sql(f"""
#select count(distinct NUM_ICN) AS Institutional_VEN100FA_Missing_EDW_ICNs_Issue_107
#  from {catalog}.{schema_name}.{BIAR_TblNm} 
# where CAST(DTE_PAID AS DATE) between CAST('2022-07-01' AS DATE) and CAST('2023-09-30' AS DATE)
#   and NUM_ICN not in (
#                         select distinct ICN_NBR
#                           from {catalog}.{schema_name}.{VEN100FA}
#                          where UPPER(TRIM(CLM_TYP_CD)) in (
#                                'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
#                               ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
#                               ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
#                               ,'PART A INSTITUTIONAL','PART B INSTITUTIONAL','PART C INSTITUTIONAL'
#                              )
#                       )
#;
#               """)
#display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(*) AS Institutional_VEN100FA_count_wo_CTCde
 from {catalog}.{schema_name}.{VEN100FA}
where CLM_TYP_CD is NULL
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct NUM_ICN) AS Institutional_EDW_temp_cl_inst_staging_distinct_NUM_ICN
 from {catalog}.{schema_name}.{EDW_clStgng_TblNm}
--where CDE_CLM_TYPE in ('A','C','I','L','O')
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct NUM_ICN||NUM_DTL) AS Institutional_EDW_temp_cl_inst_staging_NUM_ICN__NUM_DTL
 from {catalog}.{schema_name}.{EDW_clStgng_TblNm}
--where CDE_CLM_TYPE in ('A','C','I','L','O')
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Institutional_EDW_temp_cl_inst_staging_distinct_ID_MEDICAID
  from {catalog}.{schema_name}.{EDW_clStgng_TblNm}
--where CDE_CLM_TYPE in ('A','C','I','L','O')
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Institutional_EDW_temp_cl_inst_staging_count
 from {catalog}.{schema_name}.{EDW_clStgng_TblNm}
--where CDE_CLM_TYPE in ('A','C','I','L','O')
;
               """)
display(sql_out)
#sql_out = spark.sql(f"""
#select count(distinct NUM_ICN) AS Institutional_EDW_temp_cl_inst_staging_Missing_EDW_ICNs_Issue_107
#  from {catalog}.{schema_name}.{BIAR_TblNm} 
# where CAST(DTE_PAID AS DATE) between CAST('2022-07-01' AS DATE) and CAST('2023-09-30' AS DATE)
#   and NUM_ICN not in (
#                         select distinct NUM_ICN
#                           from {catalog}.{schema_name}.{EDW_clStgng_TblNm}
#                       )
#;
#               """)
#display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct NUM_ICN) AS Institutional_EDW_temp_Inst_Analytics_staging_distinct_NUM_ICN
 from {catalog}.{schema_name}.{EDW_AnlytcsStgng_TblNm}
--where CDE_CLM_TYPE in ('A','C','I','L','O')
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct NUM_ICN||NUM_DTL) AS Institutional_EDW_temp_Inst_Analytics_staging_NUM_ICN__NUM_DTL
 from {catalog}.{schema_name}.{EDW_AnlytcsStgng_TblNm}
--where CDE_CLM_TYPE in ('A','C','I','L','O')
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Institutional_EDW_temp_Inst_Analytics_staging_distinct_ID_MEDICAID
from {catalog}.{schema_name}.{EDW_AnlytcsStgng_TblNm}
--where CDE_CLM_TYPE in ('A','C','I','L','O')
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Institutional_EDW_temp_Inst_Analytics_staging_count
 from {catalog}.{schema_name}.{EDW_AnlytcsStgng_TblNm}
--where CDE_CLM_TYPE in ('A','C','I','L','O')
;
               """)
display(sql_out)
#sql_out = spark.sql(f"""
#select count(distinct NUM_ICN) AS #Institutional_EDW_temp_Inst_Analytics_staging_Missing_EDW_ICNs_Issue_107
#  from {catalog}.{schema_name}.{BIAR_TblNm} 
# where CAST(DTE_PAID AS DATE) >= CAST('2022-07-01' AS DATE)
#   and NUM_ICN not in (
#                         select distinct NUM_ICN
#                           from {catalog}.{schema_name}.{EDW_AnlytcsStgng_TblNm}
#                       )
#;
#               """)
#display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct NUM_ICN) AS Institutional_Track_Missing_ICNs_EDWVsBIAR
 from {catalog}.{schema_name}.{EDW_Anlytcs_TblNm} t1
where TRUE
  and NOT EXISTS (
                  select 1
                    from {catalog}.{schema_name}.{BIAR_TblNm} t2
                   where TRUE 
                    and t1.NUM_ICN = t2.NUM_ICN
                    and t1.NUM_DTL  = t2.NUM_DTL
                 )
;
		""")
display(sql_out)

# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct NUM_ICN) AS Institutional_EDW_temp_Inst_Analytics_distinct_NUM_ICN
 from {catalog}.{schema_name}.{EDW_Anlytcs_TblNm}
--where CDE_CLM_TYPE in ('A','C','I','L','O')
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct NUM_ICN||NUM_DTL) AS Institutional_EDW_temp_Inst_Analytics_NUM_ICN__NUM_DTL
 from {catalog}.{schema_name}.{EDW_Anlytcs_TblNm}
--where CDE_CLM_TYPE in ('A','C','I','L','O')
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Institutional_EDW_temp_Inst_Analytics_distinct_ID_MEDICAID
from {catalog}.{schema_name}.{EDW_Anlytcs_TblNm}
--where CDE_CLM_TYPE in ('A','C','I','L','O')
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Institutional_EDW_temp_Inst_Analytics_count
 from {catalog}.{schema_name}.{EDW_Anlytcs_TblNm}
--where CDE_CLM_TYPE in ('A','C','I','L','O')
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct NUM_ICN) AS Institutional_EDW_temp_Inst_Analytics_Missing_EDW_ICNs_Issue_107
  from {catalog}.{schema_name}.{BIAR_TblNm} 
 where CAST(DTE_PAID AS DATE) >= CAST('2022-07-01' AS DATE)
   and NUM_ICN not in (
                         select distinct NUM_ICN
                           from {catalog}.{schema_name}.{EDW_Anlytcs_TblNm}
                       )
;
               """)
display(sql_out)

