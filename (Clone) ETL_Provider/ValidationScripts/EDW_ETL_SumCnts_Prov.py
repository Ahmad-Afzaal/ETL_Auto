# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_SumCnts_Prov.                                                                                            *
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
#* 04/11/2024 CCRB70930/CO#43342  Jaime Zavala        Added Parms for EDW table/Upadted Notebook.                                   *
#* 05/02/2024 CCRB70930/CO#43342  Jaime Zavala        Added logic to track Issue#111/Missing NPIs in VEN117FA4.                     *
#* 06/11/2024 CCRB70930/CO#43342  Jaime Zavala        Issue# 111 Closed. Changed to Track counts in VEN117FA1 File.                 *
#************************************************************************************************************************************
#

# COMMAND ----------

# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('schema_name_cmc', 'cmc')
dbutils.widgets.text('EDW_TblNm', 'Provider_Analytics')
dbutils.widgets.text('EDW_TblNm117', 'ven117fa')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
schema_name_cmc = dbutils.widgets.get('schema_name_cmc')
EDW_TblNm = dbutils.widgets.get('EDW_TblNm')
EDW_TblNm117 = dbutils.widgets.get('EDW_TblNm117')

print("catalog:", catalog)
print("schema:", schema_name)
print("schema cmc:", schema_name_cmc)
print("EDW Table Name:", EDW_TblNm)
print("EDW 117 Table Name:", EDW_TblNm117)

# COMMAND ----------

# DBTITLE 1,Provider
sql_out = spark.sql(f"""
select count(distinct SAK_PROV) AS Provider_distinct_SAK_PROV
from {catalog}.{schema_name}.EDW_VEN117FA1_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct MEDICAID_ID) AS Provider_distinct_MEDICAID_ID
from {catalog}.{schema_name}.EDW_VEN117FA1_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Provider_distinct_count
from {catalog}.{schema_name}.EDW_VEN117FA1_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct practice_medicaid_id) AS Provider_Track_111_Missing_NPIs_Expect_4_rows
  from  {catalog}.{schema_name_cmc}.CMC_Attribution_Analytics 
 where attribution_date = '2024-03-01'
   and practice_NPI is null 
   and practice_medicaid_id <> 'NA'
   and practice_medicaid_id not in (
                                     select distinct practice_medicaid_id
                                            --,MEDICAID_ID, NPI, practice_NPI
                                       from {catalog}.{schema_name_cmc}.CMC_Attribution_Analytics t1
                                       join {catalog}.{schema_name}.edw_ven117fa1_staging t2 on t2.MEDICAID_ID = t1.practice_medicaid_id
                                      where attribution_date = '2024-03-01'
                                        and practice_NPI is null 
                                        and NPI is not null
                                   )
;
               """)
display(sql_out)

# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_PROV) AS Provider_EDW_temp_ven117fa_distinct_SAK_PROV
from {catalog}.{schema_name}.{EDW_TblNm117}
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_PROVIDER_MCAID1) AS Provider_EDW_temp_ven117fa_distinct_ID_PROVIDER_MCAID1
from {catalog}.{schema_name}.{EDW_TblNm117}
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Provider_EDW_temp_ven117fa_count
from {catalog}.{schema_name}.{EDW_TblNm117}
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_PROVIDER_MCAID1) AS Provider_EDW_Provider_Analytics_distinct_ID_PROVIDER_MCAID1
from {catalog}.{schema_name}.{EDW_TblNm}
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Provider_EDW_Provider_Analytics_distinct_count
from {catalog}.{schema_name}.{EDW_TblNm}
;
               """)
display(sql_out)

