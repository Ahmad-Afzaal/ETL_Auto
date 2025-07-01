# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_IssueStatus_Recip.                                                                                       *
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
#* 04/11/2024 CCRB70930/CO#43342  Jaime Zavala        Added Parms for EDW and BIAR tables/Upadted Notebook.                         *
#* 06/11/2024 CCRB70930/CO#43342  Jaime Zavala        Removed Issues #74 and #75/EDW placed fix in the data.                        *
#************************************************************************************************************************************
#

# COMMAND ----------

#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('EDW_TblNm', 'Recipient_Analytics')
dbutils.widgets.text('BIAR_TblNm', 'BIAR_Recipient_Analytics')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
EDW_TblNm = dbutils.widgets.get('EDW_TblNm')
BIAR_TblNm = dbutils.widgets.get('BIAR_TblNm')

print("catalog:", catalog)
print("schema:", schema_name)
print("EDW Table Name:", EDW_TblNm)
print("BIAR Table Name:", BIAR_TblNm)

# COMMAND ----------

# DBTITLE 1,SQL For Recipient Issues (NO ISSUES)
#  --========================
#  --Recipient Issue#100
#  /*========================================
#  	SQL Count Match/NonMatch
#  ========================================*/
#          select
#          count(*) Recip_Issue_100
#          , SUM(CAST(is_match ='T' AS INT)) matches_cnt
#          , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
#          , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
#          , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
#          from
#          (
#             select *
#             from
#             (
#                  select distinct
#                  t.ID_MEDICAID
#                  , t.DTE_EFFECTIVE EDW_DTE_EFFECTIVE
#                  , s.DTE_EFFECTIVE BIAR_DTE_EFFECTIVE
#                  , CASE WHEN (t.DTE_EFFECTIVE = s.DTE_EFFECTIVE or COALESCE(t.DTE_EFFECTIVE,'1990-01-01') = COALESCE(s.DTE_EFFECTIVE,'1990-01-01')) THEN 'T' ELSE 'FAIL' END IS_MATCH
#                  from {catalog}.{schema_name}.{EDW_TblNm} t
#                  join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
#             )t where true
#         )t where true
#          ;
#  --========================
#  --Recipient Issue#100 Ppltd Fld
#  WITH
#  SQL_PpltdFieldCnt AS
#  (
#   select count(*) PpltdFieldCnt
#     from {catalog}.{schema_name}.EDW_VEN114FA_Staging
#    where (DTE_EFFECTIVE is not null)
#  )
#  ,SQL_TotalCnt AS
#  (
#   select count(*) TotalCnt
#     from {catalog}.{schema_name}.EDW_VEN114FA_Staging
#  )
#  select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2))  Recip_Issue_100_Ppltd_Fld
#       -- PpltdFieldCnt
#       -- ,TotalCnt
#  from SQL_PpltdFieldCnt, SQL_TotalCnt
#  ;
#  --========================
#  --Recipient Issue#101
#  /*========================================
#  	SQL Count Match/NonMatch
#  ========================================*/
#          select
#          count(*) Recip_Issue_101
#          , SUM(CAST(is_match ='T' AS INT)) matches_cnt
#          , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
#          , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
#          , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
#          from
#          (
#             select *
#             from
#             (
#                  select distinct
#                  t.ID_MEDICAID
#                  , t.DTE_END EDW_DTE_END
#                  , s.DTE_END BIAR_DTE_END
#                  , CASE WHEN (t.DTE_END = s.DTE_END or COALESCE(t.DTE_END,'1990-01-01') = COALESCE(s.DTE_END,'1990-01-01')) THEN 'T' ELSE 'FAIL' END IS_MATCH
#                  from {catalog}.{schema_name}.{EDW_TblNm} t
#                  join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
#             )t where true
#         )t where true
#          ;
#  --========================
#  --Recipient Issue#101 Ppltd Fld
#  WITH
#  SQL_PpltdFieldCnt AS
#  (
#   select count(*) PpltdFieldCnt
#     from {catalog}.{schema_name}.EDW_VEN114FA_Staging
#    where (DTE_END is not null)
#  )
#  ,SQL_TotalCnt AS
#  (
#   select count(*) TotalCnt
#     from {catalog}.{schema_name}.EDW_VEN114FA_Staging
#  )
#  select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Recip_Issue_101_Ppltd_Fld
#       -- PpltdFieldCnt
#       -- ,TotalCnt
#  from SQL_PpltdFieldCnt, SQL_TotalCnt
#  ;
#  sql_out = spark.sql(f"""
#  -- ========================
#  -- Recipient Issue#74
#  /*========================================
#  	SQL Count Match/NonMatch
#  ========================================*/
#          select
#          count(*) Recip_Issue_74
#          , SUM(CAST(is_match ='T' AS INT)) matches_cnt
#          , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
#          , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
#          , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
#          from
#          (
#             select *
#             from
#             (
#                  select distinct
#                  t.ID_MEDICAID
#                  , t.NUM_LONGITUDE EDW_NUM_LONGITUDE
#                  , s.NUM_LONGITUDE BIAR_NUM_LONGITUDE
#                  , CASE WHEN (t.NUM_LONGITUDE = s.NUM_LONGITUDE) or
#                              (t.NUM_LONGITUDE/1000000 - (s.NUM_LONGITUDE/1000000) between -1 and 1) or 
#                              (t.NUM_LONGITUDE IS NOT NULL and s.NUM_LONGITUDE = 0.00) or
#                              COALESCE(t.NUM_LONGITUDE,0.0) = COALESCE(s.NUM_LONGITUDE,0.0)
#                         THEN 'T' 
#                         ELSE 'FAIL' 
#                         END IS_MATCH
#  
#                  
#                  from {catalog}.{schema_name}.{EDW_TblNm} t
#                  join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
#  
#             )t where true
#         )t where true
#          ;
#                 """)
#  display(sql_out)
#  sql_out = spark.sql(f"""
#  -- ========================
#  -- Recipient Issue#74 Ppltd Fld
#  WITH
#  SQL_PpltdFieldCnt AS
#  (
#   select count(*) PpltdFieldCnt
#     from {catalog}.{schema_name}.EDW_VEN115FA_Staging
#    where (NUM_LONGITUDE is not null or NUM_LONGITUDE> 0)
#  )
#  ,SQL_TotalCnt AS
#  (
#   select count(*) TotalCnt
#     from {catalog}.{schema_name}.EDW_VEN115FA_Staging
#  )
#  select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Recip_Issue_74_Ppltd_Fld
#       -- PpltdFieldCnt
#       -- ,TotalCnt
#  from SQL_PpltdFieldCnt, SQL_TotalCnt
#  ;
#                 """)
#  display(sql_out)
#  sql_out = spark.sql(f"""
#  -- ========================
#  -- Recipient Issue#75
#  /*========================================
#  	SQL Count Match/NonMatch
#  ========================================*/
#          select
#          count(*) Recip_Issue_75
#          , SUM(CAST(is_match ='T' AS INT)) matches_cnt
#          , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
#          , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
#          , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
#          from
#          (
#             select *
#             from
#             (
#                  select distinct
#                  t.ID_MEDICAID
#                  , t.NUM_LATITUDE EDW_NUM_LATITUDE
#                  , s.NUM_LATITUDE BIAR_NUM_LATITUDE
#                  , CASE WHEN (t.NUM_LATITUDE = s.NUM_LATITUDE) or 
#                              ((t.NUM_LATITUDE/1000000) - (s.NUM_LATITUDE/1000000) between -1 and 1) or 
#                               (t.NUM_LATITUDE IS NOT NULL and s.NUM_LATITUDE = 0.00) or
#                              COALESCE(t.NUM_LATITUDE,0.0) = COALESCE(s.NUM_LATITUDE,0.0)
#                         THEN 'T' 
#                         ELSE 'FAIL' 
#                         END IS_MATCH
#  
#                  
#                  from {catalog}.{schema_name}.{EDW_TblNm} t
#                  join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
#  
#             )t where true
#         )t where true
#          ;
#                 """)
#  display(sql_out)
#  # ========================
#  # Recipient Issue#75 Ppltd Fld
#  sql_out = spark.sql(f"""
#  WITH
#  SQL_PpltdFieldCnt AS
#  (
#   select count(*) PpltdFieldCnt
#     from {catalog}.{schema_name}.EDW_VEN115FA_Staging
#    where (NUM_LATITUDE is not null or NUM_LATITUDE> 0)
#  )
#  ,SQL_TotalCnt AS
#  (
#   select count(*) TotalCnt
#     from {catalog}.{schema_name}.EDW_VEN115FA_Staging
#  )
#  select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Recip_Issue_75_Ppltd_Fld
#       -- PpltdFieldCnt
#       -- ,TotalCnt
#  from SQL_PpltdFieldCnt, SQL_TotalCnt
#  ;
#                 """)
#  display(sql_out)

