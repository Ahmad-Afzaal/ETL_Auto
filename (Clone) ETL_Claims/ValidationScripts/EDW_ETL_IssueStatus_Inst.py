# Databricks notebook source
#******************************************************************************************************************************************
#*                                                                                                                                        *
#*   NOTEBOOK:     EDW_ETL_IssueStatus_Inst.                                                                                              *
#*                                                                                                                                        *
#*   DESCRIPTION:                                                                                                                         *
#*                                                                                                                                        *
#*                                                                                                                                        *
#*   INPUT PARMS:                                                                                                                         *
#*                                                                                                                                        *
#*                                                                                                                                        *
#*   INPUT FILES:                                                                                                                         *
#*                                                                                                                                        *
#*                                                                                                                                        *
#*   OUTPUT FILE:                                                                                                                         *
#*                                                                                                                                        *
#*   EXITS:       0 - success                                                                                                             *
#*                <> 0 - failure                                                                                                          *
#*                                                                                                                                        *
#******************************************************************************************************************************************
#*                                                                                                                                        *
#*                                                 Modification Log                                                                       *
#*                                                                                                                                        *
#*    Date     CO                 Author              Description                                                                         *
#* ---------- ------------------  -----------------   ------------------------------------------------------------------------------------*
#* 06/04/2024 CCRB70930/CO#43342  Jaime Zavala        Issue#61 Modified Ppltd Fld from ALWD_OTH_PYR_AMT to THE_PAID_AMT.Removed by 100.   *
#* 07/23/2014 CCRB70930/CO#43342  Jaime Zavala        IND_HDR_DTL - Issue#113 Added.                                                      *
#* 08/01/2024 CCRB70930/CO#43342  Jaime Zavala        Added Variables to Table names.                                                     *
#* 08/15/2024 CCRB70930/CO#43342  Jaime Zavala        IND_HDR_DTL - Issue#113 Closed.                                                     *
#* 01/23/2025 CCRB70930/CO#43342  Jaime Zavala        DTE_BILLED - Issue#90 Closed.                                                       *
#******************************************************************************************************************************************
#

# COMMAND ----------

#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('EDW_TblNm', 'EDW_temp_cl_inst_staging')
dbutils.widgets.text('BIAR_TblNm', 'Inst_Analytics')
dbutils.widgets.text('VEN100FA', 'EDW_VEN100FA_Staging')
dbutils.widgets.text('VEN10006FA', 'EDW_VEN10006FA_Staging')
dbutils.widgets.text('VEN12403FA', 'EDW_VEN12403FA_Staging')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
EDW_TblNm = dbutils.widgets.get('EDW_TblNm')
BIAR_TblNm = dbutils.widgets.get('BIAR_TblNm')
VEN100FA = dbutils.widgets.get('VEN100FA')
VEN10006FA = dbutils.widgets.get('VEN10006FA')
VEN12403FA = dbutils.widgets.get('VEN12403FA')

print("catalog:", catalog)
print("schema:", schema_name)
print("EDW_TblNm:", EDW_TblNm)
print("BIAR_TblNm:", BIAR_TblNm)
print("VEN100FA:", VEN100FA)
print("VEN10006FA:", VEN10006FA)
print("VEN12403FA:", VEN12403FA)

# COMMAND ----------

# DBTITLE 1,SQL For Institutional Issues
## -- ========================
## -- Institutional Issue#88
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Inst_Issue_88
##         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##         from
##         (
##         select *
##         from
##         (
##                 select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
##                 , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                 , t.DTE_ADMISSION EDW_DTE_ADMISSION
##                 , s.DTE_ADMISSION BIAR_DTE_ADMISSION
##                 , CASE WHEN (t.DTE_ADMISSION = s.DTE_ADMISSION or CAST(COALESCE(t.DTE_ADMISSION,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_ADMISSION,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
## 				and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Institutional Issue#88 Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA} t
##   where UPPER(TRIM(t.CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )    
##     and (ADMISSION_DT is not null)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA} t
##   where UPPER(TRIM(t.CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
## )
## select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_88_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#61
/*-----------------------------------------------------------------------
--- SQL Count Match/NonMatch IND_HDR_DTL = 'H' ----
------------------------------------------------------------------------*/

       select
        count(*) Inst_Issue_61_H
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg

        from
        (

    select z.*
    from (

        select t.*
        from
        (
                select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
                , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , CAST((t.AMT_PAID_MCO) AS NUMERIC(15,2)) EDW_AMT_PAID_MCO
                , s.AMT_PAID_MCO BIAR_AMT_PAID_MCO
                , CASE WHEN ((t.AMT_PAID_MCO) = s.AMT_PAID_MCO or
                             --((t.AMT_PAID_MCO) - s.AMT_PAID_MCO between -0.01 and 0.01) or 
                             COALESCE(t.AMT_PAID_MCO,0) = COALESCE(s.AMT_PAID_MCO,0)) 
                       THEN 'T' 
                       ELSE 'FAIL' 
                       END IS_MATCH				
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.num_icn = s.num_icn  and t.num_dtl = s.num_dtl 
                where t.CDE_CLM_TYPE in(
                'A',
                'C',
                'I'
                )
                  and s.IND_HDR_DTL = 'H'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
                --and t.num_icn in ('2322019000016')
                --and s.AMT_PAID_MCO > 0


        )t where true
        --and rnum<5

    )z where true

   --and is_match = 'FAIL'
   --and is_match = 'T'

        )y where true        
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#61 Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   --join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )    
    --and (ALWD_OTH_PYR_AMT <> NULL or ALWD_OTH_PYR_AMT > 0)
    and (THE_PAID_AMT IS NOT NULL)
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   --join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_61_H_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)

# COMMAND ----------

##sql_out = spark.sql(f"""
##-- ========================
##-- Institutional Issue#113
##/*==================================
##	SQL Count Match/NonMatch
##========================================*/
##         select
##        count(*) Inst_Issue_113
##        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##        from
##        (
##           select *
##           from
##           (
##                   select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
##                   , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                   , t.IND_HDR_DTL EDW_IND_HDR_DTL
##                   , s.IND_HDR_DTL BIAR_IND_HDR_DTL
##                   , CASE WHEN (t.IND_HDR_DTL = s.IND_HDR_DTL or COALESCE(t.IND_HDR_DTL,'') = COALESCE(s.IND_HDR_DTL,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                   , s.IND_HDR_DTL     BIAR_PAID_IND
##                   , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                   , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                   from {catalog}.{schema_name}.{EDW_TblNm} t
##                   join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
##				    and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
##           )t where true
##           --and rnum<5
##       )t where true
##        ;
##               """)
##display(sql_out)
##sql_out = spark.sql(f"""
##-- ========================
##-- Institutional Issue#113 Ppltd Fld
##WITH
##SQL_PpltdFieldCnt AS
##(
## select count(*) PpltdFieldCnt
##   from {catalog}.{schema_name}.{VEN100FA} t
##  where UPPER(TRIM(t.CLM_TYP_CD)) in (
##                                    'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                   )    
##    and (HDR_DTL_PAID_IND is not null)
##)
##,SQL_TotalCnt AS
##(
## select count(*) TotalCnt
##   from {catalog}.{schema_name}.{VEN100FA} t
##  where UPPER(TRIM(t.CLM_TYP_CD)) in (
##                                    'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                   )
##)
##select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_113_Ppltd_Fld
##     -- PpltdFieldCnt
##     -- ,TotalCnt
##from SQL_PpltdFieldCnt, SQL_TotalCnt
##;
##               """)
##display(sql_out)

# COMMAND ----------

# sql_out = spark.sql(f"""
# -- ========================
# -- Institutional Issue#90
# /*========================================
# 	SQL Count Match/NonMatch
# ========================================*/
#         select
#         count(*) Inst_Issue_90
#         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
#         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
#         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
#         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
#         from
#         (
#         select *
#         from
#         (
#                 select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
#                 , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
#                 , t.DTE_BILLED EDW_DTE_BILLED
#                 , s.DTE_BILLED BIAR_DTE_BILLED
#                 ,t.DTE_BILLED - s.DTE_BILLED
#                 , CASE WHEN (t.DTE_BILLED = s.DTE_BILLED or
#                              --((t.DTE_BILLED - s.DTE_BILLED) between 1 and 3) or 
#                              CAST(COALESCE(t.DTE_BILLED,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_BILLED,'1999-01-01') AS DATE)) 
#                        THEN 'T' ELSE 'FAIL' END IS_MATCH
#                 --, CASE WHEN (t.DTE_BILLED = s.DTE_BILLED or CAST(COALESCE(t.DTE_BILLED,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_BILLED,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
#                 , s.IND_HDR_DTL     BIAR_PAID_IND
#                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
#                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
#                 from {catalog}.{schema_name}.{EDW_TblNm} t
#                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl 
#                 where t.CDE_CLM_TYPE in('A','C','I','L','O')
# 				and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
#                 -- and ((t.DTE_BILLED - s.DTE_BILLED) between 7 and -7)
#            )t where true
#            --and rnum<5
#        )t where true
#         ;
#                """)
# display(sql_out)
# sql_out = spark.sql(f"""
# -- ========================
# -- Institutional Issue#90 Ppltd Fld
# WITH
# SQL_PpltdFieldCnt AS
# (
#  select count(*) PpltdFieldCnt
#    from {catalog}.{schema_name}.{VEN100FA} t
#   where UPPER(TRIM(t.CLM_TYP_CD)) in (
#                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
#                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
#                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
#                                    )    
#     and (BILL_DT is not null)
# )
# ,SQL_TotalCnt AS
# (
#  select count(*) TotalCnt
#    from {catalog}.{schema_name}.{VEN100FA} t
#   where UPPER(TRIM(t.CLM_TYP_CD)) in (
#                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
#                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
#                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
#                                    )
# )
# select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_90_Ppltd_Fld
#      -- PpltdFieldCnt
#      -- ,TotalCnt
# from SQL_PpltdFieldCnt, SQL_TotalCnt
# ;
#                """)
# display(sql_out)

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#97
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_Issue_97
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
        select *
        from
        (
                select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
                , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.DTE_MCO_ADJUD EDW_DTE_MCO_ADJUD
                , s.DTE_MCO_ADJUD BIAR_DTE_MCO_ADJUD
                , CASE WHEN (t.DTE_MCO_ADJUD = s.DTE_MCO_ADJUD or
                            --((t.DTE_MCO_ADJUD - s.DTE_MCO_ADJUD) between 1 and 30)  or
                            CAST(COALESCE(t.DTE_MCO_ADJUD,'1999-01-01') AS DATE)= CAST(COALESCE(s.DTE_MCO_ADJUD,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
				and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#97 Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )    
    and (MCO_ADJUD_DT is not null)
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA} t
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_97_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#98
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_Issue_98
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                   select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
                   , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                   , t.DTE_ENTERED_SYS EDW_DTE_ENTERED_SYS
                   , s.DTE_ENTERED_SYS BIAR_DTE_ENTERED_SYS
                   , CASE WHEN (t.DTE_ENTERED_SYS = s.DTE_ENTERED_SYS 
                                --or (t.DTE_ENTERED_SYS - s.DTE_ENTERED_SYS) between -2 and 2
                                or COALESCE(t.DTE_ENTERED_SYS,'1990-01-01') = COALESCE(s.DTE_ENTERED_SYS,'1990-01-01')
                               ) 
                          THEN 'T' 
                          ELSE 'FAIL' 
                          END IS_MATCH
                   , s.IND_HDR_DTL     BIAR_PAID_IND
                   , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                   , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                   from {catalog}.{schema_name}.{EDW_TblNm} t
                   
                   join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
				   and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#98 Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )    
    and (ENTERED_SYS_DT is not null)
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA} t
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_98_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)
## -- ========================
## -- Institutional Issue#99
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Inst_Issue_99
##         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##         from
##         (
##            select *
##            from
##            (
##                 select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
##                 , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                 , (t.AMT_ALWD/100) EDW_AMT_ALWD
##                 , s.AMT_ALWD BIAR_AMT_ALWD
##                 , CASE WHEN ((t.AMT_ALWD/100) = s.AMT_ALWD or COALESCE(t.AMT_ALWD,0) = COALESCE(s.AMT_ALWD,0)) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
## 
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl 
## 				
##                 where t.CDE_CLM_TYPE in('A','C','I','L','O')
## 				and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
## 
##            )t where true
##            --and rnum<5
##        )t where true
##        ;
## -- ========================
## -- Institutional Issue#99 Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN10006FA} t
##   where UPPER(TRIM(t.CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )    
##     and (HDR_ALWD_AMT is not null)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN10006FA} t
##   where UPPER(TRIM(t.CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
## )
## select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_99_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;


# COMMAND ----------

## -- ========================
## -- Institutional Issue#81 A
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Inst_Issue_81 A
##         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##         from
##         (
##            select *
##            from
##            (
##                    select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
##                    , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                    , t.CDE_TYPE_OF_BILL EDW_CDE_TYPE_OF_BILL
##                    , s.CDE_TYPE_OF_BILL BIAR_CDE_TYPE_OF_BILL
##                    , CASE WHEN (t.CDE_TYPE_OF_BILL = s.CDE_TYPE_OF_BILL or COALESCE(t.CDE_TYPE_OF_BILL,'') = COALESCE(s.CDE_TYPE_OF_BILL,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                    , s.IND_HDR_DTL     BIAR_PAID_IND
##                    , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                    , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                    from {catalog}.{schema_name}.{EDW_TblNm} t
##                    join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
## 				   and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Institutional Issue#81 A Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA} t
##    join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                      'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )    
##     and (BILL_CD_TYP is not null or TRIM(BILL_CD_TYP) <> '')
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA} t
##    join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                      'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
## )
## select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_81 A_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Institutional Issue#81 B
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Inst_Issue_81 B
##         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##         from
##         (
##            select *
##            from
##            (
##                    select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
##                    , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                    , t.CDE_TYPE_OF_BILL_2 EDW_CDE_TYPE_OF_BILL_2
##                    , s.CDE_TYPE_OF_BILL_2 BIAR_CDE_TYPE_OF_BILL_2
##                    , CASE WHEN (t.CDE_TYPE_OF_BILL_2 = s.CDE_TYPE_OF_BILL_2 or COALESCE(t.CDE_TYPE_OF_BILL_2,'') = COALESCE(s.CDE_TYPE_OF_BILL_2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                    , s.IND_HDR_DTL     BIAR_PAID_IND
##                    , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                    , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                    from {catalog}.{schema_name}.{EDW_TblNm} t
##                    join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
## 				   and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Institutional Issue#81 B Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA} t
##    join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                      'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )    
##     and (BILL_CD_TYP is not null or TRIM(BILL_CD_TYP) <> '')
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA} t
##    join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                      'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
## )
## select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_81 B_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Institutional Issue#81 C
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Inst_Issue_81 C
##         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##         from
##         (
##            select *
##            from
##            (
##                    select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
##                    , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                    , t.CDE_TYPE_OF_BILL_3 EDW_CDE_TYPE_OF_BILL_3
##                    , s.CDE_TYPE_OF_BILL_3 BIAR_CDE_TYPE_OF_BILL_3
##                    , CASE WHEN (t.CDE_TYPE_OF_BILL_3 = s.CDE_TYPE_OF_BILL_3 or COALESCE(t.CDE_TYPE_OF_BILL_3,'') = COALESCE(s.CDE_TYPE_OF_BILL_3,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                    , s.IND_HDR_DTL     BIAR_PAID_IND
##                    , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                    , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                    from {catalog}.{schema_name}.{EDW_TblNm} t
##                    join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
## 				   and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Institutional Issue#81 C Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA} t
##    join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                      'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )    
##     and (BILL_CD_TYP is not null or TRIM(BILL_CD_TYP) <> '')
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA} t
##    join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                      'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
## )
## select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_81 C_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#61
-----------------------------------------------------------------------
--- SQL Count Match/NonMatch IND_HDR_DTL = 'D' ----
------------------------------------------------------------------------

       select
        count(*) Inst_Issue_61_D
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg

        from
        (

    select z.*
    from (

        select t.*
        from
        (
                select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
                , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , CAST((t.AMT_PAID_MCO_2) AS NUMERIC(15,2)) EDW_AMT_PAID_MCO_2
                , s.AMT_PAID_MCO_2 BIAR_AMT_PAID_MCO_2
                , CASE WHEN ((t.AMT_PAID_MCO_2) = s.AMT_PAID_MCO_2 or ((t.AMT_PAID_MCO_2) - s.AMT_PAID_MCO_2 < 1) or COALESCE(t.AMT_PAID_MCO_2,0) = COALESCE(s.AMT_PAID_MCO_2,0)) 
                       THEN 'T' 
                       ELSE 'FAIL' 
                       END IS_MATCH				
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.num_icn = s.num_icn  and t.num_dtl = s.num_dtl 
                where t.CDE_CLM_TYPE in('L','O','I')
                  and s.IND_HDR_DTL = 'D'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
                --and t.num_icn in ('')


        )t where true
        --and rnum<5

    )z where true

   --and is_match = 'FAIL'
   --and is_match = 'T'

        )y where true        
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#61 Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   --join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )    
    --and (ALWD_OTH_PYR_AMT <> NULL or ALWD_OTH_PYR_AMT > 0)
    and (THE_PAID_AMT IS NOT NULL)
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   --join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_61_D_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#71
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_Issue_71
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
        select *
        from
        (
                select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
                , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.DTE_MCO_ADJUD_2 EDW_DTE_MCO_ADJUD_2
                , s.DTE_MCO_ADJUD_2 BIAR_DTE_MCO_ADJUD_2
                , CASE WHEN (t.DTE_MCO_ADJUD_2 = s.DTE_MCO_ADJUD_2 or 
                            --((CAST(t.DTE_MCO_ADJUD_2 AS DATE) - CAST(s.DTE_MCO_ADJUD_2 AS DATE)) between 1 and 30) or 
                            CAST(COALESCE(t.DTE_MCO_ADJUD_2,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_MCO_ADJUD_2,'1999-01-01') AS DATE)) 
                       THEN 'T' 
                       ELSE 'FAIL' 
                       END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
				and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#71 Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )   
    and (MCO_ADJUD_DTL_DT is not null)
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_71_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)
## -- ========================
## -- Institutional Issue#78
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Inst_Issue_78
##         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##         from
##         (
##            select *
##            from
##            (
##                    select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
##                    , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                    , t.CDE_POS EDW_CDE_POS
##                    , s.CDE_POS BIAR_CDE_POS
##                    , CASE WHEN (t.CDE_POS = s.CDE_POS or COALESCE(t.CDE_POS,'') = COALESCE(s.CDE_POS,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                    , s.IND_HDR_DTL     BIAR_PAID_IND
##                    , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                    , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                    from {catalog}.{schema_name}.{EDW_TblNm} t
##                    join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
## 				   and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Institutional Issue#78 Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                      'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )   
##     and (POS_CD is not null or TRIM(POS_CD) <> '')
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                      'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
## )
## select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_78_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;


# COMMAND ----------

## -- ========================
## -- Institutional Issue#60 A
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Inst_Issue_60 A
##         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##         from
##         (
##            select *
##            from
##            (
##                    select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
##                    , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                    , t.ID_PROVIDER_MCAID EDW_ID_PROVIDER_MCAID
##                    , s.ID_PROVIDER_MCAID BIAR_ID_PROVIDER_MCAID
##                    , CASE WHEN (t.ID_PROVIDER_MCAID = s.ID_PROVIDER_MCAID or COALESCE(t.ID_PROVIDER_MCAID,'') = COALESCE(s.ID_PROVIDER_MCAID,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                    , s.IND_HDR_DTL     BIAR_PAID_IND
##                    , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                    , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                    from {catalog}.{schema_name}.{EDW_TblNm} t
##                    join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
## 				   and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Institutional Issue#60 A Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
##     and (RPA_PROV_SAK_ID <> NULL or RPA_PROV_SAK_ID > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
## )
## select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_60 A_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Institutional Issue#60 B
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Inst_Issue_60 B
##         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##         from
##         (
##            select *
##            from
##            (
##                    select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
##                    , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                    , t.ID_PROVIDER_NPI EDW_ID_PROVIDER_NPI
##                    , s.ID_PROVIDER_NPI BIAR_ID_PROVIDER_NPI
##                    , CASE WHEN (t.ID_PROVIDER_NPI = s.ID_PROVIDER_NPI or (TRIM(t.ID_PROVIDER_NPI) = '' and s.ID_PROVIDER_NPI is null) or COALESCE(t.ID_PROVIDER_NPI,'') = COALESCE(s.ID_PROVIDER_NPI,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                    , s.IND_HDR_DTL     BIAR_PAID_IND
##                    , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                    , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                    from {catalog}.{schema_name}.{EDW_TblNm} t
##                    join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
## 				   and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Institutional Issue#60 B Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
##     and (RPA_PROV_SAK_ID <> NULL or RPA_PROV_SAK_ID > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
## )
## select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_60 B_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Institutional Issue#60 C
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Inst_Issue_60 C
##         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##         from
##         (
##         select *
##         from
##         (
##                 select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
##                 , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                 , t.ATTENDING_CDE_PROV_TYPE EDW_ATTENDING_CDE_PROV_TYPE
##                 , s.ATTENDING_CDE_PROV_TYPE BIAR_ATTENDING_CDE_PROV_TYPE
##                 , CASE WHEN (t.ATTENDING_CDE_PROV_TYPE = s.ATTENDING_CDE_PROV_TYPE or (t.ATTENDING_CDE_PROV_TYPE='  ' and s.ATTENDING_CDE_PROV_TYPE='##') or COALESCE(t.ATTENDING_CDE_PROV_TYPE,'') = COALESCE(s.ATTENDING_CDE_PROV_TYPE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
## 				and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Institutional Issue#60 C Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
##     and (RPA_PROV_SAK_ID <> NULL or RPA_PROV_SAK_ID > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
## )
## select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_60 C_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;


# COMMAND ----------

## -- ========================
## -- Institutional Issue#60 D
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Inst_Issue_60 D
##         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##         from
##         (
##         select *
##         from
##         (
##                 select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
##                 , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                 , t.CDE_SVC_COUNTY EDW_CDE_SVC_COUNTY
##                 , s.CDE_SVC_COUNTY BIAR_CDE_SVC_COUNTY
##                 , CASE WHEN (t.CDE_SVC_COUNTY = s.CDE_SVC_COUNTY or COALESCE(t.CDE_SVC_COUNTY,'') = COALESCE(s.CDE_SVC_COUNTY,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
## 				and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Institutional Issue#60 D Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
##     and (RPA_PROV_SAK_ID <> NULL or RPA_PROV_SAK_ID > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
## )
## select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_60_D_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Institutional Issue#60 E
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Inst_Issue_60 E
##         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##         from
##         (
##            select *
##            from
##            (
##                    select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
##                    , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                    , t.CDE_TAXONOMY EDW_CDE_TAXONOMY
##                    , s.CDE_TAXONOMY BIAR_CDE_TAXONOMY
##                    , CASE WHEN (t.CDE_TAXONOMY = s.CDE_TAXONOMY or (t.CDE_TAXONOMY = '          ' and s.CDE_TAXONOMY = '##########' ) or COALESCE(t.CDE_TAXONOMY,'') = COALESCE(s.CDE_TAXONOMY,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                    , s.IND_HDR_DTL     BIAR_PAID_IND
##                    , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                    , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                    from {catalog}.{schema_name}.{EDW_TblNm} t
##                    join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
## 				   and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Institutional Issue#60 E Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
##     and (RPA_PROV_SAK_ID <> NULL or RPA_PROV_SAK_ID > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
## )
## select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_60 E_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Institutional Issue#60 F
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Inst_Issue_60 F
##         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##         from
##         (
##         select *
##         from
##         (
##                 select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
##                 , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                 , t.CDE_PROV_SPEC_PRIM EDW_CDE_PROV_SPEC_PRIM
##                 , s.CDE_PROV_SPEC_PRIM BIAR_CDE_PROV_SPEC_PRIM
##                 , CASE WHEN (t.CDE_PROV_SPEC_PRIM = s.CDE_PROV_SPEC_PRIM or COALESCE(t.CDE_PROV_SPEC_PRIM,'') = COALESCE(s.CDE_PROV_SPEC_PRIM,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
## 				and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Institutional Issue#60 F Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
##     and (RPA_PROV_SAK_ID <> NULL or RPA_PROV_SAK_ID > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
##                                     ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
##                                     ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
##                                    )
## )
## select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_60 F_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#83 A
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_Issue_83_A
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
        select *
        from
        (
                select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
                , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.ID_PROVIDER_MCAID_4 EDW_ID_PROVIDER_MCAID_4
                , s.ID_PROVIDER_MCAID_4 BIAR_ID_PROVIDER_MCAID_4
                , CASE WHEN (t.ID_PROVIDER_MCAID_4 = s.ID_PROVIDER_MCAID_4 or COALESCE(t.ID_PROVIDER_MCAID_4,'') = COALESCE(s.ID_PROVIDER_MCAID_4,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
				and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#83 A Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA}
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                   'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )
    and (ORP_PROV_SAK_ID is not null or ORP_PROV_SAK_ID > 0)
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA}
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                   'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_83_A_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#83 B
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_Issue_83_B
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
        select *
        from
        (
                select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
                , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.ID_PROVIDER_NPI_4 EDW_ID_PROVIDER_NPI_4
                , s.ID_PROVIDER_NPI_4 BIAR_ID_PROVIDER_NPI_4
                , CASE WHEN (t.ID_PROVIDER_NPI_4 = s.ID_PROVIDER_NPI_4 or COALESCE(t.ID_PROVIDER_NPI_4,'') = COALESCE(s.ID_PROVIDER_NPI_4,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
				and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#83 B Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA}
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                   'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )
    and (ORP_PROV_SAK_ID is not null or ORP_PROV_SAK_ID > 0)
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA}
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                   'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_83_B_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#83 C
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_Issue_83_C
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
        select *
        from
        (
                select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
                , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.REFERRING_CDE_PROV_TYPE_PRIM EDW_REFERRING_CDE_PROV_TYPE_PRIM
                , s.REFERRING_CDE_PROV_TYPE_PRIM BIAR_REFERRING_CDE_PROV_TYPE_PRIM
                , CASE WHEN (t.REFERRING_CDE_PROV_TYPE_PRIM = s.REFERRING_CDE_PROV_TYPE_PRIM or COALESCE(t.REFERRING_CDE_PROV_TYPE_PRIM,'') = COALESCE(s.REFERRING_CDE_PROV_TYPE_PRIM,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
				and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#83 C Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA}
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                   'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )
    and (ORP_PROV_SAK_ID is not null or ORP_PROV_SAK_ID > 0)
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA}
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                   'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_83_C_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#83 D
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_Issue_83_D
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
        select *
        from
        (
                select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
                , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.CDE_PROV_SPEC_PRIM_4 EDW_CDE_PROV_SPEC_PRIM_4
                , s.CDE_PROV_SPEC_PRIM_4 BIAR_CDE_PROV_SPEC_PRIM_4
                , CASE WHEN (t.CDE_PROV_SPEC_PRIM_4 = s.CDE_PROV_SPEC_PRIM_4 or COALESCE(t.CDE_PROV_SPEC_PRIM_4,'') = COALESCE(s.CDE_PROV_SPEC_PRIM_4,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
				and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional Issue#83 D Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA}
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                   'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )
    and (ORP_PROV_SAK_ID is not null or ORP_PROV_SAK_ID > 0)
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA}
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                   'LTC','INPATIENT','PART B INPATIENT','PART C LTC','PART A OUTPATIENT'
                                    ,'INSTITUTIONAL','PART C OUTPATIENT','PART A LTC','PART B OUTPATIENT'
                                    ,'PART B LTC','PART A INPATIENT','PART C INPATIENT','OUTPATIENT'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Inst_Issue_83_D_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)

