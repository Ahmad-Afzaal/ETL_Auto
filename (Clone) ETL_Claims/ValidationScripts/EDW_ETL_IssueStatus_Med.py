# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_IssueStatus_Med.                                                                                        *
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
#* 06/04/2024 CCRB70930/CO#43342  Jaime Zavala        Issue#61 Modified Ppltd Fld from ALWD_OTH_PYR_AMT to THE_PAID_AMT.            *
#* 07/23/2014 CCRB70930/CO#43342  Jaime Zavala        IND_HDR_DTL - Issue#113 Added.                                                *
#*                                                    AMT_PAID2   - Issue#112 Added.                                                *
#* 08/01/2024 CCRB70930/CO#43342  Jaime Zavala        Added Variables to Table names.                                               *
#* 08/15/2024 CCRB70930/CO#43342  Jaime Zavala        IND_HDR_DTL - Issue#113 Closed.                                               *
#* 01/23/2025 CCRB70930/CO#43342  Jaime Zavala        DTE_BILLED - Issue#90 Closed.                                                 *
#************************************************************************************************************************************
#

# COMMAND ----------

#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('EDW_TblNm', 'EDW_temp_cl_Med_staging')
dbutils.widgets.text('BIAR_TblNm', 'Med_Analytics')
dbutils.widgets.text('VEN100FA', 'EDW_VEN100FA_Staging')
dbutils.widgets.text('VEN12403FA', 'EDW_VEN12403FA_Staging')
dbutils.widgets.text('VEN10004FA', 'EDW_VEN10004FA_Staging')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
EDW_TblNm = dbutils.widgets.get('EDW_TblNm')
BIAR_TblNm = dbutils.widgets.get('BIAR_TblNm')
VEN100FA = dbutils.widgets.get('VEN100FA')
VEN12403FA = dbutils.widgets.get('VEN12403FA')
VEN10004FA = dbutils.widgets.get('VEN10004FA')

print("catalog:", catalog)
print("schema:", schema_name)
print("EDW_TblNm:", EDW_TblNm)
print("BIAR_TblNm:", BIAR_TblNm)
print("VEN100FA:", VEN100FA)
print("VEN12403FA:", VEN12403FA)
print("VEN10004FA:", VEN10004FA)

# COMMAND ----------

# DBTITLE 1,SQL For Medical Issues
## -- ========================
## -- Medical Issue#88
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Med_Issue_88
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
##                 , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                 , t.DTE_ADMISSION EDW_DTE_ADMISSION
##                 , s.DTE_ADMISSION BIAR_DTE_ADMISSION
##                 , CASE WHEN (t.DTE_ADMISSION = s.DTE_ADMISSION or CAST(COALESCE(t.DTE_ADMISSION,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_ADMISSION,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('B','D','M')
## 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Medical Issue#88 Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA} t
##   where UPPER(TRIM(t.CLM_TYP_CD)) in (
##                                     'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
##                                    """)    
##     and (ADMISSION_DT is not null)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA} t
##   where UPPER(TRIM(t.CLM_TYP_CD)) in (
##                                     'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
##                                    """)
## )
## select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Med_Issue_88_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Medical Issue#89
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Med_Issue_89
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
##                 , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                 , t.DTE_DISCHARGE EDW_DTE_DISCHARGE
##                 , s.DTE_DISCHARGE BIAR_DTE_DISCHARGE
##                 , CASE WHEN (t.DTE_DISCHARGE = s.DTE_DISCHARGE or CAST(COALESCE(t.DTE_DISCHARGE,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_DISCHARGE,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('B','D','M')
## 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Medical Issue#89 Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA} t
##   where UPPER(TRIM(t.CLM_TYP_CD)) in (
##                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
##                                    """)    
##     and (DISCHARGE_DT is not null)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA} t
##   where UPPER(TRIM(t.CLM_TYP_CD)) in (
##                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
##                                    """)
## )
## select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Med_Issue_89_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;

# COMMAND ----------

##sql_out = spark.sql(f"""
##-- ========================
##-- Medical Issue#113
##/*========================================
##	SQL Count Match/NonMatch
##========================================*/
##        select
##        count(*) Med_Issue_113
##        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##        from
##        (
##        select *
##        from
##        (
##                select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
##                , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                , t.IND_HDR_DTL EDW_IND_HDR_DTL
##                , s.IND_HDR_DTL BIAR_IND_HDR_DTL
##                , CASE WHEN (t.IND_HDR_DTL = s.IND_HDR_DTL or CAST(COALESCE(t.IND_HDR_DTL,'') AS CHAR(1)) = CAST(COALESCE(s.IND_HDR_DTL,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                , s.IND_HDR_DTL     BIAR_PAID_IND
##                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                from {catalog}.{schema_name}.{EDW_TblNm} t
##                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('B','D','M')
##				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##           )t where true
##           --and rnum<5
##       )t where true
##        ;
##               """)
##display(sql_out)
##sql_out = spark.sql(f"""
##-- ========================
##-- Medical Issue#113 Ppltd Fld
##WITH
##SQL_PpltdFieldCnt AS
##(
## select count(*) PpltdFieldCnt
##   from {catalog}.{schema_name}.{VEN100FA} t
##  where UPPER(TRIM(t.CLM_TYP_CD)) in (
##                                   'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
##                                   )    
##    and (HDR_DTL_PAID_IND is not null)
##)
##,SQL_TotalCnt AS
##(
## select count(*) TotalCnt
##   from {catalog}.{schema_name}.{VEN100FA} t
##  where UPPER(TRIM(t.CLM_TYP_CD)) in (
##                                   'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
##                                   )
##)
##select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Med_Issue_113_Ppltd_Fld
##     -- PpltdFieldCnt
##     -- ,TotalCnt
##from SQL_PpltdFieldCnt, SQL_TotalCnt
##;
##               """)
##display(sql_out)


# COMMAND ----------

# sql_out = spark.sql(f"""
# -- ========================
# -- Medical Issue#90
# /*========================================
# 	SQL Count Match/NonMatch
# ========================================*/
#         select
#         count(*) Med_Issue_90
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
#                 , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
#                 , t.DTE_BILLED EDW_DTE_BILLED
#                 , s.DTE_BILLED BIAR_DTE_BILLED
#                 , CASE WHEN (t.DTE_BILLED = s.DTE_BILLED or 
#                             --((s.DTE_BILLED - t.DTE_BILLED) between -3 and 3) or 
#                             CAST(COALESCE(t.DTE_BILLED,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_BILLED,'1999-01-01') AS DATE)) 
#                       THEN 'T' 
#                       ELSE 'FAIL' 
#                       END IS_MATCH
#                 , s.IND_HDR_DTL     BIAR_PAID_IND
#                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
#                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
#                 from {catalog}.{schema_name}.{EDW_TblNm} t
#                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('B','D','M')
# 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
#            )t where true
#            --and rnum<5
#        )t where true
#         ;
#                """)
# display(sql_out)
# sql_out = spark.sql(f"""
# -- ========================
# -- Medical Issue#90 Ppltd Fld
# WITH
# SQL_PpltdFieldCnt AS
# (
#  select count(*) PpltdFieldCnt
#    from {catalog}.{schema_name}.{VEN100FA} t
#   where UPPER(TRIM(t.CLM_TYP_CD)) in (
#                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
#                                    )    
#     and (BILL_DT is not null)
# )
# ,SQL_TotalCnt AS
# (
#  select count(*) TotalCnt
#    from {catalog}.{schema_name}.{VEN100FA} t
#   where UPPER(TRIM(t.CLM_TYP_CD)) in (
#                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
#                                    )
# )
# select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Med_Issue_90_Ppltd_Fld
#      -- PpltdFieldCnt
#      -- ,TotalCnt
# from SQL_PpltdFieldCnt, SQL_TotalCnt
# ;
#                """)
# display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#97
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_Issue_97
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
                , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.DTE_MCO_ADJUD1 EDW_DTE_MCO_ADJUD1
                , s.DTE_MCO_ADJUD1 BIAR_DTE_MCO_ADJUD1
                , CASE WHEN (t.DTE_MCO_ADJUD1 = s.DTE_MCO_ADJUD1 or 
                            -- ((t.DTE_MCO_ADJUD1 - s.DTE_MCO_ADJUD1) between 1 and 30)  or
                            COALESCE(t.DTE_MCO_ADJUD1,'1990-01-01') = COALESCE(s.DTE_MCO_ADJUD1,'1990-01-01')) 
                       THEN 'T' 
                       ELSE 'FAIL' 
                       END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('B','D','M')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#97 Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                   'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )    
    and (MCO_ADJUD_DT is not null)
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA} t
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                   'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Med_Issue_97_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#61
/*-----------------------------------------------------------------------
--- SQL Count Match/NonMatch IND_HDR_DTL = 'D' ----
------------------------------------------------------------------------*/

       select
        count(*) Med_Issue_61
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
                , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , CAST(t.AMT_PAID_MCO2 AS NUMERIC(15,2)) EDW_AMT_PAID_MCO2
                , s.AMT_PAID_MCO2 BIAR_AMT_PAID_MCO2
                , CASE WHEN (t.AMT_PAID_MCO2 = s.AMT_PAID_MCO2 or
                            -- (t.AMT_PAID_MCO2 - s.AMT_PAID_MCO2 between -0.01 and 0.01) or
                             COALESCE(t.AMT_PAID_MCO2,0) = COALESCE(s.AMT_PAID_MCO2,0)) 
                       THEN 'T' 
                       ELSE 'FAIL' 
                       END IS_MATCH				
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.num_icn1 = s.num_icn1  and t.num_dtl = s.num_dtl 
                where t.CDE_CLM_TYPE in('B','D','M')
                  and s.IND_HDR_DTL = 'D'
				  and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
                --and t.num_icn1 in ('5922020000466')
                --and s.AMT_PAID_MCO_2 > 0


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
-- Medical Issue#61 Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   --join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
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
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Med_Issue_61_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#112
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_Issue_112
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
                , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.AMT_PAID2 EDW_AMT_PAID2
                , s.AMT_PAID2 BIAR_AMT_PAID2
                , CASE WHEN (t.AMT_PAID2/100 = s.AMT_PAID2 or
                             --(t.AMT_PAID2/100 - s.AMT_PAID2 between -1 and 1) or
                             COALESCE(t.AMT_PAID2,0)  = COALESCE(s.AMT_PAID2,0)
                            )
						THEN 'T' 
						ELSE 'FAIL' 
						END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('B','D','M')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)

sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#112 Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   --join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
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
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Med_Issue_112_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#86 A
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_Issue_86_A
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
                , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.CDE_TOOTH_SURFACE_1 EDW_CDE_TOOTH_SURFACE_1
                , s.CDE_TOOTH_SURFACE_1 BIAR_CDE_TOOTH_SURFACE_1
                , CASE WHEN (t.CDE_TOOTH_SURFACE_1 = s.CDE_TOOTH_SURFACE_1 or COALESCE(t.CDE_TOOTH_SURFACE_1,'') = COALESCE(s.CDE_TOOTH_SURFACE_1,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('B','D','M')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#86 A Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN10004FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )    
    and (TTH_SRFC_CD is not null or TRIM(TTH_SRFC_CD) <> '')
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Med_Issue_86_A_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#86 B
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_Issue_86_B
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
                , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.CDE_TOOTH_SURFACE_2 EDW_CDE_TOOTH_SURFACE_2
                , s.CDE_TOOTH_SURFACE_2 BIAR_CDE_TOOTH_SURFACE_2
                , CASE WHEN (t.CDE_TOOTH_SURFACE_2 = s.CDE_TOOTH_SURFACE_2 or COALESCE(t.CDE_TOOTH_SURFACE_2,'') = COALESCE(s.CDE_TOOTH_SURFACE_2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('B','D','M')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#86 B Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN10004FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )    
    and (TTH_SRFC_CD is not null or TRIM(TTH_SRFC_CD) <> '')
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Med_Issue_86_B_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#86 C
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_Issue_86_C
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
                , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.CDE_TOOTH_SURFACE_3 EDW_CDE_TOOTH_SURFACE_3
                , s.CDE_TOOTH_SURFACE_3 BIAR_CDE_TOOTH_SURFACE_3
                , CASE WHEN (t.CDE_TOOTH_SURFACE_3 = s.CDE_TOOTH_SURFACE_3 or COALESCE(t.CDE_TOOTH_SURFACE_3,'') = COALESCE(s.CDE_TOOTH_SURFACE_3,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('B','D','M')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#86 C Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN10004FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )    
    and (TTH_SRFC_CD is not null or TRIM(TTH_SRFC_CD) <> '')
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Med_Issue_86_C_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#86 D
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_Issue_86_D
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
                , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.CDE_TOOTH_SURFACE_4 EDW_CDE_TOOTH_SURFACE_4
                , s.CDE_TOOTH_SURFACE_4 BIAR_CDE_TOOTH_SURFACE_4
                , CASE WHEN (t.CDE_TOOTH_SURFACE_4 = s.CDE_TOOTH_SURFACE_4 or COALESCE(t.CDE_TOOTH_SURFACE_4,'') = COALESCE(s.CDE_TOOTH_SURFACE_4,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('B','D','M')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#86 D Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN10004FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )    
    and (TTH_SRFC_CD is not null or TRIM(TTH_SRFC_CD) <> '')
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Med_Issue_86_D_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#86 E
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_Issue_86_E
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
                , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.CDE_TOOTH_SURFACE_5 EDW_CDE_TOOTH_SURFACE_5
                , s.CDE_TOOTH_SURFACE_5 BIAR_CDE_TOOTH_SURFACE_5
                , CASE WHEN (t.CDE_TOOTH_SURFACE_5 = s.CDE_TOOTH_SURFACE_5 or COALESCE(t.CDE_TOOTH_SURFACE_5,'') = COALESCE(s.CDE_TOOTH_SURFACE_5,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('B','D','M')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#86 E Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN10004FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )    
    and (TTH_SRFC_CD is not null or TRIM(TTH_SRFC_CD) <> '')
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Med_Issue_86_E_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#86 F
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_Issue_86_F
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
                , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.CDE_TOOTH_SURFACE_6 EDW_CDE_TOOTH_SURFACE_6
                , s.CDE_TOOTH_SURFACE_6 BIAR_CDE_TOOTH_SURFACE_6
                , CASE WHEN (t.CDE_TOOTH_SURFACE_6 = s.CDE_TOOTH_SURFACE_6 or COALESCE(t.CDE_TOOTH_SURFACE_6,'') = COALESCE(s.CDE_TOOTH_SURFACE_6,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('B','D','M')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#86 F Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN10004FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )    
    and (TTH_SRFC_CD is not null or TRIM(TTH_SRFC_CD) <> '')
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Med_Issue_86_F_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#71
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_Issue_71
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
                , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM

                , t.DTE_MCO_ADJUD2 EDW_DTE_MCO_ADJUD2
                , s.DTE_MCO_ADJUD2 BIAR_DTE_MCO_ADJUD2
                , CASE WHEN (t.DTE_MCO_ADJUD2 = s.DTE_MCO_ADJUD2 or 
                            -- ((CAST(t.DTE_MCO_ADJUD2 AS DATE) - CAST(s.DTE_MCO_ADJUD2 AS DATE)) between 1 and 30) or
                            CAST(COALESCE(t.DTE_MCO_ADJUD2,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_MCO_ADJUD2,'1999-01-01') AS DATE)) 
                       THEN 'T' 
                       ELSE 'FAIL' 
                       END IS_MATCH

                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t


                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl 
                where t.CDE_CLM_TYPE in('B','D','M')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Medical Issue#71 Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )    
    and (MCO_ADJUD_DTL_DT is not null)
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                    'PART C PROFESSIONAL','DENTAL','PART B PROFESSIONAL','PROFESSIONAL','PART A PROFESSIONAL'
                                   )
)
select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) Med_Issue_71_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)

