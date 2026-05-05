# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_IssueStatus_Phar.                                                                                        *
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
#* 03/27/2024 CCRB70930/CO#43342  Jaime Zavala        Issue#61 Modified Ppltd Fld from ALWD_OTH_PYR_AMT to PD_MCO_AMT.              *
#* 06/04/2024 CCRB70930/CO#43342  Jaime Zavala        Issue#61 Modified Ppltd Fld from PD_MCO_AMT to THE_PAID_AMT.                  *
#*                                                    Removed the by 100 from compare criteria.                                     *
#* 07/22/2014 CCRB70930/CO#43342  Jaime Zavala        IND_HDR_DTL - Issue#113 Added.                                                *
#* 08/01/2024 CCRB70930/CO#43342  Jaime Zavala        Added Variables to Table names.                                               *
#* 08/15/2024 CCRB70930/CO#43342  Jaime Zavala        IND_HDR_DTL - Issue#113 Closed.                                               *
#************************************************************************************************************************************
#

# COMMAND ----------

#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('EDW_TblNm', 'EDW_temp_cl_Phar_staging')
dbutils.widgets.text('BIAR_TblNm', 'Phar_Analytics')
dbutils.widgets.text('VEN100FA', 'EDW_VEN100FA_Staging')
dbutils.widgets.text('VEN12403FA', 'EDW_VEN12403FA_Staging')
dbutils.widgets.text('Prov_TblNm', 'Provider_Analytics')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
EDW_TblNm = dbutils.widgets.get('EDW_TblNm')
BIAR_TblNm = dbutils.widgets.get('BIAR_TblNm')
VEN100FA = dbutils.widgets.get('VEN100FA')
VEN12403FA = dbutils.widgets.get('VEN12403FA')
Prov_TblNm = dbutils.widgets.get('Prov_TblNm')

print("catalog:", catalog)
print("schema:", schema_name)
print("EDW_TblNm:", EDW_TblNm)
print("BIAR_TblNm:", BIAR_TblNm)
print("VEN100FA:", VEN100FA)
print("VEN12403FA:", VEN12403FA)
print("Prov_TblNm:", Prov_TblNm)

# COMMAND ----------

# DBTITLE 1,SQL For Pharmacy Issues
## -- ========================
## -- Pharmacy Issue#104
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_Issue_104
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
##                 , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                 , t.IND_CLAIM EDW_IND_CLAIM
##                 , s.IND_CLAIM BIAR_IND_CLAIM
##                 , CASE WHEN (t.IND_CLAIM = s.IND_CLAIM or COALESCE(t.IND_CLAIM,'') = COALESCE(s.IND_CLAIM,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
## 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Pharmacy Issue#104 Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA} t
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                      'P','Q'
##                                    )    
##   and (ENCTR_OR_FFS_DESC is not null)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA} t
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                      'P','Q'
##                                    )
## )
## select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_104_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
sql_out = spark.sql(f"""
-- ========================
-- Pharmacy Issue#61
-----------------------------------------------------------------------
--- SQL Count Match/NonMatch IND_HDR_DTL = 'H' ----
------------------------------------------------------------------------

       select
        count(*) Phar_Issue_61
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
                , CAST(CAST(t.AMT_PAID_MCO1 AS NUMERIC(12,2)) AS NUMERIC(15,2)) EDW_AMT_PAID_MCO1
                , s.AMT_PAID_MCO1 BIAR_AMT_PAID_MCO1
                , CASE WHEN (CAST(CAST(t.AMT_PAID_MCO1 AS NUMERIC(12,2)) AS numeric(15,2)) = s.AMT_PAID_MCO1 
				             or (CAST(CAST(t.AMT_PAID_MCO1 AS NUMERIC(12,2)) AS NUMERIC (15,2)) - s.AMT_PAID_MCO1) between -1 and 1
							 or COALESCE(CAST(t.AMT_PAID_MCO1 AS NUMERIC(12,2)),0) = COALESCE(s.AMT_PAID_MCO1,0)) 
                       THEN 'T' 
                       ELSE 'FAIL' 
                       END IS_MATCH				
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.num_icn1 = s.num_icn1  and t.num_dtl = s.num_dtl 
                where t.CDE_CLM_TYPE in('P','Q')
                  and s.IND_HDR_DTL = 'H'
				  and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
                --and t.num_icn1 in ('5822018000458')
                --and s.AMT_PAID_MCO1 > 0


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
-- Pharmacy Issue#61 Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   -- join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                     'P','Q'
                                   )    
    -- and (ALWD_OTH_PYR_AMT <> NULL or ALWD_OTH_PYR_AMT > 0)
    -- and (PD_MCO_AMT <> NULL or PD_MCO_AMT > 0)
    and (THE_PAID_AMT IS NOT NULL)
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA} t
   -- join {catalog}.{schema_name}.{VEN12403FA} s on t.ICN_NBR = s.ICN_NBR and t.DTL_NBR = s.DTL_NBR
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                     'P','Q'
                                   )
)
select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_61_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)

# COMMAND ----------

##sql_out = spark.sql(f"""
##-- ========================
##-- Pharmacy Issue#113
##/*========================================
##	SQL Count Match/NonMatch
##========================================*/
##        select
##        count(*) Phar_Issue_113
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
##                , CASE WHEN (t.IND_HDR_DTL = s.IND_HDR_DTL or COALESCE(t.IND_HDR_DTL,'') = COALESCE(s.IND_HDR_DTL,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                , s.IND_HDR_DTL     BIAR_PAID_IND
##                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                from {catalog}.{schema_name}.{EDW_TblNm} t
##                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
##				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##           )t where true
##           --and rnum<5
##       )t where true
##        ;
##               """)
##display(sql_out)
##sql_out = spark.sql(f"""
##-- ============================
##-- Pharmacy Issue#113 Ppltd Fld
##WITH
##SQL_PpltdFieldCnt AS
##(
## select count(*) PpltdFieldCnt
##   from {catalog}.{schema_name}.{VEN100FA} t
##  where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                   )    
##    and (HDR_DTL_PAID_IND IS NOT NULL)
##)
##,SQL_TotalCnt AS
##(
## select count(*) TotalCnt
##   from {catalog}.{schema_name}.{VEN100FA} t
##  where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                   )
##)
##select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_113_Ppltd_Fld
##     -- PpltdFieldCnt
##     -- ,TotalCnt
##from SQL_PpltdFieldCnt, SQL_TotalCnt
##;
##               """)
##display(sql_out)

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy Issue#90
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_Issue_90
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
                , t.DTE_BILLED EDW_DTE_BILLED
                , s.DTE_BILLED BIAR_DTE_BILLED
                , CASE WHEN (t.DTE_BILLED = s.DTE_BILLED or CAST(COALESCE(t.DTE_BILLED,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_BILLED,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Pharmacy Issue#90 Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA} t
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'P',
                                    'Q'
                                   )    
    and (BILL_DT is not null)
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA} t
  where UPPER(TRIM(t.CLM_TYP_CD)) in (
                                    'P',
                                    'Q'
                                   )
)
select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_90_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Pharmacy Issue#97
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_Issue_97
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
                , CASE WHEN (t.DTE_MCO_ADJUD2 = s.DTE_MCO_ADJUD2 or CAST(COALESCE(t.DTE_MCO_ADJUD2,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_MCO_ADJUD2,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Pharmacy Issue#97 Ppltd Fld
WITH
SQL_PpltdFieldCnt AS
(
 select count(*) PpltdFieldCnt
   from {catalog}.{schema_name}.{VEN100FA}
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                   'P','Q'
                                   )   
    and (MCO_ADJUD_DT is not null)
)
,SQL_TotalCnt AS
(
 select count(*) TotalCnt
   from {catalog}.{schema_name}.{VEN100FA}
  where UPPER(TRIM(CLM_TYP_CD)) in (
                                   'P','Q'
                                   )
)
select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_97_Ppltd_Fld
     -- PpltdFieldCnt
     -- ,TotalCnt
from SQL_PpltdFieldCnt, SQL_TotalCnt
;
               """)
display(sql_out)
## -- ========================
## -- Pharmacy Issue#78
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_Issue_78
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
##                 , t.CDE_POS EDW_CDE_POS
##                 , s.CDE_POS BIAR_CDE_POS
##                 , CASE WHEN (t.CDE_POS = s.CDE_POS or COALESCE(t.CDE_POS,'1999-01-01')::date = COALESCE(s.CDE_POS,'1999-01-01')::date) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
## 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Pharmacy Issue#78 Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                    'P','Q'
##                                    )   
##     and (POS_CD is not null or TRIM(POS_CD) <> '')
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                    'P','Q'
##                                    )
## )
## select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_78_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Pharmacy Issue#60 A
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_Issue_60 A
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
##                 , t.ID_PROVIDER_MCAID3 EDW_ID_PROVIDER_MCAID3
##                 , s.ID_PROVIDER_MCAID3 BIAR_ID_PROVIDER_MCAID3
##                 , CASE WHEN (t.ID_PROVIDER_MCAID3 = s.ID_PROVIDER_MCAID3 or COALESCE(t.ID_PROVIDER_MCAID3,'1999-01-01')::date = COALESCE(s.ID_PROVIDER_MCAID3,'1999-01-01')::date) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
## 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Pharmacy Issue#60 A Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
##     and (SAK_PROV <> NULL or SAK_PROV > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
## )
## select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_60 A_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Pharmacy Issue#60 B
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_Issue_60 B
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
##                 , t.ID_PROVIDER_NPI3 EDW_ID_PROVIDER_NPI3
##                 , s.ID_PROVIDER_NPI3 BIAR_ID_PROVIDER_NPI3
##                 , CASE WHEN (t.ID_PROVIDER_NPI3 = s.ID_PROVIDER_NPI3 or COALESCE(t.ID_PROVIDER_NPI3,'') = COALESCE(s.ID_PROVIDER_NPI3,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
## 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Pharmacy Issue#60 B Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
##     and (SAK_PROV <> NULL or SAK_PROV > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
## )
## select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_60 B_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Pharmacy Issue#60 C
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_Issue_60 C
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
##                 , t.RENDERING_CDE_PROV_TYPE_PRIM EDW_RENDERING_CDE_PROV_TYPE_PRIM
##                 , s.RENDERING_CDE_PROV_TYPE_PRIM BIAR_RENDERING_CDE_PROV_TYPE_PRIM
##                 , CASE WHEN (t.RENDERING_CDE_PROV_TYPE_PRIM = s.RENDERING_CDE_PROV_TYPE_PRIM or COALESCE(t.RENDERING_CDE_PROV_TYPE_PRIM,'') = COALESCE(s.RENDERING_CDE_PROV_TYPE_PRIM,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
## 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Pharmacy Issue#60 C Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
##     and (SAK_PROV <> NULL or SAK_PROV > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
## )
## select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_60 C_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Pharmacy Issue#60 D
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_Issue_60 D
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
##                 , t.CDE_SVC_COUNTY3 EDW_CDE_SVC_COUNTY3
##                 , s.CDE_SVC_COUNTY3 BIAR_CDE_SVC_COUNTY3
##                 , CASE WHEN (t.CDE_SVC_COUNTY3 = s.CDE_SVC_COUNTY3 or COALESCE(t.CDE_SVC_COUNTY3,'') = COALESCE(s.CDE_SVC_COUNTY3,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
## 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Pharmacy Issue#60 D Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
##     and (SAK_PROV <> NULL or SAK_PROV > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
## )
## select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_60 D_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Pharmacy Issue#60 E
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_Issue_60 E
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
##                 , t.CDE_TAXONOMY3 EDW_CDE_TAXONOMY3
##                 , s.CDE_TAXONOMY3 BIAR_CDE_TAXONOMY3
##                 , CASE WHEN (t.CDE_TAXONOMY3 = s.CDE_TAXONOMY3 or COALESCE(t.CDE_TAXONOMY3,'') = COALESCE(s.CDE_TAXONOMY3,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
## 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Pharmacy Issue#60 E Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
##     and (SAK_PROV <> NULL or SAK_PROV > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
## )
## select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_60 E_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Pharmacy Issue#60 F
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_Issue_60 F
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
##                 , t.CDE_PROV_SPEC_PRIM3 EDW_CDE_PROV_SPEC_PRIM3
##                 , s.CDE_PROV_SPEC_PRIM3 BIAR_CDE_PROV_SPEC_PRIM3
##                 , CASE WHEN (t.CDE_PROV_SPEC_PRIM3 = s.CDE_PROV_SPEC_PRIM3 or COALESCE(t.CDE_PROV_SPEC_PRIM3,'') = COALESCE(s.CDE_PROV_SPEC_PRIM3,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
## 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Pharmacy Issue#60 F Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
##     and (SAK_PROV <> NULL or SAK_PROV > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
## )
## select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_60 F_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Pharmacy Issue#73 A
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_Issue_73 A
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
##                 , t.ID_PROVIDER_MCAID7 EDW_ID_PROVIDER_MCAID7
##                 , s.ID_PROVIDER_MCAID7 BIAR_ID_PROVIDER_MCAID7
##                 , CASE WHEN (t.ID_PROVIDER_MCAID7 = s.ID_PROVIDER_MCAID7 or COALESCE(t.ID_PROVIDER_MCAID7,'') = COALESCE(s.ID_PROVIDER_MCAID7,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
## 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Pharmacy Issue#73 A Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
##     and (ORP_PROV_SAK_ID is not null or ORP_PROV_SAK_ID > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
## )
## select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_73 A_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Pharmacy Issue#73 B
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_Issue_73 B
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
##                 , t.ID_PROVIDER_NPI7 EDW_ID_PROVIDER_NPI7
##                 , s.ID_PROVIDER_NPI7 BIAR_ID_PROVIDER_NPI7
##                 , CASE WHEN (t.ID_PROVIDER_NPI7 = s.ID_PROVIDER_NPI7 or COALESCE(t.ID_PROVIDER_NPI7,'1999-01-01')::date = COALESCE(s.ID_PROVIDER_NPI7,'1999-01-01')::date) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
## 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Pharmacy Issue#73 B Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
##     and (ORP_PROV_SAK_ID is not null or ORP_PROV_SAK_ID > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
## )
## select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_73 B_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Pharmacy Issue#73 C
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_Issue_73 C
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
##                 , t.PRESCRIBING_CDE_PROV_TYPE_PRIM EDW_PRESCRIBING_CDE_PROV_TYPE_PRIM
##                 , s.PRESCRIBING_CDE_PROV_TYPE_PRIM BIAR_PRESCRIBING_CDE_PROV_TYPE_PRIM
##                 , CASE WHEN (t.PRESCRIBING_CDE_PROV_TYPE_PRIM = s.PRESCRIBING_CDE_PROV_TYPE_PRIM or COALESCE(t.PRESCRIBING_CDE_PROV_TYPE_PRIM,'') = COALESCE(s.PRESCRIBING_CDE_PROV_TYPE_PRIM,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
## 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Pharmacy Issue#73 C Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
##     and (ORP_PROV_SAK_ID is not null or ORP_PROV_SAK_ID > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
## )
## select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_73 C_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ========================
## -- Pharmacy Issue#73 D
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_Issue_73 D
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
##                 , t.CDE_PROV_SPEC_PRIM7 EDW_CDE_PROV_SPEC_PRIM7
##                 , s.CDE_PROV_SPEC_PRIM7 BIAR_CDE_PROV_SPEC_PRIM7
##                 , CASE WHEN (t.CDE_PROV_SPEC_PRIM7 = s.CDE_PROV_SPEC_PRIM7 or COALESCE(t.CDE_PROV_SPEC_PRIM7,'') = COALESCE(s.CDE_PROV_SPEC_PRIM7,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
## 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ========================
## -- Pharmacy Issue#73 D Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
##     and (ORP_PROV_SAK_ID is not null or ORP_PROV_SAK_ID > 0)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
## )
## select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_73 D_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ====================
## -- Pharmacy Issue#106 A
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_Issue_106 A
##         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##         from
##         (
##         select *
##         from
##         (
##         
##                 select row_number() over(partition by t.cde_clm_type, t.IND_CLAIM order by t.num_dtl) rnum
##                 , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
##                 , t.ID_PROVIDER_MCAID8 EDW_ID_PROVIDER_MCAID8
##                 , s.ID_PROVIDER_MCAID8 BIAR_ID_PROVIDER_MCAID8
##                 , provEdw.NAME EDW_NAME
##                 , provBiar.NAME BIAR_NAME
##                 , CASE WHEN (t.ID_PROVIDER_MCAID8 = s.ID_PROVIDER_MCAID8 or
##                              provEdw.NAME = provBiar.NAME or
##                              COALESCE(t.ID_PROVIDER_MCAID8,'') = COALESCE(s.ID_PROVIDER_MCAID8,'')) 
##                        THEN 'T' 
##                        ELSE 'FAIL' 
##                        END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 , s.IND_CLAIM BIAR_IND_CLAIM
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl 
##                 left join {catalog}.{schema_name}.{Prov_TblNm} provEdw on t.ID_PROVIDER_MCAID8 = provEdw.ID_PROVIDER_MCAID1
##                 left join {catalog}.{schema_name}.{Prov_TblNm} provBiar on s.ID_PROVIDER_MCAID8 = provBiar.ID_PROVIDER_MCAID1
##                 where t.CDE_CLM_TYPE in(
##                 'P'
##                 ,
##                 'Q'
##                 )
## 				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##                 
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ==============================
## -- Pharmacy Issue#106 A Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
##     and (SUBMITTER_CD is not null)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
## )
## select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_106 A_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ====================
## -- Pharmacy Issue#106 B
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_Issue_106 B
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
##                 , t.MCP_CDE_PROV_TYPE_PRIM EDW_MCP_CDE_PROV_TYPE_PRIM
##                 , s.MCP_CDE_PROV_TYPE_PRIM BIAR_MCP_CDE_PROV_TYPE_PRIM
##                 , CASE WHEN (t.MCP_CDE_PROV_TYPE_PRIM = s.MCP_CDE_PROV_TYPE_PRIM or COALESCE(t.MCP_CDE_PROV_TYPE_PRIM,'') = COALESCE(s.MCP_CDE_PROV_TYPE_PRIM,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl 
##                 where t.CDE_CLM_TYPE in('P','Q')
## 		          and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ==============================
## -- Pharmacy Issue#106 B Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
##     and (SUBMITTER_CD is not null)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
## )
## select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_106 B_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;
## -- ====================
## -- Pharmacy Issue#106 C
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_Issue_106 C
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
##                 , t.CDE_PROV_SPEC_PRIM8 EDW_CDE_PROV_SPEC_PRIM8
##                 , s.CDE_PROV_SPEC_PRIM8 BIAR_CDE_PROV_SPEC_PRIM8
##                 , CASE WHEN (t.CDE_PROV_SPEC_PRIM8 = s.CDE_PROV_SPEC_PRIM8 or COALESCE(t.CDE_PROV_SPEC_PRIM8,'') = COALESCE(s.CDE_PROV_SPEC_PRIM8,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
## 		        and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
## -- ==============================
## -- Pharmacy Issue#106 C Ppltd Fld
## WITH
## SQL_PpltdFieldCnt AS
## (
##  select count(*) PpltdFieldCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
##     and (SUBMITTER_CD is not null)
## )
## ,SQL_TotalCnt AS
## (
##  select count(*) TotalCnt
##    from {catalog}.{schema_name}.{VEN100FA}
##   where UPPER(TRIM(CLM_TYP_CD)) in (
##                                     'P','Q'
##                                    )
## )
## select CASE WHEN TotalCnt > 0 THEN CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC (15,2)) ELSE 0.00 END Phar_Issue_106 C_Ppltd_Fld
##      -- PpltdFieldCnt
##      -- ,TotalCnt
## from SQL_PpltdFieldCnt, SQL_TotalCnt
## ;

