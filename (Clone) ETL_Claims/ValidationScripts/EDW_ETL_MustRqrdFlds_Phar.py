# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_MustRqrdFlds_Phar.                                                                                       *
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
#* 06/19/2024 CCRB70930/CO#43342  Jaime Zavala        AMT_PAID1 Added as Required Field for Validation.                             *
#* 07/22/2014 CCRB70930/CO#43342  Jaime Zavala        IND_HDR_DTL - Issue#113 Added.                                                *
#* 08/01/2024 CCRB70930/CO#43342  Jaime Zavala        Added Variables to Table names.                                               *
#* 08/15/2024 CCRB70930/CO#43342  Jaime Zavala        IND_HDR_DTL - Issue#113 Closed.                                               *
#* 01/16/2025 CCRB70930/CO#43342  Jaime Zavala        Added CDE_FUND_CODE as required field for CPC.                                *
#* 03/04/2025 CCRB70930/CO#43342  Jaime Zavala        CDE_FUND_CODE - Set to match when the first 6 characters are same.            *
#* 04/17/2025 CCRB70930/CO#43342  Jaime Zavala        Added the following requried fields:                                          *
#*                                                    BILLING_EXTERNALPROVID                                                        *
#*                                                    BILLING_MEDICAID_ID                                                           *
#*                                                    BILLING_PROVID                                                                *
#*                                                    PRESCRIBING_EXTERNALPROVID                                                    *
#*                                                    PRESCRIBING_PROVID                                                            *
#*                                                    RENDERING_EXTERNALPROVID                                                      *
#*                                                    RENDERING_PROVID                                                              *
#*                                                    IS_NON_DUPLICATE_IND                                                          *
#*                                                    CLAIM_ACTIVE_IND                                                              *
#*                                                    LAST_CLAIM_IND                                                                *
#*                                                    IS_DKP_IND                                                                    *
#*                                                    NA83                                                                          *
#* 04/21/2025 CCRB70930/CO#43342  Jaime Zavala        Added CDE_AID_CATEGORY as requried field.                                     *
#************************************************************************************************************************************
#

# COMMAND ----------

# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('EDW_TblNm', 'EDW_temp_cl_Phar_staging')
dbutils.widgets.text('BIAR_TblNm', 'Phar_Analytics_BIAR_EDW')
dbutils.widgets.text('Prov_TblNm', 'Provider_Analytics')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
EDW_TblNm = dbutils.widgets.get('EDW_TblNm')
BIAR_TblNm = dbutils.widgets.get('BIAR_TblNm')
Prov_TblNm = dbutils.widgets.get('Prov_TblNm')

print("catalog:", catalog)
print("schema:", schema_name)
print("EDW_TblNm:", EDW_TblNm)
print("BIAR_TblNm:", BIAR_TblNm)
print("Prov_TblNm:", Prov_TblNm)

# COMMAND ----------

# DBTITLE 1,Pharmacy Must Required Fields
sql_out = spark.sql(f"""
-- ========================
-- Pharmacy NUM_ICN1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_NUM_ICN1
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select row_number() over(partition by t.cde_clm_type, t.NUM_ICN1 order by t.num_dtl) rnum
                , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.NUM_ICN1
                , t.NUM_ICN1 EDW_NUM_ICN1
                , s.NUM_ICN1 BIAR_NUM_ICN1
                , CASE WHEN (t.NUM_ICN1 = s.NUM_ICN1 or COALESCE(t.NUM_ICN1,'') = COALESCE(s.NUM_ICN1,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy IND_CLAIM
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_IND_CLAIM
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
                , t.IND_CLAIM EDW_IND_CLAIM
                , s.IND_CLAIM BIAR_IND_CLAIM
                , CASE WHEN (t.IND_CLAIM = s.IND_CLAIM or COALESCE(t.IND_CLAIM,'') = COALESCE(s.IND_CLAIM,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy CDE_CLM_TYPE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_CDE_CLM_TYPE
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select row_number() over(partition by t.cde_clm_type, t.CDE_CLM_TYPE order by t.num_dtl) rnum
                , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.CDE_CLM_TYPE
                , t.CDE_CLM_TYPE EDW_CDE_CLM_TYPE
                , s.CDE_CLM_TYPE BIAR_CDE_CLM_TYPE
                , CASE WHEN (t.CDE_CLM_TYPE = s.CDE_CLM_TYPE or COALESCE(t.CDE_CLM_TYPE,'') = COALESCE(s.CDE_CLM_TYPE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy CDE_HDR_STATUS
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_CDE_HDR_STATUS
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select row_number() over(partition by t.cde_clm_type, t.CDE_HDR_STATUS order by t.num_dtl) rnum
                , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.CDE_HDR_STATUS
                , t.CDE_HDR_STATUS EDW_CDE_HDR_STATUS
                , s.CDE_HDR_STATUS BIAR_CDE_HDR_STATUS
                , CASE WHEN (t.CDE_HDR_STATUS = s.CDE_HDR_STATUS or COALESCE(t.CDE_HDR_STATUS,'') = COALESCE(s.CDE_HDR_STATUS,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy CDE_AID_CATEGORY
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_CDE_AID_CATEGORY
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select row_number() over(partition by t.cde_clm_type, t.CDE_AID_CATEGORY order by t.num_dtl) rnum
                , t.num_icn1, t.num_dtl, t.CDE_CLM_TYPE, t.CDE_AID_CATEGORY
                , t.CDE_AID_CATEGORY EDW_CDE_AID_CATEGORY
                , s.CDE_AID_CATEGORY BIAR_CDE_AID_CATEGORY
                , CASE WHEN (t.CDE_AID_CATEGORY = s.CDE_AID_CATEGORY or COALESCE(t.CDE_AID_CATEGORY,'') = COALESCE(s.CDE_AID_CATEGORY,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_AID_CATEGORY  BIAR_HDR_STATUS
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
-- Pharmacy ID_MEDICAID1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_ID_MEDICAID1
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
                , t.ID_MEDICAID1 EDW_ID_MEDICAID1
                , s.ID_MEDICAID1 BIAR_ID_MEDICAID1
                , CASE WHEN (t.ID_MEDICAID1 = s.ID_MEDICAID1 or CAST(COALESCE(t.ID_MEDICAID1,'1999-01-01') AS DATE) = CAST(COALESCE(s.ID_MEDICAID1,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy DTE_BIRTH
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_DTE_BIRTH
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
                , t.DTE_BIRTH EDW_DTE_BIRTH
                , s.DTE_BIRTH BIAR_DTE_BIRTH
                , CASE WHEN (t.DTE_BIRTH = s.DTE_BIRTH or CAST(COALESCE(t.DTE_BIRTH,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_BIRTH,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy BILLING_EXTERNALPROVID
/*========================================
	SQL Count Match/NonMatch
========================================*/
       select
       count(*) Phar_BILLING_EXTERNALPROVID_no_match_expected
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
               , t.BILLING_EXTERNALPROVID EDW_BILLING_EXTERNALPROVID
               , s.NA9 BIAR_BILLING_EXTERNALPROVID
               , CASE WHEN (t.BILLING_EXTERNALPROVID = s.NA9 or COALESCE(t.BILLING_EXTERNALPROVID,'') = COALESCE(s.NA9,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy BILLING_MEDICAID_ID
/*========================================
	SQL Count Match/NonMatch
========================================*/
       select
       count(*) Phar_BILLING_MEDICAID_ID_no_match_expected
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
               , t.BILLING_MEDICAID_ID EDW_BILLING_MEDICAID_ID
               , s.NA11 BIAR_BILLING_MEDICAID_ID
               , CASE WHEN (t.BILLING_MEDICAID_ID = s.NA11 or COALESCE(t.BILLING_MEDICAID_ID,'') = COALESCE(s.NA11,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy BILLING_PROVID
/*========================================
	SQL Count Match/NonMatch
========================================*/
       select
       count(*) Phar_BILLING_PROVID_no_match_expected
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
               , t.BILLING_PROVID EDW_BILLING_PROVID
               , s.NA13 BIAR_BILLING_PROVID
               , CASE WHEN (t.BILLING_PROVID = s.NA13 or COALESCE(t.BILLING_PROVID,'') = COALESCE(s.NA13,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy AMT_TPL_APPLD1
-----------------------------------------------------------------------
--- SQL Count Match/NonMatch  ----
------------------------------------------------------------------------

       select
        count(*) Phar_AMT_TPL_APPLD1
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
                , CAST(CAST(t.AMT_TPL_APPLD1 AS NUMERIC(15,2))/100 AS NUMERIC(15,2)) EDW_AMT_TPL_APPLD1
                , s.AMT_TPL_APPLD1 BIAR_AMT_TPL_APPLD1
                , CASE WHEN (CAST(CAST(t.AMT_TPL_APPLD1 AS NUMERIC(15,2))/100 AS NUMERIC(15,2)) = s.AMT_TPL_APPLD1 or (CAST(CAST(t.AMT_TPL_APPLD1 AS NUMERIC(15,2))/100 AS NUMERIC(15,2)) - s.AMT_TPL_APPLD1 between -1 and 1) or COALESCE(CAST(t.AMT_TPL_APPLD1 AS NUMERIC(15,2)),0) = COALESCE(s.AMT_TPL_APPLD1,0)) 
                       THEN 'T' 
                       ELSE 'FAIL' 
                       END IS_MATCH				
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.num_icn1 = s.num_icn1  and t.num_dtl = s.num_dtl 
                where t.CDE_CLM_TYPE in('P','Q')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)


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
-- Pharmacy IND_HDR_DTL
/*========================================
	SQL Count Match/NonMatch
========================================*/
       select
       count(*) Phar_IND_HDR_DTL_no_match_expected
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
               , t.IND_HDR_DTL EDW_IND_HDR_DTL
               , s.IND_HDR_DTL BIAR_IND_HDR_DTL
               , CASE WHEN (t.IND_HDR_DTL = s.IND_HDR_DTL or COALESCE(t.IND_HDR_DTL,'') = COALESCE(s.IND_HDR_DTL,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
## -- ========================
## -- Pharmacy DTE_BILLED
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_DTE_BILLED
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
##                 , t.DTE_BILLED EDW_DTE_BILLED
##                 , s.DTE_BILLED BIAR_DTE_BILLED
##                 , CASE WHEN (t.DTE_BILLED = s.DTE_BILLED or CAST(COALESCE(t.DTE_BILLED,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_BILLED,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
##				   and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
sql_out = spark.sql(f"""
-- ========================
-- Pharmacy DTE_PAID1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_DTE_PAID1
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
                , t.DTE_PAID1 EDW_DTE_PAID1
                , s.DTE_PAID1 BIAR_DTE_PAID1
                , CASE WHEN (t.DTE_PAID1 = CAST(DATE_FORMAT(CAST(s.DTE_PAID1 AS DATE),'MM/dd/y') AS CHAR(10)) or
                             CAST(COALESCE(t.DTE_PAID1,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_PAID1,'1999-01-01') AS DATE)) 
					   THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl 
                where t.CDE_CLM_TYPE in('P','Q')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
                
                
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy DTE_FIRST_SVC1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_DTE_FIRST_SVC1
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
                , t.DTE_FIRST_SVC1 EDW_DTE_FIRST_SVC1
                , s.DTE_FIRST_SVC1 BIAR_DTE_FIRST_SVC1
                , CASE WHEN (t.DTE_FIRST_SVC1 = s.DTE_FIRST_SVC1 or CAST(COALESCE(t.DTE_FIRST_SVC1,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_FIRST_SVC1,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy DTE_LAST_SVC1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_DTE_LAST_SVC1
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
                , t.DTE_LAST_SVC1 EDW_DTE_LAST_SVC1
                , s.DTE_LAST_SVC1 BIAR_DTE_LAST_SVC1
                , CASE WHEN (t.DTE_LAST_SVC1 = s.DTE_LAST_SVC1 or CAST(COALESCE(t.DTE_LAST_SVC1,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_LAST_SVC1,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy PRESCRIBING_EXTERNALPROVID
/*========================================
	SQL Count Match/NonMatch
========================================*/
       select
       count(*) Phar_PRESCRIBING_EXTERNALPROVID_no_match_expected
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
               , t.PRESCRIBING_EXTERNALPROVID EDW_PRESCRIBING_EXTERNALPROVID
               , s.NA33 BIAR_PRESCRIBING_EXTERNALPROVID
               , CASE WHEN (t.PRESCRIBING_EXTERNALPROVID = s.NA33 or COALESCE(t.PRESCRIBING_EXTERNALPROVID,'') = COALESCE(s.NA33,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy PRESCRIBING_PROVID
/*========================================
	SQL Count Match/NonMatch
========================================*/
       select
       count(*) Phar_PRESCRIBING_PROVID_no_match_expected
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
               , t.PRESCRIBING_PROVID EDW_PRESCRIBING_PROVID
               , s.NA36 BIAR_PRESCRIBING_PROVID
               , CASE WHEN (t.PRESCRIBING_PROVID = s.NA36 or COALESCE(t.PRESCRIBING_PROVID,'') = COALESCE(s.NA36,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy CDE_SEX
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_CDE_SEX
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
                , t.CDE_SEX EDW_CDE_SEX
                , s.CDE_SEX BIAR_CDE_SEX
                , CASE WHEN (t.CDE_SEX = s.CDE_SEX or COALESCE(t.CDE_SEX,'') = COALESCE(s.CDE_SEX,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy RENDERING_EXTERNALPROVID
/*========================================
	SQL Count Match/NonMatch
========================================*/
       select
       count(*) Phar_RENDERING_EXTERNALPROVID_no_match_expected
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
               , t.RENDERING_EXTERNALPROVID EDW_RENDERING_EXTERNALPROVID
               , s.NA43 BIAR_RENDERING_EXTERNALPROVID
               , CASE WHEN (t.RENDERING_EXTERNALPROVID = s.NA43 or COALESCE(t.RENDERING_EXTERNALPROVID,'') = COALESCE(s.NA43,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy RENDERING_PROVID
/*========================================
	SQL Count Match/NonMatch
========================================*/
       select
       count(*) Phar_RENDERING_PROVID_no_match_expected
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
               , t.RENDERING_PROVID EDW_RENDERING_PROVID
               , s.NA46 BIAR_RENDERING_PROVID
               , CASE WHEN (t.RENDERING_PROVID = s.NA46 or COALESCE(t.RENDERING_PROVID,'') = COALESCE(s.NA46,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy NUM_ADJ_ICN1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_NUM_ADJ_ICN1
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
                , t.NUM_ADJ_ICN1 EDW_NUM_ADJ_ICN1
                , s.NUM_ADJ_ICN1 BIAR_NUM_ADJ_ICN1
                , CASE WHEN (t.NUM_ADJ_ICN1 = s.NUM_ADJ_ICN1 or COALESCE(t.NUM_ADJ_ICN1,'') = COALESCE(s.NUM_ADJ_ICN1,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy DTE_ENTERED_SYS
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_DTE_ENTERED_SYS
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
                , t.DTE_ENTERED_SYS EDW_DTE_ENTERED_SYS
                , s.DTE_ENTERED_SYS BIAR_DTE_ENTERED_SYS
                , CASE WHEN (t.DTE_ENTERED_SYS = s.DTE_ENTERED_SYS or CAST(COALESCE(t.DTE_ENTERED_SYS,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_ENTERED_SYS,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy NUM_PRSCRIP
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_NUM_PRSCRIP
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
                , t.NUM_PRSCRIP EDW_NUM_PRSCRIP
                , s.NUM_PRSCRIP BIAR_NUM_PRSCRIP
                , CASE WHEN (t.NUM_PRSCRIP = s.NUM_PRSCRIP or COALESCE(t.NUM_PRSCRIP,'') = COALESCE(s.NUM_PRSCRIP,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy AMT_ALWD1
/*---------------------------------
--- SQL Count Match/NonMatch ----
---------------------------------*/
       select
        count(*) Phar_AMT_ALWD1
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
                , CAST(CAST(t.AMT_ALWD1 AS NUMERIC(15,2))/100 AS NUMERIC(15,2)) EDW_AMT_ALWD1
                , s.AMT_ALWD1 BIAR_AMT_ALWD1
                , CASE WHEN (CAST(CAST(t.AMT_ALWD1 AS NUMERIC(15,2))/100 AS NUMERIC(15,2)) = s.AMT_ALWD1 or (CAST(CAST(t.AMT_ALWD1 AS NUMERIC(15,2))/100 AS NUMERIC(15,2)) - s.AMT_ALWD1 between -1 and 1) or COALESCE(CAST(t.AMT_ALWD1 AS NUMERIC(15,2)),0) = COALESCE(s.AMT_ALWD1,0)) 
                       THEN 'T' 
                       ELSE 'FAIL' 
                       END IS_MATCH				
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.num_icn1 = s.num_icn1  and t.num_dtl = s.num_dtl 
                where t.CDE_CLM_TYPE in('P','Q')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)

        )t where true
        --and rnum<5

    )z where true

   --and is_match = 'FAIL'
   --and is_match = 'T'



        )y where true        
        ;
               """)
display(sql_out)

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy AMT_PAID1
/*---------------------------------
--- SQL Count Match/NonMatch ----
---------------------------------*/
       select
        count(*) Phar_AMT_PAID1
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
                , CAST(t.AMT_PAID1 AS NUMERIC(11,2)) EDW_AMT_PAID1
                , s.AMT_PAID1 BIAR_AMT_PAID1
                , CASE WHEN (CAST(t.AMT_PAID1 AS NUMERIC(11,2)) = s.AMT_PAID1 or 
				             (CAST(t.AMT_PAID1 AS NUMERIC(11,2)) - s.AMT_PAID1 between -1 and 1) or 
							 COALESCE(CAST(t.AMT_PAID1 AS NUMERIC(11,2)),0) = COALESCE(s.AMT_PAID1,0)
							) 
                       THEN 'T' 
                       ELSE 'FAIL' 
                       END IS_MATCH				
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.num_icn1 = s.num_icn1  and t.num_dtl = s.num_dtl 
                where t.CDE_CLM_TYPE in('P','Q')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)

        )t where true
        --and rnum<5

    )z where true

   --and is_match = 'FAIL'
   --and is_match = 'T'



        )y where true        
        ;
               """)
display(sql_out)

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy DTE_DISPENSE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_DTE_DISPENSE
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
                , t.DTE_DISPENSE EDW_DTE_DISPENSE
                , s.DTE_DISPENSE BIAR_DTE_DISPENSE
                , CASE WHEN (t.DTE_DISPENSE = s.DTE_DISPENSE or CAST(COALESCE(t.DTE_DISPENSE,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_DISPENSE,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy QTY_DISPENSE1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_QTY_DISPENSE1
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
                , t.QTY_DISPENSE1 EDW_QTY_DISPENSE1
                , s.QTY_DISPENSE1 BIAR_QTY_DISPENSE1
                , CASE WHEN (t.QTY_DISPENSE1 = s.QTY_DISPENSE1 or
                             CAST(t.QTY_DISPENSE1 AS NUMERIC(15,3)) = CAST(s.QTY_DISPENSE1 AS NUMERIC(15,3)) or
                             CAST(COALESCE(t.QTY_DISPENSE1,'') AS CHAR(1)) = CAST(COALESCE(s.QTY_DISPENSE1,'') AS CHAR(1))) 
                       THEN 'T' 
                       ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy NUM_DAY_SUPPLY
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_NUM_DAY_SUPPLY
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
                , t.NUM_DAY_SUPPLY EDW_NUM_DAY_SUPPLY
                , s.NUM_DAY_SUPPLY BIAR_NUM_DAY_SUPPLY
                , CASE WHEN (t.NUM_DAY_SUPPLY = s.NUM_DAY_SUPPLY or CAST(COALESCE(t.NUM_DAY_SUPPLY,'0.0') AS NUMERIC)  = CAST(COALESCE(s.NUM_DAY_SUPPLY,'0.0') AS NUMERIC)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy CDE_DTL_STATUS
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_CDE_DTL_STATUS
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
                , t.CDE_DTL_STATUS EDW_CDE_DTL_STATUS
                , s.CDE_DTL_STATUS BIAR_CDE_DTL_STATUS
                , CASE WHEN (t.CDE_DTL_STATUS = s.CDE_DTL_STATUS or CAST(COALESCE(t.CDE_DTL_STATUS,'') AS CHAR(1)) = CAST(COALESCE(s.CDE_DTL_STATUS,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy DERIVED4/no match expected
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_DERIVED4_no_match_expected
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
                , t.DERIVED4 EDW_DERIVED4
                , s.DERIVED4 BIAR_DERIVED4
                , CASE WHEN (t.DERIVED4 = s.DERIVED4 or CAST(COALESCE(t.DERIVED4,'1999-01-01') AS DATE) = CAST(COALESCE(s.DERIVED4,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy IS_NON_DUPLICATE_IND
/*========================================
	SQL Count Match/NonMatch
========================================*/
       select
       count(*) Phar_IS_NON_DUPLICATE_IND_no_match_expected
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
               , t.IS_NON_DUPLICATE_IND EDW_IS_NON_DUPLICATE_IND
               , s.IS_NON_DUPLICATE_IND BIAR_IS_NON_DUPLICATE_IND
               , CASE WHEN (t.IS_NON_DUPLICATE_IND = s.IS_NON_DUPLICATE_IND or COALESCE(t.IS_NON_DUPLICATE_IND,'') = COALESCE(s.IS_NON_DUPLICATE_IND,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy AMT_ALWD2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_AMT_ALWD2
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
                , CAST(t.AMT_ALWD2/100 AS NUMERIC(10,2)) EDW_AMT_ALWD2
                , s.AMT_ALWD2 BIAR_AMT_ALWD2
                , CASE WHEN (CAST(t.AMT_ALWD2/100 AS NUMERIC(10,2)) = s.AMT_ALWD2 ) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy CLAIM_ACTIVE_IND
/*========================================
	SQL Count Match/NonMatch
========================================*/
       select
       count(*) Phar_CLAIM_ACTIVE_IND_no_match_expected
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
               , t.CLAIM_ACTIVE_IND EDW_CLAIM_ACTIVE_IND
               , s.CLAIM_ACTIVE_IND BIAR_CLAIM_ACTIVE_IND
               , CASE WHEN (t.CLAIM_ACTIVE_IND = s.CLAIM_ACTIVE_IND or COALESCE(t.CLAIM_ACTIVE_IND,'') = COALESCE(s.CLAIM_ACTIVE_IND,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy LAST_CLAIM_IND
/*========================================
	SQL Count Match/NonMatch
========================================*/
       select
       count(*) Phar_LAST_CLAIM_IND_no_match_expected
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
               , t.LAST_CLAIM_IND EDW_LAST_CLAIM_IND
               , s.LAST_CLAIM_IND BIAR_LAST_CLAIM_IND
               , CASE WHEN (t.LAST_CLAIM_IND = s.LAST_CLAIM_IND or COALESCE(t.LAST_CLAIM_IND,'') = COALESCE(s.LAST_CLAIM_IND,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy IS_DKP_IND
/*========================================
	SQL Count Match/NonMatch
========================================*/
       select
       count(*) Phar_IS_DKP_IND_no_match_expected
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
               , t.IS_DKP_IND EDW_IS_DKP_IND
               , s.IS_DKP_IND BIAR_IS_DKP_IND
               , CASE WHEN (t.IS_DKP_IND = s.IS_DKP_IND or COALESCE(t.IS_DKP_IND,'') = COALESCE(s.IS_DKP_IND,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy NUM_DTL
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_NUM_DTL
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
                , t.NUM_DTL EDW_NUM_DTL
                , s.NUM_DTL BIAR_NUM_DTL
                , CASE WHEN (t.NUM_DTL = s.NUM_DTL ) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy DTE_FIRST_SVC2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_DTE_FIRST_SVC2
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
                , t.DTE_FIRST_SVC2 EDW_DTE_FIRST_SVC2
                , s.DTE_FIRST_SVC2 BIAR_DTE_FIRST_SVC2
                , CASE WHEN (t.DTE_FIRST_SVC2 = s.DTE_FIRST_SVC2 or CAST(COALESCE(t.DTE_FIRST_SVC2,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_FIRST_SVC2,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy DTE_LAST_SVC2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_DTE_LAST_SVC2
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
                , t.DTE_LAST_SVC2 EDW_DTE_LAST_SVC2
                , s.DTE_LAST_SVC2 BIAR_DTE_LAST_SVC2
                , CASE WHEN (t.DTE_LAST_SVC2 = s.DTE_LAST_SVC2 or CAST(COALESCE(t.DTE_LAST_SVC2,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_LAST_SVC2,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy AMT_BILLED2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_AMT_BILLED2
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
                , CAST(t.AMT_BILLED2 AS NUMERIC(10,2)) EDW_AMT_BILLED2
                , s.AMT_BILLED2 BIAR_AMT_BILLED2
                , CASE WHEN (CAST(t.AMT_BILLED2 AS NUMERIC(10,2)) = s.AMT_BILLED2) or
                            (CAST(t.AMT_BILLED2 AS NUMERIC(10,2)) - s.AMT_BILLED2 between -1 and 1)
                       THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy DTE_PAID2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_DTE_PAID2
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
                , t.DTE_PAID2 EDW_DTE_PAID2
                , s.DTE_PAID2 BIAR_DTE_PAID2
                , CASE WHEN (t.DTE_PAID2 = s.DTE_PAID2 or CAST(COALESCE(t.DTE_PAID2,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_PAID2,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy NA83
/*========================================
	SQL Count Match/NonMatch
========================================*/
       select
       count(*) Phar_NA83_no_match_expected
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
               , t.NA83 EDW_NA83
               , s.NA83 BIAR_NA83
               , CASE WHEN (t.NA83 = s.NA83 or COALESCE(t.NA83,'') = COALESCE(s.NA83,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

## -- ========================
## -- Pharmacy DTE_MCO_ADJUD2
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Phar_DTE_MCO_ADJUD2
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
##                 , t.DTE_MCO_ADJUD2 EDW_DTE_MCO_ADJUD2
##                 , s.DTE_MCO_ADJUD2 BIAR_DTE_MCO_ADJUD2
##                 , CASE WHEN (t.DTE_MCO_ADJUD2 = s.DTE_MCO_ADJUD2 or CAST(COALESCE(t.DTE_MCO_ADJUD2,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_MCO_ADJUD2,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 , s.IND_HDR_DTL     BIAR_PAID_IND
##                 , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
##                 , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('P','Q')
##				   and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
##            )t where true
##            --and rnum<5
##        )t where true
##         ;
sql_out = spark.sql(f"""
-- ========================
-- Pharmacy CDE_POS
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_CDE_POS
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
                , t.CDE_POS EDW_CDE_POS
                , s.CDE_POS BIAR_CDE_POS
                , CASE WHEN (t.CDE_POS = s.CDE_POS or CAST(COALESCE(t.CDE_POS,'') AS CHAR(2)) = CAST(COALESCE(s.CDE_POS,'') AS CHAR(2))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy CDE_NDC
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_CDE_NDC
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
                , t.CDE_NDC EDW_CDE_NDC
                , s.CDE_NDC BIAR_CDE_NDC
                , CASE WHEN (t.CDE_NDC = s.CDE_NDC or CAST(COALESCE(t.CDE_NDC,'') AS CHAR(1)) = CAST(COALESCE(s.CDE_NDC,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy CDE_THERA_CLS_SPEC
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_CDE_THERA_CLS_SPEC
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
                , t.CDE_THERA_CLS_SPEC EDW_CDE_THERA_CLS_SPEC
                , s.CDE_THERA_CLS_SPEC BIAR_CDE_THERA_CLS_SPEC
                , CASE WHEN (t.CDE_THERA_CLS_SPEC = s.CDE_THERA_CLS_SPEC or COALESCE(t.CDE_THERA_CLS_SPEC,'') = COALESCE(s.CDE_THERA_CLS_SPEC,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy CDE_FUND_CODE/no match expected
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_CDE_FUND_CODE_no_match_expected
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
                , t.CDE_FUND_CODE EDW_CDE_FUND_CODE
                , s.CDE_FUND_CODE BIAR_CDE_FUND_CODE
                , CASE WHEN (t.CDE_FUND_CODE = s.CDE_FUND_CODE or 
                                SUBSTRING(t.CDE_FUND_CODE,1,6) = SUBSTRING(s.CDE_FUND_CODE,1,6) or
                                COALESCE(t.CDE_FUND_CODE,'') = COALESCE(s.CDE_FUND_CODE,'')
                            ) THEN 'T' 
                       ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy CDE_RATE_TYPE/no match expected
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_CDE_RATE_TYPE_no_match_expected
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
                , t.CDE_RATE_TYPE EDW_CDE_RATE_TYPE
                , s.CDE_RATE_TYPE BIAR_CDE_RATE_TYPE
                , CASE WHEN (t.CDE_RATE_TYPE = s.CDE_RATE_TYPE or CAST(COALESCE(t.CDE_RATE_TYPE,'') AS CHAR(1)) = CAST(COALESCE(s.CDE_RATE_TYPE,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy NUM_ADJ_ICN2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_NUM_ADJ_ICN2
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
                , t.NUM_ADJ_ICN2 EDW_NUM_ADJ_ICN2
                , s.NUM_ADJ_ICN2 BIAR_NUM_ADJ_ICN2
                , CASE WHEN (t.NUM_ADJ_ICN2 = s.NUM_ADJ_ICN2 or COALESCE(t.NUM_ADJ_ICN2,'') = COALESCE(s.NUM_ADJ_ICN2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy ID_PROVIDER_MCAID2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_ID_PROVIDER_MCAID2
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
                , t.ID_PROVIDER_MCAID2 EDW_ID_PROVIDER_MCAID2
                , s.ID_PROVIDER_MCAID2 BIAR_ID_PROVIDER_MCAID2
                , CASE WHEN (t.ID_PROVIDER_MCAID2 = s.ID_PROVIDER_MCAID2 or COALESCE(t.ID_PROVIDER_MCAID2,'') = COALESCE(s.ID_PROVIDER_MCAID2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Pharmacy ID_PROVIDER_NPI2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_ID_PROVIDER_NPI2
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
                , t.ID_PROVIDER_NPI2 EDW_ID_PROVIDER_NPI2
                , s.ID_PROVIDER_NPI2 BIAR_ID_PROVIDER_NPI2
                , CASE WHEN (t.ID_PROVIDER_NPI2 = s.ID_PROVIDER_NPI2 or COALESCE(t.ID_PROVIDER_NPI2,'') = COALESCE(s.ID_PROVIDER_NPI2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Pharmacy BILLING_CDE_PROV_TYPE_PRIM
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_BILLING_CDE_PROV_TYPE_PRIM
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
                , t.BILLING_CDE_PROV_TYPE_PRIM EDW_BILLING_CDE_PROV_TYPE_PRIM
                , s.BILLING_CDE_PROV_TYPE_PRIM BIAR_BILLING_CDE_PROV_TYPE_PRIM
                , CASE WHEN (t.BILLING_CDE_PROV_TYPE_PRIM = s.BILLING_CDE_PROV_TYPE_PRIM or COALESCE(t.BILLING_CDE_PROV_TYPE_PRIM,'') = COALESCE(s.BILLING_CDE_PROV_TYPE_PRIM,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- ===========================
-- Pharmacy ID_PROVIDER_MCAID3
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_ID_PROVIDER_MCAID3
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
                , t.ID_PROVIDER_MCAID3 EDW_ID_PROVIDER_MCAID3
                , s.ID_PROVIDER_MCAID3 BIAR_ID_PROVIDER_MCAID3
                , CASE WHEN (t.ID_PROVIDER_MCAID3 = s.ID_PROVIDER_MCAID3 or COALESCE(t.ID_PROVIDER_MCAID3,'') = COALESCE(s.ID_PROVIDER_MCAID3,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- ===========================
-- Pharmacy ID_PROVIDER_MCAID7
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_ID_PROVIDER_MCAID7
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
                , t.ID_PROVIDER_MCAID7 EDW_ID_PROVIDER_MCAID7
                , s.ID_PROVIDER_MCAID7 BIAR_ID_PROVIDER_MCAID7
                , CASE WHEN (t.ID_PROVIDER_MCAID7 = s.ID_PROVIDER_MCAID7 or COALESCE(t.ID_PROVIDER_MCAID7,'') = COALESCE(s.ID_PROVIDER_MCAID7,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- =========================
-- Pharmacy ID_PROVIDER_NPI7
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_ID_PROVIDER_NPI7
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
                , t.ID_PROVIDER_NPI7 EDW_ID_PROVIDER_NPI7
                , s.ID_PROVIDER_NPI7 BIAR_ID_PROVIDER_NPI7
                , CASE WHEN (t.ID_PROVIDER_NPI7 = s.ID_PROVIDER_NPI7 or COALESCE(t.ID_PROVIDER_NPI7,'') = COALESCE(s.ID_PROVIDER_NPI7,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- =========================
-- Pharmacy CDE_PROV_SPEC_PRIM7
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_CDE_PROV_SPEC_PRIM7
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
                , t.CDE_PROV_SPEC_PRIM7 EDW_CDE_PROV_SPEC_PRIM7
                , s.CDE_PROV_SPEC_PRIM7 BIAR_CDE_PROV_SPEC_PRIM7
                , CASE WHEN (t.CDE_PROV_SPEC_PRIM7 = s.CDE_PROV_SPEC_PRIM7 or COALESCE(t.CDE_PROV_SPEC_PRIM7,'') = COALESCE(s.CDE_PROV_SPEC_PRIM7,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl 
				where t.CDE_CLM_TYPE in('P','Q')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===========================
-- Pharmacy ID_PROVIDER_MCAID8/no match expected
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Phar_ID_PROVIDER_MCAID8_no_match_expected
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
                , t.ID_PROVIDER_MCAID8 EDW_ID_PROVIDER_MCAID8
                , s.ID_PROVIDER_MCAID8 BIAR_ID_PROVIDER_MCAID8
                , provEdw.NAME EDW_NAME
                , provBiar.NAME BIAR_NAME
                , CASE WHEN (t.ID_PROVIDER_MCAID8 = s.ID_PROVIDER_MCAID8 or
                             provEdw.NAME = provBiar.NAME or
                             COALESCE(t.ID_PROVIDER_MCAID8,'') = COALESCE(s.ID_PROVIDER_MCAID8,'')) 
                       THEN 'T' 
                       ELSE 'FAIL' 
                       END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                , s.IND_CLAIM BIAR_IND_CLAIM
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl 
                left join {catalog}.{schema_name}.{Prov_TblNm} provEdw on t.ID_PROVIDER_MCAID8 = provEdw.ID_PROVIDER_MCAID1
                left join {catalog}.{schema_name}.{Prov_TblNm} provBiar on s.ID_PROVIDER_MCAID8 = provBiar.ID_PROVIDER_MCAID1
                where t.CDE_CLM_TYPE in(
                'P'
                ,
                'Q'
                )
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
                
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)

