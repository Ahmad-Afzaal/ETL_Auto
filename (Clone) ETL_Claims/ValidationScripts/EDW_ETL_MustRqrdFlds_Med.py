# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_MustRqrdFlds_Med.                                                                                         *
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
#* 03/27/2024 CCRB70930/CO#43342  Jaime Zavala        DTE_ADMISSION  Modified to Matched when 1 day of difference.                  *
#* 06/19/2024 CCRB70930/CO#43342  Jaime Zavala        AMT_PAID2 Added as Required Field for Validation.                             *
#* 07/23/2014 CCRB70930/CO#43342  Jaime Zavala        IND_HDR_DTL - Issue#113 Added.                                                *
#*                                                    AMT_PAID2   - Issue#112 Added.                                                *
#* 08/01/2024 CCRB70930/CO#43342  Jaime Zavala        Added Variables to Table names.                                               *
#* 08/15/2024 CCRB70930/CO#43342  Jaime Zavala        IND_HDR_DTL - Issue#113 Closed.                                               *
#* 01/16/2025 CCRB70930/CO#43342  Jaime Zavala        Added CDE_FUND_CODE as required field for CPC.                                *
#* 01/23/2025 CCRB70930/CO#43342  Jaime Zavala        DTE_BILLED - Issue#90 Closed.                                                 *
#* 03/04/2025 CCRB70930/CO#43342  Jaime Zavala        CDE_FUND_CODE - Set to match when the first 6 characters are same.            *
#* 04/17/2025 CCRB70930/CO#43342  Jaime Zavala        Added the following requried fields:                                          *
#*                                                    BILLING_EXTERNALPROVID                                                        *
#*                                                    BILLING_MEDICAID_ID                                                           *
#*                                                    BILLING_PROVID                                                                *
#*                                                    RENDERING_EXTERNALPROVID                                                      *
#*                                                    RENDERING_PROVID                                                              *
#*                                                    IS_NON_DUPLICATE_IND                                                          *
#*                                                    CLAIM_ACTIVE_IND                                                              *
#*                                                    LAST_CLAIM_IND                                                                *
#*                                                    IS_DKP_IND                                                                    *
#*                                                    NA83                                                                          *
#* 04/21/2025 CCRB70930/CO#43342  Jaime Zavala        Added CDE_AID_CATEGORY as requried field.                                     *
#************************************************************************************************************************************


# COMMAND ----------

# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('EDW_TblNm', 'EDW_temp_cl_Med_staging')
dbutils.widgets.text('BIAR_TblNm', 'Med_Analytics_BIAR_EDW')
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

# DBTITLE 1,Medical Must Required Fields
sql_out = spark.sql(f"""
-- ========================
-- Medical NUM_ICN1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_NUM_ICN1
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
                , t.NUM_ICN1 EDW_NUM_ICN1
                , s.NUM_ICN1 BIAR_NUM_ICN1
                , CASE WHEN (t.NUM_ICN1 = s.NUM_ICN1 or CAST(COALESCE(t.NUM_ICN1,'') AS CHAR(1))  = CAST(COALESCE(s.NUM_ICN1,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical IND_CLAIM
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_IND_CLAIM
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
                , CASE WHEN (t.IND_CLAIM = s.IND_CLAIM or CAST(COALESCE(t.IND_CLAIM,'') AS CHAR(1)) = CAST(COALESCE(s.IND_CLAIM,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical CDE_CLM_TYPE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_CLM_TYPE
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
                , t.CDE_CLM_TYPE EDW_CDE_CLM_TYPE
                , s.CDE_CLM_TYPE BIAR_CDE_CLM_TYPE
                , CASE WHEN (t.CDE_CLM_TYPE = s.CDE_CLM_TYPE or CAST(COALESCE(t.CDE_CLM_TYPE,'') AS CHAR(1))  = CAST(COALESCE(s.CDE_CLM_TYPE,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical CDE_HDR_STATUS
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_HDR_STATUS
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
                , t.CDE_HDR_STATUS EDW_CDE_HDR_STATUS
                , s.CDE_HDR_STATUS BIAR_CDE_HDR_STATUS
                , CASE WHEN (t.CDE_HDR_STATUS = s.CDE_HDR_STATUS or CAST(COALESCE(t.CDE_HDR_STATUS,'') AS CHAR(1))  = CAST(COALESCE(s.CDE_HDR_STATUS,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical CDE_AID_CATEGORY
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_AID_CATEGORY
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
                , t.CDE_AID_CATEGORY EDW_CDE_AID_CATEGORY
                , s.CDE_AID_CATEGORY BIAR_CDE_AID_CATEGORY
                , CASE WHEN (t.CDE_AID_CATEGORY = s.CDE_AID_CATEGORY or CAST(COALESCE(t.CDE_AID_CATEGORY,'') AS CHAR(1))  = CAST(COALESCE(s.CDE_AID_CATEGORY,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_AID_CATEGORY  BIAR_HDR_STATUS
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
-- Medical ID_MEDICAID1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_ID_MEDICAID1
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
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('B','D','M')
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
-- Medical DTE_BIRTH
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_DTE_BIRTH
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
                , CASE WHEN (t.DTE_BIRTH = s.DTE_BIRTH or 
                             --(CAST(s.DTE_BIRTH - t.DTE_BIRTH AS INT) between 1 and 2 )or
                            CAST(COALESCE(t.DTE_BIRTH,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_BIRTH,'1999-01-01') AS DATE)) 
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
-- Medical BILLING_EXTERNALPROVID
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_BILLING_EXTERNALPROVID_no_match_expected
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
                , t.BILLING_EXTERNALPROVID, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.BILLING_EXTERNALPROVID EDW_BILLING_EXTERNALPROVID
                , s.NA9 BIAR_BILLING_EXTERNALPROVID
                , CASE WHEN (t.BILLING_EXTERNALPROVID = s.NA9 or COALESCE(t.BILLING_EXTERNALPROVID,'') = COALESCE(s.NA9,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical BILLING_MEDICAID_ID
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_BILLING_MEDICAID_ID_no_match_expected
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
                , t.BILLING_MEDICAID_ID, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.BILLING_MEDICAID_ID EDW_BILLING_MEDICAID_ID
                , s.NA11 BIAR_BILLING_MEDICAID_ID
                , CASE WHEN (t.BILLING_MEDICAID_ID = s.NA11 or COALESCE(t.BILLING_MEDICAID_ID,'') = COALESCE(s.NA11,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical CDE_DIAG_PRIM
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_DIAG_PRIM
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
                , t.CDE_DIAG_PRIM EDW_CDE_DIAG_PRIM
                , s.CDE_DIAG_PRIM BIAR_CDE_DIAG_PRIM
                , CASE WHEN (t.CDE_DIAG_PRIM = s.CDE_DIAG_PRIM ) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical BILLING_PROVID
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_BILLING_PROVID_no_match_expected
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
                , t.BILLING_PROVID, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.BILLING_PROVID EDW_BILLING_PROVID
                , s.NA13 BIAR_BILLING_PROVID
                , CASE WHEN (t.BILLING_PROVID = s.NA13 or COALESCE(t.BILLING_PROVID,'') = COALESCE(s.NA13,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical CDE_ICD_VERSION
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_ICD_VERSION
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
                , t.CDE_ICD_VERSION EDW_CDE_ICD_VERSION
                , s.CDE_ICD_VERSION BIAR_CDE_ICD_VERSION
                , CASE WHEN (t.CDE_ICD_VERSION = s.CDE_ICD_VERSION or CAST(COALESCE(t.CDE_ICD_VERSION,'1999-01-01') AS DATE) = CAST(COALESCE(s.CDE_ICD_VERSION,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical DTE_ADMISSION
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_DTE_ADMISSION
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
                , t.DTE_ADMISSION EDW_DTE_ADMISSION
                , s.DTE_ADMISSION BIAR_DTE_ADMISSION
                , CASE WHEN (t.DTE_ADMISSION = s.DTE_ADMISSION or
                             (CAST(CAST(t.DTE_ADMISSION AS DATE) - CAST(s.DTE_ADMISSION AS DATE) AS NUMERIC) BETWEEN -1 and 1) or
                             CAST(COALESCE(t.DTE_ADMISSION,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_ADMISSION,'1999-01-01') AS DATE)
                            ) 
                       THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical DTE_DISCHARGE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_DTE_DISCHARGE
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
                , t.DTE_DISCHARGE EDW_DTE_DISCHARGE
                , s.DTE_DISCHARGE BIAR_DTE_DISCHARGE
                , CASE WHEN (t.DTE_DISCHARGE = s.DTE_DISCHARGE or CAST(COALESCE(t.DTE_DISCHARGE,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_DISCHARGE,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical IND_HDR_DTL
/*========================================
	SQL Count Match/NonMatch
========================================*/
       select
       count(*) Med_IND_HDR_DTL_no_match_expected
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
               , CASE WHEN (t.IND_HDR_DTL = s.IND_HDR_DTL or CAST(COALESCE(t.IND_HDR_DTL,'') AS CHAR(1)) = CAST(COALESCE(s.IND_HDR_DTL,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical DTE_BILLED
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_DTE_BILLED
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
                , CASE WHEN (t.DTE_BILLED = s.DTE_BILLED or 
                            --((s.DTE_BILLED - t.DTE_BILLED) between -3 and 3) or 
                            CAST(COALESCE(t.DTE_BILLED,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_BILLED,'1999-01-01') AS DATE)) 
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

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical DTE_PAID1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_DTE_PAID1
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
                , CASE WHEN (t.DTE_PAID1 = s.DTE_PAID1 or
                            -- ((CAST(s.DTE_BILLED AS DATE) - CAST(t.DTE_BILLED AS DATE)) between -3 and 3) or 
                            -- (year(t.DTE_PAID1) = year(s.DTE_PAID1) and month(t.DTE_PAID1) = month(s.DTE_PAID1)) or
                            CAST(COALESCE(t.DTE_PAID1,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_PAID1,'1999-01-01') AS DATE)) 
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
-- Medical DTE_FIRST_SVC1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_DTE_FIRST_SVC1
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
                , CASE WHEN (t.DTE_FIRST_SVC1 = s.DTE_FIRST_SVC1 or
                            -- ((CAST(s.DTE_FIRST_SVC1 AS DATE) - CAST(t.DTE_FIRST_SVC1 AS DATE)) between -1 and 1) or 
                            CAST(COALESCE(t.DTE_FIRST_SVC1,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_FIRST_SVC1,'1999-01-01') AS DATE)) 
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
-- Medical DTE_LAST_SVC1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_DTE_LAST_SVC1
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
                , CASE WHEN (t.DTE_LAST_SVC1 = s.DTE_LAST_SVC1 or
                            -- ((CAST(s.DTE_LAST_SVC1 AS DATE) - CAST(t.DTE_LAST_SVC1 AS DATE)) between -1 and 1) or 
                            CAST(COALESCE(t.DTE_LAST_SVC1,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_LAST_SVC1,'1999-01-01') AS DATE))
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
-- Medical CDE_SEX
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_SEX
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
                , CASE WHEN (t.CDE_SEX = s.CDE_SEX or CAST(COALESCE(t.CDE_SEX,'') AS CHAR(1)) = CAST(COALESCE(s.CDE_SEX,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical NUM_ADJ_ICN1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_NUM_ADJ_ICN1
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
-- Medical RENDERING_EXTERNALPROVID
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_RENDERING_EXTERNALPROVID_no_match_expected
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
                , t.RENDERING_EXTERNALPROVID, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.RENDERING_EXTERNALPROVID EDW_RENDERING_EXTERNALPROVID
                , s.NA43 BIAR_RENDERING_EXTERNALPROVID
                , CASE WHEN (t.RENDERING_EXTERNALPROVID = s.NA43 or COALESCE(t.RENDERING_EXTERNALPROVID,'') = COALESCE(s.NA43,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical RENDERING_PROVID
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_RENDERING_PROVID_no_match_expected
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
                , t.RENDERING_PROVID, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.RENDERING_PROVID EDW_RENDERING_PROVID
                , s.NA46 BIAR_RENDERING_PROVID
                , CASE WHEN (t.RENDERING_PROVID = s.NA46 or COALESCE(t.RENDERING_PROVID,'') = COALESCE(s.NA46,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical DTE_ENTERED_SYS
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_DTE_ENTERED_SYS
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
                , CASE WHEN (t.DTE_ENTERED_SYS = s.DTE_ENTERED_SYS or CAST(t.DTE_ENTERED_SYS - s.DTE_ENTERED_SYS AS INT) between -2 and 2 or CAST(COALESCE(t.DTE_ENTERED_SYS,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_ENTERED_SYS,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical CDE_DTL_STATUS
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_DTL_STATUS
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
                , CASE WHEN (t.CDE_DTL_STATUS = s.CDE_DTL_STATUS or COALESCE(t.CDE_DTL_STATUS,'') = COALESCE(s.CDE_DTL_STATUS,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical DERIVED4/no match expected.
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_DERIVED4_no_match_expected
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
                , CASE WHEN (t.DERIVED4 = s.DERIVED4 or COALESCE(t.DERIVED4,'') = COALESCE(s.DERIVED4,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.DERIVED4  BIAR_DTL_STATUS
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
-- Medical AMT_ALWD2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_AMT_ALWD2
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
                , t.AMT_ALWD2 EDW_AMT_ALWD2
                , s.AMT_ALWD2 BIAR_AMT_ALWD2
                , CASE WHEN (t.AMT_ALWD2 = s.AMT_ALWD2 or CAST(COALESCE(t.AMT_ALWD2,'0.0') AS NUMERIC) = CAST(COALESCE(s.AMT_ALWD2,'0.0') AS NUMERIC)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical NUM_DTL
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_NUM_DTL
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
                , CASE WHEN (t.NUM_DTL = s.NUM_DTL or CAST(COALESCE(t.NUM_DTL,'0.0') AS NUMERIC)  = CAST(COALESCE(s.NUM_DTL,'0.0') AS NUMERIC)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical DTE_FIRST_SVC2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_DTE_FIRST_SVC2
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
                , CASE WHEN (t.DTE_FIRST_SVC2 = s.DTE_FIRST_SVC2 or
                            -- (CAST(CAST(s.DTE_FIRST_SVC2 AS DATE) - CAST(t.DTE_FIRST_SVC2 AS DATE) AS INT) between -1 and 1) or 
                            CAST(COALESCE(t.DTE_FIRST_SVC2,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_FIRST_SVC2,'1999-01-01') AS DATE)) 
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
-- Medical DTE_LAST_SVC2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_DTE_LAST_SVC2
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
                , CASE WHEN (t.DTE_LAST_SVC2 = s.DTE_LAST_SVC2 or
                           -- (CAST(CAST(s.DTE_LAST_SVC2 AS DATE) - CAST(t.DTE_LAST_SVC2 AS DATE) AS INT) between -1 and 1) or 
                            CAST(COALESCE(t.DTE_LAST_SVC2,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_LAST_SVC2,'1999-01-01') AS DATE)) 
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
-- Medical AMT_BILLED2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_AMT_BILLED2
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
                , t.AMT_BILLED2 EDW_AMT_BILLED2
                , s.AMT_BILLED2 BIAR_AMT_BILLED2
                , CASE WHEN (t.AMT_BILLED2 = s.AMT_BILLED2 or CAST(COALESCE(t.AMT_BILLED2,'0.0') AS NUMERIC)  = CAST(COALESCE(s.AMT_BILLED2,'0.0') AS NUMERIC)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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

# COMMAND ----------

##sql_out = spark.sql(f"""
##-- ========================
##-- Medical AMT_PAID2
##/*========================================
##	SQL Count Match/NonMatch
##========================================*/
##        select
##        count(*) Med_AMT_PAID2
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
##                , t.AMT_PAID2 EDW_AMT_PAID2
##                , s.AMT_PAID2 BIAR_AMT_PAID2
##                , CASE WHEN (t.AMT_PAID2/100 = s.AMT_PAID2 or
##                             --(t.AMT_PAID2/100 - s.AMT_PAID2 between -1 and 1) or
##                             COALESCE(t.AMT_PAID2,0)  = COALESCE(s.AMT_PAID2,0)
##                            )
##						THEN 'T' 
##						ELSE 'FAIL' 
##						END IS_MATCH
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

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical DTE_PAID2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_DTE_PAID2
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
                , CASE WHEN (t.DTE_PAID2 = s.DTE_PAID2 or
                            --(CAST(CAST(s.DTE_PAID2 AS DATE) - CAST(t.DTE_PAID2 AS DATE) AS INT) between -30 and 30) or 
                            CAST(COALESCE(t.DTE_PAID2,'1990-01-01') AS DATE)  = CAST(COALESCE(s.DTE_PAID2,'1990-01-01') AS DATE)) 
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical AMT_TPL_APPLD2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_AMT_TPL_APPLD2
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
                , t.AMT_TPL_APPLD2 EDW_AMT_TPL_APPLD2
                , s.AMT_TPL_APPLD2 BIAR_AMT_TPL_APPLD2
                , CASE WHEN (t.AMT_TPL_APPLD2 = s.AMT_TPL_APPLD2 or CAST(COALESCE(t.AMT_TPL_APPLD2,'0.0') AS NUMERIC)  = CAST(COALESCE(s.AMT_TPL_APPLD2,'0.0') AS NUMERIC)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical IS_NON_DUPLICATE_IND
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_IS_NON_DUPLICATE_IND_no_match_expected
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
                , t.IS_NON_DUPLICATE_IND, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.IS_NON_DUPLICATE_IND EDW_IS_NON_DUPLICATE_IND
                , s.IS_NON_DUPLICATE_IND BIAR_IS_NON_DUPLICATE_IND
                , CASE WHEN (t.IS_NON_DUPLICATE_IND = s.IS_NON_DUPLICATE_IND or COALESCE(t.IS_NON_DUPLICATE_IND,'') = COALESCE(s.IS_NON_DUPLICATE_IND,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical CLAIM_ACTIVE_IND
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CLAIM_ACTIVE_IND_no_match_expected
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
                , t.CLAIM_ACTIVE_IND, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.CLAIM_ACTIVE_IND EDW_CLAIM_ACTIVE_IND
                , s.CLAIM_ACTIVE_IND BIAR_CLAIM_ACTIVE_IND
                , CASE WHEN (t.CLAIM_ACTIVE_IND = s.CLAIM_ACTIVE_IND or COALESCE(t.CLAIM_ACTIVE_IND,'') = COALESCE(s.CLAIM_ACTIVE_IND,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical LAST_CLAIM_IND
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_LAST_CLAIM_IND_no_match_expected
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
                , t.LAST_CLAIM_IND, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.LAST_CLAIM_IND EDW_LAST_CLAIM_IND
                , s.LAST_CLAIM_IND BIAR_LAST_CLAIM_IND
                , CASE WHEN (t.LAST_CLAIM_IND = s.LAST_CLAIM_IND or COALESCE(t.LAST_CLAIM_IND,'') = COALESCE(s.LAST_CLAIM_IND,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical CDE_TOOTH_NBR
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_TOOTH_NBR
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
                , t.CDE_TOOTH_NBR EDW_CDE_TOOTH_NBR
                , s.CDE_TOOTH_NBR BIAR_CDE_TOOTH_NBR
                , CASE WHEN (t.CDE_TOOTH_NBR = s.CDE_TOOTH_NBR or 
                            CAST(COALESCE(t.CDE_TOOTH_NBR,'') AS CHAR(2))  = CAST(COALESCE(s.CDE_TOOTH_NBR,'') AS CHAR(2)) or
                            (CAST(t.CDE_TOOTH_NBR AS CHARACTER(2)) = '##' and CAST(COALESCE(s.CDE_TOOTH_NBR,'##') AS CHAR(2)) ='##')
                            )
                       THEN 'T' 
                       ELSE 'FAIL' END IS_MATCH
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
-- Medical IS_DKP_IND
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_IS_DKP_IND_no_match_expected
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
                , t.IS_DKP_IND, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.IS_DKP_IND EDW_IS_DKP_IND
                , s.IS_DKP_IND BIAR_IS_DKP_IND
                , CASE WHEN (t.IS_DKP_IND = s.IS_DKP_IND or COALESCE(t.IS_DKP_IND,'') = COALESCE(s.IS_DKP_IND,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical NA83
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_NA83_no_match_expected
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
                , t.NA83, t.num_dtl, t.CDE_CLM_TYPE, t.IND_CLAIM
                , t.NA83 EDW_NA83
                , s.NA83 BIAR_NA83
                , CASE WHEN (t.NA83 = s.NA83 or COALESCE(t.NA83,'') = COALESCE(s.NA83,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical CDE_POS
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_POS
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
                , CASE WHEN (t.CDE_POS = s.CDE_POS or CAST(COALESCE(t.CDE_POS,'') AS CHAR(1)) = CAST(COALESCE(s.CDE_POS,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical CDE_NDC
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_NDC
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
                , CASE WHEN (t.CDE_NDC = s.CDE_NDC or CAST(COALESCE(t.CDE_NDC,'') AS CHAR(1))  = CAST(COALESCE(s.CDE_NDC,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical CDE_PROC_PRIM
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_PROC_PRIM
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
                , t.CDE_PROC_PRIM EDW_CDE_PROC_PRIM
                , s.CDE_PROC_PRIM BIAR_CDE_PROC_PRIM
                , CASE WHEN (t.CDE_PROC_PRIM = s.CDE_PROC_PRIM or CAST(COALESCE(t.CDE_PROC_PRIM,'') AS CHAR(1))  = CAST(COALESCE(s.CDE_PROC_PRIM,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical CDE_MODIFIER_1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_MODIFIER_1
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
                , t.CDE_MODIFIER_1 EDW_CDE_MODIFIER_1
                , s.CDE_MODIFIER_1 BIAR_CDE_MODIFIER_1
                , CASE WHEN (t.CDE_MODIFIER_1 = s.CDE_MODIFIER_1 or CAST(COALESCE(t.CDE_MODIFIER_1,'') AS CHAR(1))  = CAST(COALESCE(s.CDE_MODIFIER_1,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical CDE_MODIFIER_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_MODIFIER_2
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
                , t.CDE_MODIFIER_2 EDW_CDE_MODIFIER_2
                , s.CDE_MODIFIER_2 BIAR_CDE_MODIFIER_2
                , CASE WHEN (t.CDE_MODIFIER_2 = s.CDE_MODIFIER_2 or CAST(COALESCE(t.CDE_MODIFIER_2,'') AS CHAR(1)) = CAST(COALESCE(s.CDE_MODIFIER_2,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical CDE_MODIFIER_3
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_MODIFIER_3
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
                , t.CDE_MODIFIER_3 EDW_CDE_MODIFIER_3
                , s.CDE_MODIFIER_3 BIAR_CDE_MODIFIER_3
                , CASE WHEN (t.CDE_MODIFIER_3 = s.CDE_MODIFIER_3 or CAST(COALESCE(t.CDE_MODIFIER_3,'') AS CHAR(1)) = CAST(COALESCE(s.CDE_MODIFIER_3,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical CDE_MODIFIER_4
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_MODIFIER_4
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
                , t.CDE_MODIFIER_4 EDW_CDE_MODIFIER_4
                , s.CDE_MODIFIER_4 BIAR_CDE_MODIFIER_4
                , CASE WHEN (t.CDE_MODIFIER_4 = s.CDE_MODIFIER_4 or CAST(COALESCE(t.CDE_MODIFIER_4,'') AS CHAR(1))  = CAST(COALESCE(s.CDE_MODIFIER_4,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical CDE_FUND_CODE/no match expected.
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_FUND_CODE_no_match_expected
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
-- Medical CDE_RATE_TYPE/no match expected.
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_RATE_TYPE_no_match_expected
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
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('B','D','M')
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
-- Medical NUM_ADJ_ICN2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_NUM_ADJ_ICN2
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
                , CASE WHEN (t.NUM_ADJ_ICN2 = s.NUM_ADJ_ICN2 or CAST(COALESCE(t.NUM_ADJ_ICN2,'') AS CHAR(1)) = CAST(COALESCE(s.NUM_ADJ_ICN2,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical ID_PROVIDER_MCAID2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_ID_PROVIDER_MCAID2
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
                , CASE WHEN (t.ID_PROVIDER_MCAID2 = s.ID_PROVIDER_MCAID2 or CAST(COALESCE(t.ID_PROVIDER_MCAID2,'') AS CHAR(1)) = CAST(COALESCE(s.ID_PROVIDER_MCAID2,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical ID_PROVIDER_NPI2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_ID_PROVIDER_NPI2
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
-- Medical BILLING_CDE_PROV_TYPE_PRIM
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_BILLING_CDE_PROV_TYPE_PRIM
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
                , CASE WHEN (t.BILLING_CDE_PROV_TYPE_PRIM = s.BILLING_CDE_PROV_TYPE_PRIM or CAST(COALESCE(t.BILLING_CDE_PROV_TYPE_PRIM,'') AS CHAR(1)) = CAST(COALESCE(s.BILLING_CDE_PROV_TYPE_PRIM,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical CDE_PROV_SPEC_PRIM2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_PROV_SPEC_PRIM2
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
                , t.CDE_PROV_SPEC_PRIM2 EDW_CDE_PROV_SPEC_PRIM2
                , s.CDE_PROV_SPEC_PRIM2 BIAR_CDE_PROV_SPEC_PRIM2
                , CASE WHEN (t.CDE_PROV_SPEC_PRIM2 = s.CDE_PROV_SPEC_PRIM2 or CAST(COALESCE(t.CDE_PROV_SPEC_PRIM2,'') AS CHAR(1)) = CAST(COALESCE(s.CDE_PROV_SPEC_PRIM2,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical ID_PROVIDER_MCAID3
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_ID_PROVIDER_MCAID3
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
                , CASE WHEN (t.ID_PROVIDER_MCAID3 = s.ID_PROVIDER_MCAID3 or CAST(COALESCE(t.ID_PROVIDER_MCAID3,'') AS CHAR(1)) = CAST(COALESCE(s.ID_PROVIDER_MCAID3,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical ID_PROVIDER_NPI3
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_ID_PROVIDER_NPI3
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
                , t.ID_PROVIDER_NPI3 EDW_ID_PROVIDER_NPI3
                , s.ID_PROVIDER_NPI3 BIAR_ID_PROVIDER_NPI3
                , CASE WHEN (t.ID_PROVIDER_NPI3 = s.ID_PROVIDER_NPI3 or CAST(COALESCE(t.ID_PROVIDER_NPI3,'') AS CHAR(1)) = CAST(COALESCE(s.ID_PROVIDER_NPI3,'') AS CHAR(1))) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical RENDERING_CDE_PROV_TYPE_PRIM
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_RENDERING_CDE_PROV_TYPE_PRIM
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
                , t.RENDERING_CDE_PROV_TYPE_PRIM EDW_RENDERING_CDE_PROV_TYPE_PRIM
                , s.RENDERING_CDE_PROV_TYPE_PRIM BIAR_RENDERING_CDE_PROV_TYPE_PRIM
                , CASE WHEN (t.RENDERING_CDE_PROV_TYPE_PRIM = s.RENDERING_CDE_PROV_TYPE_PRIM or COALESCE(t.RENDERING_CDE_PROV_TYPE_PRIM,'') = COALESCE(s.RENDERING_CDE_PROV_TYPE_PRIM,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical CDE_PROV_SPEC_PRIM3
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_CDE_PROV_SPEC_PRIM3
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
                , t.CDE_PROV_SPEC_PRIM3 EDW_CDE_PROV_SPEC_PRIM3
                , s.CDE_PROV_SPEC_PRIM3 BIAR_CDE_PROV_SPEC_PRIM3
                , CASE WHEN (t.CDE_PROV_SPEC_PRIM3 = s.CDE_PROV_SPEC_PRIM3 or COALESCE(t.CDE_PROV_SPEC_PRIM3,'') = COALESCE(s.CDE_PROV_SPEC_PRIM3,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical ID_PROVIDER_MCAID4
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_ID_PROVIDER_MCAID4
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
                , t.ID_PROVIDER_MCAID4 EDW_ID_PROVIDER_MCAID4
                , s.ID_PROVIDER_MCAID4 BIAR_ID_PROVIDER_MCAID4
                , CASE WHEN (t.ID_PROVIDER_MCAID4 = s.ID_PROVIDER_MCAID4 or COALESCE(t.ID_PROVIDER_MCAID4,'') = COALESCE(s.ID_PROVIDER_MCAID4,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Medical ID_PROVIDER_NPI4
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_ID_PROVIDER_NPI4
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
                , t.ID_PROVIDER_NPI4 EDW_ID_PROVIDER_NPI4
                , s.ID_PROVIDER_NPI4 BIAR_ID_PROVIDER_NPI4
                , CASE WHEN (t.ID_PROVIDER_NPI4 = s.ID_PROVIDER_NPI4 or COALESCE(t.ID_PROVIDER_NPI4,'') = COALESCE(s.ID_PROVIDER_NPI4,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical REFERRING_CDE_PROV_TYPE_PRIM
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_REFERRING_CDE_PROV_TYPE_PRIM
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
                , t.REFERRING_CDE_PROV_TYPE_PRIM EDW_REFERRING_CDE_PROV_TYPE_PRIM
                , s.REFERRING_CDE_PROV_TYPE_PRIM BIAR_REFERRING_CDE_PROV_TYPE_PRIM
                , CASE WHEN (t.REFERRING_CDE_PROV_TYPE_PRIM = s.REFERRING_CDE_PROV_TYPE_PRIM or COALESCE(t.REFERRING_CDE_PROV_TYPE_PRIM,'') = COALESCE(s.REFERRING_CDE_PROV_TYPE_PRIM,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Medical ID_PROVIDER_MCAID8
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Med_ID_PROVIDER_MCAID8
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
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn1 = s.num_icn1 and t.num_dtl = s.num_dtl 
                left join {catalog}.{schema_name}.{Prov_TblNm} provEdw on t.ID_PROVIDER_MCAID8 = provEdw.ID_PROVIDER_MCAID1
                left join {catalog}.{schema_name}.{Prov_TblNm} provBiar on s.ID_PROVIDER_MCAID8 = provBiar.ID_PROVIDER_MCAID1
                where t.CDE_CLM_TYPE in('B','D','M')
				and CAST(s.DTE_PAID1 AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)

