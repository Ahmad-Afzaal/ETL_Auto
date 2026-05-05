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
#* 07/23/2014 CCRB70930/CO#43342  Jaime Zavala        IND_HDR_DTL - Issue#113 Added.                                                *
#* 08/01/2024 CCRB70930/CO#43342  Jaime Zavala        Added Variables to Table names.                                               *
#* 08/15/2024 CCRB70930/CO#43342  Jaime Zavala        IND_HDR_DTL - Issue#113 Closed.                                               *
#* 01/15/2025 CCRB70930/CO#43342  Jaime Zavala        Added CDE_ADMIT_SOURCE as required field for CPC.                             *
#* 01/16/2025 CCRB70930/CO#43342  Jaime Zavala        Added CDE_FUND_CODE as required field for CPC.                                *
#* 01/23/2025 CCRB70930/CO#43342  Jaime Zavala        DTE_BILLED - Issue#90 Closed.                                                 *
#* 03/03/2025 CCRB70930/CO#43342  Jaime Zavala        CDE_FUND_CODE - Set to match when the first 6 characters are same.            *
#* 04/17/2025 CCRB70930/CO#43342  Jaime Zavala        Added the following requried fields:                                          *
#*                                                    ATTENDING_EXTERNALPROVID                                                      *
#*                                                    ATTENDING_PROVID                                                              *
#*                                                    BILLING_EXTERNALPROVID                                                        *
#*                                                    BILLING_MEDICAID_ID                                                           *
#*                                                    BILLING_PROVID                                                                *
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
dbutils.widgets.text('EDW_TblNm', 'EDW_temp_cl_inst_staging')
dbutils.widgets.text('BIAR_TblNm', 'Inst_Analytics_BIAR_EDW')
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

# DBTITLE 1,Institutional Must Required Fields
sql_out = spark.sql(f"""
-- ========================
-- Institutional NUM_ICN
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_NUM_ICN
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
                   , t.NUM_ICN EDW_NUM_ICN
                   , s.NUM_ICN BIAR_NUM_ICN
                   , CASE WHEN (t.NUM_ICN = s.NUM_ICN or COALESCE(t.NUM_ICN,'') = COALESCE(s.NUM_ICN,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional IND_CLAIM
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_IND_CLAIM
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
                   , t.IND_CLAIM EDW_IND_CLAIM
                   , s.IND_CLAIM BIAR_IND_CLAIM
                   , CASE WHEN (t.IND_CLAIM = s.IND_CLAIM or COALESCE(t.IND_CLAIM,'') = COALESCE(s.IND_CLAIM,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional CDE_CLM_TYPE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_CLM_TYPE
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
                   , t.num_icn, t.num_dtl, t.CDE_CLM_TYPE, t.CDE_CLM_TYPE
                   , t.CDE_CLM_TYPE EDW_CDE_CLM_TYPE
                   , s.CDE_CLM_TYPE BIAR_CDE_CLM_TYPE
                   , CASE WHEN (t.CDE_CLM_TYPE = s.CDE_CLM_TYPE or COALESCE(t.CDE_CLM_TYPE,'') = COALESCE(s.CDE_CLM_TYPE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional CDE_HDR_STATUS
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_HDR_STATUS
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
                   , t.CDE_HDR_STATUS EDW_CDE_HDR_STATUS
                   , s.CDE_HDR_STATUS BIAR_CDE_HDR_STATUS
                   , CASE WHEN (t.CDE_HDR_STATUS = s.CDE_HDR_STATUS or COALESCE(t.CDE_HDR_STATUS,'') = COALESCE(s.CDE_HDR_STATUS,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional CDE_AID_CATEGORY
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_AID_CATEGORY
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
                   , t.CDE_AID_CATEGORY EDW_CDE_AID_CATEGORY
                   , s.CDE_AID_CATEGORY BIAR_CDE_AID_CATEGORY
                   , CASE WHEN (t.CDE_AID_CATEGORY = s.CDE_AID_CATEGORY or COALESCE(t.CDE_AID_CATEGORY,'') = COALESCE(s.CDE_AID_CATEGORY,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                   , s.IND_HDR_DTL     BIAR_PAID_IND
                   , s.CDE_AID_CATEGORY  BIAR_HDR_STATUS
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
-- Institutional ID_MEDICAID
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_ID_MEDICAID
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
                   , t.ID_MEDICAID EDW_ID_MEDICAID
                   , s.ID_MEDICAID BIAR_ID_MEDICAID
                   , CASE WHEN (t.ID_MEDICAID = s.ID_MEDICAID or COALESCE(t.ID_MEDICAID,'') = COALESCE(s.ID_MEDICAID,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional DTE_BIRTH
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_DTE_BIRTH
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
                , t.DTE_BIRTH EDW_DTE_BIRTH
                , s.DTE_BIRTH BIAR_DTE_BIRTH
                , CASE WHEN (t.DTE_BIRTH = s.DTE_BIRTH or CAST(COALESCE(t.DTE_BIRTH,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_BIRTH,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional ATTENDING_EXTERNALPROVID
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_ATTENDING_EXTERNALPROVID_no_match_expected
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
                   , t.ATTENDING_EXTERNALPROVID EDW_ATTENDING_EXTERNALPROVID
                   , s.NA4 BIAR_ATTENDING_EXTERNALPROVID
                   , CASE WHEN (t.ATTENDING_EXTERNALPROVID = s.NA4 or COALESCE(t.ATTENDING_EXTERNALPROVID,'') = COALESCE(s.NA4,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional ATTENDING_PROVID
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_ATTENDING_PROVID_no_match_expected
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
                   , t.ATTENDING_PROVID EDW_ATTENDING_PROVID
                   , s.NA7 BIAR_ATTENDING_PROVID
                   , CASE WHEN (t.ATTENDING_PROVID = s.NA7 or COALESCE(t.ATTENDING_PROVID,'') = COALESCE(s.NA7,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional BILLING_EXTERNALPROVID
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_BILLING_EXTERNALPROVID_no_match_expected
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
                   , t.BILLING_EXTERNALPROVID EDW_BILLING_EXTERNALPROVID
                   , s.NA9 BIAR_BILLING_EXTERNALPROVID
                   , CASE WHEN (t.BILLING_EXTERNALPROVID = s.NA9 or COALESCE(t.BILLING_EXTERNALPROVID,'') = COALESCE(s.NA9,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional BILLING_MEDICAID_ID
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_BILLING_MEDICAID_ID_no_match_expected
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
                   , t.BILLING_MEDICAID_ID EDW_BILLING_MEDICAID_ID
                   , s.NA11 BIAR_BILLING_MEDICAID_ID
                   , CASE WHEN (t.BILLING_MEDICAID_ID = s.NA11 or COALESCE(t.BILLING_MEDICAID_ID,'') = COALESCE(s.NA11,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional BILLING_PROVID
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_BILLING_PROVID_no_match_expected
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
                   , t.BILLING_PROVID EDW_BILLING_PROVID
                   , s.NA13 BIAR_BILLING_PROVID
                   , CASE WHEN (t.BILLING_PROVID = s.NA13 or COALESCE(t.BILLING_PROVID,'') = COALESCE(s.NA13,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional CDE_SOI
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_SOI
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
                   , t.CDE_SOI EDW_CDE_SOI
                   , s.CDE_SOI BIAR_CDE_SOI
                   , CASE WHEN (t.CDE_SOI = s.CDE_SOI or COALESCE(t.CDE_SOI,'') = COALESCE(s.CDE_SOI,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional CDE_PATIENT_STATUS
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_PATIENT_STATUS
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
                   , t.CDE_PATIENT_STATUS EDW_CDE_PATIENT_STATUS
                   , s.CDE_PATIENT_STATUS BIAR_CDE_PATIENT_STATUS
                   , CASE WHEN (t.CDE_PATIENT_STATUS = s.CDE_PATIENT_STATUS or COALESCE(t.CDE_PATIENT_STATUS,'') = COALESCE(s.CDE_PATIENT_STATUS,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional CDE_ADMIT_SOURCE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_ADMIT_SOURCE
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
                   , t.CDE_ADMIT_SOURCE EDW_CDE_ADMIT_SOURCE
                   , s.CDE_ADMIT_SOURCE BIAR_CDE_ADMIT_SOURCE
                   , CASE WHEN (t.CDE_ADMIT_SOURCE = s.CDE_ADMIT_SOURCE or COALESCE(t.CDE_ADMIT_SOURCE,'') = COALESCE(s.CDE_ADMIT_SOURCE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ===========================
-- Institutional DTE_ADMISSION
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_DTE_ADMISSION
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
-- Institutional CDE_DRG
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_DRG
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
                   , t.CDE_DRG EDW_CDE_DRG
                   , s.CDE_DRG BIAR_CDE_DRG
                   , CASE WHEN (t.CDE_DRG = s.CDE_DRG or COALESCE(t.CDE_DRG,'') = COALESCE(s.CDE_DRG,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional DTE_DISCHARGE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_DTE_DISCHARGE
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
                , t.DTE_DISCHARGE EDW_DTE_DISCHARGE
                , s.DTE_DISCHARGE BIAR_DTE_DISCHARGE
                , CASE WHEN (t.DTE_DISCHARGE = s.DTE_DISCHARGE or CAST(COALESCE(t.DTE_DISCHARGE,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_DISCHARGE,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional AMT_DAY_OUTLIER
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_AMT_DAY_OUTLIER
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
                   , t.AMT_DAY_OUTLIER EDW_AMT_DAY_OUTLIER
                   , s.AMT_DAY_OUTLIER BIAR_AMT_DAY_OUTLIER
                   , CASE WHEN (t.AMT_DAY_OUTLIER = s.AMT_DAY_OUTLIER or COALESCE(t.AMT_DAY_OUTLIER,'0.0') = COALESCE(s.AMT_DAY_OUTLIER,'0.0')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional AMT_COST_OUTLIER
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_AMT_COST_OUTLIER
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
                   , t.AMT_COST_OUTLIER EDW_AMT_COST_OUTLIER
                   , s.AMT_COST_OUTLIER BIAR_AMT_COST_OUTLIER
                   , CASE WHEN (t.AMT_COST_OUTLIER = s.AMT_COST_OUTLIER or COALESCE(t.AMT_COST_OUTLIER,'0.0') = COALESCE(s.AMT_COST_OUTLIER,'0.0')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional AMT_TPL_APPLD
/*------------------------------------------------------------------------
--- SQL Count Match/NonMatch  IND_HDR_DTL = 'H' ----
-------------------------------------------------------------------------*/
       select
        count(*) Inst_AMT_TPL_APPLD
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
                , CAST(t.AMT_TPL_APPLD/100 AS NUMERIC(15,2)) EDW_AMT_TPL_APPLD
                , s.AMT_TPL_APPLD BIAR_AMT_TPL_APPLD
                , CASE WHEN ((t.AMT_TPL_APPLD/100) = s.AMT_TPL_APPLD or ((t.AMT_TPL_APPLD/100) - s.AMT_TPL_APPLD between -1 and 1) or COALESCE(t.AMT_TPL_APPLD,0) = COALESCE(s.AMT_TPL_APPLD,0)) 
                       THEN 'T' 
                       ELSE 'FAIL' 
                       END IS_MATCH				
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.num_icn = s.num_icn  and t.num_dtl = s.num_dtl 
                where t.CDE_CLM_TYPE in('A','C','I')
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
                  and s.IND_HDR_DTL = 'H'

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
-- Institutional IND_HDR_DTL
/*==================================
	SQL Count Match/NonMatch
========================================*/
        select
       count(*) Inst_IND_HDR_DTL_no_match_expected
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
                  , t.IND_HDR_DTL EDW_IND_HDR_DTL
                  , s.IND_HDR_DTL BIAR_IND_HDR_DTL
                  , CASE WHEN (t.IND_HDR_DTL = s.IND_HDR_DTL or COALESCE(t.IND_HDR_DTL,'') = COALESCE(s.IND_HDR_DTL,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional DTE_BILLED
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_DTE_BILLED
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
                , t.DTE_BILLED EDW_DTE_BILLED
                , s.DTE_BILLED BIAR_DTE_BILLED
                ,t.DTE_BILLED - s.DTE_BILLED
                , CASE WHEN (t.DTE_BILLED = s.DTE_BILLED or
                             --((t.DTE_BILLED - s.DTE_BILLED) between 1 and 3) or 
                             CAST(COALESCE(t.DTE_BILLED,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_BILLED,'1999-01-01') AS DATE)) 
                       THEN 'T' ELSE 'FAIL' END IS_MATCH
                --, CASE WHEN (t.DTE_BILLED = s.DTE_BILLED or CAST(COALESCE(t.DTE_BILLED,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_BILLED,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl 
                where t.CDE_CLM_TYPE in('A','C','I','L','O')
				and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
                -- and ((t.DTE_BILLED - s.DTE_BILLED) between 7 and -7)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional DTE_PAID
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_DTE_PAID
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
                , t.DTE_PAID EDW_DTE_PAID
                , s.DTE_PAID BIAR_DTE_PAID
                --, s.DTE_PAID - t.DTE_PAID
                , CASE WHEN (t.DTE_PAID = s.DTE_PAID or 
                             --(year(t.DTE_PAID) = year(s.DTE_PAID) and month(t.DTE_PAID) = month(s.DTE_PAID)) or 
                             CAST(COALESCE(t.DTE_PAID,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_PAID,'1999-01-01') AS DATE)) 
                       THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl 
                where t.CDE_CLM_TYPE in('A','C','I','L','O')
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)

           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional DTE_FIRST_SVC
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_DTE_FIRST_SVC
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
                , t.DTE_FIRST_SVC EDW_DTE_FIRST_SVC
                , s.DTE_FIRST_SVC BIAR_DTE_FIRST_SVC
                , CASE WHEN (t.DTE_FIRST_SVC = s.DTE_FIRST_SVC or CAST(COALESCE(t.DTE_FIRST_SVC,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_FIRST_SVC,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional DTE_LAST_SVC
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_DTE_LAST_SVC
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
                , t.DTE_LAST_SVC EDW_DTE_LAST_SVC
                , s.DTE_LAST_SVC BIAR_DTE_LAST_SVC
                , CASE WHEN (t.DTE_LAST_SVC = s.DTE_LAST_SVC or CAST(COALESCE(t.DTE_LAST_SVC,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_LAST_SVC,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional CDE_SEX
/*---------------------------------
--- SQL Count Match/NonMatch ----
---------------------------------*/

        select
        count(*) Inst_CDE_SEX
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
		   
                   , t.CDE_SEX EDW_CDE_SEX
                   , s.CDE_SEX BIAR_CDE_SEX
                   , CASE WHEN (t.CDE_SEX = s.CDE_SEX or COALESCE(t.CDE_SEX,'') = COALESCE(s.CDE_SEX,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
		   
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
-- Institutional NUM_ADJ_ICN
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_NUM_ADJ_ICN
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
                   , t.NUM_ADJ_ICN EDW_NUM_ADJ_ICN
                   , s.NUM_ADJ_ICN BIAR_NUM_ADJ_ICN
                   , CASE WHEN (t.NUM_ADJ_ICN = s.NUM_ADJ_ICN or COALESCE(t.NUM_ADJ_ICN,'') = COALESCE(s.NUM_ADJ_ICN,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional AMT_ALWD
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_AMT_ALWD
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
                , t.AMT_ALWD EDW_AMT_ALWD
                , s.AMT_ALWD BIAR_AMT_ALWD
                , CASE WHEN (t.AMT_ALWD = s.AMT_ALWD or COALESCE(t.AMT_ALWD,0) = COALESCE(s.AMT_ALWD,0)) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS

                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl 
				
                where t.CDE_CLM_TYPE in('A','C','I','L','O')
				and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)

           )t where true
           --and rnum<5
       )t where true
       ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional AMT_PAID
/*========================================
        SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_AMT_PAID
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
                   , t.AMT_PAID EDW_AMT_PAID
                   , s.AMT_PAID BIAR_AMT_PAID
                   , CASE WHEN (t.AMT_PAID = s.AMT_PAID or COALESCE(t.AMT_PAID,'0.0') = COALESCE(s.AMT_PAID,'0.0')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- ==============================
-- Institutional CDE_TYPE_OF_BILL
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_TYPE_OF_BILL
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
                   , t.CDE_TYPE_OF_BILL EDW_CDE_TYPE_OF_BILL
                   , s.CDE_TYPE_OF_BILL BIAR_CDE_TYPE_OF_BILL
                   , CASE WHEN (t.CDE_TYPE_OF_BILL = s.CDE_TYPE_OF_BILL or COALESCE(t.CDE_TYPE_OF_BILL,'') = COALESCE(s.CDE_TYPE_OF_BILL,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- ================================
-- Institutional CDE_TYPE_OF_BILL_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_TYPE_OF_BILL_2
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
                   , t.CDE_TYPE_OF_BILL_2 EDW_CDE_TYPE_OF_BILL_2
                   , s.CDE_TYPE_OF_BILL_2 BIAR_CDE_TYPE_OF_BILL_2
                   , CASE WHEN (t.CDE_TYPE_OF_BILL_2 = s.CDE_TYPE_OF_BILL_2 or COALESCE(t.CDE_TYPE_OF_BILL_2,'') = COALESCE(s.CDE_TYPE_OF_BILL_2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- ================================
-- Institutional CDE_TYPE_OF_BILL_3
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_TYPE_OF_BILL_3
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
                   , t.CDE_TYPE_OF_BILL_3 EDW_CDE_TYPE_OF_BILL_3
                   , s.CDE_TYPE_OF_BILL_3 BIAR_CDE_TYPE_OF_BILL_3
                   , CASE WHEN (t.CDE_TYPE_OF_BILL_3 = s.CDE_TYPE_OF_BILL_3 or COALESCE(t.CDE_TYPE_OF_BILL_3,'') = COALESCE(s.CDE_TYPE_OF_BILL_3,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional CDE_DTL_STATUS
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_DTL_STATUS
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
                   , t.CDE_DTL_STATUS EDW_CDE_DTL_STATUS
                   , s.CDE_DTL_STATUS BIAR_CDE_DTL_STATUS
                   , CASE WHEN (t.CDE_DTL_STATUS = s.CDE_DTL_STATUS or COALESCE(t.CDE_DTL_STATUS,'') = COALESCE(s.CDE_DTL_STATUS,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional DERIVED_4/no match expected.
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_DERIVED_4_no_match_expected
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
                , t.DERIVED_4 EDW_DERIVED_4
                , s.DERIVED_4 BIAR_DERIVED_4
                , CASE WHEN (t.DERIVED_4 = s.DERIVED_4 or CAST(COALESCE(t.DERIVED_4,'1999-01-01') AS DATE) = CAST(COALESCE(s.DERIVED_4,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional IS_NON_DUPLICATE_IND
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_IS_NON_DUPLICATE_IND_no_match_expected
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
                   , t.IS_NON_DUPLICATE_IND EDW_IS_NON_DUPLICATE_IND
                   , s.IS_NON_DUPLICATE_IND BIAR_IS_NON_DUPLICATE_IND
                   , CASE WHEN (t.IS_NON_DUPLICATE_IND = s.IS_NON_DUPLICATE_IND or COALESCE(t.IS_NON_DUPLICATE_IND,'') = COALESCE(s.IS_NON_DUPLICATE_IND,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional AMT_ALWD_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_AMT_ALWD_2
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
                , (t.AMT_ALWD_2/100) EDW_AMT_ALWD_2
                , s.AMT_ALWD_2 BIAR_AMT_ALWD_2
                , CASE WHEN ((t.AMT_ALWD_2/100) = s.AMT_ALWD_2 or 
                             -- (t.AMT_ALWD_2/100*-1) = s.AMT_ALWD_2 or
                             COALESCE(t.AMT_ALWD_2,0) = COALESCE(s.AMT_ALWD_2,0)
                            ) 
                       THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS

                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl 
				
                where t.CDE_CLM_TYPE in('A','C','I','L','O')
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
                --and t.IND_CLAIM='F'

           )t where true
           --and rnum<5
       )t where true
       ;
               """)
display(sql_out)

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional CLAIM_ACTIVE_IND
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CLAIM_ACTIVE_IND_no_match_expected
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
                   , t.CLAIM_ACTIVE_IND EDW_CLAIM_ACTIVE_IND
                   , s.CLAIM_ACTIVE_IND BIAR_CLAIM_ACTIVE_IND
                   , CASE WHEN (t.CLAIM_ACTIVE_IND = s.CLAIM_ACTIVE_IND or COALESCE(t.CLAIM_ACTIVE_IND,'') = COALESCE(s.CLAIM_ACTIVE_IND,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional LAST_CLAIM_IND
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_LAST_CLAIM_IND_no_match_expected
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
                   , t.LAST_CLAIM_IND EDW_LAST_CLAIM_IND
                   , s.LAST_CLAIM_IND BIAR_LAST_CLAIM_IND
                   , CASE WHEN (t.LAST_CLAIM_IND = s.LAST_CLAIM_IND or COALESCE(t.LAST_CLAIM_IND,'') = COALESCE(s.LAST_CLAIM_IND,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional IS_DKP_IND
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_IS_DKP_IND_no_match_expected
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
                   , t.IS_DKP_IND EDW_IS_DKP_IND
                   , s.IS_DKP_IND BIAR_IS_DKP_IND
                   , CASE WHEN (t.IS_DKP_IND = s.IS_DKP_IND or COALESCE(t.IS_DKP_IND,'') = COALESCE(s.IS_DKP_IND,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional NUM_DAYS_COVD
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_NUM_DAYS_COVD
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
                , t.NUM_DAYS_COVD EDW_NUM_DAYS_COVD
                , s.NUM_DAYS_COVD BIAR_NUM_DAYS_COVD
                , CASE WHEN (t.NUM_DAYS_COVD = s.NUM_DAYS_COVD or COALESCE(t.NUM_DAYS_COVD,'0.0') = COALESCE(s.NUM_DAYS_COVD,'0.0')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional NUM_DTL
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_NUM_DTL
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
                   , t.NUM_DTL EDW_NUM_DTL
                   , s.NUM_DTL BIAR_NUM_DTL
                   , CASE WHEN (t.NUM_DTL = s.NUM_DTL or COALESCE(t.NUM_DTL,'0.0') = COALESCE(s.NUM_DTL,'0.0')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional DTE_FIRST_SVC_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_DTE_FIRST_SVC_2
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
                , t.DTE_FIRST_SVC_2 EDW_DTE_FIRST_SVC_2
                , s.DTE_FIRST_SVC_2 BIAR_DTE_FIRST_SVC_2
                , CASE WHEN (t.DTE_FIRST_SVC_2 = s.DTE_FIRST_SVC_2 or CAST(COALESCE(t.DTE_FIRST_SVC_2,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_FIRST_SVC_2,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional DTE_LAST_SVC_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_DTE_LAST_SVC_2
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
                , t.DTE_LAST_SVC_2 EDW_DTE_LAST_SVC_2
                , s.DTE_LAST_SVC_2 BIAR_DTE_LAST_SVC_2
                , CASE WHEN (t.DTE_LAST_SVC_2 = s.DTE_LAST_SVC_2 or 
				             CAST(COALESCE(t.DTE_LAST_SVC_2,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_LAST_SVC_2,'1999-01-01') AS DATE)
							 ) 
					   THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional CDE_REVENUE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_REVENUE
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
                   , t.CDE_REVENUE EDW_CDE_REVENUE
                   , s.CDE_REVENUE BIAR_CDE_REVENUE
                   , CASE WHEN (t.CDE_REVENUE = s.CDE_REVENUE or COALESCE(t.CDE_REVENUE,'') = COALESCE(s.CDE_REVENUE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional AMT_BILLED_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_AMT_BILLED_2
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
                , (t.AMT_BILLED_2/100) EDW_AMT_BILLED_2
                , s.AMT_BILLED_2 BIAR_AMT_BILLED_2
                , CASE WHEN ((t.AMT_BILLED_2/100) = s.AMT_BILLED_2 or COALESCE(t.AMT_BILLED_2,0) = COALESCE(s.AMT_BILLED_2,0)) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS

                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl 
				
                where t.CDE_CLM_TYPE in('A','C','I','L','O')
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)

           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional AMT_NON_COVERED
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_AMT_NON_COVERED
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
                , (t.AMT_NON_COVERED/100) EDW_AMT_NON_COVERED
                , s.AMT_NON_COVERED BIAR_AMT_NON_COVERED
                , CASE WHEN ((t.AMT_NON_COVERED/100) = s.AMT_NON_COVERED or COALESCE(t.AMT_NON_COVERED,0) = COALESCE(s.AMT_NON_COVERED,0)) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS

                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl 
				
                where t.CDE_CLM_TYPE in('A','C','I','L','O')
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)

           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional AMT_PAID_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_AMT_PAID_2
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
                   , t.AMT_PAID_2 EDW_AMT_PAID_2
                   , s.AMT_PAID_2 BIAR_AMT_PAID_2
                   , CASE WHEN (t.AMT_PAID_2 = s.AMT_PAID_2 or COALESCE(t.AMT_PAID_2,'0.0') = COALESCE(s.AMT_PAID_2,'0.0')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional DTE_PAID_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_DTE_PAID_2
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
                , t.DTE_PAID_2 EDW_DTE_PAID_2
                , s.DTE_PAID_2 BIAR_DTE_PAID_2
                , CASE WHEN (t.DTE_PAID_2 = s.DTE_PAID_2 or (year(t.DTE_PAID_2) = year(s.DTE_PAID_2) and month(t.DTE_PAID_2) = month(s.DTE_PAID_2)) or CAST(COALESCE(t.DTE_PAID_2,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_PAID_2,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl 
                where t.CDE_CLM_TYPE in('A','C','I','L','O')
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)

           )t where true
           --and rnum<5
       )t where true
       ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Institutional AMT_TPL_APPLD_2
-----------------------------------------------------------------------
--- SQL Count Match/NonMatch IND_HDR_DTL = 'D' ----
------------------------------------------------------------------------

       select
        count(*) Inst_AMT_TPL_APPLD_2
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
                , CAST(t.AMT_TPL_APPLD_2/100 AS NUMERIC(15,2)) EDW_AMT_TPL_APPLD_2
                , s.AMT_TPL_APPLD_2 BIAR_AMT_TPL_APPLD_2
                , CASE WHEN ((t.AMT_TPL_APPLD_2/100) = s.AMT_TPL_APPLD_2 or ((t.AMT_TPL_APPLD_2/100) - s.AMT_TPL_APPLD_2 < 1) or COALESCE(t.AMT_TPL_APPLD_2,0) = COALESCE(s.AMT_TPL_APPLD_2,0)) 
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional NA83
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_NA83_no_match_expected
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
                   , t.NA83 EDW_NA83
                   , s.NA83 BIAR_NA83
                   , CASE WHEN (t.NA83 = s.NA83 or COALESCE(t.NA83,'') = COALESCE(s.NA83,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional CDE_POS
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_POS
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
                   , t.CDE_POS EDW_CDE_POS
                   , s.CDE_POS BIAR_CDE_POS
                   , CASE WHEN (t.CDE_POS = s.CDE_POS or COALESCE(t.CDE_POS,'') = COALESCE(s.CDE_POS,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional CDE_NDC
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_NDC
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
                   , t.CDE_NDC EDW_CDE_NDC
                   , s.CDE_NDC BIAR_CDE_NDC
                   , CASE WHEN (t.CDE_NDC = s.CDE_NDC or COALESCE(t.CDE_NDC,'') = COALESCE(s.CDE_NDC,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional CDE_PROC_PRIM
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_PROC_PRIM
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
                   , t.CDE_PROC_PRIM EDW_CDE_PROC_PRIM
                   , s.CDE_PROC_PRIM BIAR_CDE_PROC_PRIM
                   , CASE WHEN (t.CDE_PROC_PRIM = s.CDE_PROC_PRIM or COALESCE(t.CDE_PROC_PRIM,'') = COALESCE(s.CDE_PROC_PRIM,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional CDE_MODIFIER_1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_MODIFIER_1
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
                   , t.CDE_MODIFIER_1 EDW_CDE_MODIFIER_1
                   , s.CDE_MODIFIER_1 BIAR_CDE_MODIFIER_1
                   , CASE WHEN (t.CDE_MODIFIER_1 = s.CDE_MODIFIER_1 or COALESCE(t.CDE_MODIFIER_1,'') = COALESCE(s.CDE_MODIFIER_1,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional CDE_MODIFIER_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_MODIFIER_2
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
                   , t.CDE_MODIFIER_2 EDW_CDE_MODIFIER_2
                   , s.CDE_MODIFIER_2 BIAR_CDE_MODIFIER_2
                   , CASE WHEN (t.CDE_MODIFIER_2 = s.CDE_MODIFIER_2 or COALESCE(t.CDE_MODIFIER_2,'') = COALESCE(s.CDE_MODIFIER_2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional CDE_MODIFIER_3
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_MODIFIER_3
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
                   , t.CDE_MODIFIER_3 EDW_CDE_MODIFIER_3
                   , s.CDE_MODIFIER_3 BIAR_CDE_MODIFIER_3
                   , CASE WHEN (t.CDE_MODIFIER_3 = s.CDE_MODIFIER_3 or COALESCE(t.CDE_MODIFIER_3,'') = COALESCE(s.CDE_MODIFIER_3,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional CDE_MODIFIER_4
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_MODIFIER_4
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
                   , t.CDE_MODIFIER_4 EDW_CDE_MODIFIER_4
                   , s.CDE_MODIFIER_4 BIAR_CDE_MODIFIER_4
                   , CASE WHEN (t.CDE_MODIFIER_4 = s.CDE_MODIFIER_4 or COALESCE(t.CDE_MODIFIER_4,'') = COALESCE(s.CDE_MODIFIER_4,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional CDE_FUND_CODE/no match expected.
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_FUND_CODE_no_match_expected
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
                   join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl where t.CDE_CLM_TYPE in('A','C','I','L','O')
				    and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)

# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional CDE_RATE_TYPE/no match expected.
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_RATE_TYPE_no_match_expected
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
                   , t.CDE_RATE_TYPE EDW_CDE_RATE_TYPE
                   , s.CDE_RATE_TYPE BIAR_CDE_RATE_TYPE
                   , CASE WHEN (t.CDE_RATE_TYPE = s.CDE_RATE_TYPE or COALESCE(t.CDE_RATE_TYPE,'') = COALESCE(s.CDE_RATE_TYPE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional NUM_ADJ_ICN_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_NUM_ADJ_ICN_2
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
                   , t.NUM_ADJ_ICN_2 EDW_NUM_ADJ_ICN_2
                   , s.NUM_ADJ_ICN_2 BIAR_NUM_ADJ_ICN_2
                   , CASE WHEN (t.NUM_ADJ_ICN_2 = s.NUM_ADJ_ICN_2 or COALESCE(t.NUM_ADJ_ICN_2,'') = COALESCE(s.NUM_ADJ_ICN_2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- ===============================
-- Institutional ID_PROVIDER_MCAID
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_ID_PROVIDER_MCAID_no_match_expected
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
                   , t.ID_PROVIDER_MCAID EDW_ID_PROVIDER_MCAID
                   , s.ID_PROVIDER_MCAID BIAR_ID_PROVIDER_MCAID
                   , CASE WHEN (t.ID_PROVIDER_MCAID = s.ID_PROVIDER_MCAID or COALESCE(t.ID_PROVIDER_MCAID,'') = COALESCE(s.ID_PROVIDER_MCAID,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- =============================
-- Institutional ID_PROVIDER_NPI
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_ID_PROVIDER_NPI_no_match_expected
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
                   , t.ID_PROVIDER_NPI EDW_ID_PROVIDER_NPI
                   , s.ID_PROVIDER_NPI BIAR_ID_PROVIDER_NPI
                   , CASE WHEN (t.ID_PROVIDER_NPI = s.ID_PROVIDER_NPI or (TRIM(t.ID_PROVIDER_NPI) = '' and s.ID_PROVIDER_NPI is null) or COALESCE(t.ID_PROVIDER_NPI,'') = COALESCE(s.ID_PROVIDER_NPI,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- =====================================
-- Institutional ATTENDING_CDE_PROV_TYPE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_ATTENDING_CDE_PROV_TYPE_no_match_expected
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
                , t.ATTENDING_CDE_PROV_TYPE EDW_ATTENDING_CDE_PROV_TYPE
                , s.ATTENDING_CDE_PROV_TYPE BIAR_ATTENDING_CDE_PROV_TYPE
                , CASE WHEN (t.ATTENDING_CDE_PROV_TYPE = s.ATTENDING_CDE_PROV_TYPE or (t.ATTENDING_CDE_PROV_TYPE='  ' and s.ATTENDING_CDE_PROV_TYPE='##') or COALESCE(t.ATTENDING_CDE_PROV_TYPE,'') = COALESCE(s.ATTENDING_CDE_PROV_TYPE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ================================
-- Institutional CDE_PROV_SPEC_PRIM
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_PROV_SPEC_PRIM_no_match_expected
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
                , t.CDE_PROV_SPEC_PRIM EDW_CDE_PROV_SPEC_PRIM
                , s.CDE_PROV_SPEC_PRIM BIAR_CDE_PROV_SPEC_PRIM
                , CASE WHEN (t.CDE_PROV_SPEC_PRIM = s.CDE_PROV_SPEC_PRIM or
                             COALESCE(t.CDE_PROV_SPEC_PRIM,'') = COALESCE(s.CDE_PROV_SPEC_PRIM,'')
							 ) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional ID_PROVIDER_MCAID_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_ID_PROVIDER_MCAID_2
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
                   , t.ID_PROVIDER_MCAID_2 EDW_ID_PROVIDER_MCAID_2
                   , s.ID_PROVIDER_MCAID_2 BIAR_ID_PROVIDER_MCAID_2
                   , CASE WHEN (t.ID_PROVIDER_MCAID_2 = s.ID_PROVIDER_MCAID_2 or COALESCE(t.ID_PROVIDER_MCAID_2,'') = COALESCE(s.ID_PROVIDER_MCAID_2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional ID_PROVIDER_NPI_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_ID_PROVIDER_NPI_2
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
                   , t.ID_PROVIDER_NPI_2 EDW_ID_PROVIDER_NPI_2
                   , s.ID_PROVIDER_NPI_2 BIAR_ID_PROVIDER_NPI_2
                   , CASE WHEN (t.ID_PROVIDER_NPI_2 = s.ID_PROVIDER_NPI_2 or COALESCE(t.ID_PROVIDER_NPI_2,'') = COALESCE(s.ID_PROVIDER_NPI_2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional BILLING_CDE_PROV_TYPE_PRIM
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_BILLING_CDE_PROV_TYPE_PRIM
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
                   , t.BILLING_CDE_PROV_TYPE_PRIM EDW_BILLING_CDE_PROV_TYPE_PRIM
                   , s.BILLING_CDE_PROV_TYPE_PRIM BIAR_BILLING_CDE_PROV_TYPE_PRIM
                   , CASE WHEN (t.BILLING_CDE_PROV_TYPE_PRIM = s.BILLING_CDE_PROV_TYPE_PRIM or COALESCE(t.BILLING_CDE_PROV_TYPE_PRIM,'') = COALESCE(s.BILLING_CDE_PROV_TYPE_PRIM,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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
-- Institutional CDE_PROV_SPEC_PRIM_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_CDE_PROV_SPEC_PRIM_2
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
                   , t.CDE_PROV_SPEC_PRIM_2 EDW_CDE_PROV_SPEC_PRIM_2
                   , s.CDE_PROV_SPEC_PRIM_2 BIAR_CDE_PROV_SPEC_PRIM_2
                   , CASE WHEN (t.CDE_PROV_SPEC_PRIM_2 = s.CDE_PROV_SPEC_PRIM_2 or COALESCE(t.CDE_PROV_SPEC_PRIM_2,'') = COALESCE(s.CDE_PROV_SPEC_PRIM_2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
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


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Institutional ID_PROVIDER_MCAID_8
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Inst_ID_PROVIDER_MCAID_8
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
                , t.ID_PROVIDER_MCAID_8 EDW_ID_PROVIDER_MCAID_8
                , s.ID_PROVIDER_MCAID_8 BIAR_ID_PROVIDER_MCAID_8
                , provEdw.NAME EDW_NAME
                , provBiar.NAME BIAR_NAME
                , CASE WHEN (t.ID_PROVIDER_MCAID_8 = s.ID_PROVIDER_MCAID_8 or
                             provEdw.NAME = provBiar.NAME or
                             COALESCE(t.ID_PROVIDER_MCAID_8,'') = COALESCE(s.ID_PROVIDER_MCAID_8,'')) 
                       THEN 'T' 
                       ELSE 'FAIL' 
                       END IS_MATCH
                , s.IND_HDR_DTL     BIAR_PAID_IND
                , s.CDE_HDR_STATUS  BIAR_HDR_STATUS
                , s.CDE_DTL_STATUS  BIAR_DTL_STATUS
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm}  s on t.num_icn = s.num_icn and t.num_dtl = s.num_dtl 
                left join {catalog}.{schema_name}.{Prov_TblNm} provEdw on t.ID_PROVIDER_MCAID_8 = provEdw.ID_PROVIDER_MCAID1
                left join {catalog}.{schema_name}.{Prov_TblNm} provBiar on s.ID_PROVIDER_MCAID_8 = provBiar.ID_PROVIDER_MCAID1
                where t.CDE_CLM_TYPE in(
                'A','C','I',
                'L',
                'O'
                )
				and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)                
                
           )t where true
           --and rnum<5
       )t where true
        ;
               """)
display(sql_out)

