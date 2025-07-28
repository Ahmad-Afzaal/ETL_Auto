# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_MustRqrdFlds_DxPx.                                                                                       *
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
#* 08/01/2024 CCRB70930/CO#43342  Jaime Zavala        Added Variables to Table names.                                               *
#************************************************************************************************************************************


# COMMAND ----------

# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('EDW_TblNm', 'EDW_temp_DiagSurgProcValCodes_Analytics')
dbutils.widgets.text('BIAR_TblNm', 'DiagSurgProcValCodes_Analytics')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
EDW_TblNm = dbutils.widgets.get('EDW_TblNm')
BIAR_TblNm = dbutils.widgets.get('BIAR_TblNm')

print("catalog:", catalog)
print("schema:", schema_name)
print("EDW_TblNm:", EDW_TblNm)
print("BIAR_TblNm:", BIAR_TblNm)

# COMMAND ----------

# DBTITLE 1,Diagnosis Must Required Fields
sql_out = spark.sql(f"""
-- ======================
-- Diagnosis NUM_ICN
/*==========================================
   Diagnosis Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Dx_NUM_ICN
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID
                , t.NUM_ICN EDW_NUM_ICN
                , s.NUM_ICN BIAR_NUM_ICN
                , CASE WHEN (t.NUM_ICN = s.NUM_ICN or COALESCE(t.NUM_ICN,'') = COALESCE(s.NUM_ICN,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN
                where t.DERIVED = 'D'
                  and s.DERIVED = 'D'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;               """)
display(sql_out)
sql_out = spark.sql(f"""

-- ======================
-- Diagnosis DTE_PAID
/*==========================================
   Diagnosis Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Dx_DTE_PAID
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID
                , t.DTE_PAID EDW_DTE_PAID
                , s.DTE_PAID BIAR_DTE_PAID
                , CASE WHEN (t.DTE_PAID = s.DTE_PAID or CAST(COALESCE(t.DTE_PAID,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_PAID,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN
                where t.DERIVED = 'D'
                  and s.DERIVED = 'D'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;               """)
display(sql_out)
sql_out = spark.sql(f"""

-- ======================
-- Diagnosis CDE_HDR_STATUS
/*==========================================
   Diagnosis Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Dx_CDE_HDR_STATUS
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID
                , t.CDE_HDR_STATUS EDW_CDE_HDR_STATUS
                , s.CDE_HDR_STATUS BIAR_CDE_HDR_STATUS
                , CASE WHEN (t.CDE_HDR_STATUS = s.CDE_HDR_STATUS or COALESCE(t.CDE_HDR_STATUS,'') = COALESCE(s.CDE_HDR_STATUS,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN
                where t.DERIVED = 'D'
                  and s.DERIVED = 'D'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;               """)
display(sql_out)
sql_out = spark.sql(f"""

-- ======================
-- Diagnosis IND_CLAIM
/*==========================================
   Diagnosis Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Dx_IND_CLAIM
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID
                , t.IND_CLAIM EDW_IND_CLAIM
                , s.IND_CLAIM BIAR_IND_CLAIM
                , CASE WHEN (t.IND_CLAIM = s.IND_CLAIM or COALESCE(t.IND_CLAIM,'') = COALESCE(s.IND_CLAIM,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN
                where t.DERIVED = 'D'
                  and s.DERIVED = 'D'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;               """)
display(sql_out)
sql_out = spark.sql(f"""

-- ======================
-- Diagnosis CDE_DIAG_SEQ
/*==========================================
   Procedure Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Dx_CDE_DIAG_SEQ
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID, t.CDE_DIAG
                , t.CDE_DIAG_SEQ EDW_CDE_DIAG_SEQ
                , s.CDE_DIAG_SEQ BIAR_CDE_DIAG_SEQ
                , CASE WHEN (t.CDE_DIAG_SEQ = s.CDE_DIAG_SEQ or COALESCE(t.CDE_DIAG_SEQ,'') = COALESCE(s.CDE_DIAG_SEQ,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN and TRIM(t.CDE_DIAG) = TRIM(s.CDE_DIAG)
                where t.DERIVED = 'D'
                  and s.DERIVED = 'D'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;               """)
display(sql_out)
sql_out = spark.sql(f"""

-- ======================
-- Diagnosis CDE_DIAG
/*==========================================
   Procedure Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Dx_CDE_DIAG
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID,t.CDE_DIAG_SEQ
                , t.CDE_DIAG EDW_CDE_DIAG
                , s.CDE_DIAG BIAR_CDE_DIAG
                , CASE WHEN (t.CDE_DIAG = s.CDE_DIAG or COALESCE(t.CDE_DIAG,'') = COALESCE(s.CDE_DIAG,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN and trim(t.CDE_DIAG_SEQ) = trim(s.CDE_DIAG_SEQ)
                where t.DERIVED = 'D'
                  and s.DERIVED = 'D'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;               """)
display(sql_out)
sql_out = spark.sql(f"""

-- ======================
-- Diagnosis CDE_POA
/*==========================================
   Diagnosis Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Dx_CDE_POA
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID
                , t.CDE_POA EDW_CDE_POA
                , s.CDE_POA BIAR_CDE_POA
                , CASE WHEN (t.CDE_POA = s.CDE_POA or COALESCE(t.CDE_POA,'') = COALESCE(s.CDE_POA,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN
                where t.DERIVED = 'D'
                  and s.DERIVED = 'D'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;               """)
display(sql_out)
sql_out = spark.sql(f"""

-- ======================
-- Diagnosis ID_MEDICAID
/*==========================================
   Diagnosis Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Dx_ID_MEDICAID
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID
                , t.ID_MEDICAID EDW_ID_MEDICAID
                , s.ID_MEDICAID BIAR_ID_MEDICAID
                , CASE WHEN (t.ID_MEDICAID = s.ID_MEDICAID or COALESCE(t.ID_MEDICAID,'') = COALESCE(s.ID_MEDICAID,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN
                where t.DERIVED = 'D'
                  and s.DERIVED = 'D'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;               """)
display(sql_out)
sql_out = spark.sql(f"""

-- ======================
-- Diagnosis CDE_CLM_TYPE
/*==========================================
   Diagnosis Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Dx_CDE_CLM_TYPE
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID
                , t.CDE_CLM_TYPE EDW_CDE_CLM_TYPE
                , s.CDE_CLM_TYPE BIAR_CDE_CLM_TYPE
                , CASE WHEN (t.CDE_CLM_TYPE = s.CDE_CLM_TYPE or COALESCE(t.CDE_CLM_TYPE,'') = COALESCE(s.CDE_CLM_TYPE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN
                where t.DERIVED = 'D'
                  and s.DERIVED = 'D'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;               """)
display(sql_out)


# COMMAND ----------

# DBTITLE 1,Procedure Must Required Fields
sql_out = spark.sql(f"""
-- ======================
-- Procedures NUM_ICN
/*==========================================
   Procedure Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Px_NUM_ICN
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID
                , t.NUM_ICN EDW_NUM_ICN
                , s.NUM_ICN BIAR_NUM_ICN
                , CASE WHEN (t.NUM_ICN = s.NUM_ICN or COALESCE(t.NUM_ICN,'') = COALESCE(s.NUM_ICN,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN
                where t.DERIVED = 'P'
                  and s.DERIVED = 'P'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;
		""")
display(sql_out)
sql_out = spark.sql(f"""
-- ======================
-- Procedures DTE_PAID
/*==========================================
   Procedures Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Px_DTE_PAID
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID
                , t.DTE_PAID EDW_DTE_PAID
                , s.DTE_PAID BIAR_DTE_PAID
                , CASE WHEN (t.DTE_PAID = s.DTE_PAID or (year(t.DTE_PAID) = year(s.DTE_PAID) and month(t.DTE_PAID) = month(s.DTE_PAID)) or CAST(COALESCE(t.DTE_PAID,'1999-01-01') AS DATE) = CAST(COALESCE(s.DTE_PAID,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN
                where t.DERIVED = 'P'
                  and s.DERIVED = 'P'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;
		""")
display(sql_out)
sql_out = spark.sql(f"""
-- ======================
-- Procedures CDE_HDR_STATUS
/*==========================================
   Procedure Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Px_CDE_HDR_STATUS
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID
                , t.CDE_HDR_STATUS EDW_CDE_HDR_STATUS
                , s.CDE_HDR_STATUS BIAR_CDE_HDR_STATUS
                , CASE WHEN (t.CDE_HDR_STATUS = s.CDE_HDR_STATUS or COALESCE(t.CDE_HDR_STATUS,'') = COALESCE(s.CDE_HDR_STATUS,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN
                where t.DERIVED = 'P'
                  and s.DERIVED = 'P'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;
		""")
display(sql_out)
sql_out = spark.sql(f"""
-- ======================
-- Procedures IND_CLAIM
/*==========================================
   Procedure Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Px_IND_CLAIM
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID
                , t.IND_CLAIM EDW_IND_CLAIM
                , s.IND_CLAIM BIAR_IND_CLAIM
                , CASE WHEN (t.IND_CLAIM = s.IND_CLAIM or COALESCE(t.IND_CLAIM,'') = COALESCE(s.IND_CLAIM,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN
                where t.DERIVED = 'P'
                  and s.DERIVED = 'P'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;
		""")
display(sql_out)
sql_out = spark.sql(f"""
-- ======================
-- Procedures P_NUM_SEQ
/*==========================================
   Procedure Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Px_P_NUM_SEQ
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID, t.CDE_PROC_ICD9
                , t.P_NUM_SEQ EDW_P_NUM_SEQ
                , s.P_NUM_SEQ BIAR_P_NUM_SEQ
                , CASE WHEN (t.P_NUM_SEQ = s.P_NUM_SEQ or COALESCE(t.P_NUM_SEQ,'') = COALESCE(s.P_NUM_SEQ,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN and trim(t.CDE_PROC_ICD9) = trim(s.CDE_PROC_ICD9)
                where t.DERIVED = 'P'
                  and s.DERIVED = 'P'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;
		""")
display(sql_out)
sql_out = spark.sql(f"""
-- ========================
-- Procedures CDE_PROC_ICD9
/*==========================================
   Procedure Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Px_CDE_PROC_ICD9
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID, t.P_NUM_SEQ
                , t.CDE_PROC_ICD9 EDW_CDE_PROC_ICD9
                , s.CDE_PROC_ICD9 BIAR_CDE_PROC_ICD9
                , CASE WHEN (t.CDE_PROC_ICD9 = s.CDE_PROC_ICD9 or COALESCE(t.CDE_PROC_ICD9,'') = COALESCE(s.CDE_PROC_ICD9,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN and trim(t.P_NUM_SEQ) = trim(s.P_NUM_SEQ)
                where t.DERIVED = 'P'
                  and s.DERIVED = 'P'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;
		""")
display(sql_out)
sql_out = spark.sql(f"""
-- ======================
-- Procedures DTE_ICD_9_CM_PROC
/*==========================================
   Procedure Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Px_DTE_ICD_9_CM_PROC
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID, t.P_NUM_SEQ
                , t.DTE_ICD_9_CM_PROC EDW_DTE_ICD_9_CM_PROC
                , s.DTE_ICD_9_CM_PROC BIAR_DTE_ICD_9_CM_PROC
                , CASE WHEN (t.DTE_ICD_9_CM_PROC = s.DTE_ICD_9_CM_PROC or COALESCE(t.DTE_ICD_9_CM_PROC,'') = COALESCE(s.DTE_ICD_9_CM_PROC,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN and trim(t.P_NUM_SEQ) = trim(s.P_NUM_SEQ)
                where t.DERIVED = 'P'
                  and s.DERIVED = 'P'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;
		""")
display(sql_out)
sql_out = spark.sql(f"""
-- ======================
-- Procedures ID_MEDICAID
/*==========================================
   Procedure Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Px_ID_MEDICAID
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID
                , t.ID_MEDICAID EDW_ID_MEDICAID
                , s.ID_MEDICAID BIAR_ID_MEDICAID
                , CASE WHEN (t.ID_MEDICAID = s.ID_MEDICAID or COALESCE(t.ID_MEDICAID,'') = COALESCE(s.ID_MEDICAID,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN
                where t.DERIVED = 'P'
                  and s.DERIVED = 'P'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;
		""")
display(sql_out)
sql_out = spark.sql(f"""
-- ======================
-- Procedures CDE_CLM_TYPE
/*==========================================
   Procedure Codes  EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Px_CDE_CLM_TYPE
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.NUM_ICN, t.ID_MEDICAID
                , t.CDE_CLM_TYPE EDW_CDE_CLM_TYPE
                , s.CDE_CLM_TYPE BIAR_CDE_CLM_TYPE
                , CASE WHEN (t.CDE_CLM_TYPE = s.CDE_CLM_TYPE or COALESCE(t.CDE_CLM_TYPE,'') = COALESCE(s.CDE_CLM_TYPE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.NUM_ICN = s.NUM_ICN
                where t.DERIVED = 'P'
                  and s.DERIVED = 'P'
				  and CAST(s.DTE_PAID AS DATE) < CAST('2023-01-27' AS DATE)
           )t where true
       )t where true
        ;
		""")
display(sql_out)

