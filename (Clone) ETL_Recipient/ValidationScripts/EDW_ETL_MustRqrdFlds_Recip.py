# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_MustRqrdFlds_Recip.                                                                                      *
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
#* 06/11/2024 CCRB70930/CO#43342  Jaime Zavala        Added logic for NUM_LONGITUDE and NUM_LATITUDE/EDW placed a fix in the data.  *
#* 05/27/2025 INC0074781/DF#24548 Jaime Zavala        Added logic for LNG_CDE_DESC new field.                                       *
#************************************************************************************************************************************
#

# COMMAND ----------

# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('EDW_TblNm', 'Recipient_Analytics')
dbutils.widgets.text('BIAR_TblNm', 'BIAR_Recipient_Analytics')
dbutils.widgets.text('VEN130FA', 'EDW_VEN130FA_Staging')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
EDW_TblNm = dbutils.widgets.get('EDW_TblNm')
BIAR_TblNm = dbutils.widgets.get('BIAR_TblNm')
VEN130FA = dbutils.widgets.get('VEN130FA')

print("catalog:", catalog)
print("schema:", schema_name)
print("EDW Table Name:", EDW_TblNm)
print("BIAR Table Name:", BIAR_TblNm)
print("VE Reference TableNm: ", VEN130FA)

# COMMAND ----------

# DBTITLE 1,Recipient Required Fields
sql_out = spark.sql(f"""
-- ========================
-- Recipients ID_MEDICAID_by_SAK_RECIP
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_ID_MEDICAID_by_SAK_RECIP
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
                t.ID_MEDICAID
                , t.ID_MEDICAID EDW_ID_MEDICAID
                , s.ID_MEDICAID BIAR_ID_MEDICAID
                , CASE WHEN (t.ID_MEDICAID = s.ID_MEDICAID or NVL(t.ID_MEDICAID,'') = NVL(s.ID_MEDICAID,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.SAK_RECIP = s.SAK_RECIP
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients SAK_RECIP
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_SAK_RECIP
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
                t.ID_MEDICAID
                , t.SAK_RECIP EDW_SAK_RECIP
                , s.SAK_RECIP BIAR_SAK_RECIP
                , CASE WHEN (t.SAK_RECIP = s.SAK_RECIP or NVL(t.SAK_RECIP,'') = NVL(s.SAK_RECIP,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients NAM_LAST
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_NAM_LAST
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
                t.ID_MEDICAID
                , t.NAM_LAST EDW_NAM_LAST
                , s.NAM_LAST BIAR_NAM_LAST
                , CASE WHEN (t.NAM_LAST = s.NAM_LAST or NVL(t.NAM_LAST,'') = NVL(s.NAM_LAST,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients NAM_FIRST
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_NAM_FIRST
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
                t.ID_MEDICAID
                , t.NAM_FIRST EDW_NAM_FIRST
                , s.NAM_FIRST BIAR_NAM_FIRST
                , CASE WHEN (t.NAM_FIRST = s.NAM_FIRST or NVL(t.NAM_FIRST,'') = NVL(s.NAM_FIRST,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients NAM_MID_INIT
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_NAM_MID_INIT
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
                t.ID_MEDICAID
                , t.NAM_MID_INIT EDW_NAM_MID_INIT
                , s.NAM_MID_INIT BIAR_NAM_MID_INIT
                , CASE WHEN (t.NAM_MID_INIT = s.NAM_MID_INIT or NVL(t.NAM_MID_INIT,'') = NVL(s.NAM_MID_INIT,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients NUM_SSN
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_NUM_SSN
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
                t.ID_MEDICAID
                , t.NUM_SSN EDW_NUM_SSN
                , s.NUM_SSN BIAR_NUM_SSN
                , CASE WHEN (t.NUM_SSN = s.NUM_SSN or NVL(t.NUM_SSN,'') = NVL(s.NUM_SSN,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients CDE_RACE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_CDE_RACE
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
                t.ID_MEDICAID
                , t.CDE_RACE EDW_CDE_RACE
                , s.CDE_RACE BIAR_CDE_RACE
                , CASE WHEN (t.CDE_RACE = s.CDE_RACE or NVL(t.CDE_RACE,'') = NVL(s.CDE_RACE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients CDE_RACE_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_CDE_RACE_2
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
                t.ID_MEDICAID
                , t.CDE_RACE_2 EDW_CDE_RACE_2
                , s.CDE_RACE_2 BIAR_CDE_RACE_2
                , CASE WHEN (t.CDE_RACE_2 = s.CDE_RACE_2 or NVL(t.CDE_RACE_2,'') = NVL(s.CDE_RACE_2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients CDE_RACE_3
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_CDE_RACE_3
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
                t.ID_MEDICAID
                , t.CDE_RACE_3 EDW_CDE_RACE_3
                , s.CDE_RACE_3 BIAR_CDE_RACE_3
                , CASE WHEN (t.CDE_RACE_3 = s.CDE_RACE_3 or NVL(t.CDE_RACE_3,'') = NVL(s.CDE_RACE_3,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients CDE_RACE_4
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_CDE_RACE_4
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
                t.ID_MEDICAID
                , t.CDE_RACE_4 EDW_CDE_RACE_4
                , s.CDE_RACE_4 BIAR_CDE_RACE_4
                , CASE WHEN (t.CDE_RACE_4 = s.CDE_RACE_4 or NVL(t.CDE_RACE_4,'') = NVL(s.CDE_RACE_4,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients CDE_RACE_5
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_CDE_RACE_5
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
                t.ID_MEDICAID
                , t.CDE_RACE_5 EDW_CDE_RACE_5
                , s.CDE_RACE_5 BIAR_CDE_RACE_5
                , CASE WHEN (t.CDE_RACE_5 = s.CDE_RACE_5 or NVL(t.CDE_RACE_5,'') = NVL(s.CDE_RACE_5,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients CDE_RACE_6
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_CDE_RACE_6
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
                t.ID_MEDICAID
                , t.CDE_RACE_6 EDW_CDE_RACE_6
                , s.CDE_RACE_6 BIAR_CDE_RACE_6
                , CASE WHEN (t.CDE_RACE_6 = s.CDE_RACE_6 or NVL(t.CDE_RACE_6,'') = NVL(s.CDE_RACE_6,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients CDE_RACE_7
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_CDE_RACE_7
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
                t.ID_MEDICAID
                , t.CDE_RACE_7 EDW_CDE_RACE_7
                , s.CDE_RACE_7 BIAR_CDE_RACE_7
                , CASE WHEN (t.CDE_RACE_7 = s.CDE_RACE_7 or NVL(t.CDE_RACE_7,'') = NVL(s.CDE_RACE_7,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients CDE_ETHNIC
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_CDE_ETHNIC
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
                t.ID_MEDICAID
                , t.CDE_ETHNIC EDW_CDE_ETHNIC
                , s.CDE_ETHNIC BIAR_CDE_ETHNIC
                , CASE WHEN (t.CDE_ETHNIC = s.CDE_ETHNIC or NVL(t.CDE_ETHNIC,'') = NVL(s.CDE_ETHNIC,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients CDE_SOURCE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_CDE_SOURCE
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
                t.ID_MEDICAID
                , t.CDE_SOURCE EDW_CDE_SOURCE
                , s.CDE_SOURCE BIAR_CDE_SOURCE
                , CASE WHEN (t.CDE_SOURCE = s.CDE_SOURCE or NVL(t.CDE_SOURCE,'') = NVL(s.CDE_SOURCE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients CDE_SEX
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_CDE_SEX
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
                t.ID_MEDICAID
                , t.CDE_SEX EDW_CDE_SEX
                , s.CDE_SEX BIAR_CDE_SEX
                , CASE WHEN (t.CDE_SEX = s.CDE_SEX or NVL(t.CDE_SEX,'') = NVL(s.CDE_SEX,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients DTE_BIRTH
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_DTE_BIRTH
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
                t.ID_MEDICAID
                , t.DTE_BIRTH EDW_DTE_BIRTH
                , s.DTE_BIRTH BIAR_DTE_BIRTH
                , CASE WHEN (t.DTE_BIRTH = s.DTE_BIRTH or NVL(t.DTE_BIRTH,'1990-01-01') = NVL(s.DTE_BIRTH,'1990-01-01')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients DTE_DEATH
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_DTE_DEATH
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
                t.ID_MEDICAID
                , t.DTE_DEATH EDW_DTE_DEATH
                , s.DTE_DEATH BIAR_DTE_DEATH
                , CASE WHEN (t.DTE_DEATH = s.DTE_DEATH or NVL(t.DTE_DEATH,'1990-01-01') = NVL(s.DTE_DEATH,'1990-01-01')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients NUM_CASE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_NUM_CASE
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
                t.ID_MEDICAID
                , t.NUM_CASE EDW_NUM_CASE
                , s.NUM_CASE BIAR_NUM_CASE
                , CASE WHEN (t.NUM_CASE = s.NUM_CASE or NVL(t.NUM_CASE,'') = NVL(s.NUM_CASE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients CDE_LANGUAGE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_CDE_LANGUAGE
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
                t.ID_MEDICAID
                , t.CDE_LANGUAGE EDW_CDE_LANGUAGE
                , s.CDE_LANGUAGE BIAR_CDE_LANGUAGE
                , CASE WHEN (t.CDE_LANGUAGE = s.CDE_LANGUAGE or NVL(t.CDE_LANGUAGE,'') = NVL(s.CDE_LANGUAGE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients CDE_LIV_ARNG
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_CDE_LIV_ARNG
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
                t.ID_MEDICAID
                , t.CDE_LIV_ARNG EDW_CDE_LIV_ARNG
                , s.CDE_LIV_ARNG BIAR_CDE_LIV_ARNG
                , CASE WHEN (t.CDE_LIV_ARNG = s.CDE_LIV_ARNG or NVL(t.CDE_LIV_ARNG,'') = NVL(s.CDE_LIV_ARNG,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients CDE_SSI_STATUS
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_CDE_SSI_STATUS
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
                t.ID_MEDICAID
                , t.CDE_SSI_STATUS EDW_CDE_SSI_STATUS
                , s.CDE_SSI_STATUS BIAR_CDE_SSI_STATUS
                , CASE WHEN (t.CDE_SSI_STATUS = s.CDE_SSI_STATUS or NVL(t.CDE_SSI_STATUS,'') = NVL(s.CDE_SSI_STATUS,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients CDE_MARITAL
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_CDE_MARITAL
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
                t.ID_MEDICAID
                , t.CDE_MARITAL EDW_CDE_MARITAL
                , s.CDE_MARITAL BIAR_CDE_MARITAL
                , CASE WHEN (t.CDE_MARITAL = s.CDE_MARITAL or NVL(t.CDE_MARITAL,'') = NVL(s.CDE_MARITAL,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipient DTE_EFFECTIVE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_DTE_EFFECTIVE
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
                t.ID_MEDICAID
                , t.DTE_EFFECTIVE EDW_DTE_EFFECTIVE
                , s.DTE_EFFECTIVE BIAR_DTE_EFFECTIVE
                , CASE WHEN (t.DTE_EFFECTIVE = s.DTE_EFFECTIVE or NVL(t.DTE_EFFECTIVE,'1990-01-01') = NVL(s.DTE_EFFECTIVE,'1990-01-01')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipient DTE_END
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_DTE_END
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
                t.ID_MEDICAID
                , t.DTE_END EDW_DTE_END
                , s.DTE_END BIAR_DTE_END
                , CASE WHEN (t.DTE_END = s.DTE_END or NVL(t.DTE_END,'1990-01-01') = NVL(s.DTE_END,'1990-01-01')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipient LNG_CDE_DESC
/*========================================
	SQL Count Match/NonMatch
========================================*/
        SELECT
        count(*) Recip_LNG_CDE_DESC
        , SUM(CAST(is_match ='T' AS INT)) matches_cnt
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           SELECT *
             FROM
                (
                     SELECT distinct
                            t.ID_MEDICAID
                            , t.LNG_CDE_DESC      EDW_LNG_CDE_DESC
                            , TRIM(r.DESCRIPTION) BIAR_LNG_CDE_DESC
                            , CASE WHEN (t.LNG_CDE_DESC = TRIM(r.DESCRIPTION) or NVL(t.LNG_CDE_DESC,'') = NVL(r.DESCRIPTION,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                       FROM {catalog}.{schema_name}.{EDW_TblNm} t
                       JOIN {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
                  LEFT JOIN {catalog}.{schema_name}.{VEN130FA} r ON CODESET_NAME ='LNG' AND TRIM(CODE) = TRIM(s.CDE_LANGUAGE)
                )t where true
       )t WHERE true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients DERIVED2_no_match_expected
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_DERIVED2_no_match_expected
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
                t.ID_MEDICAID
                , t.DERIVED2 EDW_DERIVED2
                , s.DERIVED2 BIAR_DERIVED2
                , CASE WHEN (t.DERIVED2 = s.DERIVED2 or NVL(t.DERIVED2,'1990-01-01'::date) = NVL(s.DERIVED2,'1990-01-01'::date)) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)
## -- ========================
## -- Recipients DTE_EFFECTIVE
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Recip_DTE_EFFECTIVE
##         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##         from
##         (
##            select *
##            from
##            (
##                 select distinct
##                 t.ID_MEDICAID
##                 , t.DTE_EFFECTIVE EDW_DTE_EFFECTIVE
##                 , s.DTE_EFFECTIVE BIAR_DTE_EFFECTIVE
##                 , CASE WHEN (t.DTE_EFFECTIVE = s.DTE_EFFECTIVE or NVL(t.DTE_EFFECTIVE,'1990-01-01') = NVL(s.DTE_EFFECTIVE,'1990-01-01')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
##            )t where true
##        )t where true
##         ;
## -- ========================
## -- Recipients DTE_END
## /*========================================
## 	SQL Count Match/NonMatch
## ========================================*/
##         select
##         count(*) Recip_DTE_END
##         , SUM(CAST(is_match ='T' AS INT)) matches_cnt
##         , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
##         , SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
##         , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
##         from
##         (
##            select *
##            from
##            (
##                 select distinct
##                 t.ID_MEDICAID
##                 , t.DTE_END EDW_DTE_END
##                 , s.DTE_END BIAR_DTE_END
##                 , CASE WHEN (t.DTE_END = s.DTE_END or NVL(t.DTE_END,'1990-01-01') = NVL(s.DTE_END,'1990-01-01')) THEN 'T' ELSE 'FAIL' END IS_MATCH
##                 from {catalog}.{schema_name}.{EDW_TblNm} t
##                 join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
##            )t where true
##        )t where true
##         ;


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients SAK_RECIP_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_SAK_RECIP_2
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
                t.ID_MEDICAID
                , t.SAK_RECIP EDW_SAK_RECIP
                , s.SAK_RECIP BIAR_SAK_RECIP
                , CASE WHEN (t.SAK_RECIP = s.SAK_RECIP or NVL(t.SAK_RECIP,'') = NVL(s.SAK_RECIP,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients NAM_LAST_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_NAM_LAST_2
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
                t.ID_MEDICAID
                , t.NAM_LAST EDW_NAM_LAST
                , s.NAM_LAST BIAR_NAM_LAST
                , CASE WHEN (t.NAM_LAST = s.NAM_LAST or NVL(t.NAM_LAST,'') = NVL(s.NAM_LAST,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients NAM_FIRST_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_NAM_FIRST_2
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
                t.ID_MEDICAID
                , t.NAM_FIRST EDW_NAM_FIRST
                , s.NAM_FIRST BIAR_NAM_FIRST
                , CASE WHEN (t.NAM_FIRST = s.NAM_FIRST or NVL(t.NAM_FIRST,'') = NVL(s.NAM_FIRST,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients NAM_MID_INIT_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_NAM_MID_INIT_2
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
                t.ID_MEDICAID
                , t.NAM_MID_INIT EDW_NAM_MID_INIT
                , s.NAM_MID_INIT BIAR_NAM_MID_INIT
                , CASE WHEN (t.NAM_MID_INIT = s.NAM_MID_INIT or NVL(t.NAM_MID_INIT,'') = NVL(s.NAM_MID_INIT,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients NUM_SSN_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_NUM_SSN_2
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
                t.ID_MEDICAID
                , t.NUM_SSN EDW_NUM_SSN
                , s.NUM_SSN BIAR_NUM_SSN
                , CASE WHEN (t.NUM_SSN = s.NUM_SSN or NVL(t.NUM_SSN,'') = NVL(s.NUM_SSN,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients ADR_STREET_1
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_ADR_STREET_1
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
                t.ID_MEDICAID
                , t.ADR_STREET_1 EDW_ADR_STREET_1
                , s.ADR_STREET_1 BIAR_ADR_STREET_1
                , CASE WHEN (t.ADR_STREET_1 = s.ADR_STREET_1 or NVL(t.ADR_STREET_1,'') = NVL(s.ADR_STREET_1,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients ADR_STREET_2
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_ADR_STREET_2
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
                t.ID_MEDICAID
                , t.ADR_STREET_2 EDW_ADR_STREET_2
                , s.ADR_STREET_2 BIAR_ADR_STREET_2
                , CASE WHEN (t.ADR_STREET_2 = s.ADR_STREET_2 or NVL(t.ADR_STREET_2,'') = NVL(s.ADR_STREET_2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients ADR_CITY
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_ADR_CITY
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
                t.ID_MEDICAID
                , t.ADR_CITY EDW_ADR_CITY
                , s.ADR_CITY BIAR_ADR_CITY
                , CASE WHEN (t.ADR_CITY = s.ADR_CITY or NVL(t.ADR_CITY,'') = NVL(s.ADR_CITY,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients ADR_STATE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_ADR_STATE
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
                t.ID_MEDICAID
                , t.ADR_STATE EDW_ADR_STATE
                , s.ADR_STATE BIAR_ADR_STATE
                , CASE WHEN (t.ADR_STATE = s.ADR_STATE or NVL(t.ADR_STATE,'') = NVL(s.ADR_STATE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients ADR_ZIP_CODE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_ADR_ZIP_CODE
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
                t.ID_MEDICAID
                , t.ADR_ZIP_CODE EDW_ADR_ZIP_CODE
                , s.ADR_ZIP_CODE BIAR_ADR_ZIP_CODE
                , CASE WHEN (t.ADR_ZIP_CODE = s.ADR_ZIP_CODE or NVL(t.ADR_ZIP_CODE,'') = NVL(s.ADR_ZIP_CODE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients ADR_ZIP_CODE_4
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_ADR_ZIP_CODE_4
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
                t.ID_MEDICAID
                , t.ADR_ZIP_CODE_4 EDW_ADR_ZIP_CODE_4
                , s.ADR_ZIP_CODE_4 BIAR_ADR_ZIP_CODE_4
                , CASE WHEN (t.ADR_ZIP_CODE_4 = s.ADR_ZIP_CODE_4 or NVL(t.ADR_ZIP_CODE_4,'') = NVL(s.ADR_ZIP_CODE_4,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients NUM_PHONE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_NUM_PHONE
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
                t.ID_MEDICAID
                , t.NUM_PHONE EDW_NUM_PHONE
                , s.NUM_PHONE BIAR_NUM_PHONE
                , CASE WHEN (t.NUM_PHONE = s.NUM_PHONE or NVL(t.NUM_PHONE,'') = NVL(s.NUM_PHONE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients NUM_ADD_PHONE
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_NUM_ADD_PHONE
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
                t.ID_MEDICAID
                , t.NUM_ADD_PHONE EDW_NUM_ADD_PHONE
                , s.NUM_ADD_PHONE BIAR_NUM_ADD_PHONE
                , CASE WHEN (t.NUM_ADD_PHONE = s.NUM_ADD_PHONE or NVL(t.NUM_ADD_PHONE,'') = NVL(s.NUM_ADD_PHONE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Recipients CDE_COUNTY
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_CDE_COUNTY
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
                t.ID_MEDICAID
                , t.CDE_COUNTY EDW_CDE_COUNTY
                , s.CDE_COUNTY BIAR_CDE_COUNTY
                , CASE WHEN (t.CDE_COUNTY = s.CDE_COUNTY or NVL(t.CDE_COUNTY,'') = NVL(s.CDE_COUNTY,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
 -- ========================
 -- Recipients NUM_LONGITUDE
 /*========================================
 	SQL Count Match/NonMatch
 ========================================*/
         select
         count(*) Recip_NUM_LONGITUDE
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
                 t.ID_MEDICAID
                 , t.NUM_LONGITUDE EDW_NUM_LONGITUDE
                 , s.NUM_LONGITUDE BIAR_NUM_LONGITUDE
                 , CASE WHEN (t.NUM_LONGITUDE = s.NUM_LONGITUDE) or
                             (t.NUM_LONGITUDE/1000000 - (s.NUM_LONGITUDE/1000000) between -1 and 1) or 
                             (t.NUM_LONGITUDE IS NOT NULL and s.NUM_LONGITUDE = 0.00) or
                             NVL(t.NUM_LONGITUDE,0.0) = NVL(s.NUM_LONGITUDE,0.0)
                        THEN 'T' 
                        ELSE 'FAIL' 
                        END IS_MATCH
 
                 
                 from {catalog}.{schema_name}.{EDW_TblNm} t
                 join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
 
            )t where true
        )t where true
         ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
 -- ========================
 -- Recipients NUM_LATITUDE
 /*========================================
 	SQL Count Match/NonMatch
 ========================================*/
         select
         count(*) Recip_NUM_LATITUDE
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
                 t.ID_MEDICAID
                 , t.NUM_LATITUDE EDW_NUM_LATITUDE
                 , s.NUM_LATITUDE BIAR_NUM_LATITUDE
                 , CASE WHEN (t.NUM_LATITUDE = s.NUM_LATITUDE) or 
                             ((t.NUM_LATITUDE/1000000) - (s.NUM_LATITUDE/1000000) between -1 and 1) or 
                             (t.NUM_LATITUDE IS NOT NULL and s.NUM_LATITUDE = 0.00) or
                             NVL(t.NUM_LATITUDE,0.0) = NVL(s.NUM_LATITUDE,0.0)
                        THEN 'T' 
                        ELSE 'FAIL' 
                        END IS_MATCH
 
                 
                 from {catalog}.{schema_name}.{EDW_TblNm} t
                 join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
 
            )t where true
        )t where true
         ;
                """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
/*========================================
	SQL Count Match/NonMatch
========================================*/
        select
        count(*) Recip_DERIVED2_2_no_match_expected
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
                t.ID_MEDICAID
                , t.DERIVED2 EDW_DERIVED2
                , s.DERIVED2 BIAR_DERIVED2
                , CASE WHEN (t.DERIVED2 = s.DERIVED2 or NVL(t.DERIVED2,'2022-08-08') = NVL(s.DERIVED2,'2022-08-08')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID = s.ID_MEDICAID
           )t where true
       )t where true
        ;
               """)
display(sql_out)

