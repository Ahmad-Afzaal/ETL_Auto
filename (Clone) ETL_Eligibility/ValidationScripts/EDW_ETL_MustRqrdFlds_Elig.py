# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_MustRqrdFlds_Elig.                                                                                       *
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
#************************************************************************************************************************************
#

# COMMAND ----------

# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'archive_vendor_extracts')
dbutils.widgets.text('EDW_TblNm', 'EDW_temp_Eligibility_Analytics')
dbutils.widgets.text('BIAR_TblNm', 'BIAR_Eligibility_Analytics')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
EDW_TblNm = dbutils.widgets.get('EDW_TblNm')
BIAR_TblNm = dbutils.widgets.get('BIAR_TblNm')

print("catalog:", catalog)
print("schema:", schema_name)
print("EDW Table Name:", EDW_TblNm)
print("BIAR Table Name:", BIAR_TblNm)

# COMMAND ----------

# DBTITLE 1,Eligibility Must Required Fields
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-D01 ID_MEDICAID1
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_D01_ID_MEDICAID1
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.ID_MEDICAID1 EDW_ID_MEDICAID1
                , s.ID_MEDICAID1 BIAR_ID_MEDICAID1
                , CASE WHEN (t.ID_MEDICAID1 = s.ID_MEDICAID1 or COALESCE(t.ID_MEDICAID1,'') = COALESCE(s.ID_MEDICAID1,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.D_DTE_END = s.D_DTE_END
                where t.D_DTE_EFFECTIVE is not null 
                  and t.D_DTE_END is not null
                  and s.D_DTE_EFFECTIVE is not null 
                  and s.D_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-D01 DERIVED1_no_match_expected
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_D01_DERIVED1_no_match_expected
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.DERIVED1 EDW_DERIVED1
                , s.DERIVED1 BIAR_DERIVED1
                , CASE WHEN (t.DERIVED1 = s.DERIVED1 or CAST(COALESCE(t.DERIVED1,'1999-01-01') AS DATE) = CAST(COALESCE(s.DERIVED1,'1999-01-01') AS DATE)) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.D_DTE_END = s.D_DTE_END
                where t.D_DTE_EFFECTIVE is not null 
                  and t.D_DTE_END is not null
                  and s.D_DTE_EFFECTIVE is not null 
                  and s.D_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-D01 Derived2
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_D01_Derived2
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.Derived2 EDW_Derived2
                , s.Derived2 BIAR_Derived2
                , CASE WHEN (t.Derived2 = s.Derived2 or COALESCE(t.Derived2,'') = COALESCE(s.Derived2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.D_DTE_END = s.D_DTE_END
                where t.D_DTE_EFFECTIVE is not null 
                  and t.D_DTE_END is not null
                  and s.D_DTE_EFFECTIVE is not null 
                  and s.D_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-A CDE_STATUS
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_A_CDE_STATUS
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.CDE_STATUS EDW_CDE_STATUS
                , s.CDE_STATUS BIAR_CDE_STATUS
                , CASE WHEN (t.CDE_STATUS = s.CDE_STATUS 
                             or COALESCE(t.CDE_STATUS,'') = TRIM(s.CDE_STATUS) -- No need of this line on Next Run (Feb/2024)
                             or COALESCE(t.CDE_STATUS,'') = COALESCE(s.CDE_STATUS,'')
                            )
                       THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.A_DTE_END = s.A_DTE_END
                where t.A_DTE_EFFECTIVE is not null 
                  and t.A_DTE_END is not null
                  and s.A_DTE_EFFECTIVE is not null 
                  and s.A_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-D01 D_DTE_EFFECTIVE
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/

select
count(*) Elig_Part_D01_D_DTE_EFFECTIVE
, sum(CAST(is_match='T' AS INT)) matches
, CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
, sum(CAST(is_match='FAIL' AS INT)) unmatches
, CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg

from
(

    select *
    from
    (
         select distinct
         t.ID_MEDICAID1, t.NUM_CASE

         , t.D_DTE_EFFECTIVE EDW_D_DTE_EFFECTIVE
         , s.D_DTE_EFFECTIVE BIAR_D_DTE_EFFECTIVE
         , CASE WHEN (t.D_DTE_EFFECTIVE = s.D_DTE_EFFECTIVE or COALESCE(t.D_DTE_EFFECTIVE,'') = COALESCE(s.D_DTE_EFFECTIVE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH

         from {catalog}.{schema_name}.{EDW_TblNm} t

         join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.D_DTE_END = s.D_DTE_END
         
         where t.D_DTE_EFFECTIVE is not null 
           and t.D_DTE_END is not null
           and s.D_DTE_EFFECTIVE is not null 
           and s.D_DTE_END is not null
                            
    )t where true

)t where true
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-D01 D_DTE_END
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/

select
count(*) Elig_Part_D01_D_DTE_END
, sum(CAST(is_match='T' AS INT)) matches
, CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
, sum(CAST(is_match='FAIL' AS INT)) unmatches
, CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg

from
(

    select *
    from
    (
         select distinct
         t.ID_MEDICAID1, t.NUM_CASE

         , t.D_DTE_END EDW_D_DTE_END
         , s.D_DTE_END BIAR_D_DTE_END
         , CASE WHEN (t.D_DTE_END = s.D_DTE_END or COALESCE(t.D_DTE_END,'') = COALESCE(s.D_DTE_END,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH

         from {catalog}.{schema_name}.{EDW_TblNm} t

         join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.D_DTE_EFFECTIVE = s.D_DTE_EFFECTIVE
         
         where t.D_DTE_EFFECTIVE is not null 
           and t.D_DTE_END is not null
           and s.D_DTE_EFFECTIVE is not null 
           and s.D_DTE_END is not null
                            
    )t where true

)t where true
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-D01 D_CDE_AID_CATEGORY
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/

        select
        count(*) Elig_Part_D01_D_CDE_AID_CATEGORY
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg

        from
        (

           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE

                , t.D_CDE_AID_CATEGORY EDW_D_CDE_AID_CATEGORY
                , s.D_CDE_AID_CATEGORY BIAR_D_CDE_AID_CATEGORY
                , CASE WHEN (t.D_CDE_AID_CATEGORY = TRIM(s.D_CDE_AID_CATEGORY) or COALESCE(t.D_CDE_AID_CATEGORY,'') = COALESCE(s.D_CDE_AID_CATEGORY,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH

                from {catalog}.{schema_name}.{EDW_TblNm} t

                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.D_DTE_EFFECTIVE = s.D_DTE_EFFECTIVE
                
                where t.D_DTE_EFFECTIVE is not null 
                  and t.D_DTE_END is not null
                  and s.D_DTE_EFFECTIVE is not null 
                  and s.D_DTE_END is not null
                		   
           )t where true

       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-D01 D_CDE_PGM_HEALTH
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/

        select
        count(*) Elig_Part_D01_D_CDE_PGM_HEALTH
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg

        from
        (

           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE

                , t.D_CDE_PGM_HEALTH EDW_D_CDE_PGM_HEALTH
                , s.D_CDE_PGM_HEALTH BIAR_D_CDE_PGM_HEALTH
                , CASE WHEN (t.D_CDE_PGM_HEALTH = TRIM(s.D_CDE_PGM_HEALTH) or COALESCE(t.D_CDE_PGM_HEALTH,'') = COALESCE(s.D_CDE_PGM_HEALTH,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH

                from {catalog}.{schema_name}.{EDW_TblNm} t

                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.D_DTE_EFFECTIVE = s.D_DTE_EFFECTIVE
                
                where t.D_DTE_EFFECTIVE is not null 
                  and t.D_DTE_END is not null
                  and s.D_DTE_EFFECTIVE is not null 
                  and s.D_DTE_END is not null
                		   
           )t where true

       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-E ID_MEDICAID1
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_E_ID_MEDICAID1
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.ID_MEDICAID1 EDW_ID_MEDICAID1
                , s.ID_MEDICAID1 BIAR_ID_MEDICAID1
                , CASE WHEN (t.ID_MEDICAID1 = s.ID_MEDICAID1 or COALESCE(t.ID_MEDICAID1,'') = COALESCE(s.ID_MEDICAID1,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.E_DTE_END = s.E_DTE_END
                where t.E_DTE_EFFECTIVE is not null 
                  and t.E_DTE_END is not null
                  and s.E_DTE_EFFECTIVE is not null 
                  and s.E_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-E DERIVED1_no_match_expected
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_E_DERIVED1_no_match_expected
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.DERIVED1 EDW_DERIVED1
                , s.DERIVED1 BIAR_DERIVED1
                , CASE WHEN t.DERIVED1 = s.DERIVED1 or CAST(COALESCE(t.DERIVED1,'1999-01-01') AS DATE) = CAST(COALESCE(s.DERIVED1,'1999-01-01') AS DATE) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.E_DTE_END = s.E_DTE_END
                where t.E_DTE_EFFECTIVE is not null 
                  and t.E_DTE_END is not null
                  and s.E_DTE_EFFECTIVE is not null 
                  and s.E_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-E Derived2
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_E_Derived2
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.Derived2 EDW_Derived2
                , s.Derived2 BIAR_Derived2
                , CASE WHEN (t.Derived2 = s.Derived2 or COALESCE(t.Derived2,'') = COALESCE(s.Derived2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.E_DTE_END = s.E_DTE_END
                where t.E_DTE_EFFECTIVE is not null 
                  and t.E_DTE_END is not null
                  and s.E_DTE_EFFECTIVE is not null 
                  and s.E_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-E E_DTE_EFFECTIVE
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/

        select
        count(*) Elig_Part_E_E_DTE_EFFECTIVE
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg

        from
        (

           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE

                , t.E_DTE_EFFECTIVE EDW_E_DTE_EFFECTIVE
                , s.E_DTE_EFFECTIVE BIAR_E_DTE_EFFECTIVE
                , CASE WHEN (t.E_DTE_EFFECTIVE = s.E_DTE_EFFECTIVE or COALESCE(t.E_DTE_EFFECTIVE,'') = COALESCE(s.E_DTE_EFFECTIVE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.E_DTE_END = s.E_DTE_END                
                where t.E_DTE_EFFECTIVE is not null 
                  and t.E_DTE_END is not null
                  and s.E_DTE_EFFECTIVE is not null 
                  and s.E_DTE_END is not null
                
                		   
           )t where true

       )t where true


        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-E E_DTE_END
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/

        select
        count(*) Elig_Part_E_E_DTE_END
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg

        from
        (

           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE

                , t.E_DTE_END EDW_E_DTE_END
                , s.E_DTE_END BIAR_E_DTE_END
                , CASE WHEN (t.E_DTE_END = s.E_DTE_END or COALESCE(t.E_DTE_END,'') = COALESCE(s.E_DTE_END,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH

                from {catalog}.{schema_name}.{EDW_TblNm} t

                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.E_DTE_EFFECTIVE = s.E_DTE_EFFECTIVE
                
                where t.E_DTE_EFFECTIVE is not null 
                  and t.E_DTE_END is not null
                  and s.E_DTE_EFFECTIVE is not null 
                  and s.E_DTE_END is not null
                
                		   
           )t where true

       )t where true


        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-E E_ID_PROVIDER_MCAID
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/

        select
        count(*) Elig_Part_E_E_ID_PROVIDER_MCAID
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg

        from
        (

           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE

                , t.E_ID_PROVIDER_MCAID EDW_E_ID_PROVIDER_MCAID
                , s.E_ID_PROVIDER_MCAID BIAR_E_ID_PROVIDER_MCAID
                , CASE WHEN (t.E_ID_PROVIDER_MCAID = TRIM(s.E_ID_PROVIDER_MCAID) or COALESCE(t.E_ID_PROVIDER_MCAID,'') = COALESCE(s.E_ID_PROVIDER_MCAID,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH

                from {catalog}.{schema_name}.{EDW_TblNm} t

                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.E_DTE_EFFECTIVE = s.E_DTE_EFFECTIVE
                
                where t.E_DTE_EFFECTIVE is not null 
                  and t.E_DTE_END is not null
                  and s.E_DTE_EFFECTIVE is not null 
                  and s.E_DTE_END is not null
                		   
           )t where true

       )t where true


        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-I ID_MEDICAID1
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_I_ID_MEDICAID1
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.ID_MEDICAID1 EDW_ID_MEDICAID1
                , s.ID_MEDICAID1 BIAR_ID_MEDICAID1
                , CASE WHEN (t.ID_MEDICAID1 = s.ID_MEDICAID1 or COALESCE(t.ID_MEDICAID1,'') = COALESCE(s.ID_MEDICAID1,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.I_DTE_END = s.I_DTE_END
                where t.I_DTE_EFFECTIVE is not null 
                  and t.I_DTE_END is not null
                  and s.I_DTE_EFFECTIVE is not null 
                  and s.I_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-I DERIVED1_no_match_expected
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_I_DERIVED1_no_match_expected
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.DERIVED1 EDW_DERIVED1
                , s.DERIVED1 BIAR_DERIVED1
                , CASE WHEN t.DERIVED1 = s.DERIVED1 or CAST(COALESCE(t.DERIVED1,'1999-01-01') AS DATE) = CAST(COALESCE(s.DERIVED1,'1999-01-01') AS DATE) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.I_DTE_END = s.I_DTE_END
                where t.I_DTE_EFFECTIVE is not null 
                  and t.I_DTE_END is not null
                  and s.I_DTE_EFFECTIVE is not null 
                  and s.I_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-I Derived2
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_I_Derived2
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.Derived2 EDW_Derived2
                , s.Derived2 BIAR_Derived2
                , CASE WHEN (t.Derived2 = s.Derived2 or COALESCE(t.Derived2,'') = COALESCE(s.Derived2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.I_DTE_END = s.I_DTE_END
                where t.I_DTE_EFFECTIVE is not null 
                  and t.I_DTE_END is not null
                  and s.I_DTE_EFFECTIVE is not null 
                  and s.I_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-I I_DTE_EFFECTIVE
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_I_I_DTE_EFFECTIVE
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.I_DTE_EFFECTIVE EDW_I_DTE_EFFECTIVE
                , s.I_DTE_EFFECTIVE BIAR_I_DTE_EFFECTIVE
                , CASE WHEN (t.I_DTE_EFFECTIVE = s.I_DTE_EFFECTIVE or COALESCE(t.I_DTE_EFFECTIVE,'') = COALESCE(s.I_DTE_EFFECTIVE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.I_DTE_END = s.I_DTE_END
                where t.I_DTE_EFFECTIVE is not null 
                  and t.I_DTE_END is not null
                  and s.I_DTE_EFFECTIVE is not null 
                  and s.I_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-I I_DTE_END
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_I_I_DTE_END
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.I_DTE_END EDW_I_DTE_END
                , s.I_DTE_END BIAR_I_DTE_END
                , CASE WHEN (t.I_DTE_END = s.I_DTE_END or COALESCE(t.I_DTE_END,'') = COALESCE(s.I_DTE_END,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.I_DTE_EFFECTIVE = s.I_DTE_EFFECTIVE
                where t.I_DTE_EFFECTIVE is not null 
                  and t.I_DTE_END is not null
                  and s.I_DTE_EFFECTIVE is not null 
                  and s.I_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-K ID_MEDICAID1
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_K_ID_MEDICAID1
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.ID_MEDICAID1 EDW_ID_MEDICAID1
                , s.ID_MEDICAID1 BIAR_ID_MEDICAID1
                , CASE WHEN (t.ID_MEDICAID1 = s.ID_MEDICAID1 or COALESCE(t.ID_MEDICAID1,'') = COALESCE(s.ID_MEDICAID1,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.K_DTE_END = s.K_DTE_END
                where t.K_DTE_EFFECTIVE is not null 
                  and t.K_DTE_END is not null
                  and s.K_DTE_EFFECTIVE is not null 
                  and s.K_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-K DERIVED1_no_match_expected
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_K_DERIVED1_no_match_expected
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.DERIVED1 EDW_DERIVED1
                , s.DERIVED1 BIAR_DERIVED1
                , CASE WHEN t.DERIVED1 = s.DERIVED1 or CAST(COALESCE(t.DERIVED1,'1999-01-01') AS DATE) = CAST(COALESCE(s.DERIVED1,'1999-01-01') AS DATE) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.K_DTE_END = s.K_DTE_END
                where t.K_DTE_EFFECTIVE is not null 
                  and t.K_DTE_END is not null
                  and s.K_DTE_EFFECTIVE is not null 
                  and s.K_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-K Derived2
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_K_Derived2
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.Derived2 EDW_Derived2
                , s.Derived2 BIAR_Derived2
                , CASE WHEN (t.Derived2 = s.Derived2 or COALESCE(t.Derived2,'') = COALESCE(s.Derived2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.K_DTE_END = s.K_DTE_END
                where t.K_DTE_EFFECTIVE is not null 
                  and t.K_DTE_END is not null
                  and s.K_DTE_EFFECTIVE is not null 
                  and s.K_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-K K_DTE_EFFECTIVE
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_K_K_DTE_EFFECTIVE
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.K_DTE_EFFECTIVE EDW_K_DTE_EFFECTIVE
                , s.K_DTE_EFFECTIVE BIAR_K_DTE_EFFECTIVE
                , CASE WHEN (t.K_DTE_EFFECTIVE = s.K_DTE_EFFECTIVE or COALESCE(t.K_DTE_EFFECTIVE,'') = COALESCE(s.K_DTE_EFFECTIVE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.K_DTE_END = s.K_DTE_END
                where t.K_DTE_EFFECTIVE is not null 
                  and t.K_DTE_END is not null
                  and s.K_DTE_EFFECTIVE is not null 
                  and s.K_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-K K_DTE_END
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_K_K_DTE_END
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.K_DTE_END EDW_K_DTE_END
                , s.K_DTE_END BIAR_K_DTE_END
                , CASE WHEN (t.K_DTE_END = s.K_DTE_END or COALESCE(t.K_DTE_END,'') = COALESCE(s.K_DTE_END,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.K_DTE_EFFECTIVE = s.K_DTE_EFFECTIVE
                where t.K_DTE_EFFECTIVE is not null 
                  and t.K_DTE_END is not null
                  and s.K_DTE_EFFECTIVE is not null 
                  and s.K_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-L ID_MEDICAID1
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_L_ID_MEDICAID1
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.ID_MEDICAID1 EDW_ID_MEDICAID1
                , s.ID_MEDICAID1 BIAR_ID_MEDICAID1
                , CASE WHEN (t.ID_MEDICAID1 = s.ID_MEDICAID1 or COALESCE(t.ID_MEDICAID1,'') = COALESCE(s.ID_MEDICAID1,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.L_DTE_END = s.L_DTE_END
                where t.L_DTE_EFFECTIVE is not null 
                  and t.L_DTE_END is not null
                  and s.L_DTE_EFFECTIVE is not null 
                  and s.L_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-L DERIVED1_no_match_expected
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_L_DERIVED1_no_match_expected
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.DERIVED1 EDW_DERIVED1
                , s.DERIVED1 BIAR_DERIVED1
                , CASE WHEN t.DERIVED1 = s.DERIVED1 or CAST(COALESCE(t.DERIVED1,'1999-01-01') AS DATE) = CAST(COALESCE(s.DERIVED1,'1999-01-01') AS DATE) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.L_DTE_END = s.L_DTE_END
                where t.L_DTE_EFFECTIVE is not null 
                  and t.L_DTE_END is not null
                  and s.L_DTE_EFFECTIVE is not null 
                  and s.L_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-L Derived2
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_L_Derived2
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.Derived2 EDW_Derived2
                , s.Derived2 BIAR_Derived2
                , CASE WHEN (t.Derived2 = s.Derived2 or COALESCE(t.Derived2,'') = COALESCE(s.Derived2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.L_DTE_END = s.L_DTE_END
                where t.L_DTE_EFFECTIVE is not null 
                  and t.L_DTE_END is not null
                  and s.L_DTE_EFFECTIVE is not null 
                  and s.L_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-L L_DTE_EFFECTIVE
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_L_L_DTE_EFFECTIVE
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.L_DTE_EFFECTIVE EDW_L_DTE_EFFECTIVE
                , s.L_DTE_EFFECTIVE BIAR_L_DTE_EFFECTIVE
                , CASE WHEN (t.L_DTE_EFFECTIVE = s.L_DTE_EFFECTIVE or COALESCE(t.L_DTE_EFFECTIVE,'') = COALESCE(s.L_DTE_EFFECTIVE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.L_DTE_END = s.L_DTE_END
                where t.L_DTE_EFFECTIVE is not null 
                  and t.L_DTE_END is not null
                  and s.L_DTE_EFFECTIVE is not null 
                  and s.L_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-L L_DTE_END
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_L_L_DTE_END
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.L_DTE_END EDW_L_DTE_END
                , s.L_DTE_END BIAR_L_DTE_END
                , CASE WHEN (t.L_DTE_END = s.L_DTE_END or COALESCE(t.L_DTE_END,'') = COALESCE(s.L_DTE_END,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.L_DTE_EFFECTIVE = s.L_DTE_EFFECTIVE
                where t.L_DTE_EFFECTIVE is not null 
                  and t.L_DTE_END is not null
                  and s.L_DTE_EFFECTIVE is not null 
                  and s.L_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-N ID_MEDICAID1
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_N_ID_MEDICAID1
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.ID_MEDICAID1 EDW_ID_MEDICAID1
                , s.ID_MEDICAID1 BIAR_ID_MEDICAID1
                , CASE WHEN (t.ID_MEDICAID1 = s.ID_MEDICAID1 or COALESCE(t.ID_MEDICAID1,'') = COALESCE(s.ID_MEDICAID1,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.N_DTE_TPL_END = s.N_DTE_TPL_END
                where t.N_DTE_TPL_EFFECTIVE is not null 
                  and t.N_DTE_TPL_END is not null
                  and s.N_DTE_TPL_EFFECTIVE is not null 
                  and s.N_DTE_TPL_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-N DERIVED1_no_match_expected
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_N_DERIVED1_no_match_expected
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.DERIVED1 EDW_DERIVED1
                , s.DERIVED1 BIAR_DERIVED1
                , CASE WHEN t.DERIVED1 = s.DERIVED1 or CAST(COALESCE(t.DERIVED1,'1999-01-01') AS DATE) = CAST(COALESCE(s.DERIVED1,'1999-01-01') AS DATE) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.N_DTE_TPL_END = s.N_DTE_TPL_END
                where t.N_DTE_TPL_EFFECTIVE is not null 
                  and t.N_DTE_TPL_END is not null
                  and s.N_DTE_TPL_EFFECTIVE is not null 
                  and s.N_DTE_TPL_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-N Derived2
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_N_Derived2
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.Derived2 EDW_Derived2
                , s.Derived2 BIAR_Derived2
                , CASE WHEN (t.Derived2 = s.Derived2 or COALESCE(t.Derived2,'') = COALESCE(s.Derived2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.N_DTE_TPL_END = s.N_DTE_TPL_END
                where t.N_DTE_TPL_EFFECTIVE is not null 
                  and t.N_DTE_TPL_END is not null
                  and s.N_DTE_TPL_EFFECTIVE is not null 
                  and s.N_DTE_TPL_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-N N_DTE_TPL_EFFECTIVE
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/

        select
        count(*) Elig_Part_N_N_DTE_TPL_EFFECTIVE
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg

        from
        (

           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE

                , t.N_DTE_TPL_EFFECTIVE EDW_N_DTE_TPL_EFFECTIVE
                , s.N_DTE_TPL_EFFECTIVE BIAR_N_DTE_TPL_EFFECTIVE
                , CASE WHEN (t.N_DTE_TPL_EFFECTIVE = s.N_DTE_TPL_EFFECTIVE or COALESCE(t.N_DTE_TPL_EFFECTIVE,'') = COALESCE(s.N_DTE_TPL_EFFECTIVE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH

                from {catalog}.{schema_name}.{EDW_TblNm} t

                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.N_DTE_TPL_END = s.N_DTE_TPL_END
                
                where t.N_DTE_TPL_EFFECTIVE is not null 
                  and t.N_DTE_TPL_END is not null
                  and s.N_DTE_TPL_EFFECTIVE is not null 
                  and s.N_DTE_TPL_END is not null
                		   
           )t where true

       )t where true


        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-N N_DTE_TPL_END
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/

        select
        count(*) Elig_Part_N_N_DTE_TPL_END
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg

        from
        (

           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE

                , t.N_DTE_TPL_END EDW_N_DTE_TPL_END
                , s.N_DTE_TPL_END BIAR_N_DTE_TPL_END
                , CASE WHEN (t.N_DTE_TPL_END = s.N_DTE_TPL_END or COALESCE(t.N_DTE_TPL_END,'') = COALESCE(s.N_DTE_TPL_END,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH

                from {catalog}.{schema_name}.{EDW_TblNm} t

                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.N_DTE_TPL_EFFECTIVE = s.N_DTE_TPL_EFFECTIVE
                
                where t.N_DTE_TPL_EFFECTIVE is not null 
                  and t.N_DTE_TPL_END is not null
                  and s.N_DTE_TPL_EFFECTIVE is not null 
                  and s.N_DTE_TPL_END is not null
                		   
           )t where true

       )t where true


        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- =================================
-- Eligibility Part-N N_CDE_COVERAGE_no_match_expected
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/

        select
        count(*) Elig_Part_N_N_CDE_COVERAGE_no_match_expected
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg

        from
        (

           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE

                , t.N_CDE_COVERAGE EDW_N_CDE_COVERAGE
                , s.N_CDE_COVERAGE BIAR_N_CDE_COVERAGE
                , CASE WHEN (t.N_CDE_COVERAGE = TRIM(s.N_CDE_COVERAGE) or COALESCE(t.N_CDE_COVERAGE,'') = COALESCE(s.N_CDE_COVERAGE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH

                from {catalog}.{schema_name}.{EDW_TblNm} t

                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.N_DTE_TPL_EFFECTIVE = s.N_DTE_TPL_EFFECTIVE
                
                where t.N_DTE_TPL_EFFECTIVE is not null 
                  and t.N_DTE_TPL_END is not null
                  and s.N_DTE_TPL_EFFECTIVE is not null 
                  and s.N_DTE_TPL_END is not null
                		   
           )t where true

       )t where true


        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-O ID_MEDICAID1
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_O_ID_MEDICAID1
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.ID_MEDICAID1 EDW_ID_MEDICAID1
                , s.ID_MEDICAID1 BIAR_ID_MEDICAID1
                , CASE WHEN (t.ID_MEDICAID1 = s.ID_MEDICAID1 or COALESCE(t.ID_MEDICAID1,'') = COALESCE(s.ID_MEDICAID1,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.O_DTE_END = s.O_DTE_END
                where t.O_DTE_EFFECTIVE is not null 
                  and t.O_DTE_END is not null
                  and s.O_DTE_EFFECTIVE is not null 
                  and s.O_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-O DERIVED1_no_match_expected
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_O_DERIVED1_no_match_expected
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.DERIVED1 EDW_DERIVED1
                , s.DERIVED1 BIAR_DERIVED1
                , CASE WHEN t.DERIVED1 = s.DERIVED1 or CAST(COALESCE(t.DERIVED1,'1999-01-01') AS DATE) = CAST(COALESCE(s.DERIVED1,'1999-01-01') AS DATE) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.O_DTE_END = s.O_DTE_END
                where t.O_DTE_EFFECTIVE is not null 
                  and t.O_DTE_END is not null
                  and s.O_DTE_EFFECTIVE is not null 
                  and s.O_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-O Derived2
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_O_Derived2
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.Derived2 EDW_Derived2
                , s.Derived2 BIAR_Derived2
                , CASE WHEN (t.Derived2 = s.Derived2 or COALESCE(t.Derived2,'') = COALESCE(s.Derived2,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.O_DTE_END = s.O_DTE_END
                where t.O_DTE_EFFECTIVE is not null 
                  and t.O_DTE_END is not null
                  and s.O_DTE_EFFECTIVE is not null 
                  and s.O_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-O O_DTE_EFFECTIVE
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_O_O_DTE_EFFECTIVE
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.O_DTE_EFFECTIVE EDW_O_DTE_EFFECTIVE
                , s.O_DTE_EFFECTIVE BIAR_O_DTE_EFFECTIVE
                , CASE WHEN (t.O_DTE_EFFECTIVE = s.O_DTE_EFFECTIVE or COALESCE(t.O_DTE_EFFECTIVE,'') = COALESCE(s.O_DTE_EFFECTIVE,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.O_DTE_END = s.O_DTE_END
                where t.O_DTE_EFFECTIVE is not null 
                  and t.O_DTE_END is not null
                  and s.O_DTE_EFFECTIVE is not null 
                  and s.O_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-O O_DTE_END
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_O_O_DTE_END
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.O_DTE_END EDW_O_DTE_END
                , s.O_DTE_END BIAR_O_DTE_END
                , CASE WHEN (t.O_DTE_END = s.O_DTE_END or COALESCE(t.O_DTE_END,'') = COALESCE(s.O_DTE_END,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.O_DTE_EFFECTIVE = s.O_DTE_EFFECTIVE
                where t.O_DTE_EFFECTIVE is not null 
                  and t.O_DTE_END is not null
                  and s.O_DTE_EFFECTIVE is not null 
                  and s.O_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-O O_IND_SNP
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_O_O_IND_SNP
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.O_IND_SNP EDW_O_IND_SNP
                , s.O_IND_SNP BIAR_O_IND_SNP
                , CASE WHEN (t.O_IND_SNP = s.O_IND_SNP or COALESCE(t.O_IND_SNP,'') = COALESCE(s.O_IND_SNP,'')) THEN 'T' ELSE 'FAIL' END IS_MATCH
                from {catalog}.{schema_name}.{EDW_TblNm} t
                join {catalog}.{schema_name}.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.O_DTE_END = s.O_DTE_END
                where t.O_DTE_EFFECTIVE is not null 
                  and t.O_DTE_END is not null
                  and s.O_DTE_EFFECTIVE is not null 
                  and s.O_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)
sql_out = spark.sql(f"""
-- ===============================
-- Eligibility Part-O O_TXT_PLAN_NAME
/*==========================================
   Eligibility EXTRACTS COMPARISON - Counts
============================================*/
        select
        count(*) Elig_Part_O_O_TXT_PLAN_NAME
        , sum(CAST(is_match='T' AS INT)) matches
        , CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
        , sum(CAST(is_match='FAIL' AS INT)) unmatches
        , CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
        from
        (
           select *
           from
           (
                select distinct
                t.ID_MEDICAID1, t.NUM_CASE
                , t.O_TXT_PLAN_NAME EDW_O_TXT_PLAN_NAME
                , s.O_TXT_PLAN_NAME BIAR_O_TXT_PLAN_NAME
                , CASE WHEN (CAST(t.O_TXT_PLAN_NAME AS STRING) = CAST(s.O_TXT_PLAN_NAME AS STRING) or
                             REGEXP_EXTRACT(t.O_TXT_PLAN_NAME,'(\\w+)',1) = REGEXP_EXTRACT(s.O_TXT_PLAN_NAME,'(\\w+)',1) or
                             COALESCE(t.O_TXT_PLAN_NAME,'') = COALESCE(s.O_TXT_PLAN_NAME,'')) 
                       THEN 'T' ELSE 'FAIL' END IS_MATCH
                from oh_apm_stg.archive_vendor_extracts.{EDW_TblNm} t
                join oh_apm_stg.archive_vendor_extracts.{BIAR_TblNm} s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.O_DTE_END = s.O_DTE_END
                where t.O_DTE_EFFECTIVE is not null 
                  and t.O_DTE_END is not null
                  and s.O_DTE_EFFECTIVE is not null 
                  and s.O_DTE_END is not null
           )t where true
       )t where true
        ;
               """)
display(sql_out)


