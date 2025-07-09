# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_MustRqrdFlds_Prov.                                                                                       *
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
#* 04/09/2024 CCRB70930/CO#43342  Jaime Zavala        Segregate scripts.                                                            *
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
dbutils.widgets.text('EDW_TblNm', 'EDW_temp_Provider_Analytics')
dbutils.widgets.text('BIAR_TblNm', 'BIAR_Provider_Analytics')
dbutils.widgets.text('EDW_TblNm117', 'EDW_temp_ven117fa')
dbutils.widgets.text('BIAR_TblNm117', 'BIAR_ven117fa')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
EDW_TblNm = dbutils.widgets.get('EDW_TblNm')
BIAR_TblNm = dbutils.widgets.get('BIAR_TblNm')
EDW_TblNm117 = dbutils.widgets.get('EDW_TblNm117')
BIAR_TblNm117 = dbutils.widgets.get('BIAR_TblNm117')

print("catalog:", catalog)
print("schema:", schema_name)
print("EDW Table Name:", EDW_TblNm)
print("BIAR Table Name:", BIAR_TblNm)
print("EDW 117 Table Name:", EDW_TblNm117)
print("BIAR 117 Table Name:", BIAR_TblNm117)

# COMMAND ----------

# DBTITLE 1,Providers Required Fields
sql_out = spark.sql(f"""
-- ========================
-- Providers ID_PROVIDER_MCAID1
/*==========================================
	PROVIDER EXTRACTS COMPARISON - Counts
==========================================*/
SELECT
	COUNT(*) Prov_ID_PROVIDER_MCAID1
	,SUM(CAST(is_match ='T' AS INT)) matches_cnt
	,CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
	,SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
	,CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
	
FROM(
	select distinct
	t.ID_PROVIDER_MCAID1
	
	,t.ID_PROVIDER_MCAID1 EDW_ID_PROVIDER_MCAID1
	,s.ID_PROVIDER_MCAID1 BIAR_ID_PROVIDER_MCAID1
	,CASE WHEN(t.ID_PROVIDER_MCAID1 = s.ID_PROVIDER_MCAID1 
	OR COALESCE(t.ID_PROVIDER_MCAID1, '') = COALESCE(s.ID_PROVIDER_MCAID1, '')) THEN 'T' ELSE 'FAIL' END IS_MATCH
		
	FROM {catalog}.{schema_name}.{EDW_TblNm117} t
	JOIN {catalog}.{schema_name}.{BIAR_TblNm117} s 
	ON t.ID_PROVIDER_MCAID1 = s.ID_PROVIDER_MCAID1
	
)t WHERE true;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Providers NAME/no match expected.
/*==========================================
	PROVIDER EXTRACTS COMPARISON - Counts
==========================================*/
SELECT
	COUNT(*) Prov_NAME_no_match_expected
	,SUM(CAST(is_match ='T' AS INT)) matches_cnt
	,CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
	,SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
	,CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
	
FROM(
	select distinct
	t.ID_PROVIDER_MCAID1
	
	,t.NAME EDW_NAME
	,s.NAME BIAR_NAME
	,CASE WHEN(t.NAME = s.NAME 
	OR COALESCE(t.NAME, '') = COALESCE(s.NAME, '')) THEN 'T' ELSE 'FAIL' END IS_MATCH
	FROM {catalog}.{schema_name}.{EDW_TblNm117} t
	JOIN {catalog}.{schema_name}.{BIAR_TblNm117} s 
	--ON t.ID_PROVIDER_MCAID1 = s.ID_PROVIDER_MCAID1
	ON t.sak_prov = s.sak_prov
	
)t WHERE true;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Providers ID_PROVIDER_NPI/no match expected
/*==========================================
	PROVIDER EXTRACTS COMPARISON - Counts
==========================================*/
SELECT
	COUNT(*) Prov_ID_PROVIDER_NPI_no_match_expected
	,SUM(CAST(is_match ='T' AS INT)) matches_cnt
	,CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
	,SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
	,CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
	
FROM(
	select distinct
	t.ID_PROVIDER_MCAID1
	
	,t.ID_PROVIDER_NPI AS EDW_ID_PROVIDER_NPI
	,s.ID_PROVIDER_NPI AS BIAR_ID_PROVIDER_NPI
	,CASE WHEN(t.ID_PROVIDER_NPI = s.ID_PROVIDER_NPI 
	--OR (t.ID_PROVIDER_NPI IS NOT NULL and s.ID_PROVIDER_NPI IS NULL)
	OR COALESCE(t.ID_PROVIDER_NPI, '') = COALESCE(s.ID_PROVIDER_NPI, '')) THEN 'T' ELSE 'FAIL' END IS_MATCH
	FROM {catalog}.{schema_name}.{EDW_TblNm117} t
	JOIN {catalog}.{schema_name}.{BIAR_TblNm117} s 
	ON t.ID_PROVIDER_MCAID1 = s.ID_PROVIDER_MCAID1
	--ON t.sak_prov = s.sak_prov
	
)t WHERE true;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Providers CDE_PROV_TYPE
/*========================================
	PROVIDER EXTRACTS COMPARISON - Counts
========================================*/

SELECT
	COUNT(*) Prov_CDE_PROV_TYPE
	,SUM(CAST(is_match ='T' AS INT)) matches_cnt
	,CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
	,SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
	,CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
	
FROM(
	select distinct
	t.ID_PROVIDER_MCAID1
	
	,t.CDE_PROV_TYPE EDW_CDE_PROV_TYPE
	,s.CDE_PROV_TYPE BIAR_CDE_PROV_TYPE
	,CASE WHEN(t.CDE_PROV_TYPE = s.CDE_PROV_TYPE 
	OR COALESCE(t.CDE_PROV_TYPE, '') = COALESCE(s.CDE_PROV_TYPE, '')) THEN 'T' ELSE 'FAIL' END IS_MATCH
		
							   
									
									
 
	FROM {catalog}.{schema_name}.{EDW_TblNm} t
	JOIN {catalog}.{schema_name}.{BIAR_TblNm} s 
	ON t.ID_PROVIDER_MCAID1 = s.ID_PROVIDER_MCAID1
	
)t WHERE true;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Providers ADR_MAIL_STRT1
/*========================================
	PROVIDER EXTRACTS COMPARISON - Counts
========================================*/

SELECT
	COUNT(*) Prov_ADR_MAIL_STRT1
	,SUM(CAST(is_match ='T' AS INT)) matches_cnt
	,CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
	,SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
	,CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
	
FROM(
	select distinct
	t.ID_PROVIDER_MCAID1
	
	,t.ADR_MAIL_STRT1 EDW_ADR_MAIL_STRT1
	,s.ADR_MAIL_STRT1 BIAR_ADR_MAIL_STRT1
	,CASE WHEN(TRIM(UPPER(t.ADR_MAIL_STRT1)) = TRIM(UPPER(s.ADR_MAIL_STRT1))
	OR COALESCE(t.ADR_MAIL_STRT1, '') = COALESCE(s.ADR_MAIL_STRT1, '')) THEN 'T' ELSE 'FAIL' END IS_MATCH
	FROM {catalog}.{schema_name}.{EDW_TblNm117} t
	JOIN {catalog}.{schema_name}.{BIAR_TblNm117} s 
	--ON t.ID_PROVIDER_MCAID1 = s.ID_PROVIDER_MCAID1
	ON t.sak_prov = s.sak_prov
	
)t WHERE true;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Providers ADR_MAIL_STRT2_0
/*========================================
	PROVIDER EXTRACTS COMPARISON - Counts
========================================*/

SELECT
	COUNT(*) Prov_ADR_MAIL_STRT2_0
	,SUM(CAST(is_match ='T' AS INT)) matches_cnt
	,CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
	,SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
	,CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
	
FROM(
	select distinct
	t.ID_PROVIDER_MCAID1
	
	,t.ADR_MAIL_STRT2_0 EDW_ADR_MAIL_STRT2_0
	,s.ADR_MAIL_STRT2_0 BIAR_ADR_MAIL_STRT2_0
	,CASE WHEN(TRIM(UPPER(t.ADR_MAIL_STRT2_0)) = TRIM(UPPER(s.ADR_MAIL_STRT2_0)) 
	OR COALESCE(t.ADR_MAIL_STRT2_0, '') = COALESCE(s.ADR_MAIL_STRT2_0, '')) THEN 'T' ELSE 'FAIL' END IS_MATCH
		
	FROM {catalog}.{schema_name}.{EDW_TblNm117} t
	JOIN {catalog}.{schema_name}.{BIAR_TblNm117} s 
	--ON t.ID_PROVIDER_MCAID1 = s.ID_PROVIDER_MCAID1
	ON t.sak_prov = s.sak_prov
	
)t WHERE true;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Providers ADR_MAIL_CITY1
/*========================================
	PROVIDER EXTRACTS COMPARISON - Counts
========================================*/

SELECT
	COUNT(*) Prov_ADR_MAIL_CITY1
	,SUM(CAST(is_match ='T' AS INT)) matches_cnt
	,CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
	,SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
	,CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
	
FROM(
	select distinct
	t.ID_PROVIDER_MCAID1
	
	,t.ADR_MAIL_CITY1 EDW_ADR_MAIL_CITY1
	,s.ADR_MAIL_CITY1 BIAR_ADR_MAIL_CITY1
	,CASE WHEN(TRIM(UPPER(t.ADR_MAIL_CITY1)) = TRIM(UPPER(s.ADR_MAIL_CITY1))
	OR COALESCE(t.ADR_MAIL_CITY1, '') = COALESCE(s.ADR_MAIL_CITY1, '')) THEN 'T' ELSE 'FAIL' END IS_MATCH
		
	FROM {catalog}.{schema_name}.{EDW_TblNm117} t
	JOIN {catalog}.{schema_name}.{BIAR_TblNm117} s 
	--ON t.ID_PROVIDER_MCAID1 = s.ID_PROVIDER_MCAID1
	ON t.sak_prov = s.sak_prov
	
)t WHERE true;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Providers ADR_MAIL_STATE1
/*========================================
	PROVIDER EXTRACTS COMPARISON - Counts
========================================*/

SELECT
	COUNT(*) Prov_ADR_MAIL_STATE1
	,SUM(CAST(is_match ='T' AS INT)) matches_cnt
	,CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
	,SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
	,CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
	
FROM(
	select distinct
	t.ID_PROVIDER_MCAID1
	
	,t.ADR_MAIL_STATE1 EDW_ADR_MAIL_STATE1
	,s.ADR_MAIL_STATE1 BIAR_ADR_MAIL_STATE1
	,CASE WHEN(t.ADR_MAIL_STATE1 = s.ADR_MAIL_STATE1 
	OR COALESCE(t.ADR_MAIL_STATE1, '') = COALESCE(s.ADR_MAIL_STATE1, '')) THEN 'T' ELSE 'FAIL' END IS_MATCH
	FROM {catalog}.{schema_name}.{EDW_TblNm117} t
	JOIN {catalog}.{schema_name}.{BIAR_TblNm117} s 
	--ON t.ID_PROVIDER_MCAID1 = s.ID_PROVIDER_MCAID1
	ON t.sak_prov = s.sak_prov

	
)t WHERE true;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Providers ADR_MAIL_ZIP1
/*========================================
	PROVIDER EXTRACTS COMPARISON - Counts
========================================*/

SELECT
	COUNT(*) Prov_ADR_MAIL_ZIP1
	,SUM(CAST(is_match ='T' AS INT)) matches_cnt
	,CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
	,SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
	,CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
	
FROM(
	select distinct
	t.ID_PROVIDER_MCAID1
	
	,t.ADR_MAIL_ZIP1 EDW_ADR_MAIL_ZIP1
	,s.ADR_MAIL_ZIP1 BIAR_ADR_MAIL_ZIP1
	,CASE WHEN(SUBSTR(t.ADR_MAIL_ZIP1,1,5) = SUBSTR(s.ADR_MAIL_ZIP1,1,5) 
	OR COALESCE(t.ADR_MAIL_ZIP1, '') = COALESCE(s.ADR_MAIL_ZIP1, '')) THEN 'T' ELSE 'FAIL' END IS_MATCH
		
	FROM {catalog}.{schema_name}.{EDW_TblNm117} t
	JOIN {catalog}.{schema_name}.{BIAR_TblNm117} s 
	--ON t.ID_PROVIDER_MCAID1 = s.ID_PROVIDER_MCAID1
	ON t.sak_prov = s.sak_prov
	
)t WHERE true;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Providers ADR_MAIL_ZIP1 4-digit zip code extension.
/*========================================
	PROVIDER EXTRACTS COMPARISON - Counts
========================================*/

SELECT
	COUNT(*) Prov_ADR_MAIL_ZIP1_4_digit_zip_code_extension
	,SUM(CAST(is_match ='T' AS INT)) matches_cnt
	,CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
	,SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
	,CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
	
FROM(
	select distinct
	t.ID_PROVIDER_MCAID1
	
	,t.ADR_MAIL_ZIP1 EDW_ADR_MAIL_ZIP1
	,s.ADR_MAIL_ZIP1 BIAR_ADR_MAIL_ZIP1
	,CASE WHEN(t.ADR_MAIL_ZIP1 = s.ADR_MAIL_ZIP1
	OR COALESCE(t.ADR_MAIL_ZIP1, '') = COALESCE(s.ADR_MAIL_ZIP1, '')) THEN 'T' ELSE 'FAIL' END IS_MATCH
		
	FROM {catalog}.{schema_name}.{EDW_TblNm117} t
	JOIN {catalog}.{schema_name}.{BIAR_TblNm117} s 
	--ON t.ID_PROVIDER_MCAID1 = s.ID_PROVIDER_MCAID1
	ON t.sak_prov = s.sak_prov
	
)t WHERE true;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Providers ADR_MAIL_ZIP2
/*========================================
	PROVIDER EXTRACTS COMPARISON - Counts
========================================*/

SELECT
	COUNT(*) Prov_ADR_MAIL_ZIP2
	,SUM(CAST(is_match ='T' AS INT)) matches_cnt
	,CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
	,SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
	,CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
	
FROM(
	select distinct
	t.ID_PROVIDER_MCAID1
	
	,t.ADR_MAIL_ZIP2 EDW_ADR_MAIL_ZIP2
	,s.ADR_MAIL_ZIP2 BIAR_ADR_MAIL_ZIP2
	,CASE WHEN(SUBSTR(t.ADR_MAIL_ZIP2,1,5) = SUBSTR(s.ADR_MAIL_ZIP2,1,5) 
	OR COALESCE(t.ADR_MAIL_ZIP2, '') = COALESCE(s.ADR_MAIL_ZIP2, '')) THEN 'T' ELSE 'FAIL' END IS_MATCH
		
	FROM {catalog}.{schema_name}.{EDW_TblNm117} t
	JOIN {catalog}.{schema_name}.{BIAR_TblNm117} s 
	--ON t.ID_PROVIDER_MCAID1 = s.ID_PROVIDER_MCAID1
	ON t.sak_prov = s.sak_prov

	
)t WHERE true;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Providers ADR_MAIL_ZIP2 4-digit zip code extension.
/*========================================
	PROVIDER EXTRACTS COMPARISON - Counts
========================================*/

SELECT
	COUNT(*) Prov_ADR_MAIL_ZIP2_4_digit_zip_code_extension
	,SUM(CAST(is_match ='T' AS INT)) matches_cnt
	,CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
	,SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
	,CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
	
FROM(
	select distinct
	t.ID_PROVIDER_MCAID1
	
	,t.ADR_MAIL_ZIP2 EDW_ADR_MAIL_ZIP2
	,s.ADR_MAIL_ZIP2 BIAR_ADR_MAIL_ZIP2
	,CASE WHEN(SUBSTR(t.ADR_MAIL_ZIP2,1,5) = SUBSTR(s.ADR_MAIL_ZIP2,1,5) 
	OR COALESCE(t.ADR_MAIL_ZIP2, '') = COALESCE(s.ADR_MAIL_ZIP2, '')) THEN 'T' ELSE 'FAIL' END IS_MATCH
		
	FROM {catalog}.{schema_name}.{EDW_TblNm117} t
	JOIN {catalog}.{schema_name}.{BIAR_TblNm117} s 
	--ON t.ID_PROVIDER_MCAID1 = s.ID_PROVIDER_MCAID1
	ON t.sak_prov = s.sak_prov

	
)t WHERE true;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Providers ADR_MAIL_ZIP3
/*========================================
	PROVIDER EXTRACTS COMPARISON - Counts
========================================*/

SELECT
	COUNT(*) Prov_ADR_MAIL_ZIP3
	,SUM(CAST(is_match ='T' AS INT)) matches_cnt
	,CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
	,SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
	,CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
	
FROM(
	select distinct
	t.ID_PROVIDER_MCAID1
	
	,t.ADR_MAIL_ZIP3 EDW_ADR_MAIL_ZIP3
	,s.ADR_MAIL_ZIP3 BIAR_ADR_MAIL_ZIP3
	,CASE WHEN(SUBSTR(t.ADR_MAIL_ZIP3,1,5) = SUBSTR(s.ADR_MAIL_ZIP3,1,5)
	OR COALESCE(t.ADR_MAIL_ZIP3, '') = COALESCE(s.ADR_MAIL_ZIP3, '')) THEN 'T' ELSE 'FAIL' END IS_MATCH
		
	FROM {catalog}.{schema_name}.{EDW_TblNm117} t
	JOIN {catalog}.{schema_name}.{BIAR_TblNm117} s 
	--ON t.ID_PROVIDER_MCAID1 = s.ID_PROVIDER_MCAID1
	ON t.sak_prov = s.sak_prov

	
)t WHERE true;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
-- ========================
-- Providers ADR_MAIL_ZIP3 4-digit zip code extension.
/*========================================
	PROVIDER EXTRACTS COMPARISON - Counts
========================================*/

SELECT
	COUNT(*) Prov_ADR_MAIL_ZIP3_4_digit_zip_code_extension
	,SUM(CAST(is_match ='T' AS INT)) matches_cnt
	,CAST(100*sum(CAST(is_match='T' AS INT))/count(*) AS NUMERIC(15,2)) matches_pcntg
	,SUM(CAST(is_match ='FAIL' as INT)) unmatches_cnt
	,CAST(100*sum(CAST(is_match='FAIL' AS INT))/count(*) AS NUMERIC(15,2)) unmatches_pcntg
	
FROM(
	select distinct
	t.ID_PROVIDER_MCAID1
	
	,t.ADR_MAIL_ZIP3 EDW_ADR_MAIL_ZIP3
	,s.ADR_MAIL_ZIP3 BIAR_ADR_MAIL_ZIP3
	,CASE WHEN(t.ADR_MAIL_ZIP3 = s.ADR_MAIL_ZIP3 
	OR COALESCE(t.ADR_MAIL_ZIP3, '') = COALESCE(s.ADR_MAIL_ZIP3, '')) THEN 'T' ELSE 'FAIL' END IS_MATCH
		
	FROM {catalog}.{schema_name}.{EDW_TblNm117} t
	JOIN {catalog}.{schema_name}.{BIAR_TblNm117} s 
	--ON t.ID_PROVIDER_MCAID1 = s.ID_PROVIDER_MCAID1
	ON t.sak_prov = s.sak_prov
		
)t WHERE true;
               """)
display(sql_out)

