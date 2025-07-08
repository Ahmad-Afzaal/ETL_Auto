# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     ETL_Recip Analytics.                                                                                             *
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
#* 05/27/2025 INC0074781/DF#24548 Jaime Zavala        Produced changes to include LNG_CDE_DESC into VEN114 and Recipient Anlytcs.   *
#************************************************************************************************************************************
#

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('ven_114fa', 'EDW_ven114fa_staging')
dbutils.widgets.text('ven_115fa', 'EDW_ven115fa_staging')
dbutils.widgets.text('VEN130FA', 'EDW_VEN130FA_Staging')
dbutils.widgets.text('temp_ven114fa', 'edw_temp_ven114fa')
dbutils.widgets.text('temp_ven115fa', 'edw_temp_ven115fa')
dbutils.widgets.text('recp_table', 'EDW_temp_Recipient_Analytics')

# COMMAND ----------

schema_name = dbutils.widgets.get('schema_name')
ven_114fa = dbutils.widgets.get('ven_114fa')
ven_115fa=dbutils.widgets.get('ven_115fa')
catalog = dbutils.widgets.get('catalog')
temp_ven114fa = dbutils.widgets.get('temp_ven114fa')
temp_ven115fa = dbutils.widgets.get('temp_ven115fa')
recp_table = dbutils.widgets.get('recp_table')
VEN130FA = dbutils.widgets.get('VEN130FA')

print("VEN114FA :",ven_114fa)
print("VEN115FA :",ven_115fa)
print()
print("VEN114FA Analytics: ",temp_ven114fa)
print("VEN115FA Analytics: ",temp_ven115fa)
print("Recipient Analytics: ", recp_table)
print("VE Reference TableNm: ", VEN130FA)


# COMMAND ----------

# DBTITLE 1,Load VEN114FA Analytics Table
# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE  ${catalog}.${schema_name}.${temp_ven114fa} AS
# MAGIC SELECT
# MAGIC   DISTINCT 'R' AS DERIVED1,
# MAGIC   CAST(NULL AS STRING) AS `NA1`,
# MAGIC   CAST(NULL AS STRING) AS `NA2`,
# MAGIC   CAST(NULL AS STRING) AS `NA3`,
# MAGIC   CAST(NULL AS STRING) AS `NA4`,
# MAGIC   CAST(NULL AS STRING) AS `ID_MED_RECIP_PREV`,
# MAGIC   CAST(NULL AS STRING) AS `ID_CRISE`,
# MAGIC   -- NULL AS `NA1`,
# MAGIC   TRIM(ID_MEDICAID) AS ID_MEDICAID,
# MAGIC   CAST(SAK_RECIP AS STRING) AS SAK_RECIP,
# MAGIC   -- NULL AS ID_MED_RECIP_PREV,
# MAGIC   -- NULL AS `NA2`,
# MAGIC   TRIM(
# MAGIC     REGEXP_REPLACE(
# MAGIC       REGEXP_REPLACE(NAM_LAST, '\\"', '"'),
# MAGIC       '^"|"$',
# MAGIC       ''
# MAGIC     )
# MAGIC   ) AS NAM_LAST,
# MAGIC   TRIM(
# MAGIC     REGEXP_REPLACE(
# MAGIC       REGEXP_REPLACE(NAM_FIRST, '\\"', '"'),
# MAGIC       '^"|"$',
# MAGIC       ''
# MAGIC     )
# MAGIC   ) AS NAM_FIRST,
# MAGIC   TRIM(
# MAGIC     REGEXP_REPLACE(
# MAGIC       REGEXP_REPLACE(NAM_MID_INIT, '\\"', '"'),
# MAGIC       '^"|"$',
# MAGIC       ''
# MAGIC     )
# MAGIC   ) AS NAM_MID_INIT,
# MAGIC   TRIM(NUM_SSN) AS NUM_SSN,
# MAGIC   -- NULL AS `NA3`,
# MAGIC   TRIM(CDE_RACE) AS CDE_RACE,
# MAGIC   TRIM(CDE_RACE_2) AS CDE_RACE_2,
# MAGIC   TRIM(CDE_RACE_3) AS CDE_RACE_3,
# MAGIC   TRIM(CDE_RACE_4) AS CDE_RACE_4,
# MAGIC   TRIM(CDE_RACE_5) AS CDE_RACE_5,
# MAGIC   TRIM(CDE_RACE_6) AS CDE_RACE_6,
# MAGIC   TRIM(CDE_RACE_7) AS CDE_RACE_7,
# MAGIC   TRIM(CDE_ETHNIC) AS CDE_ETHNIC,
# MAGIC   TRIM(CDE_SOURCE) AS CDE_SOURCE,
# MAGIC   TRIM(CDE_SEX) AS CDE_SEX,
# MAGIC   CAST(DTE_BIRTH AS DATE) AS DTE_BIRTH,
# MAGIC   CAST(DTE_DEATH AS DATE) AS DTE_DEATH,
# MAGIC   -- NULL AS `NA4`,
# MAGIC   TRIM(NUM_CASE) AS NUM_CASE,
# MAGIC   TRIM(CDE_LANGUAGE) AS CDE_LANGUAGE,
# MAGIC   CAST(
# MAGIC     TRIM(
# MAGIC       FIRST_VALUE(CDE_LIV_ARNG) OVER (PARTITION BY SAK_RECIP)
# MAGIC     ) AS STRING
# MAGIC   ) AS CDE_LIV_ARNG,
# MAGIC   CAST(
# MAGIC     TRIM(
# MAGIC       FIRST_VALUE(CDE_SSI_STATUS) OVER (PARTITION BY SAK_RECIP)
# MAGIC     ) AS STRING
# MAGIC   ) AS CDE_SSI_STATUS,
# MAGIC   TRIM(CDE_MARITAL) AS CDE_MARITAL,
# MAGIC   -- NULL AS ID_CRISE,
# MAGIC   CAST(REPORT_DTE AS DATE) AS DERIVED2,
# MAGIC   FIRST_VALUE(CAST(DTE_EFFECTIVE AS DATE)) OVER (
# MAGIC     PARTITION BY SAK_RECIP
# MAGIC     ORDER BY
# MAGIC       DTE_END DESC,
# MAGIC       DTE_EFFECTIVE DESC
# MAGIC   ) AS DTE_EFFECTIVE,
# MAGIC   FIRST_VALUE(CAST(DTE_END AS DATE)) OVER (
# MAGIC     PARTITION BY SAK_RECIP
# MAGIC     ORDER BY
# MAGIC       DTE_END DESC,
# MAGIC       DTE_EFFECTIVE DESC
# MAGIC   ) AS DTE_END,
# MAGIC   TRIM(DESCRIPTION) AS LNG_CDE_DESC
# MAGIC FROM ${catalog}.${schema_name}.${ven_114fa}
# MAGIC LEFT JOIN ${catalog}.${schema_name}.${VEN130FA} ON CODESET_NAME ='LNG' AND TRIM(CODE) = TRIM(CDE_LANGUAGE)
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from  ${catalog}.${schema_name}.${temp_ven114fa}

# COMMAND ----------

# DBTITLE 1,Load VEN115FA Analytics Table
# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE ${catalog}.${schema_name}.${temp_ven115fa} AS
# MAGIC SELECT
# MAGIC   DISTINCT 'A' AS DERIVED1,
# MAGIC   CAST(NULL AS STRING) AS NA1,
# MAGIC   TRIM(demo.ID_MEDICAID) AS ID_MEDICAID,
# MAGIC   CAST(addr.SAK_RECIP AS STRING) AS SAK_RECIP,
# MAGIC   CAST(NULL AS STRING) AS ID_MED_RECIP_PREV,
# MAGIC   CAST(NULL AS STRING) AS NA2,
# MAGIC   CAST(
# MAGIC     TRIM(
# MAGIC       REGEXP_REPLACE(
# MAGIC         REGEXP_REPLACE(demo.NAM_LAST, '\\"', '"'),
# MAGIC         '^"|"$',
# MAGIC         ''
# MAGIC       )
# MAGIC     ) AS STRING
# MAGIC   ) AS NAM_LAST,
# MAGIC   CAST(
# MAGIC     TRIM(
# MAGIC       REGEXP_REPLACE(
# MAGIC         REGEXP_REPLACE(demo.NAM_FIRST, '\\"', '"'),
# MAGIC         '^"|"$',
# MAGIC         ''
# MAGIC       )
# MAGIC     ) AS STRING
# MAGIC   ) AS NAM_FIRST,
# MAGIC   CAST(
# MAGIC     TRIM(
# MAGIC       REGEXP_REPLACE(
# MAGIC         REGEXP_REPLACE(demo.NAM_MID_INIT, '\\"', '"'),
# MAGIC         '^"|"$',
# MAGIC         ''
# MAGIC       )
# MAGIC     ) AS STRING
# MAGIC   ) AS NAM_MID_INIT,
# MAGIC   TRIM(demo.NUM_SSN) AS NUM_SSN,
# MAGIC   CAST(NULL AS STRING) AS NA3,
# MAGIC   CAST(
# MAGIC     TRIM(
# MAGIC       REGEXP_REPLACE(
# MAGIC         REGEXP_REPLACE(ADR_STREET_1, '\\"', '"'),
# MAGIC         '^"|"$',
# MAGIC         ''
# MAGIC       )
# MAGIC     ) AS STRING
# MAGIC   ) AS ADR_STREET_1,
# MAGIC   CAST(
# MAGIC     TRIM(
# MAGIC       REGEXP_REPLACE(
# MAGIC         REGEXP_REPLACE(ADR_STREET_2, '\\"', '"'),
# MAGIC         '^"|"$',
# MAGIC         ''
# MAGIC       )
# MAGIC     ) AS STRING
# MAGIC   ) AS ADR_STREET_2,
# MAGIC   CAST(TRIM(addr.ADR_CTY) AS STRING) AS ADR_CITY,
# MAGIC   TRIM(ADR_STATE) AS ADR_STATE,
# MAGIC   TRIM(ADR_ZIP_CODE) AS ADR_ZIP_CODE,
# MAGIC   TRIM(ADR_ZIP_CODE_4) AS ADR_ZIP_CODE_4,
# MAGIC   TRIM(NUM_PHONE) AS NUM_PHONE,
# MAGIC   TRIM(NUM_PHONE_INTL) AS NUM_PHONE_ADDL,
# MAGIC   TRIM(NUM_ADD_PHONE) AS NUM_ADD_PHONE,
# MAGIC   TRIM(CDE_COUNTY) AS CDE_COUNTY,
# MAGIC   NUM_LONGITUDE,
# MAGIC   NUM_LATITUDE,
# MAGIC   CAST(REPORT_DTE AS DATE) AS DERIVED2
# MAGIC FROM
# MAGIC    ${catalog}.${schema_name}.${ven_115fa} addr
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       DISTINCT sak_recip,
# MAGIC       id_medicaid,
# MAGIC       nam_last,
# MAGIC       nam_first,
# MAGIC       nam_mid_init,
# MAGIC       num_ssn
# MAGIC     FROM
# MAGIC        ${catalog}.${schema_name}.${ven_114fa}
# MAGIC   ) demo ON addr.sak_recip = demo.sak_recip;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from  ${catalog}.${schema_name}.${temp_ven115fa}

# COMMAND ----------

# DBTITLE 1,Load Recipient Analytics Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${catalog}.${schema_name}.${recp_table} AS
# MAGIC SELECT distinct 
# MAGIC   addr.DERIVED1,
# MAGIC   addr.NA1,
# MAGIC   addr.ID_MEDICAID,
# MAGIC   LPAD(TRIM(addr.SAK_RECIP), 9) AS SAK_RECIP,
# MAGIC   -- Assuming SAK_RECIP is a string, left-padded to 9 characters
# MAGIC   addr.ID_MED_RECIP_PREV,
# MAGIC   addr.NA2,
# MAGIC   addr.NAM_LAST,
# MAGIC   addr.NAM_FIRST,
# MAGIC   addr.NAM_MID_INIT,
# MAGIC   addr.NUM_SSN,
# MAGIC   addr.NA3,
# MAGIC   addr.ADR_STREET_1,
# MAGIC   addr.ADR_STREET_2,
# MAGIC   addr.ADR_CITY,
# MAGIC   addr.ADR_STATE,
# MAGIC   addr.ADR_ZIP_CODE,
# MAGIC   addr.ADR_ZIP_CODE_4,
# MAGIC   addr.NUM_PHONE,
# MAGIC   addr.NUM_ADD_PHONE,
# MAGIC   addr.NUM_PHONE_ADDL,
# MAGIC   addr.CDE_COUNTY,
# MAGIC   CASE
# MAGIC     WHEN addr.NUM_LONGITUDE != 0.0 THEN (addr.NUM_LONGITUDE * 1000000)
# MAGIC     ELSE addr.NUM_LONGITUDE
# MAGIC   END AS NUM_LONGITUDE,
# MAGIC   CASE
# MAGIC     WHEN addr.NUM_LATITUDE != 0.0 THEN (addr.NUM_LATITUDE * 1000000)
# MAGIC     ELSE addr.NUM_LATITUDE
# MAGIC   END AS NUM_LATITUDE,
# MAGIC   addr.DERIVED2,
# MAGIC   demo.CDE_RACE,
# MAGIC   demo.CDE_RACE_2,
# MAGIC   demo.CDE_RACE_3,
# MAGIC   demo.CDE_RACE_4,
# MAGIC   demo.CDE_RACE_5,
# MAGIC   demo.CDE_RACE_6,
# MAGIC   demo.CDE_RACE_7,
# MAGIC   demo.CDE_ETHNIC,
# MAGIC   demo.CDE_SOURCE,
# MAGIC   demo.CDE_SEX,
# MAGIC   demo.DTE_BIRTH,
# MAGIC   COALESCE(demo.DTE_DEATH, '2299-12-31') AS DTE_DEATH,
# MAGIC   demo.NA4,
# MAGIC   demo.NUM_CASE,
# MAGIC   demo.CDE_LANGUAGE,
# MAGIC   demo.CDE_LIV_ARNG,
# MAGIC   demo.CDE_SSI_STATUS,
# MAGIC   demo.CDE_MARITAL,
# MAGIC   demo.ID_CRISE,
# MAGIC   demo.DTE_EFFECTIVE,
# MAGIC   demo.DTE_END,
# MAGIC   demo.LNG_CDE_DESC
# MAGIC FROM
# MAGIC   ${catalog}.${schema_name}.${temp_ven114fa} demo
# MAGIC   JOIN ${catalog}.${schema_name}.${temp_ven115fa} addr ON demo.ID_MEDICAID = addr.ID_MEDICAID;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from  ${catalog}.${schema_name}.${recp_table}
# MAGIC ;
