# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_SumCnts_Rfrnc_VE.                                                                                        *
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
#* 04/01/2024 CCRB70930/CO#43342  Jaime Zavala        Initial Release.                                                              *
#************************************************************************************************************************************


# COMMAND ----------

# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')

print("catalog:", catalog)
print("schema:", schema_name)

# COMMAND ----------

# DBTITLE 1,Reference Vendor Extracts
sql_out = spark.sql(f"""
WITH
ETL_LoadCnt AS
(
  select count(*) AS LoadRowCnt
    from {catalog}.{schema_name}.EDW_VEN130FA_Staging
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.EDW_CtlLog_Rfrnc_Staging
   where FileType = 'VEN130FA'
)
select LoadRowCnt AS RfrncVE_VEN130FA_count
      ,EDWRowCnt
      ,CASE WHEN (LoadRowCnt = EDWRowCnt) THEN 'T' ELSE 'FAIL' END AS IS_MATCH
from ETL_LoadCnt, EDW_RowCnt
;
		""")
display(sql_out)

