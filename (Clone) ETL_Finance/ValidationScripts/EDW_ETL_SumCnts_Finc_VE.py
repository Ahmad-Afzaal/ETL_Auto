# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_SumCnts_Finc_VE.                                                                                         *
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
#* 06/10/2024 CCRB70930/CO#43342  Jaime Zavala        Initial Release.                                                              *
#* 12/06/2024 CCRB70930/CO#43342  Jaime Zavala        Removed the logic for VEN114FB.                                               *
#* 03/26/2025 CCRB70930/CO#43342  Jaime Zavala        Added the logic for VEN16FB.                                                  *
#************************************************************************************************************************************
#

# COMMAND ----------

# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('EDW_CtlLog', 'EDW_CtlLog_Finc_Staging')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
EDW_CtlLog = dbutils.widgets.get('EDW_CtlLog')

print("catalog:", catalog)
print("schema:", schema_name)
print("EDW_CtlLog:", EDW_CtlLog)

# COMMAND ----------

# DBTITLE 1,Finance/Capitation Vendor Extracts
sql_out = spark.sql(f"""
WITH
ETL_LoadCnt AS
(
  select count(*) AS LoadRowCnt
    from {catalog}.{schema_name}.EDW_VEN113FB_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = "VEN113FB"
)
select LoadRowCnt AS FincVE_VEN113FB_count
      ,EDWRowCnt
      ,CASE WHEN (LoadRowCnt = EDWRowCnt) THEN 'T' ELSE 'FAIL' END AS IS_MATCH
from ETL_LoadCnt, EDW_RowCnt
;
		""")
display(sql_out)


# COMMAND ----------

# DBTITLE 1,Finance/Capitation Member Condition Vendor Extracts
sql_out = spark.sql(f"""
WITH
ETL_LoadCnt AS
(
  select count(*) AS LoadRowCnt
    from {catalog}.{schema_name}.EDW_VEN16FB_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = "VEN16FB"
)
select LoadRowCnt AS FincVE_VEN16FB_count
      ,EDWRowCnt
      ,CASE WHEN (LoadRowCnt = EDWRowCnt) THEN 'T' ELSE 'FAIL' END AS IS_MATCH
from ETL_LoadCnt, EDW_RowCnt
;
		""")
display(sql_out)

