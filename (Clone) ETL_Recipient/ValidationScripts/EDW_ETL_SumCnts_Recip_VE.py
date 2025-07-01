# Databricks notebook source
# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'archive_vendor_extracts')
dbutils.widgets.text('EDW_CtlLog', 'EDW_CtlLog_Recip_Staging')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
EDW_CtlLog = dbutils.widgets.get('EDW_CtlLog')

print("catalog:", catalog)
print("schema:", schema_name)
print("EDW_CtlLog:", EDW_CtlLog)

# COMMAND ----------

# DBTITLE 1,Recipient Vendor Extracts
sql_out = spark.sql(f"""
WITH
ETL_LoadCnt AS
(
  select count(*) AS LoadRowCnt
    from {catalog}.{schema_name}.EDW_VEN114FA_Staging
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'VEN114FA'
)
select LoadRowCnt AS ClmsVE_VEN114FA_count
      ,EDWRowCnt
      ,CASE WHEN (LoadRowCnt = EDWRowCnt) THEN 'T' ELSE 'FAIL' END AS IS_MATCH
from ETL_LoadCnt, EDW_RowCnt
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
WITH
ETL_LoadCnt AS
(
  select count(*) AS LoadRowCnt
    from {catalog}.{schema_name}.EDW_VEN115FA_Staging
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'VEN115FA'
)
select LoadRowCnt AS ClmsVE_VEN115FA_count
      ,EDWRowCnt
      ,CASE WHEN (LoadRowCnt = EDWRowCnt) THEN 'T' ELSE 'FAIL' END AS IS_MATCH
from ETL_LoadCnt, EDW_RowCnt
;
		""")
display(sql_out)

