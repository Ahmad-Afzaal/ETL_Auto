# Databricks notebook source
# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'archive_vendor_extracts')
dbutils.widgets.text('EDW_CtlLog', 'EDW_CtlLog_Prov_Staging')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
EDW_CtlLog = dbutils.widgets.get('EDW_CtlLog')

print("catalog:", catalog)
print("schema:", schema_name)
print("EDW_CtlLog:", EDW_CtlLog)

# COMMAND ----------

# DBTITLE 1,Provider Vendor Extracts
sql_out = spark.sql(f"""
WITH
ETL_LoadCnt AS
(
  select count(*) AS LoadRowCnt
    from {catalog}.{schema_name}.EDW_VEN117FA1_Staging
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'VEN117FA1'
)
select LoadRowCnt AS ProvVE_VEN117FA1_count
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
    from {catalog}.{schema_name}.EDW_VEN117FA2_Staging
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'VEN117FA2'
)
select LoadRowCnt AS ProvVE_VEN117FA2_count
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
    from {catalog}.{schema_name}.EDW_VEN117FA3_Staging
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'VEN117FA3'
)
select LoadRowCnt AS ProvVE_VEN117FA3_count
      ,EDWRowCnt
      ,CASE WHEN (LoadRowCnt = EDWRowCnt) THEN 'T' ELSE 'FAIL' END AS IS_MATCH
from ETL_LoadCnt, EDW_RowCnt
;
		""")
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
WITH
ETL_LoadCnt AS
(
  select count(*) AS LoadRowCnt
    from {catalog}.{schema_name}.EDW_VEN117FA4_Staging
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'VEN117FA4'
)
select LoadRowCnt AS ProvVE_VEN117FA4_count
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
    from {catalog}.{schema_name}.EDW_VEN117FA5_Staging
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'VEN117FA5'
)
select LoadRowCnt AS ProvVE_VEN117FA5_count
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
    from {catalog}.{schema_name}.EDW_VEN117FA6_Staging
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'VEN117FA6'
)
select LoadRowCnt AS ProvVE_VEN117FA6_count
      ,EDWRowCnt
      ,CASE WHEN (LoadRowCnt = EDWRowCnt) THEN 'T' ELSE 'FAIL' END AS IS_MATCH
from ETL_LoadCnt, EDW_RowCnt
;
		""")
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
WITH
ETL_LoadCnt AS
(
  select count(*) AS LoadRowCnt
    from {catalog}.{schema_name}.EDW_VEN117FA7_Staging
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'VEN117FA7'
)
select LoadRowCnt AS ProvVE_VEN117FA7_count
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
    from {catalog}.{schema_name}.EDW_VEN117FA9_Staging
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'VEN117FA9'
)
select LoadRowCnt AS ProvVE_VEN117FA9_count
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
    from {catalog}.{schema_name}.EDW_VEN117FA10_Staging
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'VEN117FA10'
)
select LoadRowCnt AS ProvVE_VEN117FA10_count
      ,EDWRowCnt
      ,CASE WHEN (LoadRowCnt = EDWRowCnt) THEN 'T' ELSE 'FAIL' END AS IS_MATCH
from ETL_LoadCnt, EDW_RowCnt
;
		""")
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
WITH
ETL_LoadCnt AS
(
  select count(*) AS LoadRowCnt
    from {catalog}.{schema_name}.EDW_VEN117FA11_Staging
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'VEN117FA11'
)
select LoadRowCnt AS ProvVE_VEN117FA11_count
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
    from {catalog}.{schema_name}.EDW_VEN117FA12_Staging
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'VEN117FA12'
)
select LoadRowCnt AS ProvVE_VEN117FA12_count
      ,EDWRowCnt
      ,CASE WHEN (LoadRowCnt = EDWRowCnt) THEN 'T' ELSE 'FAIL' END AS IS_MATCH
from ETL_LoadCnt, EDW_RowCnt
;
		""")
display(sql_out)

