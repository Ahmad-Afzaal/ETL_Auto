# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_SumCnts_Clms_VE.                                                                                         *
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
#* 06/03/2024 CCRB70930/CO#43342  Jaime Zavala        Added Claims Supplementals Extracts VEN10008 and VEN10009.                    *
#* 08/01/2024 CCRB70930/CO#43342  Jaime Zavala        Added Variables to Table names and File types.                                *
#* 04/10/2025 CCRB70930/CO#43342  Jaime Zavala        NewExtract.- VEN12501FA- Multi Provider Claim Extract.                        *
#************************************************************************************************************************************


# COMMAND ----------

# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')

#
# Tables and File Types
#
dbutils.widgets.text('VEN100FA', 'EDW_VEN100FA_Staging')
dbutils.widgets.text('FileTyp_VEN100', 'VEN100FA')
dbutils.widgets.text('VEN10002FA', 'EDW_VEN10002FA_Staging')
dbutils.widgets.text('FileTyp_VEN10002', 'VEN10002FA')
dbutils.widgets.text('VEN10003FA', 'EDW_VEN10003FA_Staging')
dbutils.widgets.text('FileTyp_VEN10003', 'VEN10003FA')
dbutils.widgets.text('VEN10004FA', 'EDW_VEN10004FA_Staging')
dbutils.widgets.text('FileTyp_VEN10004', 'VEN10004FA')
dbutils.widgets.text('VEN10005FA', 'EDW_VEN10005FA_Staging')
dbutils.widgets.text('FileTyp_VEN10005', 'VEN10005FA')
dbutils.widgets.text('VEN10006FA', 'EDW_VEN10006FA_Staging')
dbutils.widgets.text('FileTyp_VEN10006', 'VEN10006FA')
dbutils.widgets.text('VEN10007FA', 'EDW_VEN10007FA_Staging')  
dbutils.widgets.text('FileTyp_VEN10007', 'VEN10007FA')
dbutils.widgets.text('VEN10008FA', 'EDW_VEN10008FA_Staging')
dbutils.widgets.text('FileTyp_VEN10008', 'VEN10008FA')
dbutils.widgets.text('VEN10009FA', 'EDW_VEN10009FA_Staging')
dbutils.widgets.text('FileTyp_VEN10009', 'VEN10009FA')
dbutils.widgets.text('VEN12401FA', 'EDW_VEN12401FA_Staging')
dbutils.widgets.text('FileTyp_VEN12401', 'VEN12401FA')
dbutils.widgets.text('VEN12403FA', 'EDW_VEN12403FA_Staging')
dbutils.widgets.text('FileTyp_VEN12403', 'VEN12403FA')
dbutils.widgets.text('VEN12404FA', 'EDW_VEN12404FA_Staging')
dbutils.widgets.text('FileTyp_VEN12404', 'VEN12404FA')
dbutils.widgets.text('VEN12501FA', 'EDW_VEN12501FA_Staging')
dbutils.widgets.text('FileTyp_VEN12501', 'VEN12501FA')
dbutils.widgets.text('EDW_CtlLog', 'EDW_CtlLog_Clms_Staging')

#
# Get Variables
#
catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
VEN100FA = dbutils.widgets.get('VEN100FA')		
FileTyp_VEN100 = dbutils.widgets.get('FileTyp_VEN100')	
VEN10002FA = dbutils.widgets.get('VEN10002FA')		
FileTyp_VEN10002 = dbutils.widgets.get('FileTyp_VEN10002')
VEN10003FA = dbutils.widgets.get('VEN10003FA')		
FileTyp_VEN10003 = dbutils.widgets.get('FileTyp_VEN10003')
VEN10004FA = dbutils.widgets.get('VEN10004FA')		
FileTyp_VEN10004 = dbutils.widgets.get('FileTyp_VEN10004')
VEN10005FA = dbutils.widgets.get('VEN10005FA')		
FileTyp_VEN10005 = dbutils.widgets.get('FileTyp_VEN10005')
VEN10006FA = dbutils.widgets.get('VEN10006FA')		
FileTyp_VEN10006 = dbutils.widgets.get('FileTyp_VEN10006')
VEN10007FA = dbutils.widgets.get('VEN10007FA')		
FileTyp_VEN10007 = dbutils.widgets.get('FileTyp_VEN10007')
VEN10008FA = dbutils.widgets.get('VEN10008FA')		
FileTyp_VEN10008 = dbutils.widgets.get('FileTyp_VEN10008')
VEN10009FA = dbutils.widgets.get('VEN10009FA')		
FileTyp_VEN10009 = dbutils.widgets.get('FileTyp_VEN10009')
VEN12401FA = dbutils.widgets.get('VEN12401FA')		
FileTyp_VEN12401 = dbutils.widgets.get('FileTyp_VEN12401')
VEN12403FA = dbutils.widgets.get('VEN12403FA')		
FileTyp_VEN12403 = dbutils.widgets.get('FileTyp_VEN12403')
VEN12404FA = dbutils.widgets.get('VEN12404FA')		
FileTyp_VEN12404 = dbutils.widgets.get('FileTyp_VEN12404')
VEN12501FA = dbutils.widgets.get('VEN12501FA')		
FileTyp_VEN12501 = dbutils.widgets.get('FileTyp_VEN12501')

EDW_CtlLog = dbutils.widgets.get('EDW_CtlLog')		

#
# Print Variables
#
print("catalog:", catalog)
print("schema:", schema_name)
print("VEN100FA:", VEN100FA)		
print("FileTyp_VEN100:", FileTyp_VEN100)	
print("VEN10002FA:", VEN10002FA)		
print("FileTyp_VEN10002:", FileTyp_VEN10002)
print("VEN10003FA:", VEN10003FA)		
print("FileTyp_VEN10003:", FileTyp_VEN10003)
print("VEN10004FA:", VEN10004FA)		
print("FileTyp_VEN10004:", FileTyp_VEN10004)
print("VEN10005FA:", VEN10005FA)		
print("FileTyp_VEN10005:", FileTyp_VEN10005)
print("VEN10006FA:", VEN10006FA)		
print("FileTyp_VEN10006:", FileTyp_VEN10006)
print("VEN10007FA:", VEN10007FA)		
print("FileTyp_VEN10007:", FileTyp_VEN10007)
print("VEN10008FA:", VEN10008FA)		
print("FileTyp_VEN10008:", FileTyp_VEN10008)
print("VEN10009FA:", VEN10009FA)		
print("FileTyp_VEN10009:", FileTyp_VEN10009)
print("VEN12401FA:", VEN12401FA)		
print("FileTyp_VEN12401:", FileTyp_VEN12401)
print("VEN12403FA:", VEN12403FA)		
print("FileTyp_VEN12403:", FileTyp_VEN12403)
print("VEN12404FA:", VEN12404FA)		
print("FileTyp_VEN12404:", FileTyp_VEN12404)
print("VEN12501FA:", VEN12501FA)		
print("FileTyp_VEN12501:", FileTyp_VEN12501)
print("EDW_CtlLog:", EDW_CtlLog)		

# COMMAND ----------

# DBTITLE 1,Clms Vendor Extracts
sql_out = spark.sql(f"""
WITH
ETL_LoadCnt AS
(
  select count(*) AS LoadRowCnt
    from {catalog}.{schema_name}.{VEN100FA}
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = '{FileTyp_VEN100}'
     -- and FileDte = '20240229'  -- Optional criteria when more than one ctl file exist.
)
select LoadRowCnt AS ClmsVE_VEN100FA_count
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
    from {catalog}.{schema_name}.{VEN10002FA}
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = '{FileTyp_VEN10002}'
)
select LoadRowCnt AS ClmsVE_VEN10002FA_count
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
    from {catalog}.{schema_name}.{VEN10003FA}
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = '{FileTyp_VEN10003}'
)
select LoadRowCnt AS ClmsVE_VEN10003FA_count
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
    from {catalog}.{schema_name}.{VEN10004FA}
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = '{FileTyp_VEN10004}'
)
select LoadRowCnt AS ClmsVE_VEN10004FA_count
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
    from {catalog}.{schema_name}.{VEN10005FA}
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = '{FileTyp_VEN10005}'
)
select LoadRowCnt AS ClmsVE_VEN10005FA_count
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
    from {catalog}.{schema_name}.{VEN10006FA}
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = '{FileTyp_VEN10006}'
)
select LoadRowCnt AS ClmsVE_VEN10006FA_count
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
    from {catalog}.{schema_name}.{VEN10007FA}
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = '{FileTyp_VEN10007}'
)
select LoadRowCnt AS ClmsVE_VEN10007FA_count
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
    from {catalog}.{schema_name}.{VEN10008FA}
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = '{FileTyp_VEN10008}'
)
select LoadRowCnt AS ClmsVE_VEN10008FA_count
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
    from {catalog}.{schema_name}.{VEN10009FA}
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = '{FileTyp_VEN10009}'
)
select LoadRowCnt AS ClmsVE_VEN10009FA_count
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
    from {catalog}.{schema_name}.{VEN12401FA}
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = '{FileTyp_VEN12401}'
)
select LoadRowCnt AS ClmsVE_VEN12401FA_count
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
    from {catalog}.{schema_name}.{VEN12403FA}
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = '{FileTyp_VEN12403}'
)
select LoadRowCnt AS ClmsVE_VEN12403FA_count
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
    from {catalog}.{schema_name}.{VEN12404FA}
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = '{FileTyp_VEN12404}'
)
select LoadRowCnt AS ClmsVE_VEN12404FA_count
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
    from {catalog}.{schema_name}.{VEN12501FA}
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = '{FileTyp_VEN12501}'
)
select LoadRowCnt AS ClmsVE_VEN12501FA_count
      ,EDWRowCnt
      ,CASE WHEN (LoadRowCnt = EDWRowCnt) THEN 'T' ELSE 'FAIL' END AS IS_MATCH
from ETL_LoadCnt, EDW_RowCnt
;
		""")
display(sql_out)

