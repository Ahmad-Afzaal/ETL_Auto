# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_SumCnts_DxPx_VE.                                                                                         *
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
#* 08/01/2024 CCRB70930/CO#43342  Jaime Zavala        Added Variables to Table names and File types.                                *
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
dbutils.widgets.text('VEN10001FA', 'EDW_VEN10001FA_Staging')
dbutils.widgets.text('FileTyp_VEN10001', 'VEN10001FA')
dbutils.widgets.text('VEN12301FA', 'EDW_VEN12301FA_Staging')
dbutils.widgets.text('FileTyp_VEN12301', 'VEN12301FA')
dbutils.widgets.text('EDW_CtlLog', 'EDW_CtlLog_Clms_Staging')

#
# Get Variables
#
catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
VEN10001FA = dbutils.widgets.get('VEN10001FA')		
FileTyp_VEN10001 = dbutils.widgets.get('FileTyp_VEN10001')
VEN12301FA = dbutils.widgets.get('VEN12301FA')		
FileTyp_VEN12301 = dbutils.widgets.get('FileTyp_VEN12301')
EDW_CtlLog = dbutils.widgets.get('EDW_CtlLog')

#
# Print Variables
#
print("catalog:", catalog)
print("schema:", schema_name)
print("VEN10001FA:", VEN10001FA)
print("FileTyp_VEN10001:", FileTyp_VEN10001)
print("VEN12301FA:", VEN12301FA)		
print("FileTyp_VEN12301:", FileTyp_VEN12301)


# COMMAND ----------

# DBTITLE 1,DxPx Vendor Extracts
sql_out = spark.sql(f"""
WITH
ETL_LoadCnt AS
(
  select count(*) AS LoadRowCnt
    from {catalog}.{schema_name}.{VEN10001FA}
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = '{FileTyp_VEN10001}'
     -- and FileDte = 20240229 -- Optional criteria when more than one ctl file exist.
)
select LoadRowCnt AS DxPxVE_VEN10001FA_count
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
    from {catalog}.{schema_name}.{VEN12301FA}
)
,EDW_RowCnt AS
(
 select Row_Count AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = '{FileTyp_VEN12301}'
)
select LoadRowCnt AS DxPxVE_VEN12301FA_count
      ,EDWRowCnt
      ,CASE WHEN (LoadRowCnt = EDWRowCnt) THEN 'T' ELSE 'FAIL' END AS IS_MATCH
from ETL_LoadCnt, EDW_RowCnt
;
		""")
display(sql_out)

