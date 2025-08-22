# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_SumCnts_Elig_VE.                                                                                         *
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
#* 04/09/2024 CCRB70930/CO#43342  Jaime Zavala        Initial Release.                                                              *
#************************************************************************************************************************************
#

# COMMAND ----------

# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('EDW_CtlLog', 'EDW_CtlLog_Elig_Staging')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
EDW_CtlLog = dbutils.widgets.get('EDW_CtlLog')

print("catalog:", catalog)
print("schema:", schema_name)
print("EDW_CtlLog:", EDW_CtlLog)

# COMMAND ----------

# DBTITLE 1,Eligibility Vendor Extracts
sql_out = spark.sql(f"""
WITH
ETL_LoadCnt AS
(
  select count(*) AS LoadRowCnt
    from {catalog}.{schema_name}.EDW_VEN116FA_PartA_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = "PARTA"
)
select LoadRowCnt AS EligVE_VEN116FA_PartA_count
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
    from {catalog}.{schema_name}.EDW_VEN116FA_PartC_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'PARTC'
)
select LoadRowCnt AS EligVE_VEN116FA_PartC_count
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
    from {catalog}.{schema_name}.EDW_VEN116FA_PartD01_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'PARTD01'
)
select LoadRowCnt AS EligVE_VEN116FA_PartD01_count
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
    from {catalog}.{schema_name}.EDW_VEN116FA_PartD02_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'PARTD02'
)
select LoadRowCnt AS EligVE_VEN116FA_PartD02_count
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
    from {catalog}.{schema_name}.EDW_VEN116FA_PartD03_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'PARTD03'
)
select LoadRowCnt AS EligVE_VEN116FA_PartD03_count
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
    from {catalog}.{schema_name}.EDW_VEN116FA_PartD04_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'PARTD04'
)
select LoadRowCnt AS EligVE_VEN116FA_PartD04_count
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
    from {catalog}.{schema_name}.EDW_VEN116FA_PartD05_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'PARTD05'
)
select LoadRowCnt AS EligVE_VEN116FA_PartD05_count
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
    from {catalog}.{schema_name}.EDW_VEN116FA_PartE_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'PARTE'
)
select LoadRowCnt AS EligVE_VEN116FA_PartE_count
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
    from {catalog}.{schema_name}.EDW_VEN116FA_PartG_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'PARTG'
)
select LoadRowCnt AS EligVE_VEN116FA_PartG_count
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
    from {catalog}.{schema_name}.EDW_VEN116FA_PartH_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'PARTH'
)
select LoadRowCnt AS EligVE_VEN116FA_PartH_count
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
    from {catalog}.{schema_name}.EDW_VEN116FA_PartI_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'PARTI'
)
select LoadRowCnt AS EligVE_VEN116FA_PartI_count
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
    from {catalog}.{schema_name}.EDW_VEN116FA_PartJ_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'PARTJ'
)
select LoadRowCnt AS EligVE_VEN116FA_PartJ_count
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
    from {catalog}.{schema_name}.EDW_VEN116FA_PartK_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'PARTK'
)
select LoadRowCnt AS EligVE_VEN116FA_PartK_count
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
    from {catalog}.{schema_name}.EDW_VEN116FA_PartL_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'PARTL'
)
select LoadRowCnt AS EligVE_VEN116FA_PartL_count
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
    from {catalog}.{schema_name}.EDW_VEN116FA_PartN_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'PARTN'
)
select LoadRowCnt AS EligVE_VEN116FA_PartN_count
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
    from {catalog}.{schema_name}.EDW_VEN116FA_PartO_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'PARTO'
)
select LoadRowCnt AS EligVE_VEN116FA_PartO_count
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
    from {catalog}.{schema_name}.EDW_VEN101FA_Staging
)
,EDW_RowCnt AS
(
 select SUM(Row_Count) AS EDWRowCnt
   from {catalog}.{schema_name}.{EDW_CtlLog}
   where FileType = 'VEN101FA'
)
select LoadRowCnt AS EligVE_VEN101FA_count
      ,EDWRowCnt
      ,CASE WHEN (LoadRowCnt = EDWRowCnt) THEN 'T' ELSE 'FAIL' END AS IS_MATCH
from ETL_LoadCnt, EDW_RowCnt
;
		""")
display(sql_out)

