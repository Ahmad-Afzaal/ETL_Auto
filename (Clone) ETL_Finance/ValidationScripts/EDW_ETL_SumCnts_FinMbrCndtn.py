# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_SumCnts_FinMbrCndtn.                                                                                     *
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
#* 01/06/2025 CCRB70930/CO#43342  Jaime Zavala        Initial Release.                                                              *
#************************************************************************************************************************************
#

# COMMAND ----------

# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('VEN16FB', 'EDW_VEN16FB_Staging')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
VEN16FB = dbutils.widgets.get('VEN16FB')

print("catalog:", catalog)
print("schema:", schema_name)
print("VEN16FB:", VEN16FB)

# COMMAND ----------

# DBTITLE 1,Finance_Member_Condition
sql_out = spark.sql(f"""
select count(*) AS Finance_EDW_VEN16FB_Staging_count
from {catalog}.{schema_name}.{VEN16FB}
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct MEDICAID_ID) AS Finance_EDW_VEN16FB_Staging_distinct_MEDICAID_ID
from {catalog}.{schema_name}.{VEN16FB}
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct CONDITION_SAK_ID) AS Finance_EDW_VEN16FB_Staging_distinct_CONDITION_SAK_ID
from {catalog}.{schema_name}.{VEN16FB}
;
		""")
display(sql_out)
