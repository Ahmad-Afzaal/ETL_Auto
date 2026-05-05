# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_SumCnts_Finc.                                                                                            *
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
dbutils.widgets.text('VEN113FB', 'EDW_VEN113FB_Staging')
dbutils.widgets.text('VEN16FB', 'EDW_VEN16FB_Staging')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
VEN113FB = dbutils.widgets.get('VEN113FB')
VEN16FB = dbutils.widgets.get('VEN16FB')

print("catalog:", catalog)
print("schema:", schema_name)
print("VEN113FB:", VEN113FB)
print("VEN16FB:", VEN16FB)

# COMMAND ----------

# DBTITLE 1,Finance
sql_out = spark.sql(f"""
select count(*) AS Finance_EDW_VEN113FB_Staging_count
from {catalog}.{schema_name}.{VEN113FB}
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct SAK_PROV) AS Finance_EDW_VEN113FB_Staging_distinct_SAK_PROV
from {catalog}.{schema_name}.{VEN113FB}
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Finance_EDW_VEN113FB_Staging_distinct_SAK_RECIP
from {catalog}.{schema_name}.{VEN113FB}
;
		""")
display(sql_out)

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
