# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     ProduceEligQAAnalyticsTables.                                                                                    *
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
#* 11/12/2025 CCRB70930/CO#43342  Jaime Zavala        Initial Release.                                                              *
#************************************************************************************************************************************

# COMMAND ----------

#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')  
dbutils.widgets.text('schema_name', 'etl_qa')  
dbutils.widgets.text('ve_schema_name', 'vendor_extracts')  

#-------------
#  Tables
#-------------

dbutils.widgets.text('SourceTblNm', 'Eligibility_Analytics')
dbutils.widgets.text('PrevTblNm', 'Eligibility_Analytics_Previous')
dbutils.widgets.text('CurntTblNm', 'Eligibility_Analytics_Current')

#-------------------
# Getters Section
#-------------------
catalog     = dbutils.widgets.get('catalog')
ve_schema_name = dbutils.widgets.get('ve_schema_name')
schema_name = dbutils.widgets.get('schema_name')
SourceTblNm = dbutils.widgets.get('SourceTblNm')
CurntTblNm = dbutils.widgets.get('CurntTblNm')
PrevTblNm = dbutils.widgets.get('PrevTblNm')


#---------------
# Print Section
#---------------
print("catalog:", catalog)
print("schema:", schema_name)
print("VE schema:", ve_schema_name)
print("Source Analysis Table Name:", SourceTblNm)
print("Current Table Name:", CurntTblNm)
print("Previous Table Name:", PrevTblNm)


# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE ${catalog}.${schema_name}.${PrevTblNm}
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ${catalog}.${schema_name}.${PrevTblNm}
# MAGIC SELECT * FROM ${catalog}.${schema_name}.${CurntTblNm}
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${catalog}.${schema_name}.${CurntTblNm}
# MAGIC CLONE ${catalog}.${ve_schema_name}.${SourceTblNm}
# MAGIC ;
