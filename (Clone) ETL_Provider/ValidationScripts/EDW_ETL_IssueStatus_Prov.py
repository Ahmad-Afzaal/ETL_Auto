# Databricks notebook source
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'archive_vendor_extracts')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')

print("catalog:", catalog)
print("schema:", schema_name)

# COMMAND ----------

# DBTITLE 1,SQL For Provider Issues
# MAGIC %sql
# MAGIC -- NO ISSUES OPEN
