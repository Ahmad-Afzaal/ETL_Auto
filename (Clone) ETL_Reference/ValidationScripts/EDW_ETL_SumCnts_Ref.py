# Databricks notebook source
# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')

print("catalog:", catalog)
print("schema:", schema_name)

# COMMAND ----------

# DBTITLE 1,Reference
sql_out = spark.sql(f"""
select count(*) AS Reference_EDW_VEN130FA_Staging_count
from {catalog}.{schema_name}.EDW_VEN130FA_Staging
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct CODE) AS Reference_EDW_VEN130FA_Staging_distinct_CODE
from  {catalog}.{schema_name}.EDW_VEN130FA_Staging
;
		""")
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct CODESET_NAME) AS Reference_EDW_VEN130FA_Staging_distinct_CODESET_NAME
from  {catalog}.{schema_name}.EDW_VEN130FA_Staging
;
		""")
display(sql_out)

