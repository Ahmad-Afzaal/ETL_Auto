# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     ETL_Provider_VE_Load.                                                                                            *
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
#*    Date     CO            Author          Description                                                                            *
#* ---------- ---------  -----------------   -------------------------------------------------------------------------------------- *
#* 03/01/2024            Suman Ettedi        Inital Release                                                                         *
#* 03/22/2024            Jaime Zavala        Added logic to copy from Test into Staging Tables, and drop Test Tables.               *
#************************************************************************************************************************************


# COMMAND ----------

dbutils.widgets.text('received_date', '2024-03-20')
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'archive_vendor_extracts')
dbutils.widgets.text('s3_location', 's3://gia-stg-oh-ue1-data-raw/haven/inbound/VE_EDW/weekly/dt=')
dbutils.widgets.text('Clng_Month_Gap', '-120')


# COMMAND ----------

# DBTITLE 1,Load VE Files into Test Tables
from  pyspark.sql.functions import *
received_date = dbutils.widgets.get('received_date')
catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
s3_location = dbutils.widgets.get('s3_location')
Clng_Month_Gap = dbutils.widgets.get('Clng_Month_Gap')

print("Location Path: ",s3_location)
print("Location Dte: ",received_date)
print("Cleaning Month Gap: ", Clng_Month_Gap)

files =['EDW_VEN117FA1_Staging_test' ,'EDW_VEN117FA2_Staging_test' ,'EDW_VEN117FA3_Staging_test' ,'EDW_VEN117FA4_Staging_test' ,'EDW_VEN117FA5_Staging_test' ,'EDW_VEN117FA6_Staging_test' ,'EDW_VEN117FA7_Staging_test' ,'EDW_VEN117FA9_Staging_test' ,'EDW_VEN117FA10_Staging_test','EDW_VEN117FA11_Staging_test' ,'EDW_VEN117FA12_Staging_test']
for file in files:
    file_exp= file.split('_')[1]
    print(file_exp)
    
    #
    # source_data_loc=f"{s3_location}{received_date}/*{file_exp}.*20240125*"
    #
    source_data_loc=f"{s3_location}{received_date}/*{file_exp}.*"
    df_spark = spark.read.option("mergeSchema", "true").csv(source_data_loc, sep ='|', header = True)
    df_spark = df_spark.select([col(c).alias(
            c.replace( '(', '')
            .replace( ')', '')
            .replace( ',', '')
            .replace( ';', '')
            .replace( '{', '')
            .replace( '}', '')
            .replace( '\n', '')
            .replace( '\t', '')
            .replace( ' ', '_')
        ) for c in df_spark.columns])
    # df_spark.show()
    table_name = f"{catalog}.{schema_name}.{file}"
    print(table_name)
    df_spark.write.mode('overwrite').saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Truncate Staging Tables
# MAGIC %sql
# MAGIC truncate table ${catalog}.${schema_name}.EDW_VEN117FA1_Staging;
# MAGIC truncate table ${catalog}.${schema_name}.EDW_VEN117FA2_Staging;
# MAGIC truncate table ${catalog}.${schema_name}.EDW_VEN117FA3_Staging;
# MAGIC truncate table ${catalog}.${schema_name}.EDW_VEN117FA4_Staging;
# MAGIC truncate table ${catalog}.${schema_name}.EDW_VEN117FA5_Staging;
# MAGIC truncate table ${catalog}.${schema_name}.EDW_VEN117FA6_Staging;
# MAGIC truncate table ${catalog}.${schema_name}.EDW_VEN117FA7_Staging;
# MAGIC truncate table ${catalog}.${schema_name}.EDW_VEN117FA9_Staging;
# MAGIC truncate table ${catalog}.${schema_name}.EDW_VEN117FA10_Staging;
# MAGIC truncate table ${catalog}.${schema_name}.EDW_VEN117FA11_Staging;
# MAGIC truncate table ${catalog}.${schema_name}.EDW_VEN117FA12_Staging;

# COMMAND ----------

# DBTITLE 1,Copy from Test Tables to Staging Tables
# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.EDW_VEN117FA1_Staging
# MAGIC select * from  ${catalog}.${schema_name}.EDW_VEN117FA1_Staging_test
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.EDW_VEN117FA2_Staging
# MAGIC select * from  ${catalog}.${schema_name}.EDW_VEN117FA2_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.EDW_VEN117FA3_Staging
# MAGIC select * from  ${catalog}.${schema_name}.EDW_VEN117FA3_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.EDW_VEN117FA4_Staging
# MAGIC select * from  ${catalog}.${schema_name}.EDW_VEN117FA4_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.EDW_VEN117FA5_Staging
# MAGIC select * from  ${catalog}.${schema_name}.EDW_VEN117FA5_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.EDW_VEN117FA6_Staging
# MAGIC select * from  ${catalog}.${schema_name}.EDW_VEN117FA6_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.EDW_VEN117FA7_Staging
# MAGIC select * from  ${catalog}.${schema_name}.EDW_VEN117FA7_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.EDW_VEN117FA9_Staging
# MAGIC select * from  ${catalog}.${schema_name}.EDW_VEN117FA9_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.EDW_VEN117FA10_Staging
# MAGIC select * from  ${catalog}.${schema_name}.EDW_VEN117FA10_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.EDW_VEN117FA11_Staging
# MAGIC select * from  ${catalog}.${schema_name}.EDW_VEN117FA11_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.EDW_VEN117FA12_Staging
# MAGIC select * from  ${catalog}.${schema_name}.EDW_VEN117FA12_Staging_test

# COMMAND ----------

# DBTITLE 1,Insert into Historic Tables
# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.edw_ven117fa1_historic
# MAGIC select current_timestamp() AS EXTRACTION_DATE,* from  ${catalog}.${schema_name}.EDW_VEN117FA1_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.edw_ven117fa2_historic
# MAGIC select current_timestamp() AS EXTRACTION_DATE,* from  ${catalog}.${schema_name}.EDW_VEN117FA2_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ${catalog}.${schema_name}.edw_ven117fa3_historic
# MAGIC select current_timestamp() AS EXTRACTION_DATE,* from  ${catalog}.${schema_name}.EDW_VEN117FA3_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.edw_ven117fa4_historic
# MAGIC select current_timestamp() AS EXTRACTION_DATE,* from  ${catalog}.${schema_name}.EDW_VEN117FA4_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.edw_ven117fa5_historic
# MAGIC select current_timestamp() AS EXTRACTION_DATE,* from  ${catalog}.${schema_name}.EDW_VEN117FA5_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.edw_ven117fa6_historic
# MAGIC select current_timestamp() AS EXTRACTION_DATE,* from  ${catalog}.${schema_name}.EDW_VEN117FA6_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.edw_ven117fa7_historic
# MAGIC select current_timestamp() AS EXTRACTION_DATE,* from  ${catalog}.${schema_name}.EDW_VEN117FA7_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.edw_ven117fa9_historic
# MAGIC select current_timestamp() AS EXTRACTION_DATE,* from  ${catalog}.${schema_name}.EDW_VEN117FA9_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.edw_ven117fa10_historic
# MAGIC select current_timestamp() AS EXTRACTION_DATE,* from  ${catalog}.${schema_name}.EDW_VEN117FA10_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.edw_ven117fa11_historic
# MAGIC select current_timestamp() AS EXTRACTION_DATE,* from  ${catalog}.${schema_name}.EDW_VEN117FA11_Staging_test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  ${catalog}.${schema_name}.edw_ven117fa12_historic
# MAGIC select current_timestamp() AS EXTRACTION_DATE,* from  ${catalog}.${schema_name}.EDW_VEN117FA12_Staging_test

# COMMAND ----------

# DBTITLE 1,Cleanning Historic Tables
# MAGIC %sql
# MAGIC delete  from   ${catalog}.${schema_name}.edw_ven117fa1_historic
# MAGIC where EXTRACTION_DATE < ADD_MONTHS(current_timestamp(),${Clng_Month_Gap})

# COMMAND ----------

# MAGIC %sql
# MAGIC delete  from   ${catalog}.${schema_name}.edw_ven117fa2_historic
# MAGIC where EXTRACTION_DATE < ADD_MONTHS(current_timestamp(),${Clng_Month_Gap})

# COMMAND ----------

# MAGIC %sql
# MAGIC delete  from   ${catalog}.${schema_name}.edw_ven117fa3_historic
# MAGIC where EXTRACTION_DATE < ADD_MONTHS(current_timestamp(),${Clng_Month_Gap})

# COMMAND ----------

# MAGIC %sql
# MAGIC delete  from   ${catalog}.${schema_name}.edw_ven117fa4_historic
# MAGIC where EXTRACTION_DATE < ADD_MONTHS(current_timestamp(),${Clng_Month_Gap})

# COMMAND ----------

# MAGIC %sql
# MAGIC delete  from   ${catalog}.${schema_name}.edw_ven117fa5_historic
# MAGIC where EXTRACTION_DATE < ADD_MONTHS(current_timestamp(),${Clng_Month_Gap})

# COMMAND ----------

# MAGIC %sql
# MAGIC delete  from   ${catalog}.${schema_name}.edw_ven117fa6_historic
# MAGIC where EXTRACTION_DATE < ADD_MONTHS(current_timestamp(),${Clng_Month_Gap})

# COMMAND ----------

# MAGIC %sql
# MAGIC delete  from   ${catalog}.${schema_name}.edw_ven117fa7_historic
# MAGIC where EXTRACTION_DATE < ADD_MONTHS(current_timestamp(),${Clng_Month_Gap})

# COMMAND ----------

# MAGIC %sql
# MAGIC delete  from   ${catalog}.${schema_name}.edw_ven117fa9_historic
# MAGIC where EXTRACTION_DATE < ADD_MONTHS(current_timestamp(),${Clng_Month_Gap})

# COMMAND ----------

# MAGIC %sql
# MAGIC delete  from   ${catalog}.${schema_name}.edw_ven117fa10_historic
# MAGIC where EXTRACTION_DATE < ADD_MONTHS(current_timestamp(),${Clng_Month_Gap})

# COMMAND ----------

# MAGIC %sql
# MAGIC delete  from   ${catalog}.${schema_name}.edw_ven117fa11_historic
# MAGIC where EXTRACTION_DATE < ADD_MONTHS(current_timestamp(),${Clng_Month_Gap})

# COMMAND ----------

# MAGIC %sql
# MAGIC delete  from   ${catalog}.${schema_name}.edw_ven117fa12_historic
# MAGIC where EXTRACTION_DATE < ADD_MONTHS(current_timestamp(),${Clng_Month_Gap})

# COMMAND ----------

# DBTITLE 1,Drop Test Tables
files =['EDW_VEN117FA1_Staging_test' ,'EDW_VEN117FA2_Staging_test' ,'EDW_VEN117FA3_Staging_test' ,'EDW_VEN117FA4_Staging_test' ,'EDW_VEN117FA5_Staging_test' ,'EDW_VEN117FA6_Staging_test' ,'EDW_VEN117FA7_Staging_test' ,'EDW_VEN117FA9_Staging_test' ,'EDW_VEN117FA10_Staging_test','EDW_VEN117FA11_Staging_test' ,'EDW_VEN117FA12_Staging_test']
for file in files:
    print("Dropping Table: ",file)

    sql_out = spark.sql(f"""
    drop table {catalog}.{schema_name}.{file}
    ;
                   """)
    display(sql_out)

