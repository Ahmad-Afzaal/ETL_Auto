# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_SumCnts_Elig.                                                                                            *
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
#* 03/21/2024 CCRB70930/CO#43342  Jaime Zavala        Added Summary Counts for Part: D02, D03, D04, D05, A, C, G, H, and J          *
#* 04/11/2014 CCRB70930/CO#43342  Jaime Zavala        Added Parms for EDW table/Upadted Notebook.                                   *
#************************************************************************************************************************************

# COMMAND ----------

# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('EDW_TblNm', 'Eligibility_Analytics')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
EDW_TblNm = dbutils.widgets.get('EDW_TblNm')

print("catalog:", catalog)
print("schema:", schema_name)
print("EDW Table Name:", EDW_TblNm)

# COMMAND ----------

# DBTITLE 1,Eligibility
sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Eligibility_Part_D01_distinct_SAK_RECIP
from {catalog}.{schema_name}.EDW_VEN116FA_PartD01_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Eligibility_Part_D01_distinct_ID_MEDICAID
from {catalog}.{schema_name}.EDW_VEN116FA_PartD01_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_D01_count
from {catalog}.{schema_name}.EDW_VEN116FA_PartD01_Staging
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Eligibility_Part_D01_EDW_Eligibility_Analytics_distinct_SAK_RECIP
from {catalog}.{schema_name}.{EDW_TblNm}
where D_DTE_EFFECTIVE is not null 
  and D_DTE_END is not null
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID1) AS Eligibility_Part_D01_EDW_Eligibility_Analytics_distinct_ID_MEDICAID1
from {catalog}.{schema_name}.{EDW_TblNm}
where D_DTE_EFFECTIVE is not null 
  and D_DTE_END is not null
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_D01_EDW_Eligibility_Analytics_count
from {catalog}.{schema_name}.{EDW_TblNm}
where D_DTE_EFFECTIVE is not null 
  and D_DTE_END is not null
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Eligibility_Part_E_distinct_SAK_RECIP
from {catalog}.{schema_name}.EDW_VEN116FA_PartE_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Eligibility_Part_E_distinct_ID_MEDICAID
from {catalog}.{schema_name}.EDW_VEN116FA_PartE_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_E_count
from {catalog}.{schema_name}.EDW_VEN116FA_PartE_Staging
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Eligibility_Part_E_EDW_Eligibility_Analytics_distinct_SAK_RECIP
from {catalog}.{schema_name}.{EDW_TblNm}
where E_DTE_EFFECTIVE is not null 
  and E_DTE_END is not null
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID1) AS Eligibility_Part_E_EDW_Eligibility_Analytics_distinct_ID_MEDICAID1
from {catalog}.{schema_name}.{EDW_TblNm}
where E_DTE_EFFECTIVE is not null 
  and E_DTE_END is not null
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_E_EDW_Eligibility_Analytics_count
from {catalog}.{schema_name}.{EDW_TblNm}
where E_DTE_EFFECTIVE is not null 
  and E_DTE_END is not null
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Eligibility_Part_I_distinct_SAK_RECIP
from {catalog}.{schema_name}.EDW_VEN116FA_PartI_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Eligibility_Part_I_distinct_ID_MEDICAID
from {catalog}.{schema_name}.EDW_VEN116FA_PartI_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_I_count
from {catalog}.{schema_name}.EDW_VEN116FA_PartI_Staging
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Eligibility_Part_I_EDW_Eligibility_Analytics_distinct_SAK_RECIP
from {catalog}.{schema_name}.{EDW_TblNm}
where I_DTE_EFFECTIVE is not null 
  and I_DTE_END is not null
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID1) AS Eligibility_Part_I_EDW_Eligibility_Analytics_distinct_ID_MEDICAID1
from {catalog}.{schema_name}.{EDW_TblNm}
where I_DTE_EFFECTIVE is not null 
  and I_DTE_END is not null
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_I_EDW_Eligibility_Analytics_count
from {catalog}.{schema_name}.{EDW_TblNm}
where I_DTE_EFFECTIVE is not null 
  and I_DTE_END is not null
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Eligibility_Part_K_distinct_SAK_RECIP
from {catalog}.{schema_name}.EDW_VEN116FA_PartK_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Eligibility_Part_K_distinct_ID_MEDICAID
from {catalog}.{schema_name}.EDW_VEN116FA_PartK_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_K_count
from {catalog}.{schema_name}.EDW_VEN116FA_PartK_Staging
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Eligibility_Part_K_EDW_Eligibility_Analytics_distinct_SAK_RECIP
from {catalog}.{schema_name}.{EDW_TblNm}
where K_DTE_EFFECTIVE is not null 
  and K_DTE_END is not null
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID1) AS Eligibility_Part_K_EDW_Eligibility_Analytics_distinct_ID_MEDICAID1
from {catalog}.{schema_name}.{EDW_TblNm}
where K_DTE_EFFECTIVE is not null 
  and K_DTE_END is not null
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_K_EDW_Eligibility_Analytics_count
from {catalog}.{schema_name}.{EDW_TblNm}
where K_DTE_EFFECTIVE is not null
  and K_DTE_END is not null
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Eligibility_Part_L_distinct_SAK_RECIP
from {catalog}.{schema_name}.EDW_VEN116FA_PartL_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Eligibility_Part_L_distinct_ID_MEDICAID
from {catalog}.{schema_name}.EDW_VEN116FA_PartL_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_L_count
from {catalog}.{schema_name}.EDW_VEN116FA_PartL_Staging
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Eligibility_Part_L_EDW_Eligibility_Analytics_distinct_SAK_RECIP
from {catalog}.{schema_name}.{EDW_TblNm}
where L_DTE_EFFECTIVE is not null 
  and L_DTE_END is not null
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID1) AS Eligibility_Part_L_EDW_Eligibility_Analytics_distinct_ID_MEDICAID1
from {catalog}.{schema_name}.{EDW_TblNm}
where L_DTE_EFFECTIVE is not null 
  and L_DTE_END is not null
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_L_EDW_Eligibility_Analytics_count
from {catalog}.{schema_name}.{EDW_TblNm}
where L_DTE_EFFECTIVE is not null 
  and L_DTE_END is not null
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Eligibility_Part_N_distinct_SAK_RECIP
from {catalog}.{schema_name}.EDW_VEN116FA_PartN_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Eligibility_Part_N_distinct_ID_MEDICAID
from {catalog}.{schema_name}.EDW_VEN116FA_PartN_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_N_count
from {catalog}.{schema_name}.EDW_VEN116FA_PartN_Staging
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Eligibility_Part_N_EDW_Eligibility_Analytics_distinct_SAK_RECIP
from {catalog}.{schema_name}.{EDW_TblNm}
where N_DTE_TPL_EFFECTIVE is not null 
  and N_DTE_TPL_END is not null
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID1) AS Eligibility_Part_N_EDW_Eligibility_Analytics_distinct_ID_MEDICAID1
from {catalog}.{schema_name}.{EDW_TblNm}
where N_DTE_TPL_EFFECTIVE is not null 
  and N_DTE_TPL_END is not null
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_N_EDW_Eligibility_Analytics_count
from {catalog}.{schema_name}.{EDW_TblNm}
where N_DTE_TPL_EFFECTIVE is not null 
  and N_DTE_TPL_END is not null
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Eligibility_Part_O_distinct_SAK_RECIP
from {catalog}.{schema_name}.EDW_VEN116FA_PartO_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Eligibility_Part_O_distinct_ID_MEDICAID
from {catalog}.{schema_name}.EDW_VEN116FA_PartO_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_O_count
from {catalog}.{schema_name}.EDW_VEN116FA_PartO_Staging
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Eligibility_Part_O_EDW_Eligibility_Analytics_distinct_SAK_RECIP
from {catalog}.{schema_name}.{EDW_TblNm}
where O_DTE_EFFECTIVE is not null 
  and O_DTE_END is not null
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID1) AS Eligibility_Part_O_EDW_Eligibility_Analytics_distinct_ID_MEDICAID1
from {catalog}.{schema_name}.{EDW_TblNm}
where O_DTE_EFFECTIVE is not null 
  and O_DTE_END is not null
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_O_EDW_Eligibility_Analytics_count
from {catalog}.{schema_name}.{EDW_TblNm}
where O_DTE_EFFECTIVE is not null 
  and O_DTE_END is not null
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_A_count
from {catalog}.{schema_name}.EDW_VEN116FA_PartA_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_C_count
from {catalog}.{schema_name}.EDW_VEN116FA_PartC_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_G_count
from {catalog}.{schema_name}.EDW_VEN116FA_PartG_Staging
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_H_count
from {catalog}.{schema_name}.EDW_VEN116FA_PartH_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_Part_J_count
from {catalog}.{schema_name}.EDW_VEN116FA_PartJ_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Eligibility_RecipEligFullExtract_count
from {catalog}.{schema_name}.EDW_VEN101FA_Staging
;
               """)
display(sql_out)

