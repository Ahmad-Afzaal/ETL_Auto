# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     EDW_ETL_SumCnts_Recip.                                                                                           *
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
#* 04/11/2024 CCRB70930/CO#43342  Jaime Zavala        Added Parms for EDW table/Upadted Notebook.                                   *
#* 10/08/2024 CCRB70930/CO#43342  Jaime Zavala        Added Logic for Issue#114/DOB. Criteria: 1=MoGap and 28=DaysGap.              *
#* 11/12/2024 CCRB70930/CO#43342  Jaime Zavala        Added Logic for Issue#115/Gender Multiple Values. Criteria used: 7=MoGap and  *
#*                                                    2 <= EventCnt.                                                                *
#* 05/15/2025 CCRB70930/CO#43342  Jaime Zavala        Removed Tracking_Issue_109's Logic.                                           *
#*                                                    Added Logic for Issue#116/First and Last Name Multiple Values. Criteria used: *
#*                                                    6=MoGap and EventCnt >= 1.                                                    *
#* 05/20/2025 CCRB70930/CO#43342  Jaime Zavala        Added Logic for Issue#117/Missing MedicadIds in VEN114FA extract.             *
#* 06/16/2025 CCRB70930/CO#43342  Jaime Zavala        Added Logic for Issue#118/Missing Language Code in MedicadIds.                *
#*                                                    Remove logic for Cloased Issue#117.                                           *
#************************************************************************************************************************************
#

# COMMAND ----------

# DBTITLE 1,Parms
#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('schema_name_cmc', 'CMC')
dbutils.widgets.text('EDW_TblNm', 'Recipient_Analytics')
dbutils.widgets.text('EDW_TblNm114', 'ven114fa')
dbutils.widgets.text('EDW_TblNm115', 'ven115fa')

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
schema_name_cmc = dbutils.widgets.get('schema_name_cmc')
EDW_TblNm = dbutils.widgets.get('EDW_TblNm')
EDW_TblNm114 = dbutils.widgets.get('EDW_TblNm114')
EDW_TblNm115 = dbutils.widgets.get('EDW_TblNm115')

print("catalog:", catalog)
print("schema:", schema_name)
print("schema cmc:", schema_name_cmc)
print("EDW Table Name:", EDW_TblNm)
print("EDW 114 Table Name:", EDW_TblNm114)
print("EDW 115 Table Name:", EDW_TblNm115)

# COMMAND ----------

# DBTITLE 1,Recipient
sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Recipient_distinct_SAK_RECIP
from {catalog}.{schema_name}.EDW_VEN114FA_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Recipient_distinct_ID_MEDICAID
from {catalog}.{schema_name}.EDW_VEN114FA_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Recipient_distinct_count
from {catalog}.{schema_name}.EDW_VEN114FA_Staging
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct t114.ID_MEDICAID) AS Recipient_count_distinct_t114_ID_MEDICAID_Issue_108
from {catalog}.{schema_name}.EDW_ven114fa_Staging t114
left join {catalog}.{schema_name}.EDW_ven115fa_Staging t115 on t114.SAK_RECIP = t115.SAK_RECIP
where t115.SAK_RECIP is null
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
                    select count(*) AS Recipient_count_Issue_118
                      from {catalog}.{schema_name}.edw_ven114fa_staging
                     where CDE_LANGUAGE is null
;
               """)
display(sql_out)


# COMMAND ----------

# sql_out = spark.sql(f"""
# select count(*) AS Recipient_count_Issue_117
# from {catalog}.{schema_name}.edw_ven114fa_staging
# where ID_MEDICAID  in 
# (
#  '910002796250'
# ,'910002794027'
# ,'910002796079'
# ,'910002796288'
# ,'910002794807'
# ,'910002794361'
# ,'910002795328'
# ,'910002795824'
# ,'910002795535'
# ,'910002795658'
# ,'910002793621'
# ,'910002795040'
# ,'910002794671'
# ,'910002795507'
# ,'910002794773'
# ,'910002793665'
# ,'910002794539'
# ,'910002796216'
# ,'910002794066'
# ,'910002794137'
# ,'910002795037'
# ,'910002795671'
# ,'910002796164'
# ,'999000141545'
# ,'910002796422'
# ,'910002795466'
# ,'910002794401'
# ,'910002794443'
# ,'910002795721'
# ,'910002796009'
# ,'910002793452'
# ,'910002794113'
# ,'910002794826'
# ,'910002796136'
# ,'910002794974'
# ,'910002793943'
# ,'910002794243'
# ,'910002795172'
# ,'910002796194'
# ,'910002793831'
# ,'910002794384'
# ,'901000051346'
# ,'910002795084'
# ,'999000141586'
# ,'910002794901'
# ,'910002760312'
# ,'910002794310'
# ,'910002795867'
# ,'910002795835'
# ,'910002795104'
# ,'910002794948'
# ,'910002795626'
# ,'910002793685'
# ,'910002793405'
# ,'089113761180'
# ,'910002794774'
# ,'910002793910'
# ,'910002795430'
# ,'910002796135'
# ,'910002794796'
# ,'910002796328'
# ,'910002795527'
# ,'910002794808'
# ,'910002793856'
# ,'910002795324'
# ,'910002795267'
# ,'910002795188'
# ,'910002793704'
# ,'910002796275'
# ,'910002796132'
# ,'910002793787'
# ,'910002794091'
# ,'910002795389'
# ,'910002795752'
# ,'910002794922'
# ,'089113792680'
# ,'910002795848'
# ,'910002796013'
# ,'910002793599'
# ,'910002794972'
# ,'910002795205'
# ,'910002795662'
# ,'910002794069'
# ,'910002795135'
# ,'910002796298'
# ,'910002794480'
# ,'910002795756'
# ,'910002794957'
# ,'910002795005'
# ,'910002794946'
# ,'910002793587'
# ,'910002795301'
# ,'910002795070'
# ,'910002795184'
# ,'910002795629'
# ,'910002794117'
# ,'910002795576'
# ,'910002794373'
# ,'910002795757'
# ,'910002793969'
# ,'910002794135'
# ,'910002794491'
# ,'910002793561'
# ,'910002795167'
# ,'999000141644'
# ,'910002796355'
# ,'910002796417'
# ,'910002794358'
# ,'910002794792'
# ,'910002795017'
# ,'910002794086'
# ,'910002794345'
# ,'910002795440'
# ,'910002794246'
# ,'910000966542'
# ,'910002794887'
# ,'910002794894'
# ,'089113784780'
# ,'910002796007'
# ,'910002796381'
# ,'910002796349'
# ,'910002795127'
# ,'910002794141'
# ,'910002795660'
# ,'910002794589'
# ,'910002795919'
# ,'910002794094'
# ,'910002795914'
# ,'910002794747'
# ,'910002796036'
# ,'910002795810'
# ,'910002793788'
# ,'910002795614'
# ,'910002794650'
# ,'910002795538'
# ,'910002794496'
# ,'910002794909'
# ,'910002796174'
# ,'910002794933'
# ,'089097729680'
# ,'910002795924'
# ,'910002794939'
# ,'910002795893'
# ,'910002794990'
# ,'910002795108'
# ,'910002794740'
# ,'910002794171'
# ,'910002793488'
# ,'910002795298'
# ,'910002794554'
# ,'910002794407'
# ,'910002793423'
# ,'910002794964'
# ,'910002794216'
# ,'910002794980'
# ,'910002795354'
# ,'910002794509'
# ,'910002795908'
# ,'910002795004'
# ,'910002795748'
# ,'910002794699'
# ,'910002794812'
# ,'106688045999'
# ,'910002794882'
# ,'910002794414'
# ,'910002795793'
# ,'910002794130'
# ,'910002795102'
# ,'910002795679'
# ,'910002796254'
# ,'910002793426'
# ,'910002793852'
# ,'910002795763'
# ,'910002795809'
# ,'910002795566'
# ,'910002796272'
# ,'910002794960'
# ,'910002794349'
# ,'910002794917'
# ,'910002794138'
# ,'910002794276'
# ,'910002795675'
# ,'910002794234'
# ,'910002794188'
# ,'910002794943'
# ,'910002794857'
# ,'910002793507'
# ,'910002793408'
# ,'910002793680'
# ,'999000141544'
# ,'910002793494'
# ,'910002793989'
# ,'910002794961'
# ,'910002793892'
# ,'910002794912'
# ,'910002795121'
# ,'910002795539'
# ,'910002795067'
# ,'910002794760'
# ,'910002795725'
# ,'910002794717'
# ,'910002796273'
# ,'910002796045'
# ,'910002795661'
# ,'910002793433'
# ,'910002796445'
# ,'910002795031'
# ,'910002794148'
# ,'910002793463'
# ,'910002794112'
# ,'910002794229'
# ,'910002795727'
# ,'910002795709'
# ,'910002793440'
# ,'910002793854'
# ,'910002794962'
# ,'910002795086'
# ,'910002794167'
# ,'910002793610'
# ,'910002794696'
# ,'910002795849'
# ,'910002793983'
# ,'910002795933'
# ,'910002794010'
# ,'910002793471'
# ,'910002793462'
# ,'910002795738'
# ,'910002793889'
# ,'910002795095'
# ,'910002794677'
# ,'910002796183'
# ,'910002796025'
# ,'910002793533'
# ,'910002795739'
# ,'910002793516'
# ,'910002796270'
# ,'910002794233'
# ,'910002793383'
# ,'910002795088'
# ,'910002796148'
# ,'089113795780'
# ,'910002794502'
# ,'910002793510'
# ,'910002793666'
# ,'910002795002'
# ,'910002794996'
# ,'910002793644'
# ,'910002796353'
# ,'910002796071'
# ,'910002795888'
# ,'910002793442'
# ,'910002796197'
# ,'910002794544'
# ,'910002794560'
# ,'910002788978'
# ,'910002794422'
# ,'910002795925'
# ,'910002796318'
# ,'910002795936'
# ,'910002793546'
# ,'110698750499'
# ,'910002794118'
# ,'910002795279'
# ,'910002795163'
# ,'910002794368'
# ,'910002794120'
# ,'910002794477'
# ,'910002795729'
# ,'910002795803'
# ,'910002795059'
# ,'910002795461'
# ,'910002795094'
# ,'910002795743'
# ,'910002795900'
# ,'910002794656'
# ,'910002793744'
# ,'910002794612'
# ,'910002793828'
# ,'910002795847'
# ,'910002795544'
# ,'910002794661'
# ,'910002793850'
# ,'910002795864'
# ,'910002796220'
# ,'910002794707'
# ,'910002794860'
# ,'910002795657'
# ,'910002793695'
# ,'910002795910'
# ,'910002793940'
# ,'910002793465'
# ,'910002793945'
# ,'910002795750'
# ,'910002794610'
# )
# ;

#                """)
# display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Recipient_EDW_ven114fa_distinct_SAK_RECIP
from {catalog}.{schema_name}.{EDW_TblNm114}
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Recipient_EDW_ven114fa_distinct_ID_MEDICAID
from {catalog}.{schema_name}.{EDW_TblNm114}
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Recipient_EDW_ven114fa_count
from {catalog}.{schema_name}.{EDW_TblNm114}
;
               """)
display(sql_out)
#
# Commented this code to place code for Issue#115.
#
# sql_out = spark.sql(f"""
# WITH
# SQL_PpltdFieldCnt AS
# (
#  select count(distinct t114.ID_MEDICAID) PpltdFieldCnt
#   from {catalog}.{schema_name}.EDW_ven114fa_Staging t114
#   left join {catalog}.{schema_name}.EDW_ven115fa_Staging t115 on t114.SAK_RECIP = t115.SAK_RECIP
#  where t115.SAK_RECIP is null

# )
# ,SQL_TotalCnt AS
# ( 
#  select count(distinct ID_MEDICAID) TotalCnt
#    from {catalog}.{schema_name}.EDW_ven114fa_Staging
# )
# select CAST((PpltdFieldCnt * 100 ) / TotalCnt AS NUMERIC(15,2)) AS Recipient_Pcng_Missing_Sak_Recip_Issue_108
#      -- PpltdFieldCnt
#      -- ,TotalCnt
# from SQL_PpltdFieldCnt, SQL_TotalCnt
# ;
#                """)
# display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(*) as Recipient_GenderChng_Sak_Recip_Issue_115
  from
  (
     select 
     --* 
     ID_MEDICAID, count(*) as SexCntChng
     from (
            select ID_MEDICAID
                   ,EXTRACTION_DATE
                   , lag(CDE_SEX) over (partition by ID_MEDICAID order by EXTRACTION_DATE) as Previous_CdeSex
                   , CDE_SEX
              from oh_apm_stg.vendor_extracts.edw_ven114fa_historic
             where EXTRACTION_DATE >= ADD_MONTHS(CURRENT_TIMESTAMP(),-7)
             order by ID_MEDICAID, EXTRACTION_DATE
          )t where CDE_SEX != Previous_CdeSex
     group by 1
     order by 2 desc
  ) r where r.SexCntChng >= 2
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Recipient_EDW_ven115fa_distinct_SAK_RECIP
from {catalog}.{schema_name}.{EDW_TblNm115}
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Recipient_EDW_ven115fa_distinct_ID_MEDICAID
from {catalog}.{schema_name}.{EDW_TblNm115}
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Recipient_EDW_ven115fa_distinct_count
from {catalog}.{schema_name}.{EDW_TblNm115}
;
               """)
display(sql_out)
# sql_out = spark.sql(f"""
# select count(distinct member_medicaid_id) AS Recipient_count_distinct_member_medicaid_id_Tracking_109
# from {catalog}.{schema_name_cmc}.CMC_Attribution_analytics_EDW where member_medicaid_id not in 
# (select ID_MEDICAID from {catalog}.{schema_name}.{EDW_TblNm114})
# ;
#                """)
# display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
select count(*) as Recipient_FLNameChng_Sak_Recip_Issue_116
  from
  (
     select 
     --* 
     ID_MEDICAID, count(*) as FLNmCntChng
     from (
            select ID_MEDICAID
                   ,EXTRACTION_DATE
                   , lag(NAM_FIRST||NAM_LAST) over (partition by ID_MEDICAID order by EXTRACTION_DATE) as Previous_Name
                   , NAM_FIRST||NAM_LAST as Current_Name
              from {catalog}.{schema_name}.edw_ven114fa_historic
             where EXTRACTION_DATE >= ADD_MONTHS(CURRENT_TIMESTAMP(),-6)
             order by ID_MEDICAID, EXTRACTION_DATE
          )t where Current_Name != Previous_Name
     group by 1
     order by 2 desc
  ) r where r.FLNmCntChng >= 1
;
               """)
display(sql_out)

# COMMAND ----------

sql_out = spark.sql(f"""
select count(distinct SAK_RECIP) AS Recipient_EDW_Recipient_Analytics_distinct_SAK_RECIP
from {catalog}.{schema_name}.{EDW_TblNm}
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(distinct ID_MEDICAID) AS Recipient_EDW_Recipient_Analytics_distinct_ID_MEDICAID
from {catalog}.{schema_name}.{EDW_TblNm}
;
               """)
display(sql_out)
sql_out = spark.sql(f"""
select count(*) AS Recipient_EDW_Recipient_Analytics_count
from {catalog}.{schema_name}.{EDW_TblNm}
;
               """)
display(sql_out)


# COMMAND ----------

sql_out = spark.sql(f"""
WITH
AllRecip AS 
(
    select ID_MEDICAID, 
           count(distinct DTE_BIRTH_NBR) CntsDiscDob
           ,max(DTE_BIRTH_NBR) MaxDoB
           ,min(DTE_BIRTH_NBR) MinDoB
           ,max(DTE_BIRTH_NBR)- min(DTE_BIRTH_NBR) DiffDays
      from {catalog}.{schema_name}.edw_ven114fa_historic
     WHERE EXTRACTION_DATE >= ADD_MONTHS(CURRENT_TIMESTAMP(),-1) -- 1= Mo.Gap
  group by 1
  order by 5 desc
)
select 
count(DISTINCT ID_MEDICAID) AS Recipient_count_distinct_medicaid_id_Issue_114
--
--max(MaxDoB), min(MaxDoB), max(MinDoB), min(MinDoB)
--max(DiffDays), min(DiffDays)
  from AllRecip
 where DiffDays > 28 -- 28= Days Gap
;
               """)
display(sql_out)
