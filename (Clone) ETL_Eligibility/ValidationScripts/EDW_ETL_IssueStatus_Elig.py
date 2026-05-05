# Databricks notebook source
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

# DBTITLE 1,SQL For Eligibility Issues
# MAGIC %sql
# MAGIC -- NO ISSUES OPEN/REFACTORING TO BE IMPLEMENTING IN THE FOLLOWING CODE
# MAGIC -- \echo ========================
# MAGIC -- \echo Eligibility Issue#105 Part-D01
# MAGIC -- /*==========================================
# MAGIC --    Eligibility EXTRACTS COMPARISON - Counts
# MAGIC -- ============================================*/
# MAGIC --         select
# MAGIC --         count(*) "Elig Issue#105 Part-D01"
# MAGIC --         , sum((is_match='T')::int) matches
# MAGIC --         , TRUNC(100*sum((is_match='T')::int)/count(*),2)::number(15,2) "matches %"
# MAGIC --         , sum((is_match='FAIL')::int) unmatches
# MAGIC --         , TRUNC(100*sum((is_match='FAIL')::int)/count(*),2)::number(15,2) "unmatches %"
# MAGIC --         from
# MAGIC --         (
# MAGIC --            select *
# MAGIC --            from
# MAGIC --            (
# MAGIC --                 select distinct
# MAGIC --                 t.ID_MEDICAID1, t.NUM_CASE
# MAGIC --                 , t.ID_MBI_CMS "EDW_ID_MBI_CMS"
# MAGIC --                 , s.ID_MBI_CMS "BIAR_ID_MBI_CMS"
# MAGIC --                 , CASE WHEN (t.ID_MBI_CMS = s.ID_MBI_CMS or NVL(t.ID_MBI_CMS,'') = NVL(s.ID_MBI_CMS,'')) THEN 'T' ELSE 'FAIL' END "IS_MATCH"
# MAGIC --                 from vendor_extracts.EDW_temp_Eligibility_Analytics t
# MAGIC --                 join vendor_extracts.Eligibility_Analytics s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.D_DTE_END = s.D_DTE_END
# MAGIC --                 where t.D_DTE_EFFECTIVE is not null 
# MAGIC --                   and t.D_DTE_END is not null
# MAGIC --                   and s.D_DTE_EFFECTIVE is not null 
# MAGIC --                   and s.D_DTE_END is not null
# MAGIC --            )t where true
# MAGIC --        )t where true
# MAGIC --         ;
# MAGIC -- \echo ========================
# MAGIC -- \echo Eligibility Issue#105 Part-D01 Ppltd Fld
# MAGIC -- WITH
# MAGIC -- SQL_PpltdFieldCnt AS
# MAGIC -- (
# MAGIC --  select count(*) PpltdFieldCnt
# MAGIC --    from vendor_extracts.EDW_VEN116FA_PartD01_Staging
# MAGIC --   where (ID_MBI_CMS is not null 
# MAGIC --          or TRIM(ID_MBI_CMS) <> ''
# MAGIC --          )
# MAGIC -- )
# MAGIC -- ,SQL_TotalCnt AS
# MAGIC -- (
# MAGIC --  select count(*) TotalCnt
# MAGIC --    from vendor_extracts.EDW_VEN116FA_PartD01_Staging
# MAGIC -- )
# MAGIC -- select TRUNC((PpltdFieldCnt * 100 ) / TotalCnt,2)::number(15,2) "Elig Issue#105 Part-D01 Ppltd Fld"
# MAGIC --      -- PpltdFieldCnt
# MAGIC --      -- ,TotalCnt
# MAGIC -- from SQL_PpltdFieldCnt, SQL_TotalCnt
# MAGIC -- ;
# MAGIC -- \echo ========================
# MAGIC -- \echo Eligibility Issue#105 Part-E
# MAGIC -- /*==========================================
# MAGIC --    Eligibility EXTRACTS COMPARISON - Counts
# MAGIC -- ============================================*/
# MAGIC --         select
# MAGIC --         count(*) "Elig Issue#105 Part-E"
# MAGIC --         , sum((is_match='T')::int) matches
# MAGIC --         , TRUNC(100*sum((is_match='T')::int)/count(*),2)::number(15,2) "matches %"
# MAGIC --         , sum((is_match='FAIL')::int) unmatches
# MAGIC --         , TRUNC(100*sum((is_match='FAIL')::int)/count(*),2)::number(15,2) "unmatches %"
# MAGIC --         from
# MAGIC --         (
# MAGIC --            select *
# MAGIC --            from
# MAGIC --            (
# MAGIC --                 select distinct
# MAGIC --                 t.ID_MEDICAID1, t.NUM_CASE
# MAGIC --                 , t.ID_MBI_CMS "EDW_ID_MBI_CMS"
# MAGIC --                 , s.ID_MBI_CMS "BIAR_ID_MBI_CMS"
# MAGIC --                 , CASE WHEN (t.ID_MBI_CMS = s.ID_MBI_CMS or NVL(t.ID_MBI_CMS,'') = NVL(s.ID_MBI_CMS,'')) THEN 'T' ELSE 'FAIL' END "IS_MATCH"
# MAGIC --                 from vendor_extracts.EDW_temp_Eligibility_Analytics t
# MAGIC --                 join vendor_extracts.Eligibility_Analytics s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.E_DTE_END = s.E_DTE_END
# MAGIC --                 where t.E_DTE_EFFECTIVE is not null 
# MAGIC --                   and t.E_DTE_END is not null
# MAGIC --                   and s.E_DTE_EFFECTIVE is not null 
# MAGIC --                   and s.E_DTE_END is not null
# MAGIC --            )t where true
# MAGIC --        )t where true
# MAGIC --         ;
# MAGIC -- \echo ========================
# MAGIC -- \echo Eligibility Issue#105 Part-E Ppltd Fld
# MAGIC -- WITH
# MAGIC -- SQL_PpltdFieldCnt AS
# MAGIC -- (
# MAGIC --  select count(*) PpltdFieldCnt
# MAGIC --    from vendor_extracts.EDW_VEN116FA_PartE_Staging
# MAGIC --   where (ID_MBI_CMS is not null 
# MAGIC --          or TRIM(ID_MBI_CMS) <> ''
# MAGIC --          )
# MAGIC -- )
# MAGIC -- ,SQL_TotalCnt AS
# MAGIC -- (
# MAGIC --  select count(*) TotalCnt
# MAGIC --    from vendor_extracts.EDW_VEN116FA_PartE_Staging
# MAGIC -- )
# MAGIC -- select TRUNC((PpltdFieldCnt * 100 ) / TotalCnt,2)::number(15,2) "Elig Issue#105 Part-E Ppltd Fld"
# MAGIC --      -- PpltdFieldCnt
# MAGIC --      -- ,TotalCnt
# MAGIC -- from SQL_PpltdFieldCnt, SQL_TotalCnt
# MAGIC -- ;
# MAGIC -- \echo ========================
# MAGIC -- \echo Eligibility Issue#105 Part-I
# MAGIC -- /*==========================================
# MAGIC --    Eligibility EXTRACTS COMPARISON - Counts
# MAGIC -- ============================================*/
# MAGIC --         select
# MAGIC --         count(*) "Elig Issue#105 Part-I"
# MAGIC --         , sum((is_match='T')::int) matches
# MAGIC --         , TRUNC(100*sum((is_match='T')::int)/count(*),2)::number(15,2) "matches %"
# MAGIC --         , sum((is_match='FAIL')::int) unmatches
# MAGIC --         , TRUNC(100*sum((is_match='FAIL')::int)/count(*),2)::number(15,2) "unmatches %"
# MAGIC --         from
# MAGIC --         (
# MAGIC --            select *
# MAGIC --            from
# MAGIC --            (
# MAGIC --                 select distinct
# MAGIC --                 t.ID_MEDICAID1, t.NUM_CASE
# MAGIC --                 , t.ID_MBI_CMS "EDW_ID_MBI_CMS"
# MAGIC --                 , s.ID_MBI_CMS "BIAR_ID_MBI_CMS"
# MAGIC --                 , CASE WHEN (t.ID_MBI_CMS = s.ID_MBI_CMS or NVL(t.ID_MBI_CMS,'') = NVL(s.ID_MBI_CMS,'')) THEN 'T' ELSE 'FAIL' END "IS_MATCH"
# MAGIC --                 from vendor_extracts.EDW_temp_Eligibility_Analytics t
# MAGIC --                 join vendor_extracts.Eligibility_Analytics s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.I_DTE_END = s.I_DTE_END
# MAGIC --                 where t.I_DTE_EFFECTIVE is not null 
# MAGIC --                   and t.I_DTE_END is not null
# MAGIC --                   and s.I_DTE_EFFECTIVE is not null 
# MAGIC --                   and s.I_DTE_END is not null
# MAGIC --            )t where true
# MAGIC --        )t where true
# MAGIC --         ;
# MAGIC -- \echo ========================
# MAGIC -- \echo Eligibility Issue#105 Part-I Ppltd Fld
# MAGIC -- WITH
# MAGIC -- SQL_PpltdFieldCnt AS
# MAGIC -- (
# MAGIC --  select count(*) PpltdFieldCnt
# MAGIC --    from vendor_extracts.EDW_VEN116FA_PartI_Staging
# MAGIC --   where (ID_MBI_CMS is not null 
# MAGIC --          or TRIM(ID_MBI_CMS) <> ''
# MAGIC --          )
# MAGIC -- )
# MAGIC -- ,SQL_TotalCnt AS
# MAGIC -- (
# MAGIC --  select count(*) TotalCnt
# MAGIC --    from vendor_extracts.EDW_VEN116FA_PartI_Staging
# MAGIC -- )
# MAGIC -- select TRUNC((PpltdFieldCnt * 100 ) / TotalCnt,2)::number(15,2) "Elig Issue#105 Part-I Ppltd Fld"
# MAGIC --      -- PpltdFieldCnt
# MAGIC --      -- ,TotalCnt
# MAGIC -- from SQL_PpltdFieldCnt, SQL_TotalCnt
# MAGIC -- ;
# MAGIC -- \echo ========================
# MAGIC -- \echo Eligibility Issue#105 Part-L
# MAGIC -- /*==========================================
# MAGIC --    Eligibility EXTRACTS COMPARISON - Counts
# MAGIC -- ============================================*/
# MAGIC --         select
# MAGIC --         count(*) "Elig Issue#105 Part-L"
# MAGIC --         , sum((is_match='T')::int) matches
# MAGIC --         , TRUNC(100*sum((is_match='T')::int)/count(*),2)::number(15,2) "matches %"
# MAGIC --         , sum((is_match='FAIL')::int) unmatches
# MAGIC --         , TRUNC(100*sum((is_match='FAIL')::int)/count(*),2)::number(15,2) "unmatches %"
# MAGIC --         from
# MAGIC --         (
# MAGIC --            select *
# MAGIC --            from
# MAGIC --            (
# MAGIC --                 select distinct
# MAGIC --                 t.ID_MEDICAID1, t.NUM_CASE
# MAGIC --                 , t.ID_MBI_CMS "EDW_ID_MBI_CMS"
# MAGIC --                 , s.ID_MBI_CMS "BIAR_ID_MBI_CMS"
# MAGIC --                 , CASE WHEN (t.ID_MBI_CMS = s.ID_MBI_CMS or NVL(t.ID_MBI_CMS,'') = NVL(s.ID_MBI_CMS,'')) THEN 'T' ELSE 'FAIL' END "IS_MATCH"
# MAGIC --                 from vendor_extracts.EDW_temp_Eligibility_Analytics t
# MAGIC --                 join vendor_extracts.Eligibility_Analytics s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.L_DTE_END = s.L_DTE_END
# MAGIC --                 where t.L_DTE_EFFECTIVE is not null 
# MAGIC --                   and t.L_DTE_END is not null
# MAGIC --                   and s.L_DTE_EFFECTIVE is not null 
# MAGIC --                   and s.L_DTE_END is not null
# MAGIC --            )t where true
# MAGIC --        )t where true
# MAGIC --         ;
# MAGIC -- \echo ========================
# MAGIC -- \echo Eligibility Issue#105 Part-L Ppltd Fld
# MAGIC -- WITH
# MAGIC -- SQL_PpltdFieldCnt AS
# MAGIC -- (
# MAGIC --  select count(*) PpltdFieldCnt
# MAGIC --    from vendor_extracts.EDW_VEN116FA_PartL_Staging
# MAGIC --   where (ID_MBI_CMS is not null 
# MAGIC --          or TRIM(ID_MBI_CMS) <> ''
# MAGIC --          )
# MAGIC -- )
# MAGIC -- ,SQL_TotalCnt AS
# MAGIC -- (
# MAGIC --  select count(*) TotalCnt
# MAGIC --    from vendor_extracts.EDW_VEN116FA_PartL_Staging
# MAGIC -- )
# MAGIC -- select TRUNC((PpltdFieldCnt * 100 ) / TotalCnt,2)::number(15,2) "Elig Issue#105 Part-L Ppltd Fld"
# MAGIC --      -- PpltdFieldCnt
# MAGIC --      -- ,TotalCnt
# MAGIC -- from SQL_PpltdFieldCnt, SQL_TotalCnt
# MAGIC -- ;
# MAGIC -- \echo ========================
# MAGIC -- \echo Eligibility Issue#102 Part-N
# MAGIC -- /*==========================================
# MAGIC --    Eligibility EXTRACTS COMPARISON - Counts
# MAGIC -- ============================================*/
# MAGIC -- 
# MAGIC --         select
# MAGIC --         count(*) "Elig Issue#102 Part-N"
# MAGIC --         , sum((is_match='T')::int) matches
# MAGIC --         , TRUNC(100*sum((is_match='T')::int)/count(*),2)::number(15,2) "matches %"
# MAGIC --         , sum((is_match='FAIL')::int) unmatches
# MAGIC --         , TRUNC(100*sum((is_match='FAIL')::int)/count(*),2)::number(15,2) "unmatches %"
# MAGIC -- 
# MAGIC --         from
# MAGIC --         (
# MAGIC -- 
# MAGIC --            select *
# MAGIC --            from
# MAGIC --            (
# MAGIC --                 select distinct
# MAGIC --                 t.ID_MEDICAID1, t.NUM_CASE
# MAGIC -- 
# MAGIC --                 , t.N_CDE_COVERAGE "EDW_N_CDE_COVERAGE"
# MAGIC --                 , s.N_CDE_COVERAGE "BIAR_N_CDE_COVERAGE"
# MAGIC --                 , CASE WHEN (t.N_CDE_COVERAGE = TRIM(s.N_CDE_COVERAGE) or NVL(t.N_CDE_COVERAGE,'') = NVL(s.N_CDE_COVERAGE,'')) THEN 'T' ELSE 'FAIL' END "IS_MATCH"
# MAGIC -- 
# MAGIC --                 from vendor_extracts.EDW_temp_Eligibility_Analytics t
# MAGIC -- 
# MAGIC --                 join vendor_extracts.Eligibility_Analytics s on t.ID_MEDICAID1 = s.ID_MEDICAID1 and t.NUM_CASE = s.NUM_CASE and t.N_DTE_TPL_EFFECTIVE = s.N_DTE_TPL_EFFECTIVE
# MAGIC --                 
# MAGIC --                 where t.N_DTE_TPL_EFFECTIVE is not null 
# MAGIC --                   and t.N_DTE_TPL_END is not null
# MAGIC --                   and s.N_DTE_TPL_EFFECTIVE is not null 
# MAGIC --                   and s.N_DTE_TPL_END is not null
# MAGIC --                 		   
# MAGIC --            )t where true
# MAGIC -- 
# MAGIC --        )t where true
# MAGIC -- 
# MAGIC -- 
# MAGIC --         ;
# MAGIC -- \echo ========================
# MAGIC -- \echo Eligibility Issue#102 Part-N Ppltd Fld
# MAGIC -- WITH
# MAGIC -- SQL_PpltdFieldCnt AS
# MAGIC -- (
# MAGIC --  select count(*) PpltdFieldCnt
# MAGIC --    from vendor_extracts.EDW_VEN116FA_PartN_Staging
# MAGIC --   where (CDE_COVERAGE is not null 
# MAGIC --          or TRIM(CDE_COVERAGE) <> ''
# MAGIC --          )
# MAGIC -- )
# MAGIC -- ,SQL_TotalCnt AS
# MAGIC -- (
# MAGIC --  select count(*) TotalCnt
# MAGIC --    from vendor_extracts.EDW_VEN116FA_PartN_Staging
# MAGIC -- )
# MAGIC -- select TRUNC((PpltdFieldCnt * 100 ) / TotalCnt,2)::number(15,2) "Elig Issue#102 Part-N Ppltd Fld"
# MAGIC --      -- PpltdFieldCnt
# MAGIC --      -- ,TotalCnt
# MAGIC -- from SQL_PpltdFieldCnt, SQL_TotalCnt
# MAGIC -- ;
# MAGIC
