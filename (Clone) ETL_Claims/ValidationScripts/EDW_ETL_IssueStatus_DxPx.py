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

# DBTITLE 1,SQL For Diagnosis Issues
# MAGIC %sql -- NO ISSUES OPEN 

# COMMAND ----------

# DBTITLE 1,SQL For Procedures Issues
# MAGIC %sql -- NO ISSUES OPEN /REFACTORING TO BE IMPLEMENTING IN THE FOLLOWING CODE
# MAGIC -- \echo ========================
# MAGIC -- \echo Procedures Issue#103
# MAGIC -- /*==========================================
# MAGIC --    Procedure Codes  EXTRACTS COMPARISON - Counts
# MAGIC -- ============================================*/
# MAGIC --         select
# MAGIC --         count(*) "Px Issue#103"
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
# MAGIC --                 t.NUM_ICN, t.ID_MEDICAID, t.P_NUM_SEQ
# MAGIC --                 , t.CDE_PROC_ICD9 "EDW_CDE_PROC_ICD9"
# MAGIC --                 , s.CDE_PROC_ICD9 "BIAR_CDE_PROC_ICD9"
# MAGIC --                 , CASE WHEN (t.CDE_PROC_ICD9 = s.CDE_PROC_ICD9 or NVL(t.CDE_PROC_ICD9,'') = NVL(s.CDE_PROC_ICD9,'')) THEN 'T' ELSE 'FAIL' END "IS_MATCH"
# MAGIC --                 from vendor_extracts.EDW_temp_DiagSurgProcValCodes_Analytics t
# MAGIC --                 join vendor_extracts.DiagSurgProcValCodes_Analytics s on t.NUM_ICN = s.NUM_ICN and trim(t.P_NUM_SEQ) = trim(s.P_NUM_SEQ)
# MAGIC --                 where t.DERIVED = 'P'
# MAGIC --                   and s.DERIVED = 'P'
# MAGIC -- 				  and s.DTE_PAID::date < '20230127'::date
# MAGIC --            )t where true
# MAGIC --        )t where true
# MAGIC --         ;
# MAGIC -- \echo ========================
# MAGIC -- \echo Procedures Issue#103 Ppltd Fld
# MAGIC -- WITH
# MAGIC -- SQL_PpltdFieldCnt AS
# MAGIC -- (
# MAGIC --  select count(*) PpltdFieldCnt
# MAGIC --    from vendor_extracts.EDW_VEN12301FA_Staging t
# MAGIC --   where TRUE 
# MAGIC --     and ICD_9_CM_PROC_CD is not null
# MAGIC --     and TRIM(ICD_9_CM_PROC_CD) != 'NOTFND'
# MAGIC -- )
# MAGIC -- ,SQL_TotalCnt AS
# MAGIC -- (
# MAGIC --  select count(*) TotalCnt
# MAGIC --    from vendor_extracts.EDW_VEN12301FA_Staging t
# MAGIC --   where TRUE 
# MAGIC -- )
# MAGIC -- select CASE WHEN TotalCnt > 0 THEN TRUNC((PpltdFieldCnt * 100 ) / TotalCnt,2)::number(15,2) ELSE 0.00 END "Px Issue#103 Ppltd Fld"
# MAGIC --      -- PpltdFieldCnt
# MAGIC --      -- ,TotalCnt
# MAGIC -- from SQL_PpltdFieldCnt, SQL_TotalCnt
# MAGIC -- ;
# MAGIC -- \echo ========================
# MAGIC -- \echo Pharmacy ICN Types
# MAGIC -- \echo ========================
# MAGIC -- select distinct SUBSTRING(TRIM(ICN_NBR), 6, 1) "Phar ICN CharPos 6", count(*) "Phar ICN CharPos 6 Count"
# MAGIC --  from vendor_extracts.EDW_VEN100FA_Staging
# MAGIC -- where UPPER(TRIM(CLM_TYP_CD)) in (
# MAGIC -- 'P','Q'
# MAGIC -- )
# MAGIC -- group by 1
# MAGIC -- order by 1
# MAGIC -- ;
# MAGIC
