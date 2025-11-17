# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     Elig_PartD_CompareTwoAnalyticsTables.                                                                            *
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
#* 11/04/2025 CCRB70930/CO#43342  Jaime Zavala        Initial Release.                                                              *
#* 11/17/2025 CCRB70930/CO#43342  Jaime Zavala        Modified to met QA's Spreadsheet.                                             *
#************************************************************************************************************************************


# COMMAND ----------

#-----------
# DBX Parms
#-----------
dbutils.widgets.text('catalog', 'oh_apm_stg')  
dbutils.widgets.text('schema_name', 'etl_qa')  

#-------------
#  Tables
#-------------

dbutils.widgets.text('DiffAnlysTblNm', 'EligAnlytcs_Part_D_DiffAnlys')
dbutils.widgets.text('PrevTblNm', 'Eligibility_Analytics_Previous')
dbutils.widgets.text('CurntTblNm', 'Eligibility_Analytics_Current')

#-------------------
# Getters Section
#-------------------
catalog     = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
DiffAnlysTblNm = dbutils.widgets.get('DiffAnlysTblNm')
CurntTblNm = dbutils.widgets.get('CurntTblNm')
PrevTblNm = dbutils.widgets.get('PrevTblNm')


#---------------
# Print Section
#---------------
print("catalog:", catalog)
print("schema:", schema_name)
print("Diff Analysis Table Name:", DiffAnlysTblNm)
print("Current Table Name:", CurntTblNm)
print("Previous Table Name:", PrevTblNm)


# COMMAND ----------

# DBTITLE 1,Plant Specs
# MAGIC
# MAGIC %sql
# MAGIC create or replace table ${catalog}.${schema_name}.${DiffAnlysTblNm} AS 
# MAGIC SELECT 
# MAGIC     -- Common identifiers
# MAGIC      COALESCE(PrevTbl.ID_MEDICAID1, CurntTbl.ID_MEDICAID1) AS ID_MEDICAID1          -- This is the common identifier
# MAGIC     ,COALESCE(PrevTbl.D_DTE_EFFECTIVE, CurntTbl.D_DTE_EFFECTIVE) AS D_DTE_EFFECTIVE -- This is the common identifier
# MAGIC 	-- None Compare Fileds
# MAGIC     ,CASE WHEN PrevTbl.Derived1                  != CurntTbl.Derived1                  THEN PrevTbl.Derived1                  ELSE NULL END AS PrevTbl_Derived1    -- REPORT_DTE
# MAGIC     ,CASE WHEN PrevTbl.Derived1                  != CurntTbl.Derived1                  THEN CurntTbl.Derived1                 ELSE NULL END AS CurntTbl_Derived1    
# MAGIC     -- Compare Fields
# MAGIC     ,CASE WHEN PrevTbl.Derived2                  != CurntTbl.Derived2                  THEN PrevTbl.Derived2                  ELSE NULL END AS PrevTbl_Derived2    -- ENRL_SPAN_TYP
# MAGIC     ,CASE WHEN PrevTbl.Derived2                  != CurntTbl.Derived2                  THEN CurntTbl.Derived2                 ELSE NULL END AS CurntTbl_Derived2    
# MAGIC     ,CASE WHEN PrevTbl.NUM_CASE                  != CurntTbl.NUM_CASE                  THEN PrevTbl.NUM_CASE                  ELSE NULL END AS PrevTbl_NUM_CASE
# MAGIC     ,CASE WHEN PrevTbl.NUM_CASE                  != CurntTbl.NUM_CASE                  THEN CurntTbl.NUM_CASE                 ELSE NULL END AS CurntTbl_NUM_CASE
# MAGIC     ,CASE WHEN PrevTbl.D_DTE_END                 != CurntTbl.D_DTE_END                 THEN PrevTbl.D_DTE_END                 ELSE NULL END AS PrevTbl_D_DTE_END
# MAGIC     ,CASE WHEN PrevTbl.D_DTE_END                 != CurntTbl.D_DTE_END                 THEN CurntTbl.D_DTE_END                ELSE NULL END AS CurntTbl_D_DTE_END
# MAGIC     ,CASE WHEN PrevTbl.D_CDE_AID_CATEGORY        != CurntTbl.D_CDE_AID_CATEGORY        THEN PrevTbl.D_CDE_AID_CATEGORY        ELSE NULL END AS PrevTbl_D_CDE_AID_CATEGORY
# MAGIC     ,CASE WHEN PrevTbl.D_CDE_AID_CATEGORY        != CurntTbl.D_CDE_AID_CATEGORY        THEN CurntTbl.D_CDE_AID_CATEGORY       ELSE NULL END AS CurntTbl_D_CDE_AID_CATEGORY
# MAGIC     ,CASE WHEN PrevTbl.D_CDE_PGM_HEALTH          != CurntTbl.D_CDE_PGM_HEALTH          THEN PrevTbl.D_CDE_PGM_HEALTH          ELSE NULL END AS PrevTbl_D_CDE_PGM_HEALTH
# MAGIC     ,CASE WHEN PrevTbl.D_CDE_PGM_HEALTH          != CurntTbl.D_CDE_PGM_HEALTH          THEN CurntTbl.D_CDE_PGM_HEALTH         ELSE NULL END AS CurntTbl_D_CDE_PGM_HEALTH
# MAGIC            FROM ${catalog}.${schema_name}.${CurntTblNm} CurntTbl
# MAGIC FULL OUTER JOIN ${catalog}.${schema_name}.${PrevTblNm} PrevTbl ON PrevTbl.ID_MEDICAID1    = CurntTbl.ID_MEDICAID1 
# MAGIC                                                               AND PrevTbl.D_DTE_EFFECTIVE = CurntTbl.D_DTE_EFFECTIVE
# MAGIC WHERE 
# MAGIC        PrevTbl.NUM_CASE                  != CurntTbl.NUM_CASE                  
# MAGIC     OR PrevTbl.Derived2                  != CurntTbl.Derived2                     
# MAGIC     OR PrevTbl.D_DTE_END                 != CurntTbl.D_DTE_END                
# MAGIC     OR PrevTbl.D_CDE_AID_CATEGORY        != CurntTbl.D_CDE_AID_CATEGORY                   
# MAGIC     OR PrevTbl.D_CDE_PGM_HEALTH          != CurntTbl.D_CDE_PGM_HEALTH
# MAGIC    AND PrevTbl.D_DTE_EFFECTIVE is not null
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Plan Count
# MAGIC %sql
# MAGIC  select count(*) from ${catalog}.${schema_name}.${DiffAnlysTblNm};

# COMMAND ----------

# DBTITLE 1,Plan Data Profile
# MAGIC %sql
# MAGIC  select * from ${catalog}.${schema_name}.${DiffAnlysTblNm};

# COMMAND ----------

# DBTITLE 1,Summary Plan
# MAGIC %sql
# MAGIC WITH
# MAGIC   Sum_Total AS (
# MAGIC     SELECT count(*) cntTotal
# MAGIC     FROM ${catalog}.${schema_name}.${DiffAnlysTblNm}
# MAGIC     WHERE TRUE 
# MAGIC   ),
# MAGIC   Cnt_Derived2 AS (
# MAGIC     SELECT COUNT(*) AS Diff_Derived2
# MAGIC     FROM ${catalog}.${schema_name}.${DiffAnlysTblNm}
# MAGIC     WHERE CurntTbl_Derived2 IS NOT NULL
# MAGIC        OR PrevTbl_Derived2 IS NOT NULL
# MAGIC   ),
# MAGIC   Cnt_NUM_CASE AS (
# MAGIC     SELECT COUNT(*) AS Diff_NUM_CASE
# MAGIC     FROM ${catalog}.${schema_name}.${DiffAnlysTblNm}
# MAGIC     WHERE CurntTbl_NUM_CASE IS NOT NULL
# MAGIC        OR PrevTbl_NUM_CASE IS NOT NULL
# MAGIC   ),
# MAGIC   Cnt_D_DTE_END AS (
# MAGIC     SELECT COUNT(*) AS Diff_D_DTE_END
# MAGIC     FROM ${catalog}.${schema_name}.${DiffAnlysTblNm}
# MAGIC     WHERE CurntTbl_D_DTE_END IS NOT NULL
# MAGIC        OR PrevTbl_D_DTE_END IS NOT NULL
# MAGIC   ),
# MAGIC   Cnt_D_CDE_AID_CATEGORY AS (
# MAGIC     SELECT COUNT(*) AS Diff_D_CDE_AID_CATEGORY
# MAGIC     FROM ${catalog}.${schema_name}.${DiffAnlysTblNm}
# MAGIC     WHERE CurntTbl_D_CDE_AID_CATEGORY IS NOT NULL
# MAGIC        OR PrevTbl_D_CDE_AID_CATEGORY IS NOT NULL
# MAGIC   ),
# MAGIC   Cnt_D_CDE_PGM_HEALTH AS (
# MAGIC     SELECT COUNT(*) AS Diff_D_CDE_PGM_HEALTH
# MAGIC     FROM ${catalog}.${schema_name}.${DiffAnlysTblNm}
# MAGIC     WHERE CurntTbl_D_CDE_PGM_HEALTH IS NOT NULL
# MAGIC        OR PrevTbl_D_CDE_PGM_HEALTH IS NOT NULL
# MAGIC   )
# MAGIC
# MAGIC SELECT
# MAGIC   format_number(t1.Diff_Derived2, 0)                        AS Diff_Derived2,
# MAGIC   format_number(CASE WHEN t6.cntTotal = 0 THEN 0
# MAGIC                   ELSE t1.Diff_Derived2 * 100 / t6.cntTotal
# MAGIC              END, '###.#')                                  AS Pcntg_Derived2,
# MAGIC   format_number(t2.Diff_NUM_CASE, 0)                        AS Diff_NUM_CASE,
# MAGIC   format_number(CASE WHEN t6.cntTotal = 0 THEN 0
# MAGIC                   ELSE t2.Diff_NUM_CASE * 100 / t6.cntTotal
# MAGIC              END, '###.#')                                  AS Pcntg_NUM_CASE,
# MAGIC   format_number(t3.Diff_D_DTE_END, 0)                       AS Diff_D_DTE_END,
# MAGIC   format_number(CASE WHEN t6.cntTotal = 0 THEN 0
# MAGIC                   ELSE t3.Diff_D_DTE_END * 100 / t6.cntTotal
# MAGIC              END, '###.#')                                  AS Pcntg_D_DTE_END,
# MAGIC   format_number(t4.Diff_D_CDE_AID_CATEGORY, 0)              AS Diff_D_CDE_AID_CATEGORY,
# MAGIC   format_number(CASE WHEN t6.cntTotal = 0 THEN 0
# MAGIC                   ELSE t4.Diff_D_CDE_AID_CATEGORY * 100 / t6.cntTotal
# MAGIC              END, '###.#')                                  AS Pcntg_D_CDE_AID_CATEGORY,
# MAGIC   format_number(t5.Diff_D_CDE_PGM_HEALTH, 0)                AS Diff_D_CDE_PGM_HEALTH,
# MAGIC   format_number(CASE WHEN t6.cntTotal = 0 THEN 0
# MAGIC                   ELSE t5.Diff_D_CDE_PGM_HEALTH * 100 / t6.cntTotal
# MAGIC              END, '###.#')                                  AS Pcntg_D_CDE_PGM_HEALTH,
# MAGIC   format_number(t6.cntTotal, 0)                             AS cntTotal
# MAGIC FROM
# MAGIC   Cnt_Derived2 t1,
# MAGIC   Cnt_NUM_CASE t2,
# MAGIC   Cnt_D_DTE_END t3,
# MAGIC   Cnt_D_CDE_AID_CATEGORY t4,
# MAGIC   Cnt_D_CDE_PGM_HEALTH t5,
# MAGIC   Sum_Total t6
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Total Counts
# MAGIC %sql
# MAGIC WITH
# MAGIC   Match_Counts AS (
# MAGIC     SELECT DISTINCT PrevTbl.ID_MEDICAID1
# MAGIC                    ,PrevTbl.D_DTE_EFFECTIVE 
# MAGIC           FROM ${catalog}.${schema_name}.${CurntTblNm} CurntTbl
# MAGIC           JOIN ${catalog}.${schema_name}.${PrevTblNm} PrevTbl ON PrevTbl.ID_MEDICAID1    = CurntTbl.ID_MEDICAID1 
# MAGIC                                                               AND PrevTbl.D_DTE_EFFECTIVE = CurntTbl.D_DTE_EFFECTIVE
# MAGIC     WHERE TRUE  
# MAGIC       AND PrevTbl.D_DTE_EFFECTIVE is not null
# MAGIC   ),
# MAGIC   Get_CntsNew_VsOld AS (
# MAGIC     SELECT COUNT(*) AS CntNew_VsOld
# MAGIC     FROM ${catalog}.${schema_name}.${CurntTblNm} CurntTbl
# MAGIC     WHERE TRUE
# MAGIC       AND CurntTbl.D_DTE_EFFECTIVE is not null
# MAGIC       AND NOT EXISTS
# MAGIC        (
# MAGIC         SELECT 1
# MAGIC           FROM Match_Counts t1
# MAGIC          WHERE TRUE 
# MAGIC            AND CurntTbl.ID_MEDICAID1    = t1.ID_MEDICAID1
# MAGIC            AND CurntTbl.D_DTE_EFFECTIVE = t1.D_DTE_EFFECTIVE
# MAGIC        )
# MAGIC   ),
# MAGIC   Get_CntsOld_VsNew AS (
# MAGIC     SELECT COUNT(*) AS CntOld_VsNew
# MAGIC     FROM ${catalog}.${schema_name}.${PrevTblNm} PrevTbl
# MAGIC     WHERE TRUE
# MAGIC        AND PrevTbl.D_DTE_EFFECTIVE is not null
# MAGIC       AND NOT EXISTS
# MAGIC        (
# MAGIC         SELECT 1
# MAGIC           FROM Match_Counts t4
# MAGIC          WHERE TRUE 
# MAGIC            AND PrevTbl.ID_MEDICAID1    = t4.ID_MEDICAID1
# MAGIC            AND PrevTbl.D_DTE_EFFECTIVE = t4.D_DTE_EFFECTIVE
# MAGIC        )
# MAGIC   )
# MAGIC SELECT DISTINCT format_number((SELECT COUNT(*) FROM Match_Counts), 0) AS TotalMatch
# MAGIC      , format_number(t2.CntNew_VsOld, 0) AS RecordsNewVsOld
# MAGIC      , format_number(t3.CntOld_VsNew, 0) AS RecordsOldVsNew
# MAGIC   FROM Match_Counts t1
# MAGIC      , Get_CntsNew_VsOld t2
# MAGIC      , Get_CntsOld_VsNew t3
# MAGIC ;
