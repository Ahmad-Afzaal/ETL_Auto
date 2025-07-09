# Databricks notebook source
#************************************************************************************************************************************
#*                                                                                                                                  *
#*   NOTEBOOK:     ETL_Provider_Analytics.                                                                                          *
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
#* 06/10/2024            Jaime Zavala        ID_PROVIDER_NPI FileSrcChng from VEN117FA4 to VEN117FA1.                               *
#* 10/08/2024            Jaime Zavala        NA0 NewMapping to VEN117FA1.PROV_TYP_NAME                                              *
#*                                           NA0 NewMapping to Provider_Analytics.PROV_TYP_NAME                                     *
#*                                           Implement Liquid Clustering Logic.                                                     *
#************************************************************************************************************************************

# COMMAND ----------

dbutils.widgets.text('catalog', 'oh_apm_stg')
dbutils.widgets.text('schema_name', 'vendor_extracts')
dbutils.widgets.text('ven117fa', 'ven117fa')
dbutils.widgets.text('Prov_Analytics', 'Provider_Analytics')

# COMMAND ----------

catalog = dbutils.widgets.get('catalog')
schema_name = dbutils.widgets.get('schema_name')
ven117fa = dbutils.widgets.get('ven117fa')
Prov_Analytics = dbutils.widgets.get('Prov_Analytics')


print(catalog)
print(schema_name)
print(ven117fa)
print(Prov_Analytics)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table  ${catalog}.${schema_name}.${ven117fa} AS
# MAGIC SELECT DISTINCT
# MAGIC     CAST(ProvExtrct.SAK_PROV AS VARCHAR(9)) AS SAK_PROV,
# MAGIC     TRIM(ProvExtrct.MEDICAID_ID) AS ID_PROVIDER_MCAID1,
# MAGIC     TRIM(ProvExtrct.APPL_PROV_NAME) AS NAME,
# MAGIC     TRIM(ProvExtrct.SL_OGRIP_CNTY_CD) AS CDE_COUNTY,
# MAGIC     CAST(NULL AS STRING) AS IND_OUT_STATE,  -- Removed field represented as NULL
# MAGIC     CAST(ProvExtrct.APPL_RECV_DT AS DATE) AS DTE_RECEIVED,
# MAGIC     CAST(ProvFrm1099Extrct.END_DT AS DATE) AS DTE_CYCLE,
# MAGIC     CAST(NULL AS STRING) AS ID_CLERK,  -- Removed field
# MAGIC     TRIM(ProvExtrct.TAX_ID) AS NUM_TAX_ID,
# MAGIC     TRIM(ProvFrm1099Extrct.IRS_TAX_ID) AS NUM_PROV_SSN,
# MAGIC     TRIM(ProvExtrct.NPI) AS ID_PROVIDER_NPI,
# MAGIC     TRIM(ProvExtrct.MMIS_PROV_TYP_ID) AS CDE_PROV_ID_TYPE,
# MAGIC     CAST(ProvLicnsExtrct.IS_LIC_VERIFIED AS STRING) AS IND_NPI_VERIFY,
# MAGIC     CAST(NULL AS STRING) AS ID_FACILITY,  -- Removed field
# MAGIC     CAST(NULL AS STRING) AS ID_ICDS_PROV_CONTRACT,  -- Removed field
# MAGIC     CAST(NULL AS STRING) AS DERIVED1,  -- Empty field
# MAGIC     TRIM(ProvExtrct.PROV_TYP_NAME) AS NA0,    -- New Mapping
# MAGIC     CAST(NULL AS STRING) AS DERIVED2,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS DTE_PR_ID_EFF,  -- Removed field
# MAGIC     CAST(NULL AS STRING) AS DTE_PR_ID_END,  -- Removed field
# MAGIC     CAST(NULL AS STRING) AS IND_HEALTHCARE,  -- Removed field
# MAGIC     CAST(NULL AS STRING) AS DERIVED3,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS DERIVED4,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA1,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA2,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA3,  -- Empty field
# MAGIC     CAST(ProvExtrct.ELCTRC_SGNTR_DT AS DATE) AS DTE_ELECTRONIC_SIGNATURE,
# MAGIC     CAST(NULL AS STRING) AS NA4,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA5,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA6,  -- Empty field
# MAGIC     TRIM(ProvExtrct.TXT_ACA_PMT_CNFRM) AS TXT_ACA_PAYMENT_CONFIRM,
# MAGIC     CAST(NULL AS STRING) AS AMT_ACA_FEE,  -- Removed field
# MAGIC     CAST(NULL AS STRING) AS NA7,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA8,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA9,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA10,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA11,  -- Empty field
# MAGIC     TRIM(ProvExtrct.MEDICAID_ID) AS ID_PROVIDER1,
# MAGIC     TRIM(ProvExtrct.MEDICAID_ID) AS ID_PROVIDER2,
# MAGIC     TRIM(ProvExtrct.MMIS_PROV_TYP_ID) AS CDE_PROV_TYPE,
# MAGIC     CAST(NULL AS STRING) AS CDE_ORGANIZ,  -- Removed field
# MAGIC     TRIM(ProvLicnsExtrct.LIC_TYP_CD) AS CDE_LIC_TYPE,
# MAGIC     TRIM(ProvLicnsExtrct.LIC_NBR) AS NUM_PROV_LIC,
# MAGIC     CAST(ProvLicnsExtrct.EFF_DT AS DATE) AS DTE_EFFECTIVE1,
# MAGIC     CAST(ProvLicnsExtrct.EFF_DT AS DATE) AS DTE_EFFECTIVE2,
# MAGIC     CAST(ProvLicnsExtrct.END_DT AS DATE) AS DTE_END,
# MAGIC     CAST(NULL AS STRING) AS NA12,  -- Empty field
# MAGIC     CAST(ProvExtrct.ELCTRC_SGNTR_DT AS DATE) AS DTE_EFF_MCD_AGREEMENT,
# MAGIC     CAST(NULL AS STRING) AS DTE_END_MCD_AGREEMENT,  -- Removed field
# MAGIC     CAST(NULL AS STRING) AS NA13,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA14,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS DERIVED5,  -- Empty field
# MAGIC     TRIM(ProvDEAExtrct.DEA_NBR) AS NUM_DEA,
# MAGIC     CAST(NULL AS STRING) AS DERIVED6,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS DERIVED7,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA15,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA16,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA17,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS DERIVED8,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA18,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS DERIVED9,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS DERIVED10,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA19,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA20,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA21,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA22,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA23,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA24,  -- Empty field
# MAGIC     NVL(TRIM(ProvExtrct.PROV_CNTCT_NBR), '') AS NUM_PHONE,
# MAGIC     CAST(NULL AS STRING) AS NA25,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS NA26,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS DERIVED11,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS DERIVED12,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS CDE_PROV_PGM,  -- Removed field
# MAGIC     NVL(ProvAddrssExtrct.OGRIP_LONGT_NBR, 0.000000) AS NUM_LONGITUDE,
# MAGIC     NVL(ProvAddrssExtrct.OGRIP_LAT_NBR, 0.000000) AS NUM_LATITUDE,
# MAGIC     TRIM(ProvExtrct.TAX_ID_TYP) AS IND_TAX_ID_TYPE,
# MAGIC     CAST(NULL AS STRING) AS NA27,  -- Empty field
# MAGIC     CAST(NULL AS STRING) AS DTE_EFFECTIVE0,  -- Removed field
# MAGIC         CAST(NULL AS STRING) AS CNT_TOTAL_BEDS1,
# MAGIC     CAST(NULL AS STRING) AS CNT_MCARE_BEDS,
# MAGIC     CAST(NULL AS STRING) AS CNT_MCAID_BEDS,
# MAGIC     CAST(NULL AS STRING) AS CNT_LIC_BEDS1,
# MAGIC     CAST(NULL AS STRING) AS CNT_DUAL_BEDS1,
# MAGIC     CAST(NULL AS STRING) AS CDE_BED_STATUS1,
# MAGIC     CAST(NULL AS STRING) AS CNT_WAIVER_BEDS1,
# MAGIC     CAST(NULL AS STRING) AS CNT_ICFMR_BEDS1,
# MAGIC     CAST(NULL AS STRING) AS CNT_ODH_BEDS1,
# MAGIC     CAST(NULL AS STRING) AS CNT_CINCI_BEDS1,
# MAGIC     CAST(NULL AS STRING) AS CNT_REG_HOSP_BEDS1,
# MAGIC     CAST(NULL AS STRING) AS CNT_OTHER_BEDS1,
# MAGIC     CAST(NULL AS STRING) AS CNT_MRDD_BEDS1,
# MAGIC     CAST(NULL AS STRING) AS CNT_COUNTRY_BEDS1,
# MAGIC     CAST(NULL AS STRING) AS CNT_OOS_BEDS1,
# MAGIC     CAST(NULL AS STRING) AS DTE_EFFECTIVE02,
# MAGIC     CAST(NULL AS STRING) AS CNT_TOTAL_BEDS2,
# MAGIC     CAST(NULL AS STRING) AS CNT_LIC_BEDS2,
# MAGIC     CAST(NULL AS STRING) AS CNT_DUAL_BEDS2,
# MAGIC     CAST(NULL AS STRING) AS CDE_BED_STATUS2,
# MAGIC     CAST(NULL AS STRING) AS CNT_WAIVER_BEDS2,
# MAGIC     CAST(NULL AS STRING) AS CNT_ICFMR_BEDS2,
# MAGIC     CAST(NULL AS STRING) AS CNT_ODH_BEDS2,
# MAGIC     CAST(NULL AS STRING) AS CNT_CINCI_BEDS2,
# MAGIC     CAST(NULL AS STRING) AS CNT_REG_HOSP_BEDS2,
# MAGIC     CAST(NULL AS STRING) AS CNT_OTHER_BEDS2,
# MAGIC     CAST(NULL AS STRING) AS CNT_MRDD_BEDS2,
# MAGIC     CAST(NULL AS STRING) AS CNT_COUNTRY_BEDS2,
# MAGIC     CAST(NULL AS STRING) AS CNT_OOS_BEDS2,
# MAGIC     NVL(CAST(ProvSpecExtrct.EFF_DT1 AS DATE), '0101-01-01') AS DTE_EFFECTIVE3,
# MAGIC     TRIM(ProvSpecExtrct.PROV_SPEC_CD1) AS CDE_PROV_SPEC1,
# MAGIC     NVL(CAST(ProvSpecExtrct.EFF_DT2 AS DATE), '0101-01-01') AS DTE_EFFECTIVE4,
# MAGIC     TRIM(ProvSpecExtrct.PROV_SPEC_CD2) AS CDE_PROV_SPEC2,
# MAGIC     TRIM(ProvAddrssExtrct.S_ADDRESS_STR1) AS ADR_MAIL_STRT1,
# MAGIC     TRIM(ProvAddrssExtrct.S_ADDRESS_STR2) AS ADR_MAIL_STRT2_0,
# MAGIC     TRIM(ProvAddrssExtrct.S_ADDRESS_CITY) AS ADR_MAIL_CITY1,
# MAGIC     TRIM(ProvAddrssExtrct.S_ADDRESS_STATE) AS ADR_MAIL_STATE1,
# MAGIC     TRIM(ProvAddrssExtrct.S_ADDRESS_ZIP) AS ADR_MAIL_ZIP1,
# MAGIC     TRIM(ProvExtrct.PT_ADDR_LINE_1) AS ADR_MAIL_STRT2_1,
# MAGIC     TRIM(ProvExtrct.PT_ADDR_LINE_2) AS ADR_MAIL_STRT2_3,
# MAGIC     TRIM(ProvExtrct.PT_CITY) AS ADR_MAIL_CITY2,
# MAGIC     TRIM(ProvExtrct.PT_STATE) AS ADR_MAIL_STATE2,
# MAGIC     TRIM(ProvExtrct.PT_ZIP_CD || ProvExtrct.PT_ZIP_EXT_CD) AS ADR_MAIL_ZIP2,
# MAGIC     TRIM(ProvExtrct.MT_ADDR_LINE_1) AS ADR_MAIL_STRT3,
# MAGIC     TRIM(ProvExtrct.MT_ADDR_LINE_2) AS ADR_MAIL_STRT2_4,
# MAGIC     TRIM(ProvExtrct.MT_CITY) AS ADR_MAIL_CITY3,
# MAGIC     TRIM(ProvExtrct.MT_STATE) AS ADR_MAIL_STATE3,
# MAGIC     TRIM(ProvExtrct.MT_ZIP_CD || ProvExtrct.MT_ZIP_EXT_CD) AS ADR_MAIL_ZIP3,
# MAGIC     TRIM(ProvCntrctExtrct.ENRL_STS_CD1) AS CDE_ENROLL_STATUS1,
# MAGIC     NVL(CAST(ProvCntrctExtrct.ENRL_DT1 AS DATE), '0101-01-01') AS DTE_EFFECTIVE5,
# MAGIC     TRIM(ProvCntrctExtrct.ENRL_STS_CD2) AS CDE_ENROLL_STATUS2,
# MAGIC     NVL(CAST(ProvCntrctExtrct.ENRL_DT2 AS DATE), '0101-01-01') AS DTE_EFFECTIVE6_1,
# MAGIC     TRIM(ProvCntrctExtrct.ENRL_STS_CD3) AS CDE_ENROLL_STATUS3,
# MAGIC     NVL(CAST(ProvCntrctExtrct.ENRL_DT3 AS DATE), '0101-01-01') AS DTE_EFFECTIVE6_2,
# MAGIC     '000000000' AS NA28,
# MAGIC     CAST(NULL AS STRING) AS AMT_MAX_RECOUP_AR1,
# MAGIC     CAST(NULL AS STRING) AS PCT_RECOUP1,
# MAGIC     CAST(NULL AS STRING) AS CDE_RECOUP_TYPE1,
# MAGIC     CAST(NULL AS STRING) AS AMT_SETUP1,
# MAGIC     '00000000' AS NA29,
# MAGIC     '00000' AS NA30,
# MAGIC     '00000' AS NA31,
# MAGIC     CAST(NULL AS STRING) AS DTE_EFFECTIVE7,
# MAGIC     CAST(NULL AS STRING) AS DTE_GENERIC1,
# MAGIC     '000000000' AS NA32,
# MAGIC     0.00 AS AMT_MAX_RECOUP_AR2,
# MAGIC     0.00 AS PCT_RECOUP2,
# MAGIC     CAST(NULL AS STRING) AS CDE_RECOUP_TYPE2,
# MAGIC     0.00 AS AMT_SETUP2,
# MAGIC     '00000000' AS NA33,
# MAGIC     '00000' AS NA34,
# MAGIC     '00000' AS NA35,
# MAGIC     '0101-01-01' AS DTE_EFFECTIVE8,
# MAGIC     '2299-12-31' AS DTE_GENERIC2,
# MAGIC     '000000000' AS NA36,
# MAGIC     0.00 AS AMT_MAX_RECOUP_AR3,
# MAGIC     0.00 AS PCT_RECOUP3,
# MAGIC     CAST(NULL AS STRING) AS CDE_RECOUP_TYPE3,
# MAGIC     0.00 AS AMT_SETUP3,
# MAGIC     '00000000' AS NA37,
# MAGIC     '00000' AS NA38,
# MAGIC     '00000' AS NA39,
# MAGIC     '0101-01-01' AS DTE_EFFECTIVE9,
# MAGIC     '2299-12-31' AS DTE_GENERIC3,
# MAGIC     '000000000' AS NA40,
# MAGIC     0.00 AS AMT_MAX_RECOUP_AR4,
# MAGIC     0.00 AS PCT_RECOUP4,
# MAGIC     CAST(NULL AS STRING) AS CDE_RECOUP_TYPE4,
# MAGIC     0.00 AS AMT_SETUP4,
# MAGIC     '00000000' AS NA41,
# MAGIC     '00000' AS NA42,
# MAGIC     '00000' AS NA43,
# MAGIC     '0101-01-01' AS DTE_EFFECTIVE10,
# MAGIC     '2299-12-31' AS DTE_GENERIC4,
# MAGIC     '000000000' AS NA44,
# MAGIC     0.00 AS AMT_MAX_RECOUP_AR5,
# MAGIC     0.00 AS PCT_RECOUP5,
# MAGIC     CAST(NULL AS STRING) AS CDE_RECOUP_TYPE5,
# MAGIC     0.00 AS AMT_SETUP5,
# MAGIC     '00000000' AS NA45,
# MAGIC     '00000' AS NA46,
# MAGIC     '00000' AS NA47,
# MAGIC     '0101-01-01' AS DTE_EFFECTIVE11,
# MAGIC     '2299-12-31' AS DTE_GENERIC5,
# MAGIC     CAST(NULL AS STRING) AS DTE_EFF_REVIEW1,
# MAGIC     CAST(NULL AS STRING) AS DTE_END_REVIEW1,
# MAGIC     CAST(NULL AS STRING) AS IND_ON_REVIEW,
# MAGIC     CAST(NULL AS STRING) AS CDE_REVIEW_TYPE,
# MAGIC     '0101-01-01' AS DTE_EFF_REVIEW2,
# MAGIC     '2299-12-31' AS DTE_END_REVIEW2,
# MAGIC     TRIM(ProvTaxnmyExtrct.TXNMY_CD) AS CDE_TAXONOMY,
# MAGIC     ProvTaxnmyExtrct.IS_PRIMARY_FLAG AS IND_PRIMARY,
# MAGIC     NVL(CAST(ProvTaxnmyExtrct.START_DT AS DATE), '0101-01-01') AS DTE_EFFECTIVE12,
# MAGIC     NVL(CAST(ProvTaxnmyExtrct.END_DT AS DATE), '2299-12-31') AS DTE_END1,
# MAGIC     TRIM(ProvAffltnExtrct.MEDICAID_ID_GRP) AS ID_PROVIDER_MCAID2,
# MAGIC     NVL(CAST(ProvAffltnExtrct.START_DT AS DATE), '0101-01-01') AS DTE_EFFECTIVE13,
# MAGIC     NVL(ProvAffltnExtrct.END_DT, CAST('2299-12-31' AS DATE)) AS DTE_END2,
# MAGIC     CAST(NULL AS STRING) AS AMT_RATE_PERCENT, -- Field removed
# MAGIC     '00000' AS NA48,
# MAGIC     '00000' AS NA49,
# MAGIC     '00000' AS NA50,
# MAGIC     CAST(NULL AS STRING) AS DTE_ACTIVE, -- Field removed
# MAGIC     CAST(ProvCLIAExtrct.CLIA_EFF_DT AS DATE) AS DTE_EFFECTIVE14,
# MAGIC     CAST(ProvCLIAExtrct.CLIA_END_DT AS DATE) AS DTE_END3,
# MAGIC     TRIM(ProvCLIAExtrct.CLIA_CERT_TYP) AS CDE_LAB_CODE
# MAGIC FROM  ${catalog}.${schema_name}.EDW_VEN117FA1_Staging AS ProvExtrct
# MAGIC LEFT JOIN (
# MAGIC     SELECT DISTINCT sak_prov,
# MAGIC         MAX(CASE WHEN rnum = 1 THEN ENRL_STS_CD ELSE NULL END) OVER (PARTITION BY sak_prov) AS ENRL_STS_CD1,
# MAGIC         MAX(CASE WHEN rnum = 1 THEN ENRL_DT ELSE NULL END) OVER (PARTITION BY sak_prov) AS ENRL_DT1,
# MAGIC         MAX(CASE WHEN rnum = 2 THEN ENRL_STS_CD ELSE NULL END) OVER (PARTITION BY sak_prov) AS ENRL_STS_CD2,
# MAGIC         MAX(CASE WHEN rnum = 2 THEN ENRL_DT ELSE NULL END) OVER (PARTITION BY sak_prov) AS ENRL_DT2,
# MAGIC         MAX(CASE WHEN rnum = 3 THEN ENRL_STS_CD ELSE NULL END) OVER (PARTITION BY sak_prov) AS ENRL_STS_CD3,
# MAGIC         MAX(CASE WHEN rnum = 3 THEN ENRL_DT ELSE NULL END) OVER (PARTITION BY sak_prov) AS ENRL_DT3
# MAGIC     FROM (
# MAGIC         SELECT *, ROW_NUMBER() OVER (PARTITION BY sak_prov ORDER BY ENRL_END_DT DESC, ENRL_DT ASC) as rnum
# MAGIC         FROM  ${catalog}.${schema_name}.EDW_VEN117FA12_Staging
# MAGIC     ) AS t
# MAGIC ) AS ProvCntrctExtrct ON ProvExtrct.sak_prov = ProvCntrctExtrct.sak_prov
# MAGIC LEFT JOIN (
# MAGIC     SELECT DISTINCT SAK_PROV,
# MAGIC         MAX(CASE WHEN rnum = 1 THEN MMIS_PROV_TYP_ID ELSE NULL END) OVER (PARTITION BY sak_prov) AS MMIS_PROV_TYP_ID,
# MAGIC         MAX(CASE WHEN rnum = 1 THEN PROV_SPEC_CD ELSE NULL END) OVER (PARTITION BY sak_prov) AS PROV_SPEC_CD1,
# MAGIC         MAX(CASE WHEN rnum = 2 THEN PROV_SPEC_CD ELSE NULL END) OVER (PARTITION BY sak_prov) AS PROV_SPEC_CD2,
# MAGIC         MAX(CASE WHEN rnum = 1 THEN EFF_DT ELSE NULL END) OVER (PARTITION BY sak_prov) AS EFF_DT1,
# MAGIC         MAX(CASE WHEN rnum = 2 THEN EFF_DT ELSE NULL END) OVER (PARTITION BY sak_prov) AS EFF_DT2
# MAGIC     FROM (
# MAGIC         SELECT *, ROW_NUMBER() OVER (PARTITION BY SAK_PROV ORDER BY EFF_DT DESC) as rnum
# MAGIC         FROM  ${catalog}.${schema_name}.EDW_VEN117FA3_Staging
# MAGIC     ) AS t
# MAGIC ) AS ProvSpecExtrct ON ProvExtrct.sak_prov = ProvSpecExtrct.sak_prov
# MAGIC LEFT JOIN (
# MAGIC     SELECT DISTINCT SAK_PROV,
# MAGIC         MAX(CASE WHEN TRIM(ADDR_TYP) = 'S' THEN OGRIP_LAT_NBR ELSE NULL END) OVER (PARTITION BY sak_prov ORDER BY SAK_PROV_LOC) AS OGRIP_LAT_NBR,
# MAGIC         MAX(CASE WHEN TRIM(ADDR_TYP) = 'S' THEN OGRIP_LONGT_NBR ELSE NULL END) OVER (PARTITION BY sak_prov ORDER BY SAK_PROV_LOC) AS OGRIP_LONGT_NBR,
# MAGIC         MAX(CASE WHEN TRIM(ADDR_TYP) = 'S' THEN ADDR_STR_1 ELSE NULL END) OVER (PARTITION BY sak_prov ORDER BY SAK_PROV_LOC) AS S_ADDRESS_STR1,
# MAGIC         MAX(CASE WHEN TRIM(ADDR_TYP) = 'S' THEN ADDR_STR_2 ELSE NULL END) OVER (PARTITION BY sak_prov ORDER BY SAK_PROV_LOC) AS S_ADDRESS_STR2,
# MAGIC         MAX(CASE WHEN TRIM(ADDR_TYP) = 'S' THEN ADDR_CITY ELSE NULL END) OVER (PARTITION BY sak_prov ORDER BY SAK_PROV_LOC) AS S_ADDRESS_CITY,
# MAGIC         MAX(CASE WHEN TRIM(ADDR_TYP) = 'S' THEN ADDR_STATE ELSE NULL END) OVER (PARTITION BY sak_prov ORDER BY SAK_PROV_LOC) AS S_ADDRESS_STATE,
# MAGIC         MAX(CASE WHEN TRIM(ADDR_TYP) = 'S' THEN ADDR_ZIP_5 || NVL(ADDR_ZIP_4, '') ELSE NULL END) OVER (PARTITION BY sak_prov ORDER BY SAK_PROV_LOC) AS S_ADDRESS_ZIP
# MAGIC     FROM  ${catalog}.${schema_name}.EDW_VEN117FA2_Staging
# MAGIC ) AS ProvAddrssExtrct ON ProvExtrct.sak_prov = ProvAddrssExtrct.sak_prov
# MAGIC LEFT JOIN (
# MAGIC     SELECT DISTINCT SAK_PROV,
# MAGIC         NPI, TXNMY_CD, IS_PRIMARY_FLAG, START_DT, END_DT
# MAGIC     FROM  ${catalog}.${schema_name}.EDW_VEN117FA4_Staging
# MAGIC ) AS ProvTaxnmyExtrct ON ProvExtrct.sak_prov = ProvTaxnmyExtrct.sak_prov
# MAGIC LEFT JOIN (
# MAGIC     SELECT DISTINCT SAK_PROV_GRP,
# MAGIC         MEDICAID_ID_GRP, START_DT, END_DT
# MAGIC     FROM  ${catalog}.${schema_name}.EDW_VEN117FA5_Staging
# MAGIC ) AS ProvAffltnExtrct ON ProvExtrct.SAK_PROV = ProvAffltnExtrct.SAK_PROV_GRP
# MAGIC LEFT JOIN (
# MAGIC     SELECT DISTINCT SAK_PROV,
# MAGIC         DEA_NBR
# MAGIC     FROM  ${catalog}.${schema_name}.EDW_VEN117FA6_Staging
# MAGIC ) AS ProvDEAExtrct ON ProvExtrct.sak_prov = ProvDEAExtrct.sak_prov
# MAGIC LEFT JOIN (
# MAGIC     SELECT DISTINCT SAK_PROV,
# MAGIC         CLIA_EFF_DT, CLIA_END_DT, CLIA_CERT_TYP
# MAGIC     FROM  ${catalog}.${schema_name}.EDW_VEN117FA7_Staging
# MAGIC ) AS ProvCLIAExtrct ON ProvExtrct.sak_prov = ProvCLIAExtrct.sak_prov
# MAGIC LEFT JOIN (
# MAGIC     SELECT DISTINCT SAK_PROV,
# MAGIC         IS_LIC_VERIFIED, LIC_TYP_CD, LIC_NBR, EFF_DT, END_DT
# MAGIC     FROM  ${catalog}.${schema_name}.EDW_VEN117FA9_Staging
# MAGIC ) AS ProvLicnsExtrct ON ProvExtrct.sak_prov = ProvLicnsExtrct.sak_prov
# MAGIC LEFT JOIN (
# MAGIC     SELECT DISTINCT SAK_PROV,
# MAGIC         END_DT, IRS_TAX_ID
# MAGIC     FROM  ${catalog}.${schema_name}.EDW_VEN117FA10_Staging
# MAGIC ) AS ProvFrm1099Extrct ON ProvExtrct.sak_prov = ProvFrm1099Extrct.sak_prov;

# COMMAND ----------

# DBTITLE 1,Choose Clustering Keys
# Use Spark SQL to describe the table
columnsInfo = spark.sql(f"DESCRIBE {catalog}.{schema_name}.{ven117fa}")
  
# Show the schema including column names
# columnsInfo.show(truncate=False)

# Count the number of columns
numColumns = columnsInfo.count()
print(f"Number of columns in the table {catalog}.{schema_name}.{ven117fa}: {numColumns}")

spark.sql(f"ALTER TABLE {catalog}.{schema_name}.{ven117fa} SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '{numColumns}')")

# Manually trigger the recomputation of statistics for the Delta table
spark.sql(f"ANALYZE TABLE {catalog}.{schema_name}.{ven117fa} COMPUTE STATISTICS")
print(f"Updated statistics for {catalog}.{schema_name}.{ven117fa}")


# COMMAND ----------

# DBTITLE 1,Clustering and Optimize Table
# MAGIC %sql
# MAGIC -- Clustering and optimizing the  table
# MAGIC ALTER TABLE ${catalog}.${schema_name}.${ven117fa}
# MAGIC CLUSTER BY (ID_PROVIDER_MCAID1,ID_PROVIDER_NPI,NAME,CDE_PROV_ID_TYPE);
# MAGIC --
# MAGIC -- Optimize Table
# MAGIC OPTIMIZE ${catalog}.${schema_name}.${ven117fa};
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from  ${catalog}.${schema_name}.${ven117fa}

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  ${catalog}.${schema_name}.${Prov_Analytics} AS
# MAGIC SELECT DISTINCT
# MAGIC      TRIM(ID_PROVIDER_MCAID1) AS ID_PROVIDER_MCAID1,
# MAGIC     TRIM(ADR_MAIL_STRT1) AS ADR_MAIL_STRT1,
# MAGIC     TRIM(ADR_MAIL_STRT2_0) AS ADR_MAIL_STRT2_0,
# MAGIC     TRIM(ADR_MAIL_CITY1) AS ADR_MAIL_CITY1,
# MAGIC     TRIM(ADR_MAIL_STATE1) AS ADR_MAIL_STATE1,
# MAGIC     TRIM(ADR_MAIL_ZIP1) AS ADR_MAIL_ZIP1,
# MAGIC     TRIM(CDE_PROV_TYPE) AS CDE_PROV_TYPE,
# MAGIC     NAME,
# MAGIC     TRIM(NA0) AS PROV_TYP_NAME
# MAGIC FROM  ${catalog}.${schema_name}.${ven117fa}
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Choose Clustering Keys
# Use Spark SQL to describe the table
columnsInfo = spark.sql(f"DESCRIBE {catalog}.{schema_name}.{Prov_Analytics}")
  
# Show the schema including column names
# columnsInfo.show(truncate=False)

# Count the number of columns
numColumns = columnsInfo.count()
print(f"Number of columns in the table {catalog}.{schema_name}.{Prov_Analytics}: {numColumns}")

spark.sql(f"ALTER TABLE {catalog}.{schema_name}.{Prov_Analytics} SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '{numColumns}')")

# Manually trigger the recomputation of statistics for the Delta table
spark.sql(f"ANALYZE TABLE {catalog}.{schema_name}.{Prov_Analytics} COMPUTE STATISTICS")
print(f"Updated statistics for {catalog}.{schema_name}.{Prov_Analytics}")


# COMMAND ----------

# DBTITLE 1,Clustering and Optimize Table
# MAGIC %sql
# MAGIC -- Clustering and optimizing the  table
# MAGIC ALTER TABLE ${catalog}.${schema_name}.${Prov_Analytics}
# MAGIC CLUSTER BY (ID_PROVIDER_MCAID1,NAME,CDE_PROV_TYPE,ADR_MAIL_STRT1);
# MAGIC --
# MAGIC -- Optimize Table
# MAGIC OPTIMIZE ${catalog}.${schema_name}.${Prov_Analytics};
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from  ${catalog}.${schema_name}.${Prov_Analytics}
