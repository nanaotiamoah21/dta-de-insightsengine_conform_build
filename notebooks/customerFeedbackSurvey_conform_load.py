# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Notebook Title
# MAGIC %md
# MAGIC # customerfeedbacksurvey_conform
# MAGIC | Detail Tag | Information |
# MAGIC |------------|-------------|
# MAGIC | Domain | Customer |
# MAGIC | Business Team | TBC |
# MAGIC | Purpose | Building customerfeedbacksurvey_saz_tbl in analytics layer. This data set holds one header record per Medallia VOC survey response (feedback date, store, campaign) and is the parent of customernumericfeedbackrating_saz_tbl. |
# MAGIC | Input Datasets |<ul><li> medallia_ingest_prod.medallia_survey_voc </li></ul>|
# MAGIC | Output Datasets | <ul><li> insightsengine.customerfeedbacksurvey_saz_tbl (lab: beam_explore) </li></ul> |
# MAGIC | Input Data Name | Ingest |
# MAGIC | Output Data Name | Conform |
# MAGIC | Input Data Type | Delta |
# MAGIC | Output Data Type | Delta |
# MAGIC | Airflow Dag Name | TBC |
# MAGIC | Frequency | TBC |
# MAGIC | Approx. Data Volume Processing Size |<ul><li> TBC </li></ul>|
# MAGIC | Approx. Data Record Count |<ul><li> ~ 135,000 source rows (Oct 2026) </li></ul>|
# MAGIC | Design Document | TBC |
# MAGIC | Ops Guide | TBC |
# MAGIC | Shared Code | No |
# MAGIC | Programming Language | Python/SQL |
# MAGIC | Table Sensitivity | Highly Sensitive (sourceSystemRecordID is masked) |
# MAGIC
# MAGIC ## History
# MAGIC | Date | Version | Developed By | Reason | CRQ | Test Evidence | Release Management Tracker |
# MAGIC |:----:|-------- | ------------ | ------ | --- |-------------- | -------------------------- |
# MAGIC | 22/10/2026 | 1.0 | Nana Oti-Amoah ([nana.oti-amoah@mnscorp.net](mailto:nana.oti-amoah@mnscorp.net)) | Initial build in lab (beam_explore.insightsengine) | | | |
# MAGIC
# MAGIC ## Code Navigator
# MAGIC <ol>
# MAGIC <li>Import Libraries and Declare Variables</li>
# MAGIC <li>Derive load window (latest feedbackDate in target minus lookback days)</li>
# MAGIC <li>Create staging view from medallia_survey_voc for the load window (column mapping, surrogate key, de-duplication)</li>
# MAGIC <li>Validate surrogate key and count new keys to insert</li>
# MAGIC <li>Insert-only merge on custFeedbackID (no updates - survey responses never change after submission)</li>
# MAGIC <li>Post-load checks</li>
# MAGIC <li>Optimize and Vacuum target table</li>
# MAGIC </ol>

# COMMAND ----------

# DBTITLE 1,necessary imports
import os
spark.conf.set("spark.sql.ansi.enabled","FALSE")

# Prod pattern - hashed out while in lab
# from serverless_udf.serverless_base_env import ConfigReader
# env=ConfigReader().get("ENVIRONMENT")
# spark.sql(f'''USE CATALOG beam_{env}''')

# COMMAND ----------

# DBTITLE 1,declaring variables
# db_tgt = 'analytics_customer_'+env
# db_src = 'medallia_ingest_'+env

# Lab: target lives in beam_explore.insightsengine, source read from beam_prod
db_tgt = 'beam_explore.insightsengine'
db_src = 'beam_prod.medallia_ingest_prod'

tgt_table = f'{db_tgt}.customerfeedbacksurvey_saz_tbl'
src_table = f'{db_src}.medallia_survey_voc'
src_table_name = 'medallia_survey_voc'   # ingest table name used in surrogate key and campaignName

# Source System ID - needs to be created; set the agreed value here (200 until then)
source_system_id = 200

print(db_tgt, db_src, tgt_table, src_table, source_system_id)

# COMMAND ----------

# DBTITLE 1,derive load window
from datetime import timedelta

lookback_days = 0

max_feedback_date = spark.sql(f'''SELECT max(to_date(effectiveFromDate)) FROM {tgt_table}''').first()[0]
load_from_date = '1900-01-01' if max_feedback_date is None else str(max_feedback_date - timedelta(days=lookback_days))

print(f'max feedbackDate in target: {max_feedback_date} | reading source responses from: {load_from_date}')

# COMMAND ----------

# DBTITLE 1,customerfeedbacksurvey_stg
spark.sql(f'''
select
custFeedbackID,
feedbackDate,
storeNumber,
campaignName,
from_utc_timestamp(now(),'Europe/London') createdTimestamp,
cast(null as timestamp) modifiedTimestamp,
effectiveFromDate,
cast('2999-12-31 00:00:00' as timestamp) effectiveToDate,
cast(null as timestamp) logicalDeletionTimestamp,
sourceSystemID,
sourceSystemRecordID
from
(select
md5(concat_ws('|', '{src_table_name}', nvl(a.SURVEY_ID,''), nvl(a.CODE_C_STORE_NUMBER,''))) custFeedbackID,
to_date(to_timestamp(a.RESPONSE_DATE,'yyyy-MM-dd HH:mm:ss')) feedbackDate,
a.CODE_C_STORE_NUMBER storeNumber,
case when a.SURVEY_CHANNEL is null then '{src_table_name}' else concat('{src_table_name}',' - ',a.SURVEY_CHANNEL) end campaignName,
cast({source_system_id} as int) sourceSystemID,
a.SURVEY_ID sourceSystemRecordID,
cast(a.LOAD_TIMESTAMP as timestamp) effectiveFromDate,
row_number() over (partition by md5(concat_ws('|', '{src_table_name}', nvl(a.SURVEY_ID,''), nvl(a.CODE_C_STORE_NUMBER,'')))
                   order by to_timestamp(a.RESPONSE_DATE,'yyyy-MM-dd HH:mm:ss') desc) row1
from {src_table} a
where to_date(to_timestamp(a.LOAD_TIMESTAMP,'yyyy-MM-dd HH:mm:ss')) > '{load_from_date}'
) a1 where a1.row1=1
''').createOrReplaceTempView("customerfeedbacksurvey_stg")

display(spark.table('customerfeedbacksurvey_stg').limit(5))

# COMMAND ----------

# DBTITLE 1,validate surrogate key
# Source rows in the load window vs distinct surrogate keys - a big gap means rows are collapsing onto the same key
# new_keys_to_insert = staged keys not yet in the conform table (what the insert-only merge will add)
display(spark.sql(f"""
SELECT
  (SELECT count(*) FROM {src_table}
   WHERE to_date(to_timestamp(RESPONSE_DATE,'yyyy-MM-dd HH:mm:ss')) >= '{load_from_date}') AS source_rows_in_window,
  (SELECT count(*) FROM customerfeedbacksurvey_stg)    AS staged_rows_distinct_key,
  (SELECT count(*) FROM customerfeedbacksurvey_stg s
   WHERE NOT EXISTS (SELECT 1 FROM {tgt_table} t WHERE t.custFeedbackID = s.custFeedbackID)) AS new_keys_to_insert,
  (SELECT count(DISTINCT SURVEY_ID) FROM {src_table})  AS distinct_survey_id
"""))

# COMMAND ----------

# DBTITLE 1,Delete existing records from customerfeedbacksurvey_saz_tbl
# spark.sql(f'''DELETE FROM {tgt_table}''').display()
# display(spark.sql(f'''SELECT count(*) AS rows_after_delete FROM {tgt_table}'''))

# COMMAND ----------

# DBTITLE 1,Upsert operation on customerfeedbacksurvey_saz_tbl
# Insert-only load into customerfeedbacksurvey_saz_tbl, keyed on custFeedbackID.
# Survey responses never change after submission, so there is no WHEN MATCHED / update branch -
# only responses that do not already exist in the conform layer are inserted.

try:
    spark.sql(f"""
    merge into {tgt_table} as dst
    using customerfeedbacksurvey_stg src
    on dst.custFeedbackID = src.custFeedbackID and year(dst.effectiveToDate)=2999

    when not matched then
    insert (dst.custFeedbackID,
    dst.feedbackDate,
    dst.storeNumber,
    dst.campaignName,
    dst.createdTimestamp,
    dst.modifiedTimestamp,
    dst.effectiveFromDate,
    dst.effectiveToDate,
    dst.logicalDeletionTimestamp,
    dst.sourceSystemID,
    dst.sourceSystemRecordID)
    values (src.custFeedbackID,
    src.feedbackDate,
    src.storeNumber,
    src.campaignName,
    src.createdTimestamp,
    src.modifiedTimestamp,
    src.effectiveFromDate,
    src.effectiveToDate,
    src.logicalDeletionTimestamp,
    src.sourceSystemID,
    src.sourceSystemRecordID
    )
    """).display()
except Exception as e:
    print("An error occurred: ",e)
    raise

# COMMAND ----------

# DBTITLE 1,post-load check
display(spark.sql(f"""
SELECT count(*) AS total_rows,
       count(DISTINCT custFeedbackID) AS distinct_keys,
       min(feedbackDate) AS min_feedback_date,
       max(feedbackDate) AS max_feedback_date,
       max(modifiedTimestamp) AS last_modified
FROM {tgt_table}
"""))
display(spark.sql(f"SELECT campaignName, count(*) AS n FROM {tgt_table} GROUP BY 1 ORDER BY 2 DESC"))

# COMMAND ----------

# DBTITLE 1,Optimize Tables
spark.sql(f'''OPTIMIZE {tgt_table}''')

# COMMAND ----------

# DBTITLE 1,Vacuum Tables
spark.sql(f'''VACUUM {tgt_table}''')

# COMMAND ----------

spark.sql(f'select * from {tgt_table}').display(6)