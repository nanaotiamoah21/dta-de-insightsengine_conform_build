# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Notebook Title
# MAGIC %md
# MAGIC # customernumericfeedbackrating_conform
# MAGIC | Detail Tag | Information |
# MAGIC |------------|-------------|
# MAGIC | Domain | Customer |
# MAGIC | Business Team | TBC |
# MAGIC | Purpose | Building customernumericfeedbackrating_saz_tbl in analytics layer. This data set holds one record per survey response per numeric rating category (Colleague Service, Clean and Tidy, Time to Pay) on a 1-5 scale. Child of customerfeedbacksurvey_saz_tbl (custFeedbackID) and parent of customernumericfeedbackreason_saz_tbl. |
# MAGIC | Input Datasets |<ul><li> medallia_ingest_prod.medallia_survey_voc </li></ul>|
# MAGIC | Output Datasets | <ul><li> insightsengine.customernumericfeedbackrating_saz_tbl (lab: beam_explore) </li></ul> |
# MAGIC | Input Data Name | Ingest |
# MAGIC | Output Data Name | Analytics |
# MAGIC | Input Data Type | Delta |
# MAGIC | Output Data Type | Delta |
# MAGIC | Airflow Dag Name | TBC |
# MAGIC | Frequency | TBC |
# MAGIC | Approx. Data Volume Processing Size |<ul><li> TBC </li></ul>|
# MAGIC | Approx. Data Record Count |<ul><li> ~ 225,000 rating rows (~75,000 rated responses x 3 categories, Oct 2026) </li></ul>|
# MAGIC | Design Document | TBC |
# MAGIC | Ops Guide | TBC |
# MAGIC | Shared Code | No |
# MAGIC | Programming Language | Python/SQL |
# MAGIC | Table Sensitivity | Highly Sensitive |
# MAGIC
# MAGIC ## History
# MAGIC | Date | Version | Developed By | Reason | CRQ | Test Evidence | Release Management Tracker |
# MAGIC |:----:|-------- | ------------ | ------ | --- |-------------- | -------------------------- |
# MAGIC | 22/10/2026 | 1.0 | Nana Oti-Amoah ([nana.oti-amoah@mnscorp.net](mailto:nana.oti-amoah@mnscorp.net)) | Initial build in lab (beam_explore.insightsengine) | | | |
# MAGIC
# MAGIC ## Code Navigator
# MAGIC <ol>
# MAGIC <li>Import Libraries and Declare Variables</li>
# MAGIC <li>Create staging view from medallia_survey_voc (unpivot ratings, surrogate and foreign keys, de-duplication)</li>
# MAGIC <li>Validate surrogate key</li>
# MAGIC <li>Upsert (merge) staging view into target table</li>
# MAGIC <li>Post-load checks (including orphan check against customerfeedbacksurvey_saz_tbl)</li>
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

tgt_table = f'{db_tgt}.customernumericfeedbackrating_saz_tbl'
parent_table = f'{db_tgt}.customerfeedbacksurvey_saz_tbl'
src_table = f'{db_src}.medallia_survey_voc'
src_table_name = 'medallia_survey_voc'   # ingest table name used in surrogate and foreign keys

# Source System ID - needs to be created; set the agreed value here (NULL until then)
source_system_id = 200

print(db_tgt, db_src, tgt_table, parent_table, src_table, source_system_id)

# COMMAND ----------

# DBTITLE 1,derive load window
from datetime import timedelta

lookback_days = 0

max_feedback_date = spark.sql(f'''SELECT max(to_date(effectiveFromDate)) FROM {tgt_table}''').first()[0]
load_from_date = '1900-01-01' if max_feedback_date is None else str(max_feedback_date - timedelta(days=lookback_days))

print(f'max feedbackDate in target: {max_feedback_date} | reading source responses from: {load_from_date}')

# COMMAND ----------

# DBTITLE 1,customernumericfeedbackrating_stg
spark.sql(f'''
select
custNumericFeedbackRatingID,
custFeedbackID,
ratingName,
ratingValue,
from_utc_timestamp(now(),'Europe/London') createdTimestamp,
cast(null as timestamp) modifiedTimestamp,
effectiveFromDate,
cast('2999-12-31 00:00:00' as timestamp) effectiveToDate,
cast(null as timestamp) logicalDeletionTimestamp,
sourceSystemID
from
(select
md5(concat_ws('|', '{src_table_name}', nvl(a.SURVEY_ID,''), nvl(a.CODE_C_STORE_NUMBER,''), r.ratingName)) custNumericFeedbackRatingID,
md5(concat_ws('|', '{src_table_name}', nvl(a.SURVEY_ID,''), nvl(a.CODE_C_STORE_NUMBER,''))) custFeedbackID,
r.ratingName,
cast(r.ratingValue as int) ratingValue,
cast({source_system_id} as int) sourceSystemID,
cast(a.LOAD_TIMESTAMP as timestamp) effectiveFromDate,
row_number() over (partition by md5(concat_ws('|', '{src_table_name}', nvl(a.SURVEY_ID,''), nvl(a.CODE_C_STORE_NUMBER,''), r.ratingName))
                   order by to_timestamp(a.RESPONSE_DATE,'yyyy-MM-dd HH:mm:ss') desc) row1
from {src_table} a
where to_date(to_timestamp(a.LOAD_TIMESTAMP,'yyyy-MM-dd HH:mm:ss')) > '{load_from_date}'
lateral view stack(3,
  'Colleague Service', a.COLLEAGUE_SERVICE,
  'Clean and Tidy',    a.CLEAN_AND_TIDY,
  'Time to Pay',       a.TIME_TO_PAY
) r as ratingName, ratingValue
where r.ratingValue is not null
) a1 where a1.row1=1
''').createOrReplaceTempView("customernumericfeedbackrating_stg")

display(spark.table('customernumericfeedbackrating_stg').limit(5))

# COMMAND ----------

# DBTITLE 1,validate surrogate key
# Unpivoted source rating rows vs distinct surrogate keys - a big gap means rows are collapsing onto the same key
display(spark.sql(f"""
SELECT
  (SELECT count(*) FROM {src_table}
     LATERAL VIEW stack(3, 'Colleague Service', COLLEAGUE_SERVICE, 'Clean and Tidy', CLEAN_AND_TIDY, 'Time to Pay', TIME_TO_PAY) r AS ratingName, ratingValue
     WHERE r.ratingValue IS NOT NULL)                         AS source_rating_rows,
  (SELECT count(*) FROM customernumericfeedbackrating_stg)    AS staged_rows_distinct_key,
  (SELECT count(DISTINCT SURVEY_ID) FROM {src_table})         AS distinct_survey_id
"""))

# COMMAND ----------

# DBTITLE 1,Upsert operation on customernumericfeedbackrating_saz_tbl
# Performing upsert operation on customernumericfeedbackrating_saz_tbl to make sure new records are inserted and records for which there are updates get updated.
# customernumericfeedbackrating_saz_tbl is a type1 table.

try:
    spark.sql(f"""
    merge into {tgt_table} as dst
    using customernumericfeedbackrating_stg src
    on dst.custNumericFeedbackRatingID = src.custNumericFeedbackRatingID and year(dst.effectiveToDate)=2999
    when not matched then
    insert (dst.custNumericFeedbackRatingID,
    dst.custFeedbackID,
    dst.ratingName,
    dst.ratingValue,
    dst.createdTimestamp,
    dst.modifiedTimestamp,
    dst.effectiveFromDate,
    dst.effectiveToDate,
    dst.logicalDeletionTimestamp,
    dst.sourceSystemID)
    values (src.custNumericFeedbackRatingID,
    src.custFeedbackID,
    src.ratingName,
    src.ratingValue,
    src.createdTimestamp,
    src.modifiedTimestamp,
    src.effectiveFromDate,
    src.effectiveToDate,
    src.logicalDeletionTimestamp,
    src.sourceSystemID)
    """).display()
except Exception as e:
    print("An error occurred: ",e)
    raise

# COMMAND ----------

# DBTITLE 1,post-load check
display(spark.sql(f"""
SELECT count(*) AS total_rows,
       count(DISTINCT custNumericFeedbackRatingID) AS distinct_keys,
       count(DISTINCT custFeedbackID) AS distinct_feedback_ids,
       max(modifiedTimestamp) AS last_modified
FROM {tgt_table}
"""))

# Rating distribution per category
display(spark.sql(f"SELECT ratingName, ratingValue, count(*) AS n FROM {tgt_table} GROUP BY 1,2 ORDER BY 1,2"))

# Orphan check - ratings whose custFeedbackID has no parent in customerfeedbacksurvey_saz_tbl (expect 0)
display(spark.sql(f"""
SELECT count(*) AS orphan_ratings
FROM {tgt_table} c
LEFT JOIN {parent_table} p ON c.custFeedbackID = p.custFeedbackID
WHERE p.custFeedbackID IS NULL
"""))

# COMMAND ----------

# DBTITLE 1,Optimize Tables
spark.sql(f'''OPTIMIZE {tgt_table}''')

# COMMAND ----------

# DBTITLE 1,Vacuum Tables
spark.sql(f'''VACUUM {tgt_table}''')