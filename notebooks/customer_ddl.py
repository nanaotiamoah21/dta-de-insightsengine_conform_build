# Databricks notebook source
# DBTITLE 1,Notebook Title
# MAGIC %md
# MAGIC # Customer Feedback DDL
# MAGIC
# MAGIC Creates the Delta tables for the customer feedback model (Medallia VOC), per the ERD: `customerFeedbackSurvey`, `customerNumericFeedbackRating` and `customerNumericFeedbackReason`. Each table is dropped, its storage location removed, then recreated.

# COMMAND ----------

# DBTITLE 1,variable declaration
# import os
# env = os.environ['ENVIRONMENT']
# db_tgt = 'analytics_customer_' + env
# db_stg = 'analytics_customerstg_' + env
# base_path = f"abfss://{env}@dtaeun{env}adlsanalyst01.dfs.core.windows.net/customer/analytics/sensitive"

# Lab: managed tables in beam_explore.insightsengine (no external location)
db_tgt = 'beam_explore.insightsengine'

print(db_tgt)

# COMMAND ----------

# DBTITLE 1,drop table customerfeedbacksurvey_saz_tbl
spark.sql(f""" DROP TABLE IF EXISTS {db_tgt}.customerfeedbacksurvey_saz_tbl""")

# COMMAND ----------

# DBTITLE 1,remove location customerfeedbacksurvey_saz_tbl
# dbutils.fs.rm(f"{base_path}/customerfeedbacksurvey_saz_tbl", True)

# COMMAND ----------

# DBTITLE 1,create table customerfeedbacksurvey_saz_tbl
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {db_tgt}.customerfeedbacksurvey_saz_tbl (
custFeedbackID STRING NOT NULL,
feedbackDate DATE,
storeNumber STRING,
campaignName STRING,
createdTimestamp TIMESTAMP,
modifiedTimestamp TIMESTAMP,
effectiveFromDate TIMESTAMP,
effectiveToDate TIMESTAMP,
logicalDeletionTimestamp TIMESTAMP,
sourceSystemID INT,
sourceSystemRecordID STRING)
USING delta
-- LOCATION '{{base_path}}/customerfeedbacksurvey_saz_tbl'
""")

# COMMAND ----------

# DBTITLE 1,drop table customernumericfeedbackrating_saz_tbl
spark.sql(f""" DROP TABLE IF EXISTS {db_tgt}.customernumericfeedbackrating_saz_tbl""")

# COMMAND ----------

# DBTITLE 1,remove location customernumericfeedbackrating_saz_tbl
# dbutils.fs.rm(f"{base_path}/customernumericfeedbackrating_saz_tbl", True)

# COMMAND ----------

# DBTITLE 1,create table customernumericfeedbackrating_saz_tbl
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {db_tgt}.customernumericfeedbackrating_saz_tbl (
custNumericFeedbackRatingID STRING NOT NULL,
custFeedbackID STRING NOT NULL,
ratingName STRING,
ratingValue INT,
createdTimestamp TIMESTAMP,
modifiedTimestamp TIMESTAMP,
effectiveFromDate TIMESTAMP,
effectiveToDate TIMESTAMP,
logicalDeletionTimestamp TIMESTAMP,
sourceSystemID INT)
USING delta
-- LOCATION '{{base_path}}/customernumericfeedbackrating_saz_tbl'
""")

# COMMAND ----------

# DBTITLE 1,drop table customernumericfeedbackreason_saz_tbl
spark.sql(f""" DROP TABLE IF EXISTS {db_tgt}.customernumericfeedbackreason_saz_tbl""")

# COMMAND ----------

# DBTITLE 1,remove location customernumericfeedbackreason_saz_tbl
# dbutils.fs.rm(f"{base_path}/customernumericfeedbackreason_saz_tbl", True)

# COMMAND ----------

# DBTITLE 1,create table customernumericfeedbackreason_saz_tbl
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {db_tgt}.customernumericfeedbackreason_saz_tbl (
custNumericFeedbackReasonID BIGINT GENERATED ALWAYS AS IDENTITY,
custNumericFeedbackRatingID STRING NOT NULL,
reasonCode STRING,
createdTimestamp TIMESTAMP,
modifiedTimestamp TIMESTAMP,
effectiveFromDate TIMESTAMP,
effectiveToDate TIMESTAMP,
logicalDeletionTimestamp TIMESTAMP,
sourceSystemID INT)
USING delta
-- LOCATION '{{base_path}}/customernumericfeedbackreason_saz_tbl'
""")