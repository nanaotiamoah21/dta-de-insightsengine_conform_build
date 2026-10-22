# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON GOVERNED TAG masked_string;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED medallia_ingest_prod.medallia_survey_voc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from medallia_ingest_prod.medallia_survey_voc