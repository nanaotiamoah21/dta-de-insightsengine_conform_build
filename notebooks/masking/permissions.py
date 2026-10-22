# Databricks notebook source
# DBTITLE 1,Notebook Title
# MAGIC %md
# MAGIC # Medallia Customer Survey Conform - Permissions
# MAGIC
# MAGIC Grants USAGE on the destination schema and SELECT on each active conform table to the AD group matching its sensitivity, plus the ops group. Only intended for the ppd and prod environments.

# COMMAND ----------

# DBTITLE 1,Libraries
# MAGIC %pip install pyyaml

# COMMAND ----------

# DBTITLE 1,Widgets
# Import statements
import os, yaml

# Config files live in the repo-level configs folder. Prod pattern used /mnt/projects/response/configs/...
CONFIG_DIR = "/Workspace/Users/nana.oti-amoah@mnscorp.net/dta-de-insightsengine_conform_build/configs"

# ── Widgets ────────────────────────────────────────────────
dbutils.widgets.text("env_config_path", f"{CONFIG_DIR}/env_ingest_masking_config.yml")
dbutils.widgets.text("ingest_config_path", f"{CONFIG_DIR}/customer_survey_columns_mask_config.yml")

ENV_CONFIG_PATH    = dbutils.widgets.get("env_config_path").strip()
INGEST_CONFIG_PATH = dbutils.widgets.get("ingest_config_path").strip()

# Set user id from session context
USER_ID = spark.sql("SELECT session_user()").collect()[0][0]

print(f"Env config path:         {ENV_CONFIG_PATH}")
print(f"Ingest config path:      {INGEST_CONFIG_PATH}")
print(f"User id:                 {USER_ID}\n")

# COMMAND ----------

# DBTITLE 1,Configs
# ── Load configs ───────────────────────────────────────────────
def read_yaml(path):
    # Workspace files are read directly; /mnt or cloud storage paths via spark.read.text
    if path.startswith("/Workspace/"):
        with open(path) as f:
            return yaml.safe_load(f)
    df = spark.read.text(path).coalesce(1)
    return yaml.safe_load("\n".join([row.value for row in df.collect()]))

# Resolve environment (falls back to 'lab' when ENVIRONMENT is not set)
ENV = os.environ.get("ENVIRONMENT", "").strip().lower() or "lab"
print(f"Environment:     {ENV}")

# Load env config and pick correct environment block
all_envs = read_yaml(ENV_CONFIG_PATH)
if ENV not in all_envs["environments"]:
   raise ValueError(
       f"Environment '{ENV}' not found in {ENV_CONFIG_PATH}. "
       f"Valid values: {list(all_envs['environments'].keys())}"
   )
env = all_envs["environments"][ENV]

# Load conform config
tables_cfg = read_yaml(INGEST_CONFIG_PATH)

# AD groups dict (keys match table_sensitivity in the conform config)
ad_groups_dict = {
    "s":   env["ad_groups"]["sensitive"],
    "hs":  env["ad_groups"]["highly_sensitive"],
    "ns":  env["ad_groups"]["non_sensitive"],
    "ops": env["ad_groups"]["ops"]
}

# Filter active tables
active_tables = [t["name"].lower() for t in tables_cfg["tables"] if t.get("active", True)]

# Set common variables
DEST_CATALOG = env["destination"]["catalog"]
DEST_SCHEMA  = env["destination"]["schema"]

print(f"Destination:     {DEST_CATALOG}.{DEST_SCHEMA}\n")

# Environment validation
valid_env = ["ppd", "prod"]
if ENV not in valid_env:
    raise Exception("This notebook is only intended to be run in the ppd and prod environments")

# COMMAND ----------

# DBTITLE 1,Functions
def grant_permissions(entity_name, sql, success_msg, error_msg):
    try:
        spark.sql(sql)
        print(f"✅ {success_msg}")
    except Exception as e:
        err = str(e)[:200]
        errors.append({"entity": entity_name, "status": f"ERROR: {err}"})
        print(f"❌ {error_msg} — {err}")

# COMMAND ----------

# DBTITLE 1,Permissions - Usage
errors = []
for ad_group in set(ad_groups_dict.values()):
    if ad_group.lower() != "none":
        grant_permissions(
            entity_name=ad_group,
            sql=f"GRANT USAGE ON SCHEMA {DEST_CATALOG}.{DEST_SCHEMA} TO `{ad_group}`",
            success_msg=f"Granted USAGE on schema {DEST_CATALOG}.{DEST_SCHEMA} to `{ad_group}`",
            error_msg=f"Failed to grant USAGE on schema {DEST_CATALOG}.{DEST_SCHEMA} to `{ad_group}`"
        )

# COMMAND ----------

# DBTITLE 1,Permissions - Select
counter = 1
for table_name in active_tables:
    table_cfg = [t for t in tables_cfg["tables"] if t["name"].lower() == table_name]
    dest_table = table_cfg[0]["dest_table_name"].lower()
    table_sensitivity = table_cfg[0]["table_sensitivity"].lower()
    if ENV not in valid_env:
        sensitivity_ad_group = "None"
        ops_ad_group = "None"
    else:
        sensitivity_ad_group = ad_groups_dict.get(table_sensitivity)
        ops_ad_group = ad_groups_dict.get("ops")
    groups_to_process = [("Sensitivity", sensitivity_ad_group), ("Ops", ops_ad_group)]
    for group_type, ad_group in groups_to_process:
        if not ad_group:
            err = f"{DEST_CATALOG}.{DEST_SCHEMA}.{dest_table} - No {group_type} AD group found"
            errors.append({"entity": dest_table, "status": f"ERROR: {err}"})
            print(f"❌ {counter} - {err}")
        elif ad_group.lower() == "none":
            print(f"✅ {counter} - {group_type} AD group for {DEST_CATALOG}.{DEST_SCHEMA}.{dest_table} is None")
        else:
            grant_permissions(
                entity_name=dest_table,
                sql=f"GRANT SELECT ON TABLE {DEST_CATALOG}.{DEST_SCHEMA}.{dest_table} TO `{ad_group}`",
                success_msg=f"{counter} - Granted SELECT on table {DEST_CATALOG}.{DEST_SCHEMA}.{dest_table} to `{ad_group}`",
                error_msg=f"{counter} - Failed to grant SELECT on table {DEST_CATALOG}.{DEST_SCHEMA}.{dest_table} to `{ad_group}`"
            )
    counter += 1

# COMMAND ----------

# DBTITLE 1,Clear Widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Notebook Exit
if len(errors) > 0:
    raise Exception(f"{len(errors)} error(s) found. Please resolve before proceeding.")
else:
    dbutils.notebook.exit(f"errors={len(errors)}")