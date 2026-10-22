# Databricks notebook source
# DBTITLE 1,Notebook Title
# MAGIC %md
# MAGIC # Medallia Customer Survey Conform - Column Tagging
# MAGIC
# MAGIC Applies Unity Catalog governed tags to sensitive columns in the conform tables after each load (e.g. `sourceSystemRecordID` holding the unmasked SURVEY_ID), so the ABAC policies from `masking_setup` mask them automatically. Idempotent and safe to re-run.

# COMMAND ----------

# DBTITLE 1,Libraries
# MAGIC %pip install pyyaml

# COMMAND ----------

# DBTITLE 1,Widgets
# =============================================================================
# Applies unity catalog tags to sensitive columns post-conform
#
# Tag key is auto-resolved from column data type:
#   string family   → masked_string
#   number family   → masked_number
#   datetime family → masked_date
#
# Tag value comes from conform config:
#   sensitive        → e.g. masked_string : sensitive
#   highly sensitive → e.g. masked_number : highly sensitive
#   special category → e.g. masked_string : special category
#
# UC policies auto-activate based on tag key + tag value + data type
# One tag per column — no multiple policy conflicts
# Only processes tables already conformed — skips others silently
# Idempotent — safe to re-run
#
# Assumes SURVEY_ID is read unmasked by the conform load and lands in
# customerfeedbacksurvey_saz_tbl.sourceSystemRecordID - tagged here so it is masked again.
# =============================================================================

import os, yaml

# Config files live in the repo-level configs folder. Prod pattern used /mnt/projects/response/configs/...
CONFIG_DIR = "/Workspace/Users/nana.oti-amoah@mnscorp.net/dta-de-insightsengine_conform_build/configs"

# ── Widgets ────────────────────────────────────────────────
dbutils.widgets.text("table_filter", "")
dbutils.widgets.text("pipeline_id", "manual")
dbutils.widgets.text("env_config_path", f"{CONFIG_DIR}/env_ingest_masking_config.yml")
dbutils.widgets.text("ingest_config_path", f"{CONFIG_DIR}/customer_survey_columns_mask_config.yml")

TABLE_FILTER       = dbutils.widgets.get("table_filter").strip()
PIPELINE_ID        = dbutils.widgets.get("pipeline_id").strip()
ENV_CONFIG_PATH    = dbutils.widgets.get("env_config_path").strip()
INGEST_CONFIG_PATH = dbutils.widgets.get("ingest_config_path").strip()

# Set user id from session context
USER_ID = spark.sql("SELECT session_user()").collect()[0][0]

print(f"Table filter:               {TABLE_FILTER}")
print(f"Pipeline id:                {PIPELINE_ID}")
print(f"Env config path:            {ENV_CONFIG_PATH}")
print(f"Ingest config path:         {INGEST_CONFIG_PATH}")
print(f"User id:                    {USER_ID}\n")

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
print(f"Environment:       {ENV}")

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

# Set common variables
DEST_CATALOG    = env["destination"]["catalog"]
DEST_SCHEMA     = env["destination"]["schema"]

# Derive sensitive tables from conform config
SENSITIVE_TABLES = {
    t["name"].lower(): t["sensitive_columns"]
    for t in tables_cfg["tables"]
    if t.get("sensitive_columns")
}

# Get all tables from conform config
ALL_TABLES = {
    t["name"].lower(): t["dest_table_name"]
    for t in tables_cfg["tables"]
    if t.get("dest_table_name")
}

# Build mapping: logical table name → dest_table_name from conform config
dest_name_map = {
    t["name"].lower(): t.get("dest_table_name", f"{t['name'].lower()}").lower()
    for t in tables_cfg["tables"]
}

# Apply table filter if specified
if TABLE_FILTER:
    filter_list      = [t.strip().lower() for t in TABLE_FILTER.split(",")]
    SENSITIVE_TABLES = {k: v for k, v in SENSITIVE_TABLES.items() if k in filter_list}
    ALL_TABLES       = {k: v for k, v in ALL_TABLES.items() if k in filter_list}

print(f"Destination:       {DEST_CATALOG}.{DEST_SCHEMA}")
print(f"Tables in config:  {len(SENSITIVE_TABLES)}")
print(f"Table filter:      {TABLE_FILTER or 'ALL'}\n")

# COMMAND ----------

# DBTITLE 1,Mappings
# ── Data type → tag key mapping ────────────────────────
DATATYPE_TAG_KEY_MAP = {
    # string family → masked_string
    "nvarchar":         "masked_string",
    "varchar":          "masked_string",
    "char":             "masked_string",
    "nchar":            "masked_string",
    "text":             "masked_string",
    "ntext":            "masked_string",
    "string":           "masked_string",
    "uniqueidentifier": "masked_string",
    # number family → masked_number
    "int":              "masked_number",
    "integer":          "masked_number",
    "bigint":           "masked_number",
    "long":             "masked_number",   # ← Spark internal name for bigint
    "smallint":         "masked_number",
    "tinyint":          "masked_number",
    "short":            "masked_number",   # ← Spark internal name for smallint
    # decimal family → masked_decimal
    "decimal":          "masked_decimal",
    "numeric":          "masked_decimal",
    "float":            "masked_decimal",
    "real":             "masked_decimal",
    "double":           "masked_decimal",
    "money":            "masked_decimal",
    "smallmoney":       "masked_decimal",
    # datetime family → masked_date
    "datetime":         "masked_date",
    "datetime2":        "masked_date",
    "smalldatetime":    "masked_date",
    "date":             "masked_date",
    "timestamp":        "masked_date",
    "timestamp_ntz":    "masked_date",
}

# ── Get conformed tables ───────────────────────────────────
ingest_tables = {
    r.tableName.lower() for r in
    spark.sql(f"SHOW TABLES IN {DEST_CATALOG}.{DEST_SCHEMA}").collect()
}

# Match using dest_table_name against conformed tables
tables_to_check = [t for t in ALL_TABLES       if dest_name_map.get(t, t) in ingest_tables]
tables_to_tag   = [t for t in SENSITIVE_TABLES if dest_name_map.get(t, t) in ingest_tables]
not_yet_loaded  = [t for t in SENSITIVE_TABLES if dest_name_map.get(t, t) not in ingest_tables]

print(f"Conform tables in destination:   {len(ingest_tables)}")
print(f"Tables to check:                 {len(tables_to_check)}")
print(f"Tables to tag:                   {len(tables_to_tag)}")
print(f"Skipping (not yet conformed):    {len(not_yet_loaded)}\n")

if not_yet_loaded:
    print("Tables not yet conformed (will tag once loaded):")
    for t in not_yet_loaded:
        print(f"  ○  {t} → {dest_name_map.get(t, t)}")
    print()

# COMMAND ----------

# DBTITLE 1,Functions
def remove_tags(table_name, dest_fqn):
    # Removes masking tags that are no longer expected (column removed from config, tag key/value changed)
    dest_catalog, dest_schema, dest_table_name = dest_fqn.split(".")
    existing_df = spark.sql(f"""
        SELECT column_name, tag_name, tag_value
        FROM {dest_catalog}.information_schema.column_tags
        WHERE schema_name = '{dest_schema}'
          AND table_name = '{dest_table_name}'
          AND tag_name IN ('masked_string', 'masked_number', 'masked_decimal', 'masked_date')
    """)
    existing = {(r["column_name"].lower(), r["tag_name"], r["tag_value"]) for r in existing_df.collect()}
    # If table is not in the sensitive tables list → remove all masking tags
    if table_name.lower() not in SENSITIVE_TABLES:
        for col, tag, _ in existing:
            spark.sql(f"ALTER TABLE {dest_fqn} ALTER COLUMN {col} UNSET TAGS ('{tag}')")
            print(f" * Removed tag - {dest_fqn} - {col} - {tag}")
        return
    # Build expected mapping from sensitive tables list
    expected = {}
    for entry in SENSITIVE_TABLES[table_name.lower()]:
        col = entry["column"].lower()
        dtype = entry["data_type"].lower()
        tag_value = entry["tag_value"].lower()
        expected_tag = DATATYPE_TAG_KEY_MAP.get(dtype)
        if expected_tag:
            expected[col] = (expected_tag, tag_value)
    # Compare existing vs expected
    for col, tag, tag_value in existing:
        if col not in expected:
            spark.sql(f"ALTER TABLE {dest_fqn} ALTER COLUMN {col} UNSET TAGS ('{tag}')")
            print(f" * Removed tag - {dest_fqn} - {col} - {tag}")
            continue
        expected_tag, expected_value = expected[col]
        if tag != expected_tag or (tag_value or "").lower() != expected_value:
            spark.sql(f"ALTER TABLE {dest_fqn} ALTER COLUMN {col} UNSET TAGS ('{tag}')")
            print(f" * Removed tag - {dest_fqn} - {col} - {tag}")

# COMMAND ----------

# DBTITLE 1,Remove Tags
# ── Remove tags ───────────────────────────────────────────────
for table_name in tables_to_check:
    dest_table = dest_name_map.get(table_name, table_name)
    dest_fqn   = f"{DEST_CATALOG}.{DEST_SCHEMA}.{dest_table}"
    remove_tags(table_name, dest_fqn)

if not tables_to_tag:
    print("No conformed tables to tag yet — run conform first")
    dbutils.notebook.exit("tagged=0,skipped=0,failed=0")

# COMMAND ----------

# DBTITLE 1,Parameters
# ── Fetch column data types ────────────────────────────
# Use dest_table_names for information_schema lookup (keys lower-cased so config column case does not matter)
dest_tables_to_query = [dest_name_map.get(t, t) for t in tables_to_tag]
table_filter_sql = ", ".join(f"'{dt}'" for dt in dest_tables_to_query)

col_type_map = {
    (r.table_name.lower(), r.column_name.lower()): r.data_type
    for r in spark.sql(f"""
        SELECT table_name, column_name, data_type
        FROM {DEST_CATALOG}.information_schema.columns
        WHERE table_schema = '{DEST_SCHEMA}'
          AND table_name   IN ({table_filter_sql})
    """).collect()
}

print(f"Columns fetched from information_schema: {len(col_type_map)}\n")

# COMMAND ----------

# DBTITLE 1,Apply Tags
# ── Apply tags ────────────────────────────────────────────────
print(f"{'Table':<35} {'Column':<30} {'Tag Key':<18} {'Tag Value':<22} Status")
print("-" * 113)

tag_results = []
for table_name in tables_to_tag:
    columns    = SENSITIVE_TABLES[table_name]
    dest_table = dest_name_map.get(table_name, table_name)
    dest_fqn   = f"{DEST_CATALOG}.{DEST_SCHEMA}.{dest_table}"

    for col_cfg in columns:
        col       = col_cfg["column"]
        tag_value = col_cfg["tag_value"]
        data_type = col_type_map.get((dest_table, col.lower()))

        # Column not found in dest table
        if not data_type:
            print(f"  ⚠  {table_name:<33} {col:<30} {'?':<18} {tag_value:<22} SKIPPED — column not in dest table")
            tag_results.append({"table": table_name, "column": col,
                                "status": "skipped", "reason": "column not found in dest table"})
            continue

        # Resolve tag key from data type
        base_type = data_type.lower().split("(")[0].strip()
        tag_key   = DATATYPE_TAG_KEY_MAP.get(base_type)

        if not tag_key:
            print(f"  ⚠  {table_name:<33} {col:<30} {'?':<18} {tag_value:<22} SKIPPED — no tag key mapping for type '{data_type}'")
            tag_results.append({"table": table_name, "column": col, "status": "skipped",
                                "reason": f"no tag key mapping for data type '{data_type}'"})
            continue

        # Apply tag
        try:
            spark.sql(f"""
                ALTER TABLE {dest_fqn}
                ALTER COLUMN {col}
                SET TAGS ('{tag_key}' = '{tag_value}')
            """)
            print(f"  ✓  {table_name:<33} {col:<30} {tag_key:<18} {tag_value:<22} tagged")
            tag_results.append({"table": table_name, "column": col, "tag_key": tag_key,
                                "tag_value": tag_value, "data_type": data_type, "status": "success"})
        except Exception as e:
            print(f"  ✗  {table_name:<33} {col:<30} {tag_key:<18} {tag_value:<22} ERROR: {e}")
            tag_results.append({"table": table_name, "column": col, "status": "failed", "error": str(e)})

# COMMAND ----------

# DBTITLE 1,Summary
# ── Summary ───────────────────────────────────────────────
success = [r for r in tag_results if r["status"] == "success"]
skipped = [r for r in tag_results if r["status"] == "skipped"]
failed  = [r for r in tag_results if r["status"] == "failed"]

print(f"""
{'='*65}
TAGGING SUMMARY
{'='*65}
  Tables processed:         {len(tables_to_tag)}
  Tables not yet conformed: {len(not_yet_loaded)}
  Tags applied:             {len(success)}
  Skipped columns:          {len(skipped)}
  Failed:                   {len(failed)}
{'='*65}""")

if skipped:
    print("\n  Skipped — check conform config sensitive_columns or add data type mapping:")
    for r in skipped:
        print(f"    ⚠  {r['table']}.{r['column']}: {r['reason']}")

if failed:
    print("\n  Failed:")
    for r in failed:
        print(f"    ✗  {r['table']}.{r['column']}: {r.get('error')}")

print("\n✅ Masking policies auto-activated on all successfully tagged columns")

# COMMAND ----------

# DBTITLE 1,Clear Widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Notebook Exit
if len(failed) > 0:
    failed_cols = ", ".join(f"{r['table']}.{r['column']}" for r in failed)
    raise Exception(f"Failed: {failed_cols}")
else:
    dbutils.notebook.exit(f"tagged={len(success)},skipped={len(skipped)},failed={len(failed)}")