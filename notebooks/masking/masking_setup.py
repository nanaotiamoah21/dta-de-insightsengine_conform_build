# Databricks notebook source
# DBTITLE 1,Notebook Title
# MAGIC %md
# MAGIC # Medallia Customer Survey Conform - Masking Setup
# MAGIC
# MAGIC One-time, idempotent setup for conform-layer masking: validates the destination catalog, creates the `policy_mask_string` masking function, verifies the `masked_string` governed tag and creates the ABAC column mask policies on the destination schema. Run before `tag_notebook`.

# COMMAND ----------

# DBTITLE 1,Libraries
# MAGIC %pip install pyyaml

# COMMAND ----------

# DBTITLE 1,Widgets
# =============================================================================
# One-time Medallia customer survey conform masking setup notebook
# Safe to re-run — all operations are idempotent
# =============================================================================

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

print(f"Env config path:          {ENV_CONFIG_PATH}")
print(f"Ingest config path:       {INGEST_CONFIG_PATH}")
print(f"User id:                  {USER_ID}\n")

# COMMAND ----------

# DBTITLE 1,Configs
# ── Load configs ─────────────────────────────────────────────
def read_yaml(path):
    # Workspace files are read directly; /mnt or cloud storage paths via spark.read.text
    if path.startswith("/Workspace/"):
        with open(path) as f:
            return yaml.safe_load(f)
    df = spark.read.text(path).coalesce(1)
    return yaml.safe_load("\n".join([row.value for row in df.collect()]))

# Resolve environment (falls back to 'lab' when ENVIRONMENT is not set)
ENV = os.environ.get("ENVIRONMENT", "").strip().lower() or "lab"
print(f"Environment:        {ENV}")

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

print(f"Destination:        {DEST_CATALOG}.{DEST_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Step 1: Validate Destination Catalog Connection
# ── Step 1: Validate destination catalog connection ──────────
print("Step 1 — Validating destination catalog connection...")
try:
    spark.sql(f"SHOW SCHEMAS IN {DEST_CATALOG}").count()
    print(f"  ✓ Destination catalog accessible: {DEST_CATALOG}")
except Exception as e:
    raise RuntimeError(f"Cannot access destination catalog '{DEST_CATALOG}': {e}")

# COMMAND ----------

# DBTITLE 1,Step 2: Create Masking Functions
# ── Step 2: Create Masking Functions ──────────────────────
# CREATE OR REPLACE the masking UDFs referenced by the ABAC policies in Step 4.
# Functions are not dropped first - dropping a function an active policy references
# makes every query on the tagged columns fail until it is recreated.
# ============================================================
SCHEMA_FQN = f"{DEST_CATALOG}.{DEST_SCHEMA}"
spark.sql(f"USE CATALOG {DEST_CATALOG}")

print("\nStep 2 — Creating masking functions...")

masking_functions = [
    ("policy_mask_string",
     "STRING", "STRING",
     "CASE WHEN value IS NULL THEN NULL ELSE '******' END",
     "Redacts all tagged string columns with ******"),
]

func_success = 0
for name, input_type, return_type, return_expr, comment in masking_functions:
    try:
        spark.sql(f"""
            CREATE OR REPLACE FUNCTION {SCHEMA_FQN}.{name}(value {input_type})
            RETURNS {return_type}
            DETERMINISTIC
            COMMENT '{comment}'
            RETURN {return_expr}
        """)
        print(f"  ✓ {name:<25} (Input: {input_type}, Returns: {return_type})")
        func_success += 1
    except Exception as e:
        print(f"  ✗ {name}: {e}")

print(f"  {func_success}/{len(masking_functions)} masking functions created")

# COMMAND ----------

# DBTITLE 1,Step 3: Governed Tags
# ── Step 3: Governed Tags (verify/skip if no access) ──────
# Tags are account-level objects. They should already exist.
# This cell will not fail even if access is denied
# ============================================================
tags = [
    "masked_string",
]

print("\nStep 3 — Verifying governed tags...")
tags_ok = 0
tags_skipped = 0

for tag in tags:
    try:
        spark.sql(f"CREATE TAG IF NOT EXISTS {tag} WITH ALLOWED VALUES ('sensitive', 'highly sensitive', 'special category')")
        print(f"  ✓ Tag: {tag} (created/exists)")
        tags_ok += 1
    except Exception as e:
        err_msg = str(e).lower()
        if "already exists" in err_msg:
            print(f"  ✓ Tag: {tag} (already exists)")
            tags_ok += 1
        else:
            print(f"  ⊘ Tag: {tag} (skipped — already exists at account level or no access)")
            tags_skipped += 1

print(f"  {tags_ok} verified, {tags_skipped} skipped (pre-existing at account level)")

# COMMAND ----------

# DBTITLE 1,Step 4: Create ABAC Column Mask Policies
# ── Step 4: Create ABAC Column Mask Policies ─────────────
# One policy per masked_string tag value - auto-applies masking to any column
# in the schema with the matching governed tag (no per-column SET MASK needed).
# The ETL identity running the conform loads must be in the EXCEPT list (env config).
# ============================================================

policies = [
    ("policy_mask_string_s",  "policy_mask_string", "masked_string", "sensitive",
     "Masks sensitive string columns"),
    ("policy_mask_string_hs", "policy_mask_string", "masked_string", "highly sensitive",
     "Masks highly sensitive string columns"),
    ("policy_mask_string_sc", "policy_mask_string", "masked_string", "special category",
     "Masks special category string columns"),
]

# 'None' entries in the env config are ignored; a policy with no inclusion principals is skipped
print(f"\nStep 4 — Creating {len(policies)} ABAC policies on {SCHEMA_FQN}...")
policies_apply_dict = {
    "sensitive": {
        "exception": ", ".join(f"`{item}`" for item in env["policy_mask_s_exception"] if str(item).strip().lower() != "none"),
        "inclusion": ", ".join(f"`{item}`" for item in env["policy_mask_s_inclusion"] if str(item).strip().lower() != "none")
    },
    "highly sensitive": {
        "exception": ", ".join(f"`{item}`" for item in env["policy_mask_hs_exception"] if str(item).strip().lower() != "none"),
        "inclusion": ", ".join(f"`{item}`" for item in env["policy_mask_hs_inclusion"] if str(item).strip().lower() != "none")
    },
    "special category": {
        "exception": ", ".join(f"`{item}`" for item in env["policy_mask_sc_exception"] if str(item).strip().lower() != "none"),
        "inclusion": ", ".join(f"`{item}`" for item in env["policy_mask_sc_inclusion"] if str(item).strip().lower() != "none")
    }
}

policy_success = 0
for policy_name, func_name, tag_name, tag_value, comment in policies:
    inclusion = policies_apply_dict[tag_value]["inclusion"]
    exception = policies_apply_dict[tag_value]["exception"]
    if not inclusion:
        print(f"  ⊘ {policy_name}: skipped — no inclusion principals configured for '{tag_value}'")
        continue
    except_clause = f"EXCEPT {exception}" if exception else ""   # EXCEPT omitted when no exceptions configured
    try:
        sql = f"""
            CREATE OR REPLACE POLICY {policy_name}
            ON SCHEMA {SCHEMA_FQN}
            COMMENT '{comment}'
            COLUMN MASK {SCHEMA_FQN}.{func_name}
            TO {inclusion}
            {except_clause}
            FOR TABLES
            MATCH COLUMNS has_tag_value('{tag_name}','{tag_value}') AS m
            ON COLUMN m
        """
        spark.sql(sql)
        print(f"  ✓ {policy_name:<30} (tag: {tag_name} = '{tag_value}')")
        policy_success += 1
    except Exception as e:
        print(f"  ✗ {policy_name}: {e}")

print(f"  {policy_success}/{len(policies)} ABAC policies created")

# COMMAND ----------

# DBTITLE 1,Step 5: Validate & Print Summary
# ── Step 5: Validate & Print Summary ─────────────────────
# Validates information_schema access and prints full summary
# ============================================================
print("\nStep 5 — Final validation...")
try:
    dest_count = spark.sql(f"""
        SELECT COUNT(*) FROM {DEST_CATALOG}.information_schema.tables
        WHERE table_schema = '{DEST_SCHEMA}'
    """).collect()[0][0]
    print(f"  ✓ Destination tables accessible: {dest_count} in {DEST_CATALOG}.{DEST_SCHEMA}")
except Exception as e:
    dest_count = "?"
    print(f"  ⚠ Could not validate destination: {e}")

# Verify masking functions
pdf = spark.sql(f"SHOW FUNCTIONS IN {SCHEMA_FQN}").toPandas()
mask_funcs_count = len(pdf[pdf['function'].str.contains('policy_mask')])

# Verify policies
try:
    pol_count = spark.sql(f"SHOW POLICIES ON SCHEMA {SCHEMA_FQN}").count()
except Exception:
    pol_count = "?"

print(f"""
{'='*65}
 SETUP COMPLETE
{'='*65}
  Environment:        {ENV}
  Destination:        {DEST_CATALOG}.{DEST_SCHEMA}
  Destination tables: {dest_count}
  Masking functions:  {mask_funcs_count}
  ABAC policies:      {pol_count}
{'='*65}
  Ready to run: conform loads -> masking/tag_notebook -> masking/permissions
{'='*65}
""")

# COMMAND ----------

# DBTITLE 1,Clear Widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Notebook Exit
dbutils.notebook.exit("Medallia customer survey conform masking setup completed")