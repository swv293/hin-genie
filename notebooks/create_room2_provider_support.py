# Databricks notebook source
# MAGIC %md
# MAGIC # Room 2 — Provider Support & Call Intelligence
# MAGIC
# MAGIC Creates (or refreshes) the Genie Room over the provider-support call data. Idempotent: detects an
# MAGIC existing room with the same display name and skips re-creation, so it can be safely re-run after
# MAGIC edits to the curation JSON.
# MAGIC
# MAGIC **What this notebook does**
# MAGIC 1. Reads the full curation (tables, sample questions, instructions, sample SQLs, joins, snippets,
# MAGIC    benchmarks) from `genie_config/room2_curation.json`.
# MAGIC 2. Rewrites every fully-qualified table identifier inside the JSON to use the catalog/schema you
# MAGIC    supply via the widgets, so the same JSON works across workspaces.
# MAGIC 3. If a room with the target display name already exists, prints its URL and exits.
# MAGIC 4. Otherwise, calls `POST /api/2.0/genie/spaces` with `serialized_space` so the room ships with all
# MAGIC    curation in a single API call — no manual UI import.
# MAGIC
# MAGIC **Prereqs**
# MAGIC - `sql/00_create_schemas.sql`, `sql/01_create_genie_views.sql`, `sql/02_source_table_comments.sql`,
# MAGIC   `sql/03_payer_access_filter.sql` already executed in the target catalog.
# MAGIC - The `genie_config/room2_curation.json` file from this repo is uploaded to the workspace at the path
# MAGIC   given by the `curation_json_path` widget (defaults to a workspace-files path next to this notebook).
# MAGIC - A SQL warehouse you can read from — its ID goes in the `warehouse_id` widget.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_swv01_catalog", "Unity Catalog name")
dbutils.widgets.text("schema", "genie_availity_ops", "Schema name")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse ID")
dbutils.widgets.text(
    "curation_json_path",
    "/Workspace/Repos/swami.venkatesh@databricks.com/hin-genie/genie_config/room2_curation.json",
    "Path to room2_curation.json",
)
dbutils.widgets.dropdown(
    "refresh_existing", "no", ["no", "yes"],
    "Re-apply curation if room already exists?"
)

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
CURATION_PATH = dbutils.widgets.get("curation_json_path")
REFRESH_EXISTING = dbutils.widgets.get("refresh_existing") == "yes"

DISPLAY_NAME = "Clinical Document Intelligence - Provider Support & Calls"

if not WAREHOUSE_ID:
    raise ValueError("Set the warehouse_id widget to a SQL warehouse this notebook can read from.")

print(f"Catalog:              {CATALOG}")
print(f"Schema:               {SCHEMA}")
print(f"Warehouse:            {WAREHOUSE_ID}")
print(f"Curation file:        {CURATION_PATH}")
print(f"Display name:         {DISPLAY_NAME}")
print(f"Refresh if existing:  {REFRESH_EXISTING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and rewrite the curation JSON
# MAGIC
# MAGIC The exported JSON has fully-qualified table identifiers from the workspace where it was captured.
# MAGIC We rewrite them to point at the catalog/schema this notebook is configured for.

# COMMAND ----------

import json
import re

with open(CURATION_PATH, "r") as f:
    curation = json.load(f)

serialized = curation["serialized_space"]

SOURCE_CATALOG = "serverless_stable_swv01_catalog"
SOURCE_SCHEMA = "genie_availity_ops"

if (CATALOG, SCHEMA) != (SOURCE_CATALOG, SOURCE_SCHEMA):
    serialized_str = json.dumps(serialized)
    serialized_str = serialized_str.replace(
        f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.",
        f"{CATALOG}.{SCHEMA}.",
    )
    serialized_str = re.sub(
        rf"`{re.escape(SOURCE_CATALOG)}`\.`{re.escape(SOURCE_SCHEMA)}`\.",
        f"`{CATALOG}`.`{SCHEMA}`.",
        serialized_str,
    )
    serialized = json.loads(serialized_str)

n_tables = len(serialized.get("data_sources", {}).get("tables", []))
n_sample_qs = len(serialized.get("config", {}).get("sample_questions", []))
n_example_sqls = len(serialized.get("instructions", {}).get("example_question_sqls", []))
n_joins = len(serialized.get("instructions", {}).get("join_specs", []))
snippets = serialized.get("instructions", {}).get("sql_snippets", {}) or {}
n_filters = len(snippets.get("filters", []))
n_expressions = len(snippets.get("expressions", []))
n_measures = len(snippets.get("measures", []))
n_benchmarks = len(serialized.get("benchmarks", {}).get("questions", []))

print(f"Curation loaded:")
print(f"  tables                 : {n_tables}")
print(f"  sample_questions       : {n_sample_qs}")
print(f"  example_question_sqls  : {n_example_sqls}")
print(f"  join_specs             : {n_joins}")
print(f"  sql_snippets/filters   : {n_filters}")
print(f"  sql_snippets/expressions: {n_expressions}")
print(f"  sql_snippets/measures  : {n_measures}")
print(f"  benchmark questions    : {n_benchmarks}")
print()
print("Tables:")
for t in serialized.get("data_sources", {}).get("tables", []):
    print(f"  - {t.get('identifier')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detect existing room

# COMMAND ----------

import requests

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST = ctx.apiUrl().get()
TOKEN = ctx.apiToken().get()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def list_spaces():
    spaces = []
    page_token = None
    while True:
        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token
        r = requests.get(f"{HOST}/api/2.0/genie/spaces", headers=HEADERS, params=params)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        data = r.json()
        spaces.extend(data.get("spaces", []) or [])
        page_token = data.get("next_page_token")
        if not page_token:
            break
    return spaces


existing = None
for s in list_spaces():
    title = s.get("title") or s.get("display_name") or ""
    if title == DISPLAY_NAME:
        existing = s
        break

if existing:
    print(f"Room already exists: space_id={existing.get('space_id')}")
else:
    print("No existing room with this display name — will create.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or refresh the room

# COMMAND ----------

space_id = None
created = False

if existing and not REFRESH_EXISTING:
    space_id = existing.get("space_id")
    print(f"Skipping create/refresh. Set refresh_existing=yes to re-apply curation.")
elif existing and REFRESH_EXISTING:
    space_id = existing.get("space_id")
    print(f"Refreshing existing room {space_id} with curation from JSON...")
    r = requests.patch(
        f"{HOST}/api/2.0/genie/spaces/{space_id}",
        headers=HEADERS,
        json={"serialized_space": json.dumps(serialized)},
    )
    r.raise_for_status()
    print(f"  PATCH status: {r.status_code}")
else:
    print(f"Creating new room with curation applied in a single call...")
    payload = {
        "display_name": DISPLAY_NAME,
        "description": curation.get("description", ""),
        "warehouse_id": WAREHOUSE_ID,
        "serialized_space": json.dumps(serialized),
    }
    r = requests.post(f"{HOST}/api/2.0/genie/spaces", headers=HEADERS, json=payload)
    r.raise_for_status()
    new = r.json()
    space_id = new.get("space_id")
    created = True
    print(f"  Created space_id: {space_id}")

if not space_id:
    raise RuntimeError("space_id is empty — something went wrong.")

room_url = f"{HOST}/genie/rooms/{space_id}"
print(f"\nRoom URL: {room_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify curation landed

# COMMAND ----------

cur_state = requests.get(f"{HOST}/api/2.0/genie/spaces/{space_id}", headers=HEADERS).json()
desc = cur_state.get("description", "")
refreshed = requests.patch(
    f"{HOST}/api/2.0/genie/spaces/{space_id}",
    headers=HEADERS,
    json={"description": desc},
).json()
parsed = json.loads(refreshed.get("serialized_space", "") or "{}")

actual = {
    "tables": len(parsed.get("data_sources", {}).get("tables", [])),
    "sample_questions": len(parsed.get("config", {}).get("sample_questions", [])),
    "example_question_sqls": len(parsed.get("instructions", {}).get("example_question_sqls", [])),
    "benchmark_questions": len(parsed.get("benchmarks", {}).get("questions", [])),
}

print(f"Live state of {space_id}:")
for k, v in actual.items():
    print(f"  {k:25s}: {v}")

print(f"\nOpen the room: {room_url}")
print(f"Try a question: 'Which agents have the lowest compliance scores?' or 'Show me sentiment trends for escalated calls'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC - The room is fully provisioned with tables, sample questions, instructions, sample SQL queries,
# MAGIC   joins, filter / expression / measure snippets, and benchmark questions.
# MAGIC - Re-running this notebook is a no-op unless `refresh_existing` is set to `yes`.
# MAGIC - To capture future UI edits back into the JSON, run `genie_config/export_rooms.py` from a workstation
# MAGIC   that has the Databricks CLI configured.
