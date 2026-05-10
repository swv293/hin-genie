"""
Clinical Document Intelligence -- Genie Room Provisioning (idempotent)

Provisions or refreshes two Genie Rooms:
  Room 1: Document Processing & Authorization Intelligence
  Room 2: Provider Support & Call Intelligence

Behavior:
  - If a room already exists (by display_name match), skips creation and
    preserves existing in-room curation (instructions, sample queries,
    snippets, joins).
  - If a room is missing, creates it with the table identifiers below
    and prints a reminder to import the curation JSON via the UI.

Source of truth for curation:
  - genie_config/room1_curation.json
  - genie_config/room2_curation.json

These exports capture the live in-workspace state of each room and are
the canonical reference for what each room should contain. Re-export
after any UI edits with `genie_config/export_rooms.py`.

Prerequisites:
  - pip install databricks-sdk
  - Databricks CLI configured with a workspace profile, OR
    set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.

Usage:
  python genie_config/create_rooms.py
"""

import json
import os
import sys
from pathlib import Path

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.dashboards import GenieTableIdentifier
except ImportError:
    print("ERROR: databricks-sdk is required. Install with: pip install databricks-sdk")
    sys.exit(1)


# ============================================================
# Configuration
# ============================================================

CATALOG = os.getenv("GENIE_CATALOG", "serverless_stable_swv01_catalog")
SCHEMA = "genie_availity_ops"
WAREHOUSE_ID = os.getenv("GENIE_WAREHOUSE_ID", "")  # Set via env or fill in here

CURATION_DIR = Path(__file__).parent

# Room 1: Document Processing & Authorization Intelligence
# Tables include 5 base views + 2 metric views (mv_doc_intake_metrics, mv_doc_match_metrics).
ROOM_1_CONFIG = {
    "display_name": "Clinical Document Intelligence - Document Processing",
    "description": (
        "Ask questions about clinical document intake, OCR quality, "
        "Fellegi-Sunter member/authorization matching, and pipeline KPIs. "
        "Covers daily trends, spike detection, risk tiering, and data quality degradation."
    ),
    "table_identifiers": [
        # Base curated views
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "genie_doc_intake_daily"},
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "genie_doc_match_detail"},
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "genie_auth_match_daily"},
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "genie_data_quality_daily"},
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "genie_pipeline_snapshot"},
        # Metric views — pre-defined aggregations register the canonical math
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "mv_doc_intake_metrics"},
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "mv_doc_match_metrics"},
    ],
    "curation_file": "room1_curation.json",
}

# Room 2: Provider Support & Call Intelligence
# Tables include 3 base views + 1 metric view (mv_call_quality_metrics).
ROOM_2_CONFIG = {
    "display_name": "Clinical Document Intelligence - Provider Support & Calls",
    "description": (
        "Ask questions about provider call quality scores, agent performance rankings, "
        "AI-generated sentiment analysis, and compliance tracking. "
        "Covers agent benchmarks, rolling trends, and consecutive-day compliance streaks."
    ),
    "table_identifiers": [
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "genie_call_scores"},
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "genie_call_sentiment"},
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "genie_compliance_daily"},
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "mv_call_quality_metrics"},
    ],
    "curation_file": "room2_curation.json",
}


def find_existing_space(client, display_name):
    """Return the space_id of any existing space with the given display name."""
    try:
        # The list endpoint returns spaces; iterate and match by title.
        for space in client.genie.list_spaces():
            if (space.title or "") == display_name or (getattr(space, "display_name", "") or "") == display_name:
                return space.space_id
    except Exception as e:
        print(f"  WARN: list_spaces failed ({e}); falling back to create-only behavior.")
    return None


def create_room(client, config, warehouse_id):
    """Create a Genie space if it doesn't already exist. Returns (space_id, created_bool)."""
    existing = find_existing_space(client, config["display_name"])
    if existing:
        print(f"  EXISTS — preserving in-room curation (instructions, sample queries, snippets).")
        print(f"  space_id: {existing}")
        return existing, False

    table_ids = [
        GenieTableIdentifier(
            catalog_name=t["catalog_name"],
            schema_name=t["schema_name"],
            table_name=t["table_name"],
        )
        for t in config["table_identifiers"]
    ]
    space = client.genie.create_space(
        display_name=config["display_name"],
        description=config["description"],
        table_identifiers=table_ids,
        warehouse_id=warehouse_id,
    )
    print(f"  CREATED — space_id: {space.space_id}")
    print(f"  Curation reference: {CURATION_DIR / config['curation_file']}")
    print( "  Next step: import sample queries / instructions / snippets from the JSON via UI,")
    print( "  or use export_rooms.py after manual UI curation to refresh the JSON.")
    return space.space_id, True


def main():
    if not WAREHOUSE_ID:
        print("ERROR: Set GENIE_WAREHOUSE_ID env var or edit WAREHOUSE_ID in this script.")
        sys.exit(1)

    client = WorkspaceClient()
    host = client.config.host
    print(f"Workspace: {host}")
    print(f"Catalog:   {CATALOG}")
    print(f"Schema:    {SCHEMA}")
    print(f"Warehouse: {WAREHOUSE_ID}\n")

    results = {}
    for label, config in [("room1", ROOM_1_CONFIG), ("room2", ROOM_2_CONFIG)]:
        print(f"=== {config['display_name']} ===")
        space_id, created = create_room(client, config, WAREHOUSE_ID)
        results[label] = {
            "space_id": space_id,
            "display_name": config["display_name"],
            "url": f"{host}/genie/rooms/{space_id}",
            "created_now": created,
            "curation_file": str(CURATION_DIR / config["curation_file"]),
        }
        print()

    # Persist current room IDs for downstream tooling.
    output_path = CURATION_DIR / "room_ids.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Room IDs written to: {output_path}\n")

    print("=" * 64)
    for label, r in results.items():
        print(f"  {r['display_name']}")
        print(f"    URL:           {r['url']}")
        print(f"    curation:      {r['curation_file']}")
    print("=" * 64)


if __name__ == "__main__":
    main()
