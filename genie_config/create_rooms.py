"""
Clinical Document Intelligence -- Genie Room Provisioning

Creates two Genie Rooms via the Databricks Genie API:
  Room 1: Document Processing & Authorization Intelligence
  Room 2: Provider Support & Call Intelligence

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
    from databricks.sdk.service.dashboards import GenieSpace, GenieTableIdentifier
except ImportError:
    print("ERROR: databricks-sdk is required. Install with: pip install databricks-sdk")
    sys.exit(1)

# ============================================================
# Configuration
# ============================================================

CATALOG = os.getenv("GENIE_CATALOG", "serverless_stable_swv01_catalog")
SCHEMA = "genie_availity_ops"
WAREHOUSE_ID = os.getenv("GENIE_WAREHOUSE_ID", "")  # Set via env or fill in here

# Room 1: Document Processing & Authorization Intelligence
ROOM_1_CONFIG = {
    "display_name": "Clinical Document Intelligence - Document Processing",
    "description": (
        "Ask questions about clinical document intake, OCR quality, "
        "Fellegi-Sunter member/authorization matching, and pipeline KPIs. "
        "Covers daily trends, spike detection, risk tiering, and data quality degradation."
    ),
    "table_identifiers": [
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "genie_doc_intake_daily"},
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "genie_doc_match_detail"},
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "genie_auth_match_daily"},
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "genie_data_quality_daily"},
        {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": "genie_pipeline_snapshot"},
    ],
}

# Room 2: Provider Support & Call Intelligence
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
    ],
}


def create_room(client: WorkspaceClient, config: dict, warehouse_id: str) -> dict:
    """Create a single Genie Room and return its metadata."""
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

    return {
        "space_id": space.space_id,
        "display_name": config["display_name"],
        "table_count": len(table_ids),
    }


def main():
    # Validate warehouse ID
    if not WAREHOUSE_ID:
        print("ERROR: Set GENIE_WAREHOUSE_ID environment variable or edit WAREHOUSE_ID in this script.")
        print("  Example: export GENIE_WAREHOUSE_ID='abc123def456'")
        sys.exit(1)

    # Initialize client (uses DATABRICKS_HOST + DATABRICKS_TOKEN or CLI profile)
    client = WorkspaceClient()
    host = client.config.host
    print(f"Connected to workspace: {host}")
    print(f"Catalog: {CATALOG}")
    print(f"Schema: {SCHEMA}")
    print(f"Warehouse ID: {WAREHOUSE_ID}")
    print()

    results = {}

    # Create Room 1
    print("Creating Room 1: Document Processing & Authorization Intelligence...")
    room1 = create_room(client, ROOM_1_CONFIG, WAREHOUSE_ID)
    results["room1"] = room1
    room1_url = f"{host}/genie/rooms/{room1['space_id']}"
    print(f"  Room 1 created: {room1_url}")
    print(f"  Tables: {room1['table_count']}")
    print()

    # Create Room 2
    print("Creating Room 2: Provider Support & Call Intelligence...")
    room2 = create_room(client, ROOM_2_CONFIG, WAREHOUSE_ID)
    results["room2"] = room2
    room2_url = f"{host}/genie/rooms/{room2['space_id']}"
    print(f"  Room 2 created: {room2_url}")
    print(f"  Tables: {room2['table_count']}")
    print()

    # Save room IDs to JSON for reference
    output_path = Path(__file__).parent / "room_ids.json"
    output = {
        "room1": {
            "space_id": room1["space_id"],
            "display_name": room1["display_name"],
            "url": room1_url,
        },
        "room2": {
            "space_id": room2["space_id"],
            "display_name": room2["display_name"],
            "url": room2_url,
        },
    }
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Room IDs saved to: {output_path}")

    # Summary
    print()
    print("=" * 60)
    print("Genie Rooms created successfully.")
    print(f"  Room 1 (Doc Processing): {room1_url}")
    print(f"  Room 2 (Call Intelligence): {room2_url}")
    print("=" * 60)


if __name__ == "__main__":
    main()
