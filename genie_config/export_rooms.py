"""
Clinical Document Intelligence — Genie Room Curation Export

Pulls the live in-workspace curation for both Genie Rooms and writes
each space's full configuration (tables, sample questions, text
instructions, sample SQL queries, joins, snippets, benchmarks) to
JSON. Run this after any UI edit so the repo is the source of truth.

Output:
  genie_config/room1_curation.json
  genie_config/room2_curation.json

Mechanism:
  The Databricks REST API exposes the full space configuration as a
  string field `serialized_space` on the PATCH response. This script
  performs a no-op PATCH (re-setting the description to its current
  value) to read back the serialized payload, then parses and writes
  it to disk.

Prerequisites:
  Databricks CLI configured with profile fe-vm-fevm-serverless-stable-swv01,
  or set GENIE_PROFILE to override.

Usage:
  python genie_config/export_rooms.py
"""

import json
import os
import subprocess
import sys
from pathlib import Path

PROFILE = os.getenv("GENIE_PROFILE", "fe-vm-fevm-serverless-stable-swv01")

ROOMS = [
    ("01f13f17036e100f9a7e09b2ec0393ab", "room1_curation.json"),
    ("01f13f1703ce199ebd91803207433969", "room2_curation.json"),
]

OUT_DIR = Path(__file__).parent


def run_databricks_api(method, path, payload=None):
    cmd = ["databricks", "api", method, path, f"--profile={PROFILE}"]
    if payload is not None:
        cmd += ["--json", json.dumps(payload)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"databricks api {method} {path} failed: {r.stderr}")
    return json.loads(r.stdout)


def export_room(space_id, out_filename):
    # Read current description (no-op PATCH needs a value to resend).
    current = run_databricks_api("get", f"/api/2.0/genie/spaces/{space_id}")
    description = current.get("description", "")

    # No-op PATCH returns the full serialized_space.
    refreshed = run_databricks_api("patch", f"/api/2.0/genie/spaces/{space_id}", {"description": description})

    serialized = refreshed.get("serialized_space", "")
    parsed = json.loads(serialized) if serialized else {}

    payload = {
        "space_id": refreshed.get("space_id"),
        "title": refreshed.get("title"),
        "description": refreshed.get("description"),
        "warehouse_id": refreshed.get("warehouse_id"),
        "serialized_space": parsed,
    }

    out_path = OUT_DIR / out_filename
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2)

    n_tables = len(parsed.get("data_sources", {}).get("tables", []))
    n_questions = len(parsed.get("config", {}).get("sample_questions", []))
    n_examples = len(parsed.get("instructions", {}).get("example_question_sqls", []))
    n_benchmarks = len(parsed.get("benchmarks", {}).get("questions", []))
    print(f"  {out_filename:30s} title={payload['title']!r}")
    print(f"    tables={n_tables}  sample_questions={n_questions}  example_sqls={n_examples}  benchmarks={n_benchmarks}")


def main():
    print(f"Profile: {PROFILE}")
    print(f"Output:  {OUT_DIR}\n")
    for space_id, filename in ROOMS:
        print(f"=== {space_id} ===")
        export_room(space_id, filename)
    print("\nDone. Commit the JSON files to capture this curation.")


if __name__ == "__main__":
    main()
