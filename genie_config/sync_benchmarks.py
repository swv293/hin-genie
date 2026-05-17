"""
Clinical Document Intelligence — Benchmark sync (curated-questions API).

Posts benchmarks to /api/2.0/data-rooms/{id}/curated-questions with
question_type: BENCHMARK. Each canonical Q&A is followed by 2-3 alternate
phrasings (same answer SQL, different user wording) — per Databricks
guidance, this tests space robustness across realistic phrasings.

⚠️  CRITICAL ORDERING — RUN THIS LAST ⚠️
========================================
Every PATCH to /api/2.0/genie/spaces/{id} with serialized_space WIPES
all BENCHMARK rows from /curated-questions. There is no documented way
to preserve them across PATCH.

Required workflow whenever you change the room:
  1. Apply SQL migrations (sql/04 → sql/09)
  2. PATCH serialized_space (notebooks/create_room*_*.py OR create_rooms.py)
  3. RUN THIS SCRIPT LAST

If you re-PATCH the room later for any reason, you MUST re-run this
script. Observed in this repo on 2026-05-17: a single mid-day PATCH
wiped 14 BENCHMARKs from Room 1, leaving the Benchmarks tab empty.

Why this happens (Databricks-side): the BENCHMARK type is stored
alongside SAMPLE_QUESTIONs, which serialized_space owns. A PATCH
appears to rewrite the curated-questions collection from
serialized_space.config.sample_questions, dropping anything not in
that list — BENCHMARK rows included.

Usage:
  python genie_config/sync_benchmarks.py
"""

import json
import os
import subprocess

PROFILE = os.getenv("GENIE_PROFILE", "fe-vm-fevm-serverless-stable-swv01")

ROOM_1_ID = "01f13f17036e100f9a7e09b2ec0393ab"
ROOM_2_ID = "01f13f1703ce199ebd91803207433969"

# Each tuple: (phrasings list, answer SQL).
# Multiple phrasings of the same intent share the same answer SQL — they're
# regression tests for whether Genie routes consistently across wording.

BENCHMARKS_ROOM1 = [
    # Core 1: overall match rate (3 phrasings)
    (["What is the overall document match rate?",
      "Show me the match rate",
      "How well are documents matching?"],
     "SELECT ROUND(100.0 * SUM(CASE WHEN match_class = 'match' THEN 1 ELSE 0 END) / COUNT(*), 2) AS match_rate_pct "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_doc_match_detail"),
    # Core 2: PA approval rate per payer (3 phrasings)
    (["Approval rate and SLA compliance per payer",
      "Which payer has the lowest approval rate?",
      "Compare PA approval across payers"],
     "WITH m AS (SELECT payer_code, MEASURE(approval_rate_pct) AS approval_pct, MEASURE(sla_compliance_pct) AS sla_pct "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.mv_pa_metrics GROUP BY payer_code) "
     "SELECT * FROM m ORDER BY approval_pct DESC"),
    # Core 3: urgent SLA compliance (3 phrasings)
    (["Are we hitting the CMS 72-hour urgent PA SLA?",
      "What is our urgent PA SLA compliance this week?",
      "Urgent PA on-time percentage by payer"],
     "WITH m AS (SELECT payer_code, MEASURE(sla_compliance_pct) AS sla_pct "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.mv_pa_metrics "
     "WHERE urgency = 'urgent' GROUP BY payer_code) "
     "SELECT * FROM m ORDER BY sla_pct ASC"),
    # Core 4: payer mix (3 phrasings)
    (["Show me payer mix by document volume",
      "Which payer dominates intake?",
      "Payer volume breakdown"],
     "SELECT payer_code, total_documents, total_pa_decisions, pa_approval_rate_pct "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_payer_mix "
     "ORDER BY total_documents DESC"),
    # Unique 5: docs received last 7 days
    (["How many documents were received in the last 7 days?"],
     "SELECT SUM(total_docs) AS docs_last_7_days "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_doc_intake_daily "
     "WHERE intake_date >= CURRENT_DATE - INTERVAL 7 DAYS"),
    # Unique 6: data-quality degradation
    (["Days where data quality degradation was detected"],
     "SELECT parse_date, pct_unreadable, rolling_7d_unreadable_pct, unreadable_trend_3d "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_data_quality_daily "
     "WHERE dq_degradation_flag = true ORDER BY parse_date DESC"),
]

BENCHMARKS_ROOM2 = [
    # Core 1: FCR per agency (3 phrasings)
    (["FCR rate per agency",
      "Which agencies are missing FCR target?",
      "First Call Resolution by agency"],
     "WITH m AS (SELECT agency_name, MEASURE(fcr_pct) AS fcr "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.mv_call_ops_metrics GROUP BY agency_name) "
     "SELECT * FROM m ORDER BY fcr DESC"),
    # Core 2: AHT by agency (3 phrasings)
    (["Average handle time by agency",
      "Show AHT for each agency",
      "Which agency has the longest AHT?"],
     "WITH m AS (SELECT agency_name, MEASURE(avg_handle_seconds) AS aht_sec "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.mv_call_ops_metrics GROUP BY agency_name) "
     "SELECT * FROM m ORDER BY aht_sec DESC"),
    # Core 3: compliance trend (3 phrasings)
    (["How is our compliance rate this week?",
      "Are any agencies trending below compliance?",
      "Show compliance rate over the last 14 days"],
     "WITH m AS (SELECT day, MEASURE(compliance_rate_pct) AS comp_pct "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.mv_call_quality_metrics "
     "WHERE day >= CURRENT_DATE - INTERVAL 14 DAYS GROUP BY day) "
     "SELECT * FROM m ORDER BY day DESC"),
    # Core 4: top agents (3 phrasings)
    (["Top 10 agents by call quality score",
      "Which agents have the highest scores?",
      "Show me the best agents with at least 20 calls"],
     "WITH ranked AS (SELECT agent_name, agency_name, ROUND(AVG(call_score), 1) AS avg_score, COUNT(*) AS n_calls "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_call_scores "
     "GROUP BY agent_name, agency_name HAVING COUNT(*) >= 20) "
     "SELECT agent_name, agency_name, avg_score, n_calls FROM ranked ORDER BY avg_score DESC LIMIT 10"),
    # Unique 5: sentiment distribution
    (["Sentiment distribution for escalated calls"],
     "SELECT sentiment_overall, COUNT(*) AS n "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_call_sentiment "
     "WHERE call_type ILIKE '%escal%' GROUP BY sentiment_overall ORDER BY n DESC"),
    # Unique 6: consecutive-day streaks
    (["Consecutive-day compliance streaks above 3 days"],
     "SELECT agency_name, call_type, day, days_below_threshold "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_compliance_daily "
     "WHERE days_below_threshold >= 3 ORDER BY days_below_threshold DESC, day DESC"),
]


def api(method, path, payload=None):
    cmd = ["databricks", "api", method, path, f"--profile={PROFILE}"]
    if payload is not None:
        cmd += ["--json", json.dumps(payload)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        return {"_err": r.stderr[:300]}
    try:
        return json.loads(r.stdout, strict=False)
    except Exception:
        cleaned = "".join(c if ord(c) >= 32 or c in "\n\t" else " " for c in r.stdout)
        return json.loads(cleaned, strict=False)


def list_curated(space_id):
    out = api("get", f"/api/2.0/data-rooms/{space_id}/curated-questions")
    if "_err" in out:
        print(f"  list failed: {out['_err']}")
        return []
    return out.get("curated_questions", []) or out.get("questions", []) or []


def delete_curated(space_id, question_id):
    return api("delete", f"/api/2.0/data-rooms/{space_id}/curated-questions/{question_id}")


def post_benchmark(space_id, question_text, sql):
    payload = {
        "curated_question": {
            "question_text": question_text,
            "answer_text": sql,
            "question_type": "BENCHMARK",
        }
    }
    return api("post", f"/api/2.0/data-rooms/{space_id}/curated-questions", payload)


def sync_room(space_id, label, benchmarks):
    print(f"\n=== {label} ({space_id}) ===")
    existing = list_curated(space_id)
    bench_existing = [q for q in existing if q.get("question_type") == "BENCHMARK"]
    print(f"  existing BENCHMARK rows: {len(bench_existing)} (will replace)")
    for q in bench_existing:
        qid = q.get("id") or q.get("curated_question_id") or q.get("question_id")
        if qid:
            delete_curated(space_id, qid)
    n_ok = n_fail = 0
    for phrasings, sql in benchmarks:
        for q_text in phrasings:
            out = post_benchmark(space_id, q_text, sql)
            if "_err" in out or (out.get("error_code") if isinstance(out, dict) else False):
                n_fail += 1
                print(f"  ❌ {q_text[:60]} — {out.get('_err','') or out.get('message','')[:200]}")
            else:
                n_ok += 1
                print(f"  ✅ {q_text[:60]}")
    print(f"  posted: {n_ok} ok, {n_fail} fail")


def main():
    sync_room(ROOM_1_ID, "Room 1 — Document Processing", BENCHMARKS_ROOM1)
    sync_room(ROOM_2_ID, "Room 2 — Provider Support & Calls", BENCHMARKS_ROOM2)


if __name__ == "__main__":
    main()
