"""
Clinical Document Intelligence — Benchmark sync (curated-questions API).

Per the Databricks Genie data-rooms API (and our internal feedback memory),
benchmarks live at `/api/2.0/data-rooms/{id}/curated-questions` with
`question_type: BENCHMARK`. The benchmarks block inside `serialized_space`
does NOT render in the Benchmarks tab.

This script:
  1. Reads BENCHMARKS_ROOM1 / BENCHMARKS_ROOM2 below.
  2. Lists existing curated questions for each room.
  3. Deletes any BENCHMARKs we previously POSTed (idempotent refresh).
  4. POSTs the canonical benchmark set for each room.

Run after `04 → 08` SQL has been applied and rooms have been PATCHed with
the latest serialized_space.

Usage:
  python genie_config/sync_benchmarks.py
"""

import json
import os
import subprocess
import sys

PROFILE = os.getenv("GENIE_PROFILE", "fe-vm-fevm-serverless-stable-swv01")

ROOM_1_ID = "01f13f17036e100f9a7e09b2ec0393ab"
ROOM_2_ID = "01f13f1703ce199ebd91803207433969"

BENCHMARKS_ROOM1 = [
    ("What is the overall document match rate?",
     "SELECT ROUND(100.0 * SUM(CASE WHEN match_class = 'match' THEN 1 ELSE 0 END) / COUNT(*), 2) AS match_rate_pct "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_doc_match_detail"),
    ("How many documents were received in the last 7 days?",
     "SELECT SUM(total_docs) AS docs_last_7_days "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_doc_intake_daily "
     "WHERE intake_date >= CURRENT_DATE - INTERVAL 7 DAYS"),
    ("Which document types have the worst match rate?",
     "SELECT document_type, COUNT(*) AS total_docs, ROUND(100.0 * SUM(CASE WHEN match_class = 'match' THEN 1 ELSE 0 END) / COUNT(*), 2) AS match_rate_pct "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_doc_match_detail "
     "GROUP BY document_type ORDER BY match_rate_pct ASC"),
    ("Approval rate and SLA compliance per payer",
     "WITH m AS (SELECT payer_code, MEASURE(approval_rate_pct) AS approval_pct, MEASURE(sla_compliance_pct) AS sla_pct "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.mv_pa_metrics GROUP BY payer_code) "
     "SELECT * FROM m ORDER BY approval_pct DESC"),
    ("CMS 72-hour urgent PA SLA compliance trend",
     "WITH m AS (SELECT decision_date, MEASURE(sla_compliance_pct) AS sla_pct "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.mv_pa_metrics "
     "WHERE urgency = 'urgent' GROUP BY decision_date) "
     "SELECT * FROM m ORDER BY decision_date DESC LIMIT 30"),
    ("Payer mix by document volume",
     "SELECT payer_code, total_documents, total_pa_decisions, pa_approval_rate_pct "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_payer_mix ORDER BY total_documents DESC"),
    ("Document risk tier breakdown",
     "SELECT doc_risk_tier, COUNT(*) AS doc_count, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_doc_match_detail "
     "GROUP BY doc_risk_tier ORDER BY doc_count DESC"),
    ("Days where data quality degradation was detected",
     "SELECT parse_date, pct_unreadable, rolling_7d_unreadable_pct, unreadable_trend_3d "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_data_quality_daily "
     "WHERE dq_degradation_flag = true ORDER BY parse_date DESC"),
]

BENCHMARKS_ROOM2 = [
    ("Top 10 agents by call quality score",
     "WITH ranked AS (SELECT agent_name, agent_avg_score, agent_call_count, "
     "RANK() OVER (ORDER BY agent_avg_score DESC) AS rk "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_call_scores WHERE agent_call_count >= 20) "
     "SELECT agent_name, agent_avg_score, agent_call_count FROM ranked WHERE rk <= 10 ORDER BY rk"),
    ("Agencies below compliance threshold this month",
     "SELECT agency_name, ROUND(AVG(compliance_rate)*100, 2) AS avg_compliance_pct "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_compliance_daily "
     "WHERE day >= DATE_TRUNC('MONTH', CURRENT_DATE) "
     "GROUP BY agency_name HAVING AVG(compliance_rate) < 0.85 ORDER BY avg_compliance_pct ASC"),
    ("Consecutive-day compliance streaks above 3 days",
     "SELECT agency_name, call_type, day, days_below_threshold "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_compliance_daily "
     "WHERE days_below_threshold >= 3 ORDER BY days_below_threshold DESC, day DESC"),
    ("FCR / AHT / ASA per agency",
     "WITH m AS (SELECT agency_name, MEASURE(fcr_pct) AS fcr, MEASURE(avg_handle_seconds) AS aht_sec, MEASURE(avg_wait_seconds) AS asa_sec "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.mv_call_ops_metrics GROUP BY agency_name) "
     "SELECT * FROM m ORDER BY fcr DESC"),
    ("Agencies missing the 70% FCR benchmark",
     "WITH m AS (SELECT agency_name, MEASURE(fcr_pct) AS fcr "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.mv_call_ops_metrics GROUP BY agency_name) "
     "SELECT agency_name, fcr FROM m WHERE fcr < 70 ORDER BY fcr ASC"),
    ("Sentiment distribution for escalated calls",
     "SELECT sentiment_overall, COUNT(*) AS n FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_call_sentiment "
     "WHERE call_type ILIKE '%escal%' GROUP BY sentiment_overall ORDER BY n DESC"),
    ("Calls with declining sentiment trajectory in the last week",
     "SELECT call_type, sentiment_start, sentiment_end, COUNT(*) AS n "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_call_sentiment "
     "WHERE sentiment_trajectory = 'declining' GROUP BY call_type, sentiment_start, sentiment_end ORDER BY n DESC LIMIT 20"),
    ("Bottom 10 agents by average score with call counts",
     "WITH agg AS (SELECT agent_name, agency_name, AVG(call_score) AS avg_score, COUNT(*) AS n_calls "
     "FROM serverless_stable_swv01_catalog.genie_availity_ops.genie_call_scores "
     "GROUP BY agent_name, agency_name HAVING COUNT(*) >= 20) "
     "SELECT agent_name, agency_name, ROUND(avg_score, 1) AS avg_score, n_calls "
     "FROM agg ORDER BY avg_score ASC LIMIT 10"),
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
    """List all curated questions for a room. Pagination not used (small surface)."""
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
    # 1. List existing
    existing = list_curated(space_id)
    bench_existing = [q for q in existing if q.get("question_type") == "BENCHMARK"]
    print(f"  existing BENCHMARK rows: {len(bench_existing)}")
    # 2. Delete previously POSTed benchmarks (idempotent refresh)
    for q in bench_existing:
        qid = q.get("id") or q.get("curated_question_id") or q.get("question_id")
        if qid:
            delete_curated(space_id, qid)
    # 3. POST canonical set
    n_ok = n_fail = 0
    for q_text, sql in benchmarks:
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
