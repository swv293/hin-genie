# Databricks notebook source
# MAGIC %md
# MAGIC # Clinical Document Intelligence — Genie Room Setup
# MAGIC
# MAGIC End-to-end setup notebook: creates schemas, generates synthetic data, deploys 8 curated views, and provisions two Genie Rooms via API.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Serverless or Pro SQL warehouse
# MAGIC - Unity Catalog with CREATE SCHEMA privilege on target catalog
# MAGIC - `faker`, `pandas`, `numpy` installed on the cluster (`%pip install` cell below)

# COMMAND ----------

# MAGIC %pip install faker pandas numpy --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Edit these widgets to match your workspace.

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_swv01_catalog", "Unity Catalog Name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (for Genie Rooms)")

CATALOG = dbutils.widgets.get("catalog")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")

print(f"Catalog:      {CATALOG}")
print(f"Warehouse ID: {WAREHOUSE_ID or '(not set — Genie room creation will be skipped)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Create Schemas

# COMMAND ----------

SCHEMAS = {
    "ref": "Reference/master data. Contains member golden records.",
    "raw": "Raw ingested data. Clinical documents and authorization records as received from source systems.",
    "pipeline_prd": "Production pipeline streaming tables. OCR parsing, structured extraction, and Fellegi-Sunter match scoring.",
    "dashboard_prd": "Production dashboard views. Pre-aggregated KPIs for operational dashboards.",
    "genie_availity_ops": "Curated views for Clinical Document Intelligence Genie Rooms. Read-only overlays on pipeline and call center data.",
    "transcript_intel_sdp": "Call center transcript intelligence. Scored calls, sentiment analysis, and compliance outcomes.",
}

for schema, comment in SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{comment}'")
    print(f"  ✓ {CATALOG}.{schema}")

print("\nSchemas ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Generate Synthetic Data
# MAGIC
# MAGIC Generates 9 source tables with realistic distributions: weekday-heavy timestamps, spike days, channel-weighted quality scores, Fellegi-Sunter scoring with proper log-likelihood weights.

# COMMAND ----------

import random, math, uuid, hashlib
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

ROW_COUNTS = {
    "member": 1000,
    "authorization": 3000,
    "clinical_document": 6000,
}

START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 3, 31)

DOC_TYPES = ["prior_auth_form", "clinical_note", "lab_result", "imaging_report", "discharge_summary"]
CHANNELS = ["fax", "electronic", "upload", "mail"]
CHANNEL_WEIGHTS = [0.35, 0.30, 0.25, 0.10]
LANGUAGES = ["en", "es", "mixed"]
DISPOSITIONS = ["resolved", "pending", "escalated", "complaint", "appeal_opened"]
SENTIMENTS_OVERALL = ["positive", "neutral", "negative", "mixed"]
SENTIMENTS_START = ["calm", "warm", "neutral", "uncertain", "tired", "irritated", "hot"]
SENTIMENTS_END = ["satisfied", "relieved", "flat", "grudging_accept", "complaint_opened", "enthusiastic", "neutral"]
TRAJECTORIES = ["stable", "improving", "declining", "volatile", "stable_high", "stable_low", "declining_then_flat"]
AGENCIES = ["Alpha Health Services", "Beta Provider Support", "Gamma Call Center", "Delta Solutions"]
AGENTS = [fake.name() for _ in range(40)]
CALL_TYPES = ["prior_auth_inquiry", "claims_status", "eligibility_check", "benefit_inquiry", "provider_enrollment"]
EMOTIONAL_MARKERS = ["frustration", "clarification_question", "supervisor_request", "code_switching", "hold_used"]
TOPIC_POOL = [
    "prior authorization status", "claim denial", "member eligibility", "referral needed",
    "out-of-network", "copay amount", "deductible status", "formulary exception",
    "appeals process", "provider credentialing", "timely filing", "coordination of benefits",
]

SPIKE_DAYS = set()
d = START_DATE
while d <= END_DATE:
    if random.random() < 0.06:
        SPIKE_DAYS.add(d.date())
    d += timedelta(days=1)


def random_ts(start=START_DATE, end=END_DATE, weekday_heavy=True):
    while True:
        ts = start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))
        if weekday_heavy and ts.weekday() >= 5 and random.random() < 0.6:
            continue
        if ts.date() in SPIKE_DAYS:
            if random.random() < 0.5:
                return ts
        return ts


def fs_weight(match_prob, unmatch_prob):
    if match_prob <= 0 or unmatch_prob <= 0:
        return 0.0
    return round(math.log2(match_prob / unmatch_prob), 4)


print("Generating members...")
members = []
for i in range(ROW_COUNTS["member"]):
    mid = f"MBR{i+1:06d}"
    members.append({
        "member_id": mid,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "dob": fake.date_of_birth(minimum_age=1, maximum_age=95).isoformat(),
        "ssn_last4": f"{random.randint(0,9999):04d}",
        "gender": random.choice(["M", "F"]),
        "state": fake.state_abbr(),
        "payer_code": f"PYR{random.randint(1,12):03d}",
    })

print("Generating authorizations...")
auths = []
for i in range(ROW_COUNTS["authorization"]):
    aid = f"AUTH{i+1:08d}"
    m = random.choice(members)
    auths.append({
        "auth_id": aid,
        "member_id": m["member_id"],
        "auth_type": random.choice(["inpatient", "outpatient", "imaging", "medication", "dme"]),
        "status": random.choice(["approved", "pending", "denied", "expired"]),
        "service_start": (START_DATE + timedelta(days=random.randint(0, 60))).date().isoformat(),
        "service_end": (START_DATE + timedelta(days=random.randint(61, 120))).date().isoformat(),
        "payer_code": m["payer_code"],
        "provider_npi": f"{random.randint(1000000000, 9999999999)}",
        "created_ts": random_ts().isoformat(),
    })

print("Generating clinical documents...")
docs = []
for i in range(ROW_COUNTS["clinical_document"]):
    did = str(uuid.uuid4())
    channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]
    base_q = {"fax": 0.65, "electronic": 0.92, "upload": 0.88, "mail": 0.60}[channel]
    quality = round(min(1.0, max(0.0, np.random.normal(base_q, 0.12))), 3)
    docs.append({
        "doc_id": did,
        "document_type": random.choice(DOC_TYPES),
        "source_channel": channel,
        "quality_score": quality,
        "is_readable": quality > 0.3,
        "page_count": random.randint(1, 25),
        "ingestion_timestamp": random_ts().isoformat(),
        "payer_code": f"PYR{random.randint(1,12):03d}",
    })

print("Generating parsed documents...")
parsed = []
for d in docs:
    unreadable = not d["is_readable"]
    parsed.append({
        "doc_id": d["doc_id"],
        "unreadable_flag": unreadable,
        "page_count_detected": d["page_count"] if not unreadable else 0,
        "parse_error_status": random.choice([None, None, None, "timeout", "corrupt_header"]) if not unreadable else "unreadable",
        "ingest_ts": d["ingestion_timestamp"],
    })

print("Generating structured extractions...")
structured = []
for d in docs:
    if not d["is_readable"]:
        continue
    m = random.choice(members)
    a = random.choice(auths)
    structured.append({
        "doc_id": d["doc_id"],
        "member_id_on_form": m["member_id"] if random.random() > 0.15 else None,
        "auth_id": a["auth_id"] if random.random() > 0.20 else None,
        "dob_extracted": m["dob"] if random.random() > 0.10 else None,
        "ssn4_extracted": m["ssn_last4"] if random.random() > 0.25 else None,
        "missing_dob": random.random() < 0.10,
        "missing_ssn4": random.random() < 0.25,
        "extraction_ts": d["ingestion_timestamp"],
    })

print("Generating member match candidates...")
member_matches = []
for s in structured:
    m = random.choice(members)
    name_w = fs_weight(random.uniform(0.7, 0.99), random.uniform(0.01, 0.3))
    dob_w = fs_weight(random.uniform(0.6, 0.99), random.uniform(0.01, 0.2)) if not s["missing_dob"] else 0.0
    ssn_w = fs_weight(random.uniform(0.8, 0.99), random.uniform(0.01, 0.1)) if not s["missing_ssn4"] else 0.0
    total = round(name_w + dob_w + ssn_w, 4)
    if total > 8:
        mc = "match"
    elif total > 3:
        mc = "possible_match"
    else:
        mc = "non_match"
    member_matches.append({
        "doc_id": s["doc_id"],
        "candidate_member_id": m["member_id"],
        "name_weight": name_w,
        "dob_weight": dob_w,
        "ssn4_weight": ssn_w,
        "total_weight": total,
        "match_class": mc,
        "pipeline_run_ts": s["extraction_ts"],
    })

print("Generating auth match candidates...")
auth_matches = []
for s in structured:
    if s["auth_id"] is None:
        continue
    a = random.choice(auths)
    auth_id_w = fs_weight(random.uniform(0.8, 0.99), random.uniform(0.01, 0.15))
    date_w = fs_weight(random.uniform(0.5, 0.95), random.uniform(0.05, 0.3))
    total = round(auth_id_w + date_w, 4)
    if total > 6:
        mc = "match"
    elif total > 2:
        mc = "possible_match"
    else:
        mc = "non_match"
    auth_matches.append({
        "doc_id": s["doc_id"],
        "candidate_auth_id": a["auth_id"],
        "auth_id_weight": auth_id_w,
        "service_date_weight": date_w,
        "total_weight": total,
        "match_class": mc,
        "pipeline_run_ts": s["extraction_ts"],
    })

print("Generating match events...")
match_events = []
for mm in member_matches:
    if mm["match_class"] == "match":
        match_events.append({
            "event_id": str(uuid.uuid4()),
            "doc_id": mm["doc_id"],
            "event_type": "member_match_confirmed",
            "matched_entity_id": mm["candidate_member_id"],
            "confidence": round(min(1.0, mm["total_weight"] / 15.0), 4),
            "event_ts": mm["pipeline_run_ts"],
        })
for am in auth_matches:
    if am["match_class"] == "match":
        match_events.append({
            "event_id": str(uuid.uuid4()),
            "doc_id": am["doc_id"],
            "event_type": "auth_match_confirmed",
            "matched_entity_id": am["candidate_auth_id"],
            "confidence": round(min(1.0, am["total_weight"] / 10.0), 4),
            "event_ts": am["pipeline_run_ts"],
        })

print("Generating call center data...")
calls_scores = []
calls_sentiment = []
compliance_data = {}

for i in range(3000):
    cid = f"CALL{i+1:08d}"
    agent = random.choice(AGENTS)
    agency = random.choice(AGENCIES)
    ct = random.choice(CALL_TYPES)
    lang = random.choices(LANGUAGES, weights=[0.75, 0.15, 0.10], k=1)[0]
    disp = random.choices(DISPOSITIONS, weights=[0.45, 0.25, 0.15, 0.10, 0.05], k=1)[0]
    base_score = {"resolved": 88, "pending": 75, "escalated": 65, "complaint": 55, "appeal_opened": 60}[disp]
    score = max(0, min(100, int(np.random.normal(base_score, 10))))
    if score >= 85:
        bucket = "compliant"
    elif score >= 70:
        bucket = "needs_review"
    else:
        bucket = "high_risk"
    ts = random_ts()
    calls_scores.append({
        "call_id": cid,
        "agent_name": agent,
        "agency_name": agency,
        "call_type": ct,
        "vendor_template": f"TPL_{ct[:3].upper()}_{random.randint(1,5):02d}",
        "language": lang,
        "disposition": disp,
        "call_score": score,
        "summary_of_score": f"Score {score}: {disp} call with {'adequate' if score >= 70 else 'insufficient'} documentation.",
        "outcome_bucket": bucket,
        "created_ts": ts.isoformat(),
    })
    n_topics = random.randint(1, 4)
    calls_sentiment.append({
        "document_id": cid,
        "call_type": ct,
        "summary_text": f"Caller inquired about {random.choice(TOPIC_POOL)}. Agent {'resolved the issue' if disp == 'resolved' else 'provided guidance and next steps'}.",
        "sentiment_overall": random.choice(SENTIMENTS_OVERALL),
        "sentiment_start": random.choice(SENTIMENTS_START),
        "sentiment_end": random.choice(SENTIMENTS_END),
        "sentiment_trajectory": random.choice(TRAJECTORIES),
        "key_topics": random.sample(TOPIC_POOL, n_topics),
        "emotional_markers": random.sample(EMOTIONAL_MARKERS, random.randint(0, 3)),
    })
    day_key = (ts.date().isoformat(), ct, agency)
    if day_key not in compliance_data:
        compliance_data[day_key] = {"scored": 0, "scores": [], "compliant": 0, "needs_review": 0, "high_risk": 0}
    compliance_data[day_key]["scored"] += 1
    compliance_data[day_key]["scores"].append(score)
    compliance_data[day_key][bucket.replace("_", "_")] += 1

compliance_rows = []
for (day, ct, agency), v in compliance_data.items():
    sc = v["scored"]
    compliance_rows.append({
        "day": day,
        "call_type": ct,
        "agency_name": agency,
        "scored_calls": sc,
        "avg_call_score": round(np.mean(v["scores"]), 1),
        "compliant_calls": v["compliant"],
        "needs_review_calls": v["needs_review"],
        "high_risk_calls": v["high_risk"],
        "high_risk_rate": round(v["high_risk"] / sc, 4) if sc else 0,
        "compliance_rate": round(v["compliant"] / sc, 4) if sc else 0,
    })

print(f"\nGenerated: {len(members)} members, {len(auths)} authorizations, {len(docs)} documents")
print(f"  {len(parsed)} parsed, {len(structured)} structured, {len(member_matches)} member matches")
print(f"  {len(auth_matches)} auth matches, {len(match_events)} match events")
print(f"  {len(calls_scores)} call scores, {len(calls_sentiment)} sentiments, {len(compliance_rows)} compliance rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Write Source Tables to Unity Catalog

# COMMAND ----------

TABLE_MAP = {
    f"{CATALOG}.ref.member": members,
    f"{CATALOG}.raw.authorization": auths,
    f"{CATALOG}.raw.clinical_document": docs,
    f"{CATALOG}.pipeline_prd.clinical_doc_parsed": parsed,
    f"{CATALOG}.pipeline_prd.clinical_doc_structured": structured,
    f"{CATALOG}.pipeline_prd.doc_member_match_candidates": member_matches,
    f"{CATALOG}.pipeline_prd.doc_auth_match_candidates": auth_matches,
    f"{CATALOG}.pipeline_prd.match_events": match_events,
    f"{CATALOG}.transcript_intel_sdp.mv_call_scores": calls_scores,
    f"{CATALOG}.transcript_intel_sdp.gold_call_summaries_sentiment": calls_sentiment,
    f"{CATALOG}.transcript_intel_sdp.mv_compliance_outcomes": compliance_rows,
}

for table_name, rows in TABLE_MAP.items():
    df = spark.createDataFrame(pd.DataFrame(rows))
    df.write.mode("overwrite").saveAsTable(table_name)
    print(f"  ✓ {table_name} ({len(rows)} rows)")

print("\nSource tables written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Create Pipeline KPI Summary View
# MAGIC This dashboard view is consumed by `genie_pipeline_snapshot`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.dashboard_prd.v_pipeline_kpis AS
SELECT
  (SELECT COUNT(*) FROM {CATALOG}.ref.member) AS total_members,
  (SELECT COUNT(*) FROM {CATALOG}.raw.clinical_document) AS total_documents,
  (SELECT COUNT(*) FROM {CATALOG}.pipeline_prd.clinical_doc_parsed) AS total_parsed,
  (SELECT SUM(CASE WHEN unreadable_flag THEN 1 ELSE 0 END) FROM {CATALOG}.pipeline_prd.clinical_doc_parsed) AS unreadable_docs,
  (SELECT COUNT(*) FROM {CATALOG}.pipeline_prd.doc_member_match_candidates WHERE match_class = 'match') AS high_confidence_matches,
  (SELECT COUNT(*) FROM {CATALOG}.pipeline_prd.doc_member_match_candidates WHERE match_class = 'possible_match') AS possible_matches,
  (SELECT COUNT(*) FROM {CATALOG}.pipeline_prd.doc_member_match_candidates WHERE match_class = 'non_match') AS non_matches,
  (SELECT COUNT(*) FROM {CATALOG}.pipeline_prd.doc_auth_match_candidates WHERE match_class = 'match') AS auth_matches
""")
print(f"  ✓ {CATALOG}.dashboard_prd.v_pipeline_kpis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Create Genie Room Views (8 curated views)

# COMMAND ----------

# View 1: Document Intake Daily
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.genie_availity_ops.genie_doc_intake_daily (
  intake_date          COMMENT 'Calendar date documents were received',
  total_docs           COMMENT 'Total documents received that day across all channels',
  prior_auth_forms     COMMENT 'Count of prior authorization form documents',
  clinical_notes       COMMENT 'Count of clinical note documents',
  lab_results          COMMENT 'Count of lab result documents',
  imaging_reports      COMMENT 'Count of imaging/radiology report documents',
  discharge_summaries  COMMENT 'Count of discharge summary documents',
  unreadable_docs      COMMENT 'Count of documents flagged unreadable by OCR',
  avg_quality_score    COMMENT 'Average OCR quality score 0.0-1.0 for that day',
  via_fax              COMMENT 'Documents received via fax channel',
  via_electronic       COMMENT 'Documents received via electronic/EDI channel',
  via_upload           COMMENT 'Documents received via portal upload',
  via_mail             COMMENT 'Documents received via physical mail/scan',
  prev_day_total       COMMENT 'Previous day total docs via LAG',
  day_over_day_change  COMMENT 'Day-over-day absolute change in total docs',
  day_over_day_pct     COMMENT 'Day-over-day percent change',
  rolling_7d_avg       COMMENT '7-day trailing average of total docs',
  rolling_30d_avg      COMMENT '30-day trailing average of total docs',
  cumulative_docs      COMMENT 'Running cumulative total from earliest date',
  is_volume_spike      COMMENT 'True if total_docs exceeds 2x the 7-day rolling average'
) COMMENT 'Daily document intake with trend metrics. Includes day-over-day change, rolling averages, and spike detection. No PHI/PII.'
AS
WITH base AS (
  SELECT
    DATE(ingestion_timestamp) AS intake_date,
    COUNT(*) AS total_docs,
    SUM(CASE WHEN document_type = 'prior_auth_form' THEN 1 ELSE 0 END) AS prior_auth_forms,
    SUM(CASE WHEN document_type = 'clinical_note' THEN 1 ELSE 0 END) AS clinical_notes,
    SUM(CASE WHEN document_type = 'lab_result' THEN 1 ELSE 0 END) AS lab_results,
    SUM(CASE WHEN document_type = 'imaging_report' THEN 1 ELSE 0 END) AS imaging_reports,
    SUM(CASE WHEN document_type = 'discharge_summary' THEN 1 ELSE 0 END) AS discharge_summaries,
    SUM(CASE WHEN is_readable = false THEN 1 ELSE 0 END) AS unreadable_docs,
    ROUND(AVG(quality_score), 3) AS avg_quality_score,
    SUM(CASE WHEN source_channel = 'fax' THEN 1 ELSE 0 END) AS via_fax,
    SUM(CASE WHEN source_channel = 'electronic' THEN 1 ELSE 0 END) AS via_electronic,
    SUM(CASE WHEN source_channel = 'upload' THEN 1 ELSE 0 END) AS via_upload,
    SUM(CASE WHEN source_channel = 'mail' THEN 1 ELSE 0 END) AS via_mail
  FROM {CATALOG}.raw.clinical_document
  GROUP BY DATE(ingestion_timestamp)
)
SELECT
  *,
  LAG(total_docs) OVER (ORDER BY intake_date) AS prev_day_total,
  total_docs - LAG(total_docs) OVER (ORDER BY intake_date) AS day_over_day_change,
  ROUND(
    (total_docs - LAG(total_docs) OVER (ORDER BY intake_date)) * 100.0
    / NULLIF(LAG(total_docs) OVER (ORDER BY intake_date), 0), 2
  ) AS day_over_day_pct,
  ROUND(AVG(total_docs) OVER (
    ORDER BY intake_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 1) AS rolling_7d_avg,
  ROUND(AVG(total_docs) OVER (
    ORDER BY intake_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ), 1) AS rolling_30d_avg,
  SUM(total_docs) OVER (
    ORDER BY intake_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_docs,
  total_docs > 2.0 * AVG(total_docs) OVER (
    ORDER BY intake_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS is_volume_spike
FROM base
""")
print(f"  ✓ genie_doc_intake_daily")

# COMMAND ----------

# View 2: Document Match Detail
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.genie_availity_ops.genie_doc_match_detail (
  doc_id               COMMENT 'Document identifier (non-PII surrogate UUID)',
  document_type        COMMENT 'Type: prior_auth_form, clinical_note, lab_result, imaging_report, discharge_summary',
  source_channel       COMMENT 'Intake channel: fax, electronic, upload, mail',
  intake_date          COMMENT 'Date document was received',
  match_class          COMMENT 'Fellegi-Sunter classification: match, possible_match, non_match',
  match_weight         COMMENT 'Fellegi-Sunter total weight score',
  doc_risk_tier        COMMENT 'Risk tier: Unreadable, High Risk - No Anchors, Medium Risk - One Anchor, Low Risk - Both Anchors',
  unreadable_flag      COMMENT 'True if OCR could not parse the document',
  missing_dob          COMMENT 'True if date of birth could not be extracted',
  missing_ssn4         COMMENT 'True if last-4 SSN could not be extracted',
  has_auth_id          COMMENT 'True if an authorization ID was extracted',
  has_member_id        COMMENT 'True if a member ID was extracted',
  weight_rank_in_type  COMMENT 'Rank of this match weight within its document_type (1 = highest)',
  weight_pctile_in_type COMMENT 'Percentile rank within document_type (0.0 worst to 1.0 best)',
  type_avg_weight      COMMENT 'Average match weight for this document_type overall',
  channel_avg_weight   COMMENT 'Average match weight for this source_channel overall',
  docs_same_day_type   COMMENT 'Count of documents with same type on the same intake_date'
) COMMENT 'Per-document match status with contextual rankings and extraction quality flags. No PHI/PII.'
AS
SELECT
  m.doc_id,
  d.document_type,
  d.source_channel,
  DATE(d.ingestion_timestamp) AS intake_date,
  m.match_class,
  m.total_weight AS match_weight,
  CASE
    WHEN p.unreadable_flag THEN 'Unreadable'
    WHEN COALESCE(s.missing_dob, true) AND COALESCE(s.missing_ssn4, true) THEN 'High Risk - No Anchors'
    WHEN COALESCE(s.missing_dob, true) OR COALESCE(s.missing_ssn4, true) THEN 'Medium Risk - One Anchor'
    ELSE 'Low Risk - Both Anchors'
  END AS doc_risk_tier,
  p.unreadable_flag,
  COALESCE(s.missing_dob, true) AS missing_dob,
  COALESCE(s.missing_ssn4, true) AS missing_ssn4,
  s.auth_id IS NOT NULL AS has_auth_id,
  s.member_id_on_form IS NOT NULL AS has_member_id,
  RANK() OVER (
    PARTITION BY d.document_type ORDER BY m.total_weight DESC
  ) AS weight_rank_in_type,
  ROUND(PERCENT_RANK() OVER (
    PARTITION BY d.document_type ORDER BY m.total_weight
  ), 4) AS weight_pctile_in_type,
  ROUND(AVG(m.total_weight) OVER (
    PARTITION BY d.document_type
  ), 4) AS type_avg_weight,
  ROUND(AVG(m.total_weight) OVER (
    PARTITION BY d.source_channel
  ), 4) AS channel_avg_weight,
  COUNT(*) OVER (
    PARTITION BY d.document_type, DATE(d.ingestion_timestamp)
  ) AS docs_same_day_type
FROM {CATALOG}.pipeline_prd.doc_member_match_candidates m
LEFT JOIN {CATALOG}.pipeline_prd.clinical_doc_parsed p ON m.doc_id = p.doc_id
LEFT JOIN {CATALOG}.pipeline_prd.clinical_doc_structured s ON m.doc_id = s.doc_id
LEFT JOIN {CATALOG}.raw.clinical_document d ON m.doc_id = d.doc_id
""")
print(f"  ✓ genie_doc_match_detail")

# COMMAND ----------

# View 3: Auth Match Daily
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.genie_availity_ops.genie_auth_match_daily (
  match_date           COMMENT 'Date the match scoring was performed',
  match_class          COMMENT 'Fellegi-Sunter classification: match, possible_match, non_match',
  candidate_count      COMMENT 'Number of doc-to-auth candidate pairs in this class',
  avg_match_weight     COMMENT 'Average Fellegi-Sunter total weight',
  min_match_weight     COMMENT 'Minimum weight in this class',
  max_match_weight     COMMENT 'Maximum weight in this class',
  prev_week_count      COMMENT 'Same match_class candidate count 7 days prior',
  wow_change_pct       COMMENT 'Week-over-week percent change in candidate count',
  rolling_7d_avg_count COMMENT '7-day trailing average candidate count for this class',
  class_running_total  COMMENT 'Cumulative candidate count for this class',
  daily_class_share    COMMENT 'This class as percentage of all candidates that day'
) COMMENT 'Daily auth match volumes with WoW trends and class distribution. No PHI/PII.'
AS
WITH base AS (
  SELECT
    DATE(pipeline_run_ts) AS match_date,
    match_class,
    COUNT(*) AS candidate_count,
    ROUND(AVG(total_weight), 4) AS avg_match_weight,
    ROUND(MIN(total_weight), 4) AS min_match_weight,
    ROUND(MAX(total_weight), 4) AS max_match_weight
  FROM {CATALOG}.pipeline_prd.doc_auth_match_candidates
  GROUP BY DATE(pipeline_run_ts), match_class
)
SELECT
  *,
  LAG(candidate_count, 7) OVER (
    PARTITION BY match_class ORDER BY match_date
  ) AS prev_week_count,
  ROUND(
    (candidate_count - LAG(candidate_count, 7) OVER (
      PARTITION BY match_class ORDER BY match_date
    )) * 100.0 / NULLIF(LAG(candidate_count, 7) OVER (
      PARTITION BY match_class ORDER BY match_date
    ), 0), 2
  ) AS wow_change_pct,
  ROUND(AVG(candidate_count) OVER (
    PARTITION BY match_class
    ORDER BY match_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 1) AS rolling_7d_avg_count,
  SUM(candidate_count) OVER (
    PARTITION BY match_class
    ORDER BY match_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS class_running_total,
  ROUND(
    candidate_count * 100.0 / SUM(candidate_count) OVER (PARTITION BY match_date), 2
  ) AS daily_class_share
FROM base
""")
print(f"  ✓ genie_auth_match_daily")

# COMMAND ----------

# View 4: Data Quality Daily
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.genie_availity_ops.genie_data_quality_daily (
  parse_date                 COMMENT 'Date documents were parsed',
  total_docs                 COMMENT 'Total documents parsed that day',
  unreadable_count           COMMENT 'Documents OCR could not read',
  pct_unreadable             COMMENT 'Percentage unreadable',
  missing_dob_count          COMMENT 'Documents missing date of birth extraction',
  pct_missing_dob            COMMENT 'Percentage missing DOB',
  missing_ssn4_count         COMMENT 'Documents missing last-4 SSN extraction',
  pct_missing_ssn4           COMMENT 'Percentage missing SSN4',
  docs_with_parse_error      COMMENT 'Documents with parse errors',
  avg_parse_confidence       COMMENT 'Average OCR confidence 0.0-1.0',
  rolling_7d_unreadable_pct  COMMENT '7-day rolling average of pct_unreadable',
  rolling_7d_missing_dob_pct COMMENT '7-day rolling average of pct_missing_dob',
  prev_day_unreadable_pct    COMMENT 'Previous day pct_unreadable',
  unreadable_trend_3d        COMMENT '3-day change in pct_unreadable (positive = degrading)',
  dq_degradation_flag        COMMENT 'True if pct_unreadable exceeds 1.5x its 7-day rolling average'
) COMMENT 'Daily parsing data quality with degradation detection. No PHI/PII.'
AS
WITH base AS (
  SELECT
    DATE(p.ingest_ts) AS parse_date,
    COUNT(*) AS total_docs,
    SUM(CASE WHEN p.unreadable_flag THEN 1 ELSE 0 END) AS unreadable_count,
    ROUND(100.0 * SUM(CASE WHEN p.unreadable_flag THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_unreadable,
    SUM(CASE WHEN s.missing_dob THEN 1 ELSE 0 END) AS missing_dob_count,
    ROUND(100.0 * SUM(CASE WHEN s.missing_dob THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_missing_dob,
    SUM(CASE WHEN s.missing_ssn4 THEN 1 ELSE 0 END) AS missing_ssn4_count,
    ROUND(100.0 * SUM(CASE WHEN s.missing_ssn4 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_missing_ssn4,
    SUM(CASE WHEN p.parse_error_status IS NOT NULL THEN 1 ELSE 0 END) AS docs_with_parse_error,
    CAST(NULL AS DOUBLE) AS avg_parse_confidence
  FROM {CATALOG}.pipeline_prd.clinical_doc_parsed p
  LEFT JOIN {CATALOG}.pipeline_prd.clinical_doc_structured s ON p.doc_id = s.doc_id
  GROUP BY DATE(p.ingest_ts)
)
SELECT
  *,
  ROUND(AVG(pct_unreadable) OVER (
    ORDER BY parse_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 2) AS rolling_7d_unreadable_pct,
  ROUND(AVG(pct_missing_dob) OVER (
    ORDER BY parse_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 2) AS rolling_7d_missing_dob_pct,
  LAG(pct_unreadable) OVER (ORDER BY parse_date) AS prev_day_unreadable_pct,
  ROUND(
    pct_unreadable - LAG(pct_unreadable, 3) OVER (ORDER BY parse_date), 2
  ) AS unreadable_trend_3d,
  pct_unreadable > 1.5 * AVG(pct_unreadable) OVER (
    ORDER BY parse_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS dq_degradation_flag
FROM base
""")
print(f"  ✓ genie_data_quality_daily")

# COMMAND ----------

# View 5: Pipeline Snapshot
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.genie_availity_ops.genie_pipeline_snapshot (
  metric_name   COMMENT 'Pipeline KPI metric name',
  metric_value  COMMENT 'Current value',
  metric_unit   COMMENT 'Unit: count, percentage, or score'
) COMMENT 'Current-state pipeline KPIs in long format. No PHI/PII.'
AS
SELECT metric_name, metric_value, metric_unit FROM (
  SELECT 'Total Members' AS metric_name, CAST(total_members AS DOUBLE) AS metric_value, 'count' AS metric_unit FROM {CATALOG}.dashboard_prd.v_pipeline_kpis
  UNION ALL SELECT 'Total Documents', CAST(total_documents AS DOUBLE), 'count' FROM {CATALOG}.dashboard_prd.v_pipeline_kpis
  UNION ALL SELECT 'Total Parsed', CAST(total_parsed AS DOUBLE), 'count' FROM {CATALOG}.dashboard_prd.v_pipeline_kpis
  UNION ALL SELECT 'Unreadable Documents', CAST(unreadable_docs AS DOUBLE), 'count' FROM {CATALOG}.dashboard_prd.v_pipeline_kpis
  UNION ALL SELECT 'High Confidence Matches', CAST(high_confidence_matches AS DOUBLE), 'count' FROM {CATALOG}.dashboard_prd.v_pipeline_kpis
  UNION ALL SELECT 'Possible Matches', CAST(possible_matches AS DOUBLE), 'count' FROM {CATALOG}.dashboard_prd.v_pipeline_kpis
  UNION ALL SELECT 'Non Matches', CAST(non_matches AS DOUBLE), 'count' FROM {CATALOG}.dashboard_prd.v_pipeline_kpis
  UNION ALL SELECT 'Auth Matches', CAST(auth_matches AS DOUBLE), 'count' FROM {CATALOG}.dashboard_prd.v_pipeline_kpis
)
""")
print(f"  ✓ genie_pipeline_snapshot")

# COMMAND ----------

# View 6: Call Scores
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.genie_availity_ops.genie_call_scores (
  call_id              COMMENT 'Unique call identifier',
  agent_name           COMMENT 'Agent who handled the call',
  agency_name          COMMENT 'Partner agency (outsourced call center)',
  call_type            COMMENT 'Call type taxonomy value',
  vendor_template      COMMENT 'Vendor transcript template identifier',
  language             COMMENT 'Detected language: en, es, mixed',
  disposition          COMMENT 'Outcome: resolved, pending, escalated, complaint, appeal_opened',
  call_score           COMMENT 'Quality score 0-100 (35% confidence + 35% section coverage + 20% redaction + 10% disposition)',
  score_rationale      COMMENT 'Human-readable explanation of the score',
  outcome_bucket       COMMENT 'Triage: compliant (>=85), needs_review (70-84), high_risk (<70)',
  scored_date          COMMENT 'Date the call was scored',
  agent_avg_score      COMMENT 'This agent average score across all their calls',
  agent_rank_in_agency COMMENT 'Agent rank by avg score within agency (1 = best)',
  agent_call_count     COMMENT 'Total calls scored for this agent',
  score_pctile_in_type COMMENT 'Percentile of this score within its call_type (0.0-1.0)',
  agency_avg_score     COMMENT 'Average score for this agency overall',
  score_vs_agency_avg  COMMENT 'This score minus agency average (positive = above average)',
  agent_rolling_5_avg  COMMENT 'Rolling average of last 5 calls for this agent'
) COMMENT 'Call quality scores with agent rankings, percentiles, and rolling trends. No PHI/PII.'
AS
SELECT
  call_id, agent_name, agency_name, call_type, vendor_template,
  language, disposition, call_score,
  summary_of_score AS score_rationale,
  outcome_bucket,
  DATE(created_ts) AS scored_date,
  ROUND(AVG(call_score) OVER (PARTITION BY agent_name), 1) AS agent_avg_score,
  DENSE_RANK() OVER (
    PARTITION BY agency_name
    ORDER BY AVG(call_score) OVER (PARTITION BY agent_name, agency_name) DESC
  ) AS agent_rank_in_agency,
  COUNT(*) OVER (PARTITION BY agent_name) AS agent_call_count,
  ROUND(PERCENT_RANK() OVER (PARTITION BY call_type ORDER BY call_score), 4) AS score_pctile_in_type,
  ROUND(AVG(call_score) OVER (PARTITION BY agency_name), 1) AS agency_avg_score,
  call_score - ROUND(AVG(call_score) OVER (PARTITION BY agency_name), 1) AS score_vs_agency_avg,
  ROUND(AVG(call_score) OVER (
    PARTITION BY agent_name ORDER BY created_ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
  ), 1) AS agent_rolling_5_avg
FROM {CATALOG}.transcript_intel_sdp.mv_call_scores
""")
print(f"  ✓ genie_call_scores")

# COMMAND ----------

# View 7: Call Sentiment
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.genie_availity_ops.genie_call_sentiment (
  document_id           COMMENT 'Call identifier -- joins to genie_call_scores on call_id',
  call_type             COMMENT 'Call type taxonomy value',
  summary_text          COMMENT 'AI-generated 2-3 sentence summary of the call',
  sentiment_overall     COMMENT 'Overall: positive, neutral, negative, mixed',
  sentiment_start       COMMENT 'Opening tone: calm, warm, neutral, uncertain, tired, irritated, hot',
  sentiment_end         COMMENT 'Closing tone: satisfied, relieved, flat, grudging_accept, complaint_opened, enthusiastic, neutral',
  sentiment_trajectory  COMMENT 'Arc: stable, improving, declining, volatile, stable_high, stable_low, declining_then_flat',
  key_topics            COMMENT 'Array of short noun phrases for topics discussed',
  emotional_markers     COMMENT 'Array of tags: frustration, clarification_question, supervisor_request, code_switching, hold_used'
) COMMENT 'AI-generated call summaries with sentiment. No PHI/PII.'
AS
SELECT
  document_id, call_type, summary_text,
  sentiment_overall, sentiment_start, sentiment_end, sentiment_trajectory,
  key_topics, emotional_markers
FROM {CATALOG}.transcript_intel_sdp.gold_call_summaries_sentiment
""")
print(f"  ✓ genie_call_sentiment")

# COMMAND ----------

# View 8: Compliance Daily
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.genie_availity_ops.genie_compliance_daily (
  day                    COMMENT 'Calendar date',
  call_type              COMMENT 'Call type taxonomy',
  agency_name            COMMENT 'Partner agency name',
  scored_calls           COMMENT 'Total calls scored that day for this type+agency',
  avg_call_score         COMMENT 'Mean quality score 0-100',
  compliant_calls        COMMENT 'Calls scoring >= 85',
  needs_review_calls     COMMENT 'Calls scoring 70-84',
  high_risk_calls        COMMENT 'Calls scoring < 70',
  high_risk_rate         COMMENT 'Fraction high risk (0.0-1.0)',
  compliance_rate        COMMENT 'Fraction compliant (0.0-1.0)',
  rolling_7d_compliance  COMMENT '7-day rolling compliance rate for this agency+type',
  rolling_7d_high_risk   COMMENT '7-day rolling high risk rate',
  prev_week_compliance   COMMENT 'Compliance rate 7 days prior',
  wow_compliance_change  COMMENT 'WoW compliance change (positive = improving)',
  agency_compliance_rank COMMENT 'Agency rank by compliance on this day (1 = best)',
  days_below_threshold   COMMENT 'Consecutive days compliance_rate < 0.85 for this agency+type (0 if compliant)'
) COMMENT 'Daily compliance with rolling trends, WoW, and consecutive-day streaks. No PHI/PII.'
AS
WITH base AS (
  SELECT day, call_type, agency_name, scored_calls, avg_call_score,
    compliant_calls, needs_review_calls, high_risk_calls, high_risk_rate, compliance_rate
  FROM {CATALOG}.transcript_intel_sdp.mv_compliance_outcomes
),
with_windows AS (
  SELECT *,
    ROUND(AVG(compliance_rate) OVER (
      PARTITION BY agency_name, call_type
      ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 4) AS rolling_7d_compliance,
    ROUND(AVG(high_risk_rate) OVER (
      PARTITION BY agency_name, call_type
      ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 5) AS rolling_7d_high_risk,
    LAG(compliance_rate, 7) OVER (
      PARTITION BY agency_name, call_type ORDER BY day
    ) AS prev_week_compliance,
    ROUND(compliance_rate - LAG(compliance_rate, 7) OVER (
      PARTITION BY agency_name, call_type ORDER BY day
    ), 4) AS wow_compliance_change,
    RANK() OVER (PARTITION BY day ORDER BY compliance_rate DESC) AS agency_compliance_rank,
    CASE WHEN compliance_rate < 0.85 THEN 1 ELSE 0 END AS below_flag
  FROM base
),
streaks AS (
  SELECT *,
    SUM(CASE WHEN below_flag = 0 THEN 1 ELSE 0 END) OVER (
      PARTITION BY agency_name, call_type
      ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS streak_group
  FROM with_windows
)
SELECT
  day, call_type, agency_name, scored_calls, avg_call_score,
  compliant_calls, needs_review_calls, high_risk_calls, high_risk_rate, compliance_rate,
  rolling_7d_compliance, rolling_7d_high_risk, prev_week_compliance,
  wow_compliance_change, agency_compliance_rank,
  CASE WHEN below_flag = 1
    THEN ROW_NUMBER() OVER (PARTITION BY agency_name, call_type, streak_group ORDER BY day)
    ELSE 0
  END AS days_below_threshold
FROM streaks
""")
print(f"  ✓ genie_compliance_daily")

print("\nAll 8 Genie views created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Provision Genie Rooms via API
# MAGIC
# MAGIC Creates two rooms using the Databricks Genie API. Requires `warehouse_id` widget to be set.

# COMMAND ----------

import requests, json

if not WAREHOUSE_ID:
    print("⚠ Skipping Genie Room creation — set the 'warehouse_id' widget above and re-run this cell.")
    dbutils.notebook.exit("Views created. Set warehouse_id to also create Genie Rooms.")

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
SCHEMA = "genie_availity_ops"

def make_table(name):
    return {"catalog_name": CATALOG, "schema_name": SCHEMA, "table_name": name}

ROOM_1 = {
    "display_name": "Clinical Document Intelligence - Document Processing",
    "description": (
        "Ask questions about clinical document intake, OCR quality, "
        "Fellegi-Sunter member/authorization matching, and pipeline KPIs. "
        "Covers daily trends, spike detection, risk tiering, and data quality degradation."
    ),
    "table_identifiers": [
        make_table("genie_doc_intake_daily"),
        make_table("genie_doc_match_detail"),
        make_table("genie_auth_match_daily"),
        make_table("genie_data_quality_daily"),
        make_table("genie_pipeline_snapshot"),
    ],
}

ROOM_2 = {
    "display_name": "Clinical Document Intelligence - Provider Support & Calls",
    "description": (
        "Ask questions about provider call quality scores, agent performance rankings, "
        "AI-generated sentiment analysis, and compliance tracking. "
        "Covers agent benchmarks, rolling trends, and consecutive-day compliance streaks."
    ),
    "table_identifiers": [
        make_table("genie_call_scores"),
        make_table("genie_call_sentiment"),
        make_table("genie_compliance_daily"),
    ],
}

results = {}
for label, config in [("Room 1 (Doc Processing)", ROOM_1), ("Room 2 (Call Intelligence)", ROOM_2)]:
    print(f"Creating {label}...")
    payload = {
        "display_name": config["display_name"],
        "description": config["description"],
        "table_identifiers": config["table_identifiers"],
        "warehouse_id": WAREHOUSE_ID,
    }
    resp = requests.post(f"{host}/api/2.0/genie/spaces", headers=headers, json=payload)
    resp.raise_for_status()
    data = resp.json()
    sid = data.get("space_id", data.get("id", "unknown"))
    url = f"{host}/genie/rooms/{sid}"
    results[label] = {"space_id": sid, "url": url}
    print(f"  ✓ {label}: {url}")

print("\n" + "=" * 60)
for label, info in results.items():
    print(f"  {label}: {info['url']}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Open the room URLs above to verify. Try asking:
# MAGIC - **Room 1:** "How many documents came in yesterday?" or "Show me the top 10 highest-risk documents"
# MAGIC - **Room 2:** "Which agents have the lowest compliance scores?" or "Show me sentiment trends for escalated calls"
