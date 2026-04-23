"""
Clinical Document Intelligence -- Synthetic Data Generator

Generates all source tables needed for the Clinical Document Intelligence
Genie Room demo. Self-contained -- no external repo dependencies.

Tables generated (12 total):
  ref.member                                        (1,000 rows)
  raw.authorization                                 (3,000 rows)
  raw.clinical_document                             (6,000 rows)
  pipeline_prd.clinical_doc_parsed                  (6,000 rows)
  pipeline_prd.clinical_doc_structured              (~5,500 rows)
  pipeline_prd.doc_member_match_candidates          (~5,500 rows)
  pipeline_prd.doc_auth_match_candidates            (~4,400 rows)
  pipeline_prd.match_events                         (~5,700 rows)
  dashboard_prd.v_pipeline_kpis                     (1 row)
  transcript_intel_sdp.mv_call_scores               (3,000 rows)
  transcript_intel_sdp.gold_call_summaries_sentiment (3,000 rows)
  transcript_intel_sdp.mv_compliance_outcomes       (aggregated daily)

Prerequisites:
  pip install faker pandas numpy
  # Run on a Databricks cluster or notebook (requires PySpark)
  # OR set WRITE_MODE = "parquet" for local testing

Usage:
  python data_generator/generate_all.py
"""

import uuid
import random
import math
from datetime import datetime, timedelta, date
from typing import List

import numpy as np
import pandas as pd
from faker import Faker

# ============================================================
# CONFIGURATION -- Edit these for your environment
# ============================================================

CATALOG = "serverless_stable_swv01_catalog"

SCHEMAS = {
    "ref": "ref",
    "raw": "raw",
    "pipeline": "pipeline_prd",
    "dashboard": "dashboard_prd",
    "transcript": "transcript_intel_sdp",
}

ROW_COUNTS = {
    "members": 1000,
    "authorizations": 3000,
    "clinical_documents": 6000,
    "calls": 3000,
}

END_DATE = date.today() - timedelta(days=1)
START_DATE = END_DATE - timedelta(days=89)

SEED = 42

WRITE_MODE = "delta"  # "delta" for Spark tables, "parquet" for local files

# ============================================================
# INITIALIZATION
# ============================================================

fake = Faker("en_US")
Faker.seed(SEED)
random.seed(SEED)
np.random.seed(SEED)


def fqn(schema_key: str, table_name: str) -> str:
    return f"{CATALOG}.{SCHEMAS[schema_key]}.{table_name}"


def random_timestamp(start: date, end: date, weekday_heavy: bool = True) -> datetime:
    days = (end - start).days
    day_weights = np.array([
        3.0 if (start + timedelta(days=i)).weekday() < 5 else 1.0
        for i in range(days + 1)
    ])
    day_weights /= day_weights.sum()
    day_offset = int(np.random.choice(days + 1, p=day_weights))
    chosen_date = start + timedelta(days=day_offset)

    hour_weights = np.array([
        0.5, 0.3, 0.2, 0.2, 0.3, 0.5, 1.0, 2.5,
        4.0, 5.0, 5.0, 4.5, 3.5, 4.5, 5.0, 4.5,
        3.5, 2.5, 1.5, 1.0, 0.8, 0.7, 0.6, 0.5,
    ])
    hour_weights /= hour_weights.sum()
    hour = int(np.random.choice(24, p=hour_weights))

    return datetime(chosen_date.year, chosen_date.month, chosen_date.day,
                    hour, random.randint(0, 59), random.randint(0, 59))


def generate_spike_days(start: date, end: date, n_spikes: int = 5) -> set:
    weekdays = [
        start + timedelta(days=i) for i in range((end - start).days + 1)
        if (start + timedelta(days=i)).weekday() < 5
    ]
    return set(random.sample(weekdays, min(n_spikes, len(weekdays))))


SPIKE_DAYS = generate_spike_days(START_DATE, END_DATE)


def random_timestamp_with_spikes(start: date, end: date) -> datetime:
    ts = random_timestamp(start, end)
    while ts.date() not in SPIKE_DAYS and random.random() < 0.15:
        ts = random_timestamp(start, end)
    return ts


def fs_weight(agree: bool, m_prob: float, u_prob: float) -> float:
    if agree:
        return math.log2(m_prob / u_prob) if u_prob > 0 else 8.0
    else:
        return math.log2((1 - m_prob) / (1 - u_prob)) if (1 - u_prob) > 0 else -4.0


# ============================================================
# 1. ref.member
# ============================================================

def generate_members(n: int) -> pd.DataFrame:
    print(f"Generating {n} members...")
    languages = ["English", "Spanish", "Mandarin", "Vietnamese", "Korean",
                 "Tagalog", "Arabic", "French", "Haitian Creole", "Portuguese"]
    lang_w = [0.72, 0.13, 0.03, 0.02, 0.02, 0.02, 0.01, 0.01, 0.01, 0.03]
    races = ["White", "Black or African American", "Hispanic or Latino", "Asian",
             "American Indian or Alaska Native", "Native Hawaiian or Other Pacific Islander",
             "Two or More Races", "Unknown", "Declined to Answer"]
    race_w = [0.40, 0.18, 0.22, 0.08, 0.02, 0.01, 0.04, 0.03, 0.02]
    sources = ["EPIC", "Cerner", "Athena", "AllScripts", "eClinicalWorks", "Manual"]
    src_w = [0.35, 0.25, 0.15, 0.10, 0.10, 0.05]

    rows = []
    for _ in range(n):
        gender = random.choice(["M", "F"])
        first = fake.first_name_male() if gender == "M" else fake.first_name_female()
        last = fake.last_name()
        created = random_timestamp(START_DATE - timedelta(days=365), END_DATE)
        rows.append({
            "member_id": str(uuid.uuid4()),
            "first_name": first,
            "last_name": last,
            "middle_initial": random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") if random.random() > 0.15 else None,
            "dob": fake.date_of_birth(minimum_age=1, maximum_age=95),
            "gender": gender,
            "ssn4": f"{random.randint(1000, 9999)}",
            "address_line1": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip": fake.zipcode(),
            "phone": fake.phone_number(),
            "email": fake.email(),
            "preferred_language": random.choices(languages, weights=lang_w, k=1)[0],
            "race_ethnicity": random.choices(races, weights=race_w, k=1)[0],
            "created_at": created,
            "updated_at": created + timedelta(hours=random.randint(0, 720)),
            "source_system": random.choices(sources, weights=src_w, k=1)[0],
        })
    return pd.DataFrame(rows)


# ============================================================
# 2. raw.authorization
# ============================================================

def generate_authorizations(n: int, member_ids: List[str]) -> pd.DataFrame:
    print(f"Generating {n} authorizations...")
    statuses = ["Approved", "Pending", "Denied", "Partial", "Cancelled", "Expired", "Pended"]
    status_w = [0.40, 0.20, 0.12, 0.08, 0.05, 0.10, 0.05]
    denial_reasons = [
        "MN01", "CD02", "OON03", "NC04", "EXP05", "DUP06", "NE07", "ICI08",
    ]
    cpt_codes = [
        "99213", "99214", "99215", "27447", "27130", "72148", "70553",
        "43239", "29881", "64483", "62323", "77386", "77385", "20610",
    ]
    icd10_codes = [
        "M17.11", "M17.12", "M16.11", "M54.5", "G89.29", "K21.0",
        "E11.65", "I10", "J44.1", "F32.1", "M79.3", "R10.9",
    ]
    match_methods = ["fellegi_sunter", "exact_id", "manual_review", "rule_based"]
    match_mw = [0.55, 0.25, 0.10, 0.10]
    pend_reasons = [None, None, None, "Awaiting clinical notes", "Provider callback needed",
                    "Additional imaging required", "Peer review scheduled"]

    rows = []
    for _ in range(n):
        mid = random.choice(member_ids)
        status = random.choices(statuses, weights=status_w, k=1)[0]
        svc_start = fake.date_between(start_date=START_DATE, end_date=END_DATE)
        created = random_timestamp(START_DATE, END_DATE)
        rows.append({
            "auth_id": str(uuid.uuid4()),
            "auth_number": f"AUTH-{created.year}-{random.randint(100000, 999999):06d}",
            "member_id": mid,
            "service_from_date": svc_start,
            "service_to_date": svc_start + timedelta(days=random.randint(1, 90)),
            "procedure_code": random.choice(cpt_codes),
            "procedure_modifier": random.choice([None, None, "26", "TC", "59", "LT", "RT"]),
            "diagnosis_code": random.choice(icd10_codes),
            "rendering_provider_npi": f"{random.randint(1000000000, 1999999999)}",
            "rendering_provider_name": f"Dr. {fake.last_name()}",
            "status": status,
            "approved_units": random.randint(1, 30) if status == "Approved" else (random.randint(1, 10) if status == "Partial" else None),
            "denial_reason_code": random.choice(denial_reasons) if status in ("Denied", "Partial", "Pended") else None,
            "auth_requested_date": created.date(),
            "auth_decision_date": (created + timedelta(days=random.randint(1, 14))).date() if status != "Pending" else None,
            "clinical_doc_id": None,
            "doc_match_method": random.choices(match_methods, weights=match_mw, k=1)[0],
            "doc_received_date": created if random.random() > 0.3 else None,
            "pend_reason": random.choice(pend_reasons) if status == "Pended" else None,
            "additional_info_due_date": (created + timedelta(days=random.randint(5, 30))).date() if status == "Pended" else None,
            "created_at": created,
            "updated_at": created + timedelta(hours=random.randint(0, 168)),
            "created_by": random.choice(["system_auto", "intake_agent", "um_reviewer", "admin"]),
        })
    return pd.DataFrame(rows)


# ============================================================
# 3. raw.clinical_document
# ============================================================

def generate_clinical_documents(n: int, auth_ids: List[str], member_ids: List[str]) -> pd.DataFrame:
    print(f"Generating {n} clinical documents...")
    doc_types = ["prior_auth_form", "clinical_note", "lab_result", "imaging_report", "discharge_summary"]
    doc_tw = [0.35, 0.25, 0.20, 0.12, 0.08]
    channels = ["fax", "electronic", "upload", "mail"]
    ch_w = [0.45, 0.30, 0.18, 0.07]
    ocr_statuses = ["completed", "completed_with_warnings", "failed", "pending"]
    ocr_sw = [0.82, 0.10, 0.05, 0.03]
    match_statuses = ["matched", "possible_match", "unmatched", "pending_review"]
    ms_w = [0.55, 0.20, 0.15, 0.10]
    formats = ["PDF", "TIFF", "TIFF", "PNG", "JPEG"]

    rows = []
    for _ in range(n):
        doc_type = random.choices(doc_types, weights=doc_tw, k=1)[0]
        channel = random.choices(channels, weights=ch_w, k=1)[0]
        ocr_status = random.choices(ocr_statuses, weights=ocr_sw, k=1)[0]
        fmt = random.choice(formats)
        ext = {"PDF": ".pdf", "TIFF": ".tiff", "PNG": ".png", "JPEG": ".jpg"}[fmt]
        ingestion_ts = random_timestamp_with_spikes(START_DATE, END_DATE)
        doc_id = str(uuid.uuid4())
        file_name = f"{doc_type}_{doc_id[:8]}{ext}"

        base_q = {"fax": 0.72, "electronic": 0.92, "upload": 0.85, "mail": 0.65}[channel]
        quality = float(np.clip(np.random.normal(base_q, 0.12), 0.1, 1.0))
        ocr_conf = float(np.clip(quality + np.random.normal(0, 0.05), 0.0, 1.0))
        is_readable = random.random() > 0.08
        if ocr_status == "failed":
            is_readable = False
            quality = round(float(np.clip(np.random.normal(0.25, 0.1), 0.05, 0.5)), 3)
            ocr_conf = round(float(np.clip(np.random.normal(0.15, 0.1), 0.0, 0.4)), 3)

        match_status = random.choices(match_statuses, weights=ms_w, k=1)[0]
        match_conf = round(random.uniform(0.6, 0.99), 3) if match_status == "matched" else (
            round(random.uniform(0.3, 0.6), 3) if match_status == "possible_match" else None)
        matched_at = ingestion_ts + timedelta(minutes=random.randint(5, 120)) if match_status == "matched" else None

        rows.append({
            "doc_id": doc_id,
            "auth_id": random.choice(auth_ids) if random.random() > 0.15 else None,
            "member_id": random.choice(member_ids) if random.random() > 0.10 else None,
            "document_type": doc_type,
            "file_name": file_name,
            "file_path": f"/mnt/clinical_docs/{doc_type}/{doc_id}{ext}",
            "file_format": fmt,
            "file_size_bytes": random.randint(50, 15000) * 1024,
            "page_count": random.randint(1, 25),
            "source_channel": channel,
            "sender_fax_number": fake.phone_number() if channel == "fax" else None,
            "sender_provider_npi": f"{random.randint(1000000000, 1999999999)}" if random.random() > 0.2 else None,
            "received_timestamp": ingestion_ts - timedelta(minutes=random.randint(1, 30)),
            "ingestion_timestamp": ingestion_ts,
            "mrm_batch_id": f"MRM-{ingestion_ts.strftime('%Y%m%d')}-{random.randint(1,50):03d}",
            "cover_sheet_detected": random.random() > 0.6 if channel == "fax" else False,
            "ocr_status": ocr_status,
            "ocr_confidence_score": round(ocr_conf, 3),
            "match_status": match_status,
            "doc_match_method": random.choice(["fellegi_sunter", "exact_id", "rule_based"]) if match_status in ("matched", "possible_match") else None,
            "match_confidence_score": match_conf,
            "matched_by": random.choice(["system_auto", "um_reviewer"]) if match_status == "matched" else None,
            "matched_at": matched_at,
            "is_readable": is_readable,
            "quality_score": round(quality, 3),
            "notes": None,
            "created_at": ingestion_ts,
            "updated_at": ingestion_ts + timedelta(minutes=random.randint(1, 120)),
        })
    return pd.DataFrame(rows)


# ============================================================
# 4. pipeline_prd.clinical_doc_parsed
# ============================================================

def generate_parsed_docs(docs_df: pd.DataFrame) -> pd.DataFrame:
    print(f"Generating parsed documents ({len(docs_df)} rows)...")
    rows = []
    for _, doc in docs_df.iterrows():
        unreadable = not doc["is_readable"]
        parse_error = None
        if not unreadable and random.random() < 0.05:
            parse_error = random.choice(["timeout", "corrupt_header", "unsupported_format", "ocr_engine_failure"])
        elif unreadable:
            parse_error = "unreadable"

        rows.append({
            "doc_id": doc["doc_id"],
            "unreadable_flag": unreadable,
            "page_count_detected": doc["page_count"] if not unreadable else 0,
            "parse_error_status": parse_error,
            "ingest_ts": doc["ingestion_timestamp"],
        })
    return pd.DataFrame(rows)


# ============================================================
# 5. pipeline_prd.clinical_doc_structured
# ============================================================

def generate_structured_docs(parsed_df, members_df, auths_df, docs_df) -> pd.DataFrame:
    readable = parsed_df[~parsed_df["unreadable_flag"]].copy()
    print(f"Generating structured documents ({len(readable)} readable rows)...")
    member_list = members_df.to_dict("records")
    doc_lookup = docs_df.set_index("doc_id").to_dict("index")

    rows = []
    for _, parsed in readable.iterrows():
        doc_id = parsed["doc_id"]
        doc_info = doc_lookup.get(doc_id, {})
        m = random.choice(member_list)
        missing_dob = random.random() < 0.15
        missing_ssn4 = random.random() < 0.20

        rows.append({
            "doc_id": doc_id,
            "member_id_on_form": doc_info.get("member_id") if random.random() > 0.30 else None,
            "auth_id": doc_info.get("auth_id") if random.random() > 0.25 else None,
            "dob_extracted": str(m["dob"]) if not missing_dob else None,
            "ssn4_extracted": m["ssn4"] if not missing_ssn4 else None,
            "missing_dob": missing_dob,
            "missing_ssn4": missing_ssn4,
            "extraction_ts": parsed["ingest_ts"],
        })
    return pd.DataFrame(rows)


# ============================================================
# 6. pipeline_prd.doc_member_match_candidates
# ============================================================

def generate_member_match_candidates(structured_df, members_df) -> pd.DataFrame:
    print("Generating member match candidates...")
    member_list = members_df.to_dict("records")
    M = {"ssn4": 0.95, "dob": 0.92, "name": 0.89}
    U = {"ssn4": 0.001, "dob": 0.005, "name": 0.015}

    rows = []
    for _, s in structured_df.iterrows():
        cand = random.choice(member_list)
        name_w = fs_weight(random.random() > 0.3, M["name"], U["name"])
        dob_w = fs_weight(random.random() > 0.2, M["dob"], U["dob"]) if not s["missing_dob"] else 0.0
        ssn_w = fs_weight(random.random() > 0.15, M["ssn4"], U["ssn4"]) if not s["missing_ssn4"] else 0.0
        total = round(name_w + dob_w + ssn_w, 4)
        mc = "match" if total > 8 else ("possible_match" if total > 3 else "non_match")

        rows.append({
            "doc_id": s["doc_id"],
            "candidate_member_id": cand["member_id"],
            "name_weight": round(name_w, 4),
            "dob_weight": round(dob_w, 4),
            "ssn4_weight": round(ssn_w, 4),
            "total_weight": total,
            "match_class": mc,
            "pipeline_run_ts": s["extraction_ts"],
        })
    return pd.DataFrame(rows)


# ============================================================
# 7. pipeline_prd.doc_auth_match_candidates
# ============================================================

def generate_auth_match_candidates(structured_df, auths_df) -> pd.DataFrame:
    print("Generating auth match candidates...")
    auth_list = auths_df["auth_id"].tolist()
    rows = []
    for _, s in structured_df.iterrows():
        if s["auth_id"] is None and random.random() > 0.3:
            continue
        cand = random.choice(auth_list)
        aid_w = round(fs_weight(random.random() > 0.25, 0.92, 0.01), 4)
        date_w = round(fs_weight(random.random() > 0.35, 0.80, 0.05), 4)
        total = round(aid_w + date_w, 4)
        mc = "match" if total > 6 else ("possible_match" if total > 2 else "non_match")

        rows.append({
            "doc_id": s["doc_id"],
            "candidate_auth_id": cand,
            "auth_id_weight": aid_w,
            "service_date_weight": date_w,
            "total_weight": total,
            "match_class": mc,
            "pipeline_run_ts": s["extraction_ts"],
        })
    return pd.DataFrame(rows)


# ============================================================
# 8. pipeline_prd.match_events
# ============================================================

def generate_match_events(member_matches_df, auth_matches_df) -> pd.DataFrame:
    print("Generating match events...")
    rows = []
    for _, m in member_matches_df[member_matches_df["match_class"] == "match"].iterrows():
        rows.append({
            "event_id": str(uuid.uuid4()),
            "doc_id": m["doc_id"],
            "event_type": "member_match_confirmed",
            "matched_entity_id": m["candidate_member_id"],
            "confidence": round(min(1.0, m["total_weight"] / 15.0), 4),
            "event_ts": m["pipeline_run_ts"],
        })
    for _, a in auth_matches_df[auth_matches_df["match_class"] == "match"].iterrows():
        rows.append({
            "event_id": str(uuid.uuid4()),
            "doc_id": a["doc_id"],
            "event_type": "auth_match_confirmed",
            "matched_entity_id": a["candidate_auth_id"],
            "confidence": round(min(1.0, a["total_weight"] / 10.0), 4),
            "event_ts": a["pipeline_run_ts"],
        })
    return pd.DataFrame(rows)


# ============================================================
# 9. dashboard_prd.v_pipeline_kpis
# ============================================================

def generate_pipeline_kpis(members_df, docs_df, parsed_df, mm_df, am_df) -> pd.DataFrame:
    print("Generating pipeline KPI snapshot...")
    mc = mm_df["match_class"].value_counts()
    amc = am_df["match_class"].value_counts()
    return pd.DataFrame([{
        "total_members": len(members_df),
        "total_documents": len(docs_df),
        "total_parsed": len(parsed_df),
        "unreadable_docs": int(parsed_df["unreadable_flag"].sum()),
        "high_confidence_matches": int(mc.get("match", 0)),
        "possible_matches": int(mc.get("possible_match", 0)),
        "non_matches": int(mc.get("non_match", 0)),
        "auth_matches": int(amc.get("match", 0)),
    }])


# ============================================================
# 10. transcript_intel_sdp.mv_call_scores
# ============================================================

def generate_call_scores(n: int) -> pd.DataFrame:
    print(f"Generating {n} call scores...")
    agents = [fake.name() for _ in range(40)]
    agencies = ["Alpha Health Services", "Beta Provider Support", "Gamma Call Center", "Delta Solutions"]
    call_types = ["prior_auth_inquiry", "claims_status", "eligibility_check", "benefit_inquiry", "provider_enrollment"]
    dispositions = ["resolved", "pending", "escalated", "complaint", "appeal_opened"]
    disp_w = [0.45, 0.25, 0.15, 0.10, 0.05]
    languages = ["en", "es", "mixed"]
    lang_w = [0.75, 0.15, 0.10]

    rows = []
    for i in range(n):
        disp = random.choices(dispositions, weights=disp_w, k=1)[0]
        base = {"resolved": 88, "pending": 75, "escalated": 65, "complaint": 55, "appeal_opened": 60}[disp]
        score = max(0, min(100, int(np.random.normal(base, 10))))
        bucket = "compliant" if score >= 85 else ("needs_review" if score >= 70 else "high_risk")
        ct = random.choice(call_types)
        ts = random_timestamp(START_DATE, END_DATE)

        rows.append({
            "call_id": f"CALL{i+1:08d}",
            "document_id": f"CALL{i+1:08d}",
            "agent_name": random.choice(agents),
            "agency_name": random.choice(agencies),
            "call_type": ct,
            "vendor_template": f"TPL_{ct[:3].upper()}_{random.randint(1,5):02d}",
            "language": random.choices(languages, weights=lang_w, k=1)[0],
            "disposition": disp,
            "call_score": score,
            "summary_of_score": f"Score {score}: {disp} call with {'adequate' if score >= 70 else 'insufficient'} documentation.",
            "outcome_bucket": bucket,
            "created_ts": ts,
            "source_schema": "sdp",
        })
    return pd.DataFrame(rows)


# ============================================================
# 11. transcript_intel_sdp.gold_call_summaries_sentiment
# ============================================================

def generate_call_sentiment(scores_df: pd.DataFrame) -> pd.DataFrame:
    print(f"Generating call sentiment ({len(scores_df)} rows)...")
    overall = ["positive", "neutral", "negative", "mixed"]
    starts = ["calm", "warm", "neutral", "uncertain", "tired", "irritated", "hot"]
    ends = ["satisfied", "relieved", "flat", "grudging_accept", "complaint_opened", "enthusiastic", "neutral"]
    trajectories = ["stable", "improving", "declining", "volatile", "stable_high", "stable_low", "declining_then_flat"]
    topics = [
        "prior authorization status", "claim denial", "member eligibility", "referral needed",
        "out-of-network", "copay amount", "deductible status", "formulary exception",
        "appeals process", "provider credentialing", "timely filing", "coordination of benefits",
    ]
    markers = ["frustration", "clarification_question", "supervisor_request", "code_switching", "hold_used"]

    rows = []
    for _, c in scores_df.iterrows():
        rows.append({
            "document_id": c["call_id"],
            "call_type": c["call_type"],
            "vendor_template": c["vendor_template"],
            "summary_text": f"Caller inquired about {random.choice(topics)}. Agent {'resolved the issue' if c['disposition'] == 'resolved' else 'provided guidance and next steps'}.",
            "sentiment_overall": random.choice(overall),
            "sentiment_start": random.choice(starts),
            "sentiment_end": random.choice(ends),
            "sentiment_trajectory": random.choice(trajectories),
            "key_topics": random.sample(topics, random.randint(1, 4)),
            "emotional_markers": random.sample(markers, random.randint(0, 3)),
            "created_ts": c["created_ts"],
            "source_schema": "sdp",
        })
    return pd.DataFrame(rows)


# ============================================================
# 12. transcript_intel_sdp.mv_compliance_outcomes
# ============================================================

def generate_compliance_outcomes(scores_df: pd.DataFrame) -> pd.DataFrame:
    print("Generating compliance outcomes...")
    agg = {}
    for _, c in scores_df.iterrows():
        ts = c["created_ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        key = (ts.date(), c["call_type"], c["agency_name"])
        if key not in agg:
            agg[key] = {"scores": [], "compliant": 0, "needs_review": 0, "high_risk": 0}
        agg[key]["scores"].append(c["call_score"])
        agg[key][c["outcome_bucket"]] += 1

    rows = []
    for (day, ct, agency), v in agg.items():
        sc = len(v["scores"])
        rows.append({
            "day": day,
            "call_type": ct,
            "agency_name": agency,
            "scored_calls": sc,
            "avg_call_score": round(np.mean(v["scores"]), 1),
            "compliant_calls": v["compliant"],
            "needs_review_calls": v["needs_review"],
            "high_risk_calls": v["high_risk"],
            "high_risk_rate": round(v["high_risk"] / sc, 5) if sc else 0,
            "compliance_rate": round(v["compliant"] / sc, 16) if sc else 0,
            "source_schema": "sdp",
        })
    return pd.DataFrame(rows)


# ============================================================
# WRITE FUNCTIONS
# ============================================================

def write_delta(df: pd.DataFrame, table_name: str, spark) -> None:
    sdf = spark.createDataFrame(df)
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    print(f"  {table_name}: {sdf.count()} rows")


def write_parquet(df: pd.DataFrame, table_name: str) -> None:
    import os
    os.makedirs("output", exist_ok=True)
    path = os.path.join("output", f"{table_name.replace('.', '_')}.parquet")
    df.to_parquet(path, index=False)
    print(f"  {path}: {len(df)} rows")


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 60)
    print("Clinical Document Intelligence -- Data Generator")
    print(f"Catalog: {CATALOG}")
    print(f"Date range: {START_DATE} to {END_DATE}")
    print(f"Spike days: {sorted(SPIKE_DAYS)}")
    print(f"Write mode: {WRITE_MODE}")
    print("=" * 60)
    print()

    spark = None
    if WRITE_MODE == "delta":
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            print("Spark session initialized.")
        except ImportError:
            print("WARNING: PySpark not available. Falling back to parquet mode.")

    write = (lambda df, name: write_delta(df, name, spark)) if (WRITE_MODE == "delta" and spark) else write_parquet

    # --- Document Processing Pipeline ---
    members_df = generate_members(ROW_COUNTS["members"])
    write(members_df, fqn("ref", "member"))

    member_ids = members_df["member_id"].tolist()
    auths_df = generate_authorizations(ROW_COUNTS["authorizations"], member_ids)
    write(auths_df, fqn("raw", "authorization"))

    auth_ids = auths_df["auth_id"].tolist()
    docs_df = generate_clinical_documents(ROW_COUNTS["clinical_documents"], auth_ids, member_ids)
    write(docs_df, fqn("raw", "clinical_document"))

    parsed_df = generate_parsed_docs(docs_df)
    write(parsed_df, fqn("pipeline", "clinical_doc_parsed"))

    structured_df = generate_structured_docs(parsed_df, members_df, auths_df, docs_df)
    write(structured_df, fqn("pipeline", "clinical_doc_structured"))

    mm_df = generate_member_match_candidates(structured_df, members_df)
    write(mm_df, fqn("pipeline", "doc_member_match_candidates"))

    am_df = generate_auth_match_candidates(structured_df, auths_df)
    write(am_df, fqn("pipeline", "doc_auth_match_candidates"))

    events_df = generate_match_events(mm_df, am_df)
    write(events_df, fqn("pipeline", "match_events"))

    kpis_df = generate_pipeline_kpis(members_df, docs_df, parsed_df, mm_df, am_df)
    write(kpis_df, fqn("dashboard", "v_pipeline_kpis"))

    # --- Call Center Intelligence ---
    scores_df = generate_call_scores(ROW_COUNTS["calls"])
    write(scores_df, fqn("transcript", "mv_call_scores"))

    sentiment_df = generate_call_sentiment(scores_df)
    write(sentiment_df, fqn("transcript", "gold_call_summaries_sentiment"))

    compliance_df = generate_compliance_outcomes(scores_df)
    write(compliance_df, fqn("transcript", "mv_compliance_outcomes"))

    print()
    print("=" * 60)
    print("Data generation complete.")
    print(f"  Members:            {len(members_df):>8,}")
    print(f"  Authorizations:     {len(auths_df):>8,}")
    print(f"  Clinical Documents: {len(docs_df):>8,}")
    print(f"  Parsed Documents:   {len(parsed_df):>8,}")
    print(f"  Structured Docs:    {len(structured_df):>8,}")
    print(f"  Member Matches:     {len(mm_df):>8,}")
    print(f"  Auth Matches:       {len(am_df):>8,}")
    print(f"  Match Events:       {len(events_df):>8,}")
    print(f"  KPI Snapshot:       {len(kpis_df):>8,}")
    print(f"  Call Scores:        {len(scores_df):>8,}")
    print(f"  Call Sentiment:     {len(sentiment_df):>8,}")
    print(f"  Compliance Daily:   {len(compliance_df):>8,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
