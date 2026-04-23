"""
Clinical Document Intelligence -- Synthetic Data Generator

Generates all source tables needed for the Clinical Document Intelligence
Genie Room demo. Self-contained -- no external repo dependencies.

Tables generated:
  1. ref.member                              (1000 rows)
  2. raw.authorization                       (3000 rows)
  3. raw.clinical_document                   (6000 rows)
  4. pipeline_prd.clinical_doc_parsed        (derived from clinical_document)
  5. pipeline_prd.clinical_doc_structured    (derived from parsed, minus unreadable)
  6. pipeline_prd.doc_member_match_candidates (Fellegi-Sunter member matching)
  7. pipeline_prd.doc_auth_match_candidates  (Fellegi-Sunter auth matching)
  8. pipeline_prd.match_events               (union of member + auth match events)
  9. dashboard_prd.v_pipeline_kpis           (single-row KPI snapshot)

Prerequisites:
  pip install faker pandas numpy databricks-connect
  # OR run as a Databricks notebook (comment out the spark init block below)

Usage:
  python data_generator/generate_all.py
"""

import uuid
import random
import math
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional

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
}

ROW_COUNTS = {
    "members": 1000,
    "authorizations": 3000,
    "clinical_documents": 6000,
}

# Date range for generated data (30 days ending yesterday)
END_DATE = date.today() - timedelta(days=1)
START_DATE = END_DATE - timedelta(days=29)

# Random seed for reproducibility
SEED = 42

# Write mode: "delta" to write Delta tables via Spark, "parquet" to write local parquet
WRITE_MODE = "delta"  # Change to "parquet" for local testing


# ============================================================
# INITIALIZATION
# ============================================================

fake = Faker("en_US")
Faker.seed(SEED)
random.seed(SEED)
np.random.seed(SEED)


def full_table_name(schema_key: str, table_name: str) -> str:
    """Return fully qualified table name."""
    return f"{CATALOG}.{SCHEMAS[schema_key]}.{table_name}"


def random_timestamp(start: date, end: date, weekday_heavy: bool = True) -> datetime:
    """
    Generate a random timestamp between start and end dates.
    If weekday_heavy, weekdays get ~3x the weight of weekends.
    Includes realistic hourly distribution (peak 8am-5pm).
    """
    days = (end - start).days
    day_weights = []
    for i in range(days + 1):
        d = start + timedelta(days=i)
        # Weekdays get weight 3, weekends get weight 1
        w = 3.0 if d.weekday() < 5 else 1.0
        day_weights.append(w)
    day_weights = np.array(day_weights)
    day_weights /= day_weights.sum()

    day_offset = np.random.choice(days + 1, p=day_weights)
    chosen_date = start + timedelta(days=int(day_offset))

    # Hourly distribution: peak during business hours
    # Bimodal: small peak at 7-8am (fax overnight batch), main peak 9am-4pm
    hour_weights = [
        0.5, 0.3, 0.2, 0.2, 0.3, 0.5, 1.0, 2.5,  # 0-7
        4.0, 5.0, 5.0, 4.5, 3.5, 4.5, 5.0, 4.5,    # 8-15
        3.5, 2.5, 1.5, 1.0, 0.8, 0.7, 0.6, 0.5,    # 16-23
    ]
    hour_weights = np.array(hour_weights)
    hour_weights /= hour_weights.sum()
    hour = int(np.random.choice(24, p=hour_weights))
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return datetime(chosen_date.year, chosen_date.month, chosen_date.day, hour, minute, second)


def generate_spike_days(start: date, end: date, n_spikes: int = 3) -> set:
    """Pick a few random weekdays to be volume spike days."""
    weekdays = [
        start + timedelta(days=i)
        for i in range((end - start).days + 1)
        if (start + timedelta(days=i)).weekday() < 5
    ]
    return set(random.sample(weekdays, min(n_spikes, len(weekdays))))


SPIKE_DAYS = generate_spike_days(START_DATE, END_DATE)


def random_timestamp_with_spikes(start: date, end: date) -> datetime:
    """Like random_timestamp but with 2-3x volume on spike days."""
    ts = random_timestamp(start, end)
    # On spike days, accept all; on non-spike days, thin by 50% and retry
    while ts.date() not in SPIKE_DAYS and random.random() < 0.15:
        ts = random_timestamp(start, end)
    return ts


# ============================================================
# 1. ref.member (1000 rows)
# ============================================================

def generate_members(n: int) -> pd.DataFrame:
    """Generate member reference data with PHI tags."""
    print(f"Generating {n} members...")

    languages = ["English", "Spanish", "Mandarin", "Vietnamese", "Korean",
                 "Tagalog", "Arabic", "French", "Haitian Creole", "Portuguese"]
    lang_weights = [0.72, 0.13, 0.03, 0.02, 0.02, 0.02, 0.01, 0.01, 0.01, 0.03]

    race_ethnicities = [
        "White", "Black or African American", "Hispanic or Latino",
        "Asian", "American Indian or Alaska Native",
        "Native Hawaiian or Other Pacific Islander", "Two or More Races",
        "Unknown", "Declined to Answer",
    ]
    race_weights = [0.40, 0.18, 0.22, 0.08, 0.02, 0.01, 0.04, 0.03, 0.02]

    source_systems = ["EPIC", "Cerner", "Athena", "AllScripts", "eClinicalWorks", "Manual"]
    source_weights = [0.35, 0.25, 0.15, 0.10, 0.10, 0.05]

    rows = []
    for _ in range(n):
        gender = random.choice(["M", "F"])
        first = fake.first_name_male() if gender == "M" else fake.first_name_female()
        last = fake.last_name()
        dob = fake.date_of_birth(minimum_age=1, maximum_age=95)
        created = random_timestamp(START_DATE - timedelta(days=365), END_DATE)

        rows.append({
            "member_id": str(uuid.uuid4()),                          # PHI: identifier
            "first_name": first,                                     # PHI: name
            "last_name": last,                                       # PHI: name
            "middle_initial": random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") if random.random() > 0.15 else None,
            "dob": dob,                                              # PHI: date of birth
            "gender": gender,
            "ssn4": f"{random.randint(1000, 9999)}",                 # PHI: SSN fragment
            "address": fake.street_address(),                        # PHI: address
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip": fake.zipcode(),
            "phone": fake.phone_number(),                            # PHI: phone
            "email": fake.email(),                                   # PHI: email
            "preferred_language": random.choices(languages, weights=lang_weights, k=1)[0],
            "race_ethnicity": random.choices(race_ethnicities, weights=race_weights, k=1)[0],
            "source_system": random.choices(source_systems, weights=source_weights, k=1)[0],
            "created_at": created,
            "updated_at": created + timedelta(hours=random.randint(0, 720)),
        })

    return pd.DataFrame(rows)


# ============================================================
# 2. raw.authorization (3000 rows)
# ============================================================

def generate_authorizations(n: int, member_ids: List[str]) -> pd.DataFrame:
    """Generate authorization records linked to members."""
    print(f"Generating {n} authorizations...")

    statuses = ["Approved", "Pending", "Denied", "Partial", "Cancelled", "Expired", "Pended"]
    status_weights = [0.40, 0.20, 0.12, 0.08, 0.05, 0.10, 0.05]

    denial_reasons = [
        "Medical necessity not established",
        "Incomplete clinical documentation",
        "Out-of-network provider",
        "Service not covered under plan",
        "Prior authorization expired",
        "Duplicate request",
        "Member not eligible on date of service",
        "Insufficient clinical information",
    ]

    # Common CPT codes for prior auth
    cpt_codes = [
        "99213", "99214", "99215", "27447", "27130", "72148", "70553",
        "43239", "29881", "64483", "62323", "77386", "77385", "20610",
        "99283", "99284", "99285", "99291", "36561", "43235",
    ]

    # Common ICD-10 codes
    icd10_codes = [
        "M17.11", "M17.12", "M16.11", "M16.12", "M54.5", "M54.41",
        "G89.29", "K21.0", "E11.65", "I10", "J44.1", "F32.1",
        "Z96.641", "Z96.642", "M79.3", "R10.9", "J06.9", "N39.0",
    ]

    doc_match_methods = ["fellegi_sunter", "exact_id", "manual_review", "rule_based"]
    match_method_weights = [0.55, 0.25, 0.10, 0.10]

    rows = []
    for i in range(n):
        member_id = random.choice(member_ids)
        status = random.choices(statuses, weights=status_weights, k=1)[0]
        service_start = fake.date_between(start_date=START_DATE, end_date=END_DATE)
        service_end = service_start + timedelta(days=random.randint(1, 90))
        created = random_timestamp(START_DATE, END_DATE)
        auth_year = created.year

        denial_reason = None
        if status in ("Denied", "Partial", "Pended"):
            denial_reason = random.choice(denial_reasons)

        approved_units = None
        if status == "Approved":
            approved_units = random.randint(1, 30)
        elif status == "Partial":
            approved_units = random.randint(1, 10)

        rows.append({
            "auth_id": str(uuid.uuid4()),
            "auth_number": f"AUTH-{auth_year}-{random.randint(100000, 999999):06d}",
            "member_id": member_id,
            "service_start_date": service_start,
            "service_end_date": service_end,
            "procedure_code": random.choice(cpt_codes),
            "diagnosis_code": random.choice(icd10_codes),
            "provider_npi": f"{random.randint(1000000000, 1999999999)}",
            "provider_name": f"Dr. {fake.last_name()}",
            "status": status,
            "approved_units": approved_units,
            "denial_reason": denial_reason,
            "submitted_date": created.date(),
            "decision_date": (created + timedelta(days=random.randint(1, 14))).date() if status != "Pending" else None,
            "clinical_doc_id": None,  # Will be linked after doc generation
            "doc_match_method": random.choices(doc_match_methods, weights=match_method_weights, k=1)[0],
            "created_at": created,
            "updated_at": created + timedelta(hours=random.randint(0, 168)),
        })

    return pd.DataFrame(rows)


# ============================================================
# 3. raw.clinical_document (6000 rows)
# ============================================================

def generate_clinical_documents(
    n: int,
    auth_ids: List[str],
    member_ids: List[str],
) -> pd.DataFrame:
    """Generate clinical document records with realistic distributions."""
    print(f"Generating {n} clinical documents...")

    doc_types = ["prior_auth_form", "clinical_note", "lab_result", "imaging_report", "discharge_summary"]
    doc_type_weights = [0.35, 0.25, 0.20, 0.12, 0.08]

    source_channels = ["fax", "electronic", "upload", "mail"]
    channel_weights = [0.45, 0.30, 0.18, 0.07]

    ocr_statuses = ["completed", "completed_with_warnings", "failed", "pending"]
    ocr_status_weights = [0.82, 0.10, 0.05, 0.03]

    match_statuses = ["matched", "possible_match", "unmatched", "pending_review"]
    match_status_weights = [0.55, 0.20, 0.15, 0.10]

    file_extensions = [".pdf", ".tiff", ".tif", ".png", ".jpg"]
    ext_weights = [0.50, 0.25, 0.15, 0.05, 0.05]

    rows = []
    for _ in range(n):
        doc_type = random.choices(doc_types, weights=doc_type_weights, k=1)[0]
        channel = random.choices(source_channels, weights=channel_weights, k=1)[0]
        ocr_status = random.choices(ocr_statuses, weights=ocr_status_weights, k=1)[0]
        ext = random.choices(file_extensions, weights=ext_weights, k=1)[0]

        ingestion_ts = random_timestamp_with_spikes(START_DATE, END_DATE)

        # Quality score: fax tends lower, electronic tends higher
        if channel == "fax":
            quality = np.clip(np.random.normal(0.72, 0.15), 0.1, 1.0)
        elif channel == "electronic":
            quality = np.clip(np.random.normal(0.92, 0.05), 0.5, 1.0)
        elif channel == "upload":
            quality = np.clip(np.random.normal(0.85, 0.10), 0.3, 1.0)
        else:  # mail
            quality = np.clip(np.random.normal(0.65, 0.18), 0.1, 1.0)

        # OCR confidence correlates with quality
        ocr_confidence = np.clip(quality + np.random.normal(0, 0.05), 0.0, 1.0)

        # is_readable: ~92% readable overall
        is_readable = random.random() > 0.08

        # If OCR failed, mark unreadable
        if ocr_status == "failed":
            is_readable = False
            quality = round(float(np.clip(np.random.normal(0.25, 0.1), 0.05, 0.5)), 3)
            ocr_confidence = round(float(np.clip(np.random.normal(0.15, 0.1), 0.0, 0.4)), 3)

        doc_id = str(uuid.uuid4())
        file_name = f"{doc_type}_{doc_id[:8]}{ext}"
        file_size_kb = random.randint(50, 15000)

        rows.append({
            "doc_id": doc_id,
            "auth_id": random.choice(auth_ids) if random.random() > 0.15 else None,
            "member_id": random.choice(member_ids) if random.random() > 0.10 else None,
            "document_type": doc_type,
            "file_name": file_name,
            "file_size_bytes": file_size_kb * 1024,
            "file_extension": ext,
            "page_count": random.randint(1, 25),
            "source_channel": channel,
            "ocr_status": ocr_status,
            "ocr_confidence": round(float(ocr_confidence), 3),
            "is_readable": is_readable,
            "match_status": random.choices(match_statuses, weights=match_status_weights, k=1)[0],
            "quality_score": round(float(quality), 3),
            "ingestion_timestamp": ingestion_ts,
            "created_at": ingestion_ts,
            "updated_at": ingestion_ts + timedelta(minutes=random.randint(1, 120)),
        })

    return pd.DataFrame(rows)


# ============================================================
# 4. pipeline_prd.clinical_doc_parsed
# ============================================================

def generate_parsed_docs(docs_df: pd.DataFrame) -> pd.DataFrame:
    """Generate parsed document records from clinical documents."""
    print(f"Generating parsed documents ({len(docs_df)} rows)...")

    rows = []
    for _, doc in docs_df.iterrows():
        parse_error = None
        # 5% parse errors
        if random.random() < 0.05:
            parse_error = random.choice([
                "TIMEOUT_EXCEEDED",
                "CORRUPT_FILE",
                "UNSUPPORTED_FORMAT",
                "OCR_ENGINE_FAILURE",
                "PAGE_LIMIT_EXCEEDED",
            ])

        # 8% unreadable
        unreadable = not doc["is_readable"]

        ingest_ts = doc["ingestion_timestamp"]
        if isinstance(ingest_ts, str):
            ingest_ts = datetime.fromisoformat(ingest_ts)

        rows.append({
            "doc_id": doc["doc_id"],
            "file_path": f"/mnt/clinical_docs/{doc['document_type']}/{doc['doc_id']}{doc['file_extension']}",
            "ingest_ts": ingest_ts,
            "parse_ts": ingest_ts + timedelta(seconds=random.randint(5, 300)),
            "raw_text": f"[PLACEHOLDER raw OCR text for document {doc['doc_id'][:8]}]",
            "parse_error_status": parse_error,
            "page_count": doc["page_count"],
            "unreadable_flag": unreadable,
        })

    return pd.DataFrame(rows)


# ============================================================
# 5. pipeline_prd.clinical_doc_structured
# ============================================================

def generate_structured_docs(
    parsed_df: pd.DataFrame,
    members_df: pd.DataFrame,
    auths_df: pd.DataFrame,
    docs_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate structured extraction results from parsed documents.
    Excludes unreadable documents. Introduces realistic extraction errors.
    """
    readable = parsed_df[~parsed_df["unreadable_flag"]].copy()
    print(f"Generating structured documents ({len(readable)} readable rows)...")

    member_list = members_df.to_dict("records")
    auth_list = auths_df.to_dict("records")
    doc_lookup = docs_df.set_index("doc_id").to_dict("index")

    rows = []
    for _, parsed in readable.iterrows():
        doc_id = parsed["doc_id"]
        doc_info = doc_lookup.get(doc_id, {})
        linked_member_id = doc_info.get("member_id")
        linked_auth_id = doc_info.get("auth_id")

        # Pick a member to "extract" from (sometimes the linked one, sometimes random)
        if linked_member_id and random.random() > 0.1:
            member = next((m for m in member_list if m["member_id"] == linked_member_id), random.choice(member_list))
        else:
            member = random.choice(member_list)

        # 15% missing DOB extraction
        missing_dob = random.random() < 0.15
        # 20% missing SSN4 extraction
        missing_ssn4 = random.random() < 0.20

        # Introduce name extraction errors (~5% typos)
        extracted_first = member["first_name"]
        extracted_last = member["last_name"]
        if random.random() < 0.05 and len(extracted_first) > 2:
            pos = random.randint(1, len(extracted_first) - 1)
            extracted_first = extracted_first[:pos] + random.choice("aeiou") + extracted_first[pos + 1:]
        if random.random() < 0.05 and len(extracted_last) > 2:
            pos = random.randint(1, len(extracted_last) - 1)
            extracted_last = extracted_last[:pos] + random.choice("aeiou") + extracted_last[pos + 1:]

        parse_ts = parsed["parse_ts"]
        if isinstance(parse_ts, str):
            parse_ts = datetime.fromisoformat(parse_ts)

        rows.append({
            "doc_id": doc_id,
            "extracted_first_name": extracted_first,
            "extracted_last_name": extracted_last,
            "extracted_dob": None if missing_dob else member["dob"],
            "extracted_ssn4": None if missing_ssn4 else member["ssn4"],
            "member_id_on_form": linked_member_id if random.random() > 0.30 else None,
            "auth_id": linked_auth_id if random.random() > 0.25 else None,
            "provider_name": f"Dr. {fake.last_name()}" if random.random() > 0.10 else None,
            "missing_dob": missing_dob,
            "missing_ssn4": missing_ssn4,
            "extraction_ts": parse_ts + timedelta(seconds=random.randint(2, 60)),
        })

    return pd.DataFrame(rows)


# ============================================================
# 6. pipeline_prd.doc_member_match_candidates (Fellegi-Sunter)
# ============================================================

def fellegi_sunter_weight(
    agree: bool,
    m_prob: float,
    u_prob: float,
) -> float:
    """Compute a single Fellegi-Sunter comparison weight."""
    if agree:
        return math.log2(m_prob / u_prob) if u_prob > 0 else 8.0
    else:
        return math.log2((1 - m_prob) / (1 - u_prob)) if (1 - u_prob) > 0 else -4.0


def generate_member_match_candidates(
    parsed_df: pd.DataFrame,
    structured_df: pd.DataFrame,
    members_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each parsed document, score against 1-3 member candidates
    using Fellegi-Sunter weights.
    """
    print("Generating member match candidates...")

    structured_lookup = structured_df.set_index("doc_id").to_dict("index")
    member_list = members_df.to_dict("records")

    # Fellegi-Sunter m and u probabilities
    M_PROBS = {"ssn4": 0.95, "dob": 0.92, "first_name": 0.88, "last_name": 0.90}
    U_PROBS = {"ssn4": 0.001, "dob": 0.005, "first_name": 0.02, "last_name": 0.01}

    # Thresholds for classification
    MATCH_THRESHOLD = 8.0
    NON_MATCH_THRESHOLD = 2.0

    rows = []
    for _, parsed in parsed_df.iterrows():
        doc_id = parsed["doc_id"]
        structured = structured_lookup.get(doc_id)

        # Number of candidates: usually 1-3
        n_candidates = random.choices([1, 2, 3], weights=[0.60, 0.25, 0.15], k=1)[0]
        candidates = random.sample(member_list, min(n_candidates, len(member_list)))

        for member in candidates:
            # Determine agreement for each field
            if structured:
                ssn4_agree = (
                    not structured["missing_ssn4"]
                    and structured.get("extracted_ssn4") == member["ssn4"]
                )
                dob_agree = (
                    not structured["missing_dob"]
                    and structured.get("extracted_dob") is not None
                    and str(structured.get("extracted_dob")) == str(member["dob"])
                )
                first_agree = (
                    structured.get("extracted_first_name", "").lower()
                    == member["first_name"].lower()
                )
                last_agree = (
                    structured.get("extracted_last_name", "").lower()
                    == member["last_name"].lower()
                )
            else:
                # Unreadable/unparsed: all disagree with some noise
                ssn4_agree = random.random() < 0.01
                dob_agree = random.random() < 0.02
                first_agree = random.random() < 0.05
                last_agree = random.random() < 0.05

            w_ssn4 = round(fellegi_sunter_weight(ssn4_agree, M_PROBS["ssn4"], U_PROBS["ssn4"]), 4)
            w_dob = round(fellegi_sunter_weight(dob_agree, M_PROBS["dob"], U_PROBS["dob"]), 4)
            w_first = round(fellegi_sunter_weight(first_agree, M_PROBS["first_name"], U_PROBS["first_name"]), 4)
            w_last = round(fellegi_sunter_weight(last_agree, M_PROBS["last_name"], U_PROBS["last_name"]), 4)
            total_weight = round(w_ssn4 + w_dob + w_first + w_last, 4)

            if total_weight >= MATCH_THRESHOLD:
                match_class = "match"
            elif total_weight >= NON_MATCH_THRESHOLD:
                match_class = "possible_match"
            else:
                match_class = "non_match"

            ingest_ts = parsed["ingest_ts"]
            if isinstance(ingest_ts, str):
                ingest_ts = datetime.fromisoformat(ingest_ts)

            rows.append({
                "candidate_id": str(uuid.uuid4()),
                "doc_id": doc_id,
                "member_id": member["member_id"],
                "w_ssn4": w_ssn4,
                "w_dob": w_dob,
                "w_first": w_first,
                "w_last": w_last,
                "total_weight": total_weight,
                "match_class": match_class,
                "pipeline_run_ts": ingest_ts + timedelta(seconds=random.randint(30, 600)),
            })

    return pd.DataFrame(rows)


# ============================================================
# 7. pipeline_prd.doc_auth_match_candidates (Fellegi-Sunter)
# ============================================================

def generate_auth_match_candidates(
    parsed_df: pd.DataFrame,
    structured_df: pd.DataFrame,
    auths_df: pd.DataFrame,
    docs_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each parsed document, score against 1-2 authorization candidates
    using Fellegi-Sunter-style weights on auth_id, procedure_code, provider, dates.
    """
    print("Generating auth match candidates...")

    structured_lookup = structured_df.set_index("doc_id").to_dict("index")
    doc_lookup = docs_df.set_index("doc_id").to_dict("index")
    auth_list = auths_df.to_dict("records")

    MATCH_THRESHOLD = 6.0
    NON_MATCH_THRESHOLD = 1.5

    rows = []
    for _, parsed in parsed_df.iterrows():
        doc_id = parsed["doc_id"]
        structured = structured_lookup.get(doc_id)
        doc_info = doc_lookup.get(doc_id, {})

        n_candidates = random.choices([1, 2], weights=[0.70, 0.30], k=1)[0]
        candidates = random.sample(auth_list, min(n_candidates, len(auth_list)))

        for auth in candidates:
            # Auth ID match
            if structured and structured.get("auth_id") and structured["auth_id"] == auth["auth_id"]:
                w_auth_id = round(random.uniform(6.0, 8.0), 4)
            elif structured and structured.get("auth_id"):
                w_auth_id = round(random.uniform(-2.0, 0.5), 4)
            else:
                w_auth_id = 0.0  # No auth_id extracted

            # Member linkage
            doc_member = doc_info.get("member_id")
            if doc_member and doc_member == auth["member_id"]:
                w_member = round(random.uniform(3.0, 5.0), 4)
            elif doc_member:
                w_member = round(random.uniform(-3.0, -1.0), 4)
            else:
                w_member = 0.0

            # Date overlap (service dates vs ingestion)
            w_date = round(random.uniform(-1.0, 2.0), 4)

            # Provider name similarity
            if structured and structured.get("provider_name"):
                w_provider = round(random.uniform(-0.5, 2.5), 4)
            else:
                w_provider = 0.0

            total_weight = round(w_auth_id + w_member + w_date + w_provider, 4)

            if total_weight >= MATCH_THRESHOLD:
                match_class = "match"
            elif total_weight >= NON_MATCH_THRESHOLD:
                match_class = "possible_match"
            else:
                match_class = "non_match"

            ingest_ts = parsed["ingest_ts"]
            if isinstance(ingest_ts, str):
                ingest_ts = datetime.fromisoformat(ingest_ts)

            rows.append({
                "candidate_id": str(uuid.uuid4()),
                "doc_id": doc_id,
                "auth_id": auth["auth_id"],
                "w_auth_id": w_auth_id,
                "w_member": w_member,
                "w_date": w_date,
                "w_provider": w_provider,
                "total_weight": total_weight,
                "match_class": match_class,
                "pipeline_run_ts": ingest_ts + timedelta(seconds=random.randint(30, 600)),
            })

    return pd.DataFrame(rows)


# ============================================================
# 8. pipeline_prd.match_events (union of member + auth)
# ============================================================

def generate_match_events(
    member_matches_df: pd.DataFrame,
    auth_matches_df: pd.DataFrame,
) -> pd.DataFrame:
    """Union member and auth match events into a single event stream."""
    print("Generating match events...")

    member_events = member_matches_df[["candidate_id", "doc_id", "match_class", "total_weight", "pipeline_run_ts"]].copy()
    member_events["match_type"] = "member"
    member_events["target_id"] = member_matches_df["member_id"]

    auth_events = auth_matches_df[["candidate_id", "doc_id", "match_class", "total_weight", "pipeline_run_ts"]].copy()
    auth_events["match_type"] = "authorization"
    auth_events["target_id"] = auth_matches_df["auth_id"]

    events = pd.concat([member_events, auth_events], ignore_index=True)
    events = events.rename(columns={"candidate_id": "event_id"})
    return events


# ============================================================
# 9. dashboard_prd.v_pipeline_kpis (single-row KPI snapshot)
# ============================================================

def generate_pipeline_kpis(
    members_df: pd.DataFrame,
    docs_df: pd.DataFrame,
    parsed_df: pd.DataFrame,
    member_matches_df: pd.DataFrame,
    auth_matches_df: pd.DataFrame,
) -> pd.DataFrame:
    """Generate single-row KPI snapshot view for the pipeline."""
    print("Generating pipeline KPI snapshot...")

    total_members = len(members_df)
    total_documents = len(docs_df)
    total_parsed = len(parsed_df)
    unreadable_docs = int(parsed_df["unreadable_flag"].sum())

    member_match_counts = member_matches_df["match_class"].value_counts()
    high_confidence = int(member_match_counts.get("match", 0))
    possible = int(member_match_counts.get("possible_match", 0))
    non_matches = int(member_match_counts.get("non_match", 0))

    auth_match_counts = auth_matches_df["match_class"].value_counts()
    auth_matches = int(auth_match_counts.get("match", 0))

    return pd.DataFrame([{
        "total_members": total_members,
        "total_documents": total_documents,
        "total_parsed": total_parsed,
        "unreadable_docs": unreadable_docs,
        "high_confidence_matches": high_confidence,
        "possible_matches": possible,
        "non_matches": non_matches,
        "auth_matches": auth_matches,
        "snapshot_ts": datetime.now(),
    }])


# ============================================================
# WRITE FUNCTIONS
# ============================================================

def write_delta(df: pd.DataFrame, table_name: str, spark) -> None:
    """Write a pandas DataFrame as a Delta table via Spark."""
    sdf = spark.createDataFrame(df)
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    row_count = sdf.count()
    print(f"  Wrote {row_count} rows to {table_name}")


def write_parquet(df: pd.DataFrame, table_name: str) -> None:
    """Write a pandas DataFrame as a local parquet file (for testing)."""
    import os
    out_dir = "output"
    os.makedirs(out_dir, exist_ok=True)
    safe_name = table_name.replace(".", "_")
    path = os.path.join(out_dir, f"{safe_name}.parquet")
    df.to_parquet(path, index=False)
    print(f"  Wrote {len(df)} rows to {path}")


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

    # Initialize Spark if writing Delta
    spark = None
    if WRITE_MODE == "delta":
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            print("Spark session initialized.")
        except ImportError:
            print("WARNING: PySpark not available. Falling back to parquet mode.")
            # Fall through to parquet

    write_fn = (lambda df, name: write_delta(df, name, spark)) if (WRITE_MODE == "delta" and spark) else write_parquet

    # --- Generate all data ---

    # 1. Members
    members_df = generate_members(ROW_COUNTS["members"])
    write_fn(members_df, full_table_name("ref", "member"))

    # 2. Authorizations
    member_ids = members_df["member_id"].tolist()
    auths_df = generate_authorizations(ROW_COUNTS["authorizations"], member_ids)
    write_fn(auths_df, full_table_name("raw", "authorization"))

    # 3. Clinical Documents
    auth_ids = auths_df["auth_id"].tolist()
    docs_df = generate_clinical_documents(ROW_COUNTS["clinical_documents"], auth_ids, member_ids)
    write_fn(docs_df, full_table_name("raw", "clinical_document"))

    # Link some documents back to authorizations
    doc_ids = docs_df["doc_id"].tolist()
    for i in range(min(len(auths_df), len(doc_ids))):
        if random.random() < 0.6:
            auths_df.at[i, "clinical_doc_id"] = doc_ids[i % len(doc_ids)]
    write_fn(auths_df, full_table_name("raw", "authorization"))  # Re-write with links

    # 4. Parsed Documents
    parsed_df = generate_parsed_docs(docs_df)
    write_fn(parsed_df, full_table_name("pipeline", "clinical_doc_parsed"))

    # 5. Structured Documents
    structured_df = generate_structured_docs(parsed_df, members_df, auths_df, docs_df)
    write_fn(structured_df, full_table_name("pipeline", "clinical_doc_structured"))

    # 6. Member Match Candidates
    member_matches_df = generate_member_match_candidates(parsed_df, structured_df, members_df)
    write_fn(member_matches_df, full_table_name("pipeline", "doc_member_match_candidates"))

    # 7. Auth Match Candidates
    auth_matches_df = generate_auth_match_candidates(parsed_df, structured_df, auths_df, docs_df)
    write_fn(auth_matches_df, full_table_name("pipeline", "doc_auth_match_candidates"))

    # 8. Match Events
    match_events_df = generate_match_events(member_matches_df, auth_matches_df)
    write_fn(match_events_df, full_table_name("pipeline", "match_events"))

    # 9. Pipeline KPIs
    kpis_df = generate_pipeline_kpis(members_df, docs_df, parsed_df, member_matches_df, auth_matches_df)
    write_fn(kpis_df, full_table_name("dashboard", "v_pipeline_kpis"))

    print()
    print("=" * 60)
    print("Data generation complete.")
    print(f"  Members:           {len(members_df):>8,}")
    print(f"  Authorizations:    {len(auths_df):>8,}")
    print(f"  Clinical Documents:{len(docs_df):>8,}")
    print(f"  Parsed Documents:  {len(parsed_df):>8,}")
    print(f"  Structured Docs:   {len(structured_df):>8,}")
    print(f"  Member Matches:    {len(member_matches_df):>8,}")
    print(f"  Auth Matches:      {len(auth_matches_df):>8,}")
    print(f"  Match Events:      {len(match_events_df):>8,}")
    print(f"  KPI Snapshot:      {len(kpis_df):>8,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
