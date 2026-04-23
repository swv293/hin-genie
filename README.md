# Clinical Document Intelligence Genie Room

A national health information network processes billions of clinical, administrative, and financial transactions per year -- eligibility checks, claims, prior authorizations, remittance advice -- connecting health plans, providers, and health IT vendors through a single multi-payer portal and API layer. The core operational challenge: clinical documents (faxes, prior auth forms, lab results, discharge summaries) arrive from thousands of sources and must be parsed, identity-matched to members and authorizations, and routed to the correct payer workflow -- accurately, at scale, under SLA. This demo builds two Databricks Genie Rooms that let ops leaders and call center supervisors ask natural-language questions against curated views of the document processing pipeline and provider support call data.

## Architecture

```mermaid
flowchart LR
    subgraph Ingestion
        FAX[Fax]
        EDI[Electronic/EDI]
        UPLOAD[Portal Upload]
        MAIL[Mail/Scan]
    end

    subgraph raw
        CD[clinical_document]
        AUTH[authorization]
    end

    subgraph ref
        MEM[member]
    end

    subgraph pipeline_prd
        PARSE[clinical_doc_parsed<br/>OCR + page extraction]
        STRUCT[clinical_doc_structured<br/>field extraction]
        MMATCH[doc_member_match_candidates<br/>Fellegi-Sunter scoring]
        AMATCH[doc_auth_match_candidates<br/>Fellegi-Sunter scoring]
        EVENTS[match_events]
    end

    subgraph genie_availity_ops
        V1[genie_doc_intake_daily]
        V2[genie_doc_match_detail]
        V3[genie_auth_match_daily]
        V4[genie_data_quality_daily]
        V5[genie_pipeline_snapshot]
        V6[genie_call_scores]
        V7[genie_call_sentiment]
        V8[genie_compliance_daily]
    end

    subgraph Genie Rooms
        R1[Room 1: Document Processing<br/>& Authorization Intelligence]
        R2[Room 2: Provider Support<br/>& Call Intelligence]
    end

    FAX & EDI & UPLOAD & MAIL --> CD
    CD --> PARSE --> STRUCT
    STRUCT --> MMATCH
    STRUCT --> AMATCH
    MEM --> MMATCH
    AUTH --> AMATCH
    MMATCH & AMATCH --> EVENTS

    CD --> V1
    MMATCH & PARSE & STRUCT & CD --> V2
    AMATCH --> V3
    PARSE & STRUCT --> V4
    V5 -.-> R1
    V1 & V2 & V3 & V4 --> R1
    V6 & V7 & V8 --> R2
```

## Prerequisites

- **Databricks workspace** with Unity Catalog enabled
- **Serverless SQL warehouse** (or any SQL warehouse with access to the catalog)
- **Unity Catalog** catalog provisioned (default: `serverless_stable_swv01_catalog`)
- Python 3.10+ with `faker`, `pandas`, `numpy` installed (for data generation — generates 12 source tables)

## Quick Start

### Option A: One-click notebook (recommended)

Import `notebooks/setup_genie_rooms.py` into your Databricks workspace and run all cells. It handles everything: schema creation, synthetic data generation, view deployment, and Genie Room provisioning. Set the `catalog` and `warehouse_id` widgets at the top.

### Option B: Step-by-step

1. **Generate synthetic data** -- Run the data generator to populate all source tables:
   ```bash
   pip install faker pandas numpy
   # Edit the config section in generate_all.py to set your catalog/schema names
   python data_generator/generate_all.py
   ```

2. **Create schemas** -- Execute the schema DDL against your warehouse:
   ```sql
   -- Run sql/00_create_schemas.sql in a Databricks SQL editor
   ```

3. **Create Genie views** -- Deploy all 8 curated views:
   ```sql
   -- Run sql/01_create_genie_views.sql in a Databricks SQL editor
   ```

4. **Create Genie Rooms** -- Use the room creation script to provision both rooms via API:
   ```bash
   python genie_config/create_rooms.py
   ```

5. **Validate** -- Open each room URL and ask a test question (e.g., "How many documents came in yesterday?" or "Which agents have the lowest compliance scores?").

## Schema Inventory

| # | View | Source | Purpose |
|---|------|--------|---------|
| 1 | `genie_doc_intake_daily` | `raw.clinical_document` | Daily document intake volume by type/channel with day-over-day trends, rolling averages, and spike detection |
| 2 | `genie_doc_match_detail` | `pipeline_prd.doc_member_match_candidates` + 3 joins | Per-document match status with Fellegi-Sunter weights, risk tier, extraction flags, and contextual rankings |
| 3 | `genie_auth_match_daily` | `pipeline_prd.doc_auth_match_candidates` | Daily authorization match volumes with week-over-week trends and match class distribution |
| 4 | `genie_data_quality_daily` | `pipeline_prd.clinical_doc_parsed` + `clinical_doc_structured` | Daily parsing/extraction quality metrics with rolling trends and degradation detection |
| 5 | `genie_pipeline_snapshot` | `dashboard_prd.v_pipeline_kpis` | Current-state pipeline KPIs in long format for easy Genie querying |
| 6 | `genie_call_scores` | `transcript_intel_sdp.mv_call_scores` | Call quality scores with agent rankings, percentiles, and rolling trends |
| 7 | `genie_call_sentiment` | `transcript_intel_sdp.gold_call_summaries_sentiment` | AI-generated call summaries with sentiment analysis (overall, start, end, trajectory) |
| 8 | `genie_compliance_daily` | `transcript_intel_sdp.mv_compliance_outcomes` | Daily compliance rates with rolling trends, WoW comparison, agency rankings, and consecutive-day streaks |

### Metric Views (AI/BI)

Pre-defined measures and dimensions for Genie aggregate queries:

| # | Metric View | Source View | Key Measures |
|---|-------------|-------------|--------------|
| 9 | `mv_doc_intake_metrics` | `genie_doc_intake_daily` | total_documents, unreadable_rate_pct, channel volumes, spike_day_count |
| 10 | `mv_doc_match_metrics` | `genie_doc_match_detail` | match_rate_pct, avg_match_weight, high_risk_pct, extraction_completeness_pct |
| 11 | `mv_call_quality_metrics` | `genie_call_scores` | avg_call_score, compliance_rate_pct, high_risk_pct, escalation_rate_pct |

## Genie Rooms

### Room 1: Document Processing & Authorization Intelligence

Audience: Ops leaders, utilization management staff, product managers.

Covers document intake volume, OCR quality, Fellegi-Sunter match outcomes, authorization match trends, and pipeline KPIs. Five curated views with window functions for trend analysis, spike detection, and risk tiering.

**URL**: [Open Room 1](https://fevm-serverless-stable-swv01.cloud.databricks.com/genie/rooms/01f13f17036e100f9a7e09b2ec0393ab)

### Room 2: Provider Support & Call Intelligence

Audience: Call center supervisors, QA analysts, provider relations.

Covers call quality scoring, agent performance rankings, AI-generated sentiment analysis, and compliance tracking with consecutive-day streak detection.

**URL**: [Open Room 2](https://fevm-serverless-stable-swv01.cloud.databricks.com/genie/rooms/01f13f1703ce199ebd91803207433969)

## Teardown

All objects live in a single schema. To remove everything without affecting source tables, pipelines, or dashboards:

```sql
DROP SCHEMA serverless_stable_swv01_catalog.genie_availity_ops CASCADE;
```

## Repo Structure

```
.
├── README.md
├── notebooks/
│   └── setup_genie_rooms.py     # All-in-one Databricks notebook (import & run)
├── data_generator/
│   └── generate_all.py          # Standalone synthetic data generator
├── sql/
│   ├── 00_create_schemas.sql    # Schema DDL
│   └── 01_create_genie_views.sql # 8 Genie views + 3 metric views
└── genie_config/
    └── create_rooms.py          # Genie Room provisioning via API (CLI)
```
