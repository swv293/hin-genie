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

Works on **any Databricks workspace** with Unity Catalog enabled. If you don't have one, sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition) — no credit card, single-user workspace with a serverless SQL warehouse pre-provisioned.

- **A Databricks workspace** with Unity Catalog enabled
- **Serverless SQL warehouse** (or any SQL warehouse with access to your catalog)
- A **Unity Catalog catalog** you can write to (Free Edition gives you a `workspace` catalog by default)
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

3. **Create base Genie views** -- Deploy the original 8 curated views + 3 metric views:
   ```sql
   -- Run sql/01_create_genie_views.sql in a Databricks SQL editor
   ```

4. **Annotate the source tables** -- Propagate column-level comments to the underlying `pipeline_prd.*` tables:
   ```sql
   -- Run sql/02_source_table_comments.sql
   ```

5. **Wire up multi-payer access control** -- Create the access mapping table and the row-filter function:
   ```sql
   -- Run sql/03_payer_access_filter.sql
   ```

6. **Extend with payer dimension + PA decisions + call ops** -- Adds `payer_code` across the pipeline, `urgency` to `raw.authorization`, the `call_ops_supplemental` Delta table, and backfills:
   ```sql
   -- Run sql/04_add_payer_pa_callops.sql
   ```

7. **Create extended Genie views** -- New views over PA decisions, payer mix, and call ops, plus three new metric views:
   ```sql
   -- Run sql/05_extended_views.sql
   ```

8. **Add UC PK/FK constraints** -- Required for the Joins tab to auto-populate via FROM_SNIPPET:
   ```sql
   -- Run sql/06_uc_constraints.sql
   ```

9. **Add column synonyms** -- Helps Genie disambiguate user terms like "FCR", "ASA", "payer":
   ```sql
   -- Run sql/07_uc_labels.sql
   ```

10. **Apply row filters to source tables** -- Now real, not aspirational. Cascades through every view, metric view, and Genie conversation:
    ```sql
    -- Run sql/08_apply_row_filter.sql
    ```

11. **Spread timestamps over the last 90 days** -- Idempotent. The generator writes everything at one timestamp; this script spreads `ingestion_timestamp` / `auth_decision_date` / call ops over realistic windows so "last 7 days" / "this month" / 30-day trend questions return meaningful data:
    ```sql
    -- Run sql/09_spread_timestamps.sql
    ```

12. **Create Genie Rooms** -- Two options:

    **From a workstation (CLI):**
    ```bash
    python genie_config/create_rooms.py
    ```
    Idempotent — preserves existing curation.

    **From the workspace (notebooks, no DABs needed):**
    - `notebooks/create_room1_doc_processing.py` — provisions Room 1 (11 tables / 8 sample SQLs / rich instructions including MEASURE() preference + CMS 2026 PA SLA glossary).
    - `notebooks/create_room2_provider_support.py` — provisions Room 2 (6 tables / 8 sample SQLs / rich instructions for QA scoring + call operations).

    Each notebook reads `genie_config/roomN_curation.json`, rewrites table identifiers to the catalog/schema you set in the widgets, and `POST`s the room with `serialized_space` so it ships fully curated — no manual UI import. Re-running is a no-op unless you set `refresh_existing=yes`.

13. **POST canonical benchmarks** to the `/curated-questions` API (these don't render when sent inside `serialized_space.benchmarks`):
    ```bash
    python genie_config/sync_benchmarks.py
    ```

14. **Validate** -- Open each room URL and ask a test question. The full battery covers PA SLA compliance, payer mix, FCR/AHT/ASA, and the original document/call quality questions.

### Maintaining the rooms over time

Each room's full configuration — instructions, sample queries, joins, filters, expressions, measures, benchmarks — is exported to JSON in `genie_config/`:

```bash
python genie_config/export_rooms.py
```

Run this after any UI edit so the repo stays the source of truth. The exported JSON files (`room1_curation.json`, `room2_curation.json`) are the canonical reference for what each room should contain.

A longer-form domain reference for both rooms (Fellegi-Sunter primer, channel definitions, risk-tier ladder, cross-room investigative patterns) lives at `docs/genie_knowledge_store.md`.

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

### Extended Views (PA + payer + call ops)

| # | View | Source | Purpose |
|---|------|--------|---------|
| 9 | `genie_pa_decisions_daily` | `raw.authorization` | Per-day per-payer PA decision metrics with CMS-0057-F SLA compliance (72-hr urgent / 7-day standard) |
| 10 | `genie_payer_mix` | `raw.clinical_document` + `raw.authorization` + `ref.payer_dim` | Per-payer snapshot: volume, PA decisions, approval rate |
| 11 | `genie_call_ops_daily` | `mv_call_scores` + `call_ops_supplemental` | Per-day per-agency FCR, AHT, ASA, abandonment proxy |

### Metric Views (AI/BI)

Pre-defined measures and dimensions for Genie aggregate queries:

| # | Metric View | Source View | Key Measures |
|---|-------------|-------------|--------------|
| 12 | `mv_doc_intake_metrics` | `genie_doc_intake_daily` | total_documents, unreadable_rate_pct, channel volumes, spike_day_count |
| 13 | `mv_doc_match_metrics` | `genie_doc_match_detail` | match_rate_pct, avg_match_weight, high_risk_pct, extraction_completeness_pct |
| 14 | `mv_call_quality_metrics` | `genie_call_scores` | avg_call_score, compliance_rate_pct, high_risk_pct, escalation_rate_pct |
| 15 | `mv_pa_metrics` | `genie_pa_decisions_daily` | pa_volume, approval_rate_pct, denial_rate_pct, sla_compliance_pct, avg_days_to_decision |
| 16 | `mv_payer_mix_metrics` | `genie_payer_mix` | documents, pa_volume, pa_approval_rate_pct |
| 17 | `mv_call_ops_metrics` | `genie_call_ops_daily` | total_calls, avg_wait_seconds (ASA), avg_handle_seconds (AHT), fcr_pct |

## Genie Rooms

### Room 1: Document Processing & Authorization Intelligence

Audience: Ops leaders, utilization management staff, PA program owners, product managers, compliance officers.

Covers document intake volume, OCR quality, Fellegi-Sunter match outcomes, authorization match trends, prior-authorization decisions and CMS 2026 SLA compliance, payer mix, and pipeline KPIs. Eleven curated views (including 4 metric views) with window functions for trend analysis, spike detection, and risk tiering.

**URL**: `genie_config/create_rooms.py` (or the all-in-one notebook) prints the room URL on your workspace after provisioning. URLs follow the pattern `https://<your-workspace>.cloud.databricks.com/genie/rooms/<room_id>`.

### Room 2: Provider Support & Call Intelligence

Audience: Call center supervisors, QA analysts, provider relations, operations leads.

Covers two distinct surfaces: (1) **call QA** — quality scoring, agent rankings, AI-generated sentiment, compliance streaks; (2) **call operations** — FCR (First Call Resolution), AHT (Average Handle Time), ASA (Average Speed to Answer), abandonment proxy. Six curated views (including 2 metric views).

**URL**: same — printed by `create_rooms.py` / the notebook on your own workspace.

## Teardown

All objects live in a single schema. To remove everything without affecting source tables, pipelines, or dashboards:

```sql
-- Replace <catalog> with the catalog you set in the notebook / scripts.
DROP SCHEMA <catalog>.genie_availity_ops CASCADE;
```

## Repo Structure

```
.
├── README.md
├── notebooks/
│   ├── setup_genie_rooms.py             # All-in-one Databricks notebook (import & run)
│   ├── create_room1_doc_processing.py   # Room 1 only — fully curated provisioning
│   └── create_room2_provider_support.py # Room 2 only — fully curated provisioning
├── data_generator/
│   └── generate_all.py             # Standalone synthetic data generator
├── docs/
│   └── genie_knowledge_store.md    # Domain reference (jargon, tiers, cross-room patterns)
├── sql/
│   ├── 00_create_schemas.sql       # Schema DDL
│   ├── 01_create_genie_views.sql   # 8 base Genie views + 3 metric views
│   ├── 02_source_table_comments.sql # Column comments on pipeline_prd tables
│   ├── 03_payer_access_filter.sql  # Multi-payer mapping table + row-filter function
│   ├── 04_add_payer_pa_callops.sql # Extends schema with payer_dim, payer_code, urgency, call_ops_supplemental
│   ├── 05_extended_views.sql       # Extended views: PA decisions, payer mix, call ops + 3 new metric views
│   ├── 06_uc_constraints.sql       # UC primary/foreign keys for Joins tab auto-population
│   ├── 07_uc_labels.sql            # Column synonyms (FCR, AHT, ASA, payer, urgency)
│   ├── 08_apply_row_filter.sql     # ALTER TABLE ... SET ROW FILTER — wires the multi-payer claim
│   └── 09_spread_timestamps.sql    # Spreads single-batch timestamps over 90 days so trend questions return data
└── genie_config/
    ├── create_rooms.py             # Idempotent Genie Room provisioning (preserves curation)
    ├── export_rooms.py             # Pull live curation back to JSON (run after UI edits)
    ├── sync_benchmarks.py          # POST benchmarks to /curated-questions (not serialized_space)
    ├── room1_curation.json         # Full curation export for Room 1 (source of truth)
    └── room2_curation.json         # Full curation export for Room 2 (source of truth)
```
