# Genie Knowledge Store — Clinical Document Intelligence

This document is the canonical domain reference for the two Genie Rooms in this demo. It is loaded into both spaces as supplementary context so the assistant can reason about jargon, business rules, risk tiers, and operational conventions that live outside the table schemas.

> **For:** Genie Rooms 1 & 2 in this demo (Document Processing Intelligence; Provider Support & Call Intelligence).
> **Source of truth:** `genie_config/room1_curation.json`, `genie_config/room2_curation.json` already capture the in-room `text_instructions`. This file is the longer-form reference that complements those.

---

## 1. Business context

A **health information network (HIN) / clearinghouse** sits between providers and payers. It connects health plans, providers, and health-IT vendors through a single multi-payer portal and API layer, processing eligibility checks, claims, prior authorizations, and remittance advice at national scale.

Two operational surfaces matter for this demo:

| Surface | What it does | Audience |
|---|---|---|
| **Document processing pipeline** | Ingests clinical documents from four channels, OCR-parses them, identity-matches to members and authorizations, routes to the correct payer workflow. | Ops leaders, UM staff, product managers. |
| **Provider support call center** | Handles provider calls about transactions, eligibility, claims status, prior auth. Outsourced to multiple agencies. | Call-center supervisors, QA analysts, provider relations. |

These two surfaces are **coupled**: a degradation in document processing typically surfaces in the call center first (provider calls go up; sentiment goes down). That coupling is why the demo uses two scoped rooms with a shared data model rather than one combined room.

---

## 2. Document processing — domain reference

### Intake channels

| Channel | Description | Quality risk |
|---|---|---|
| **fax** | Inbound fax to the HIN's fax-receive infrastructure | Highest unreadable rate; OCR-quality varies with sender's hardware |
| **electronic** | EDI / API submission from a provider system | Lowest unreadable rate; metadata typically clean |
| **upload** | Provider uploads via portal | Medium quality; varies with file type |
| **mail** | Physical mail, scanned at intake | Highest variance; aging documents, handwriting |

### Document types

`prior_auth_form` · `clinical_note` · `lab_result` · `imaging_report` · `discharge_summary`

### Fellegi-Sunter primer

Fellegi-Sunter is a probabilistic record-linkage framework. For each candidate (document ↔ member, or document ↔ authorization) pair, every comparison field contributes a weight:

- **Agreement weight** = `log2(m / u) * field_weight_multiplier`, where `m = P(agree | true match)` and `u = P(agree | non-match)`.
- **Disagreement weight** = `log2((1 - m) / (1 - u)) * field_weight_multiplier`.

A higher `total_weight` = stronger evidence the pair is a true match.

The pipeline uses the following multipliers (from `pipeline_prd.fellegi_sunter_parameters`):

- SSN-last-4: 5×
- DOB: 4×
- Name: 1×

This is why the **anchors** in the risk tier are DOB and SSN4 — losing either materially weakens the match.

### Match classification

| `match_class` | Meaning | Operational consequence |
|---|---|---|
| `match` | High confidence — total weight above the upper threshold | Auto-routed to payer workflow |
| `possible_match` | Medium confidence — weight between thresholds | Goes to human review queue |
| `non_match` | Low confidence — weight below threshold | Returned to provider with rejection / clarification request |

### Risk tier ladder

| Tier | Definition | Use it for |
|---|---|---|
| **Unreadable** | OCR could not parse the document at all | Top-priority remediation; talk to provider |
| **High Risk - No Anchors** | Both DOB and SSN4 missing | Manual review only — auto-match disabled |
| **Medium Risk - One Anchor** | DOB or SSN4 missing (but not both) | Possible match candidate, weaker confidence |
| **Low Risk - Both Anchors** | Both DOB and SSN4 extracted | Eligible for high-confidence auto-match |

### Volume & quality alarms

| Flag | Definition | What it means |
|---|---|---|
| `is_volume_spike` | `total_docs > 2 × 7-day rolling avg` | Investigate intake spike — could be a payer-driven event or a provider system change |
| `dq_degradation_flag` | `pct_unreadable > 1.5 × rolling avg` | Investigate OCR / channel quality — a sender's fax hardware may have failed |

### Prior-authorization decisions and CMS 2026 SLA

`raw.authorization` carries the PA-decision lifecycle: `auth_requested_date`, `auth_decision_date`, `status` (Approved / Denied / Partial / Pended / Pending / Cancelled / Expired), `urgency` (urgent / standard), `denial_reason_code`, `procedure_code`.

`genie_pa_decisions_daily` aggregates these per (decision_date, payer_code, urgency) with **CMS-0057-F SLA flags**:

- **Urgent**: 72-hour decision SLA. `within_sla_count` = decisions where `(auth_decision_date - auth_requested_date) × 24h ≤ 72h`.
- **Standard**: 7-day (168-hour) decision SLA.

Public reporting deadline: **March 31** each year, for the previous calendar year, per the CMS Interoperability and Prior Authorization Final Rule. Required metrics include approval rate, denial rate, and average time to decision — all surfaced as named measures on `mv_pa_metrics`:

| Measure | What it returns |
|---|---|
| `pa_volume` | Total decisions in the slice |
| `approval_rate_pct` | % Approved across non-cancelled decisions |
| `denial_rate_pct` | % Denied across non-cancelled decisions |
| `sla_compliance_pct` | % of decisions made within the CMS SLA |
| `avg_days_to_decision` | Mean days from request to decision |

### Payer dimension

`ref.payer_dim` carries the five demo payers: **AETNA, UHC, BCBS, CIGNA, HUMANA**. The hash-based backfill in `sql/04_add_payer_pa_callops.sql` is deterministic — the same doc lands on the same payer every run.

`payer_code` is propagated through `raw.clinical_document`, `raw.authorization`, and the four `pipeline_prd.*` tables. The row filter on those source tables cascades through every Genie view, metric view, and conversation. Adding a payer means inserting one row into `ref.payer_dim` and granting access via `payer_access_mapping`.

### Default conventions for Genie

- "Match rate" without qualifier = **member match rate** (not auth match rate).
- Default time window = last 30 days unless the user specifies.
- `genie_pipeline_snapshot` is point-in-time KPIs — never use it for trends.
- All window/lag/percentile columns in the views are **pre-computed**; reference them directly rather than re-deriving.

---

## 3. Provider support call center — domain reference

### Call quality scoring rubric

The scorecard is weighted as follows:

| Component | Weight | What it measures |
|---|---|---|
| Transcription confidence | 35% | ASR accuracy on the call audio |
| Section coverage | 35% | Did the agent cover the required topic sections (greeting, identity verification, issue, resolution, close) |
| PII redaction compliance | 20% | Were PII / PHI mentions appropriately handled |
| Disposition quality | 10% | Was the call disposition coded correctly |

Output: `call_score_pct` 0–100, with `score_band` of `Excellent (≥ 90)` · `Good (75–89)` · `Needs Coaching (60–74)` · `At Risk (< 60)`.

### Compliance flags

- **Below-compliance day**: any day where an agency's average score is below the contractually-agreed threshold (typically 75).
- **Consecutive-day streak**: count of consecutive below-compliance days. Three or more = formal escalation per most agency contracts.
- **Week-over-week trend**: compares average score against the same day-of-week prior week, surfaces sustained drift.

### Sentiment fields

`overall_sentiment`, `start_sentiment`, `end_sentiment`, `sentiment_trajectory` — generated by AI summarization. Trajectory is one of `improving` · `stable` · `declining` · `recovered`.

A `declining` trajectory + a normal score band is a useful early-warning signal: the agent is technically compliant but losing the provider mid-call.

### Agency model

The HIN works with multiple outsourced agencies. Each agent belongs to one agency. Compliance is generally tracked at the **agency** level for contractual purposes, and at the **agent** level for coaching purposes.

### Default conventions for Genie

- "Compliance rate" = `% of agent-days at or above the threshold`, calculated per the metric view definition.
- Default time window = last 14 days.
- "Top agents" = top-N by `call_score_pct` averaged over the requested window, with a minimum-volume filter (≥ 20 calls) to avoid skew.

### Call operations vs. call QA — two distinct surfaces

Room 2 carries two distinct surfaces over the same calls:

| Surface | Tables | Answers questions like |
|---|---|---|
| **Call QA** (quality scoring) | `genie_call_scores`, `genie_call_sentiment`, `genie_compliance_daily`, `mv_call_quality_metrics` | "Top 10 agents by score", "compliance streak", "sentiment trajectory" |
| **Call operations** (timings) | `genie_call_ops_daily`, `mv_call_ops_metrics` | "First Call Resolution rate", "AHT by agency", "ASA trend", "agencies missing the FCR benchmark" |

Industry benchmarks (healthcare, 2026):

| Metric | Target | Source |
|---|---|---|
| FCR (First Call Resolution) | ≥ 70% | Five9, CloudTalk |
| AHT (Average Handle Time) | ≈ 12 min (720s) | Healthcare contact-center benchmarks |
| ASA (Average Speed to Answer) | ≈ 4.4 min (264s) | Healthcare contact-center benchmarks |
| Abandonment Rate | < 8% (alarm > 10%) | Industry standard |

`mv_call_ops_metrics` exposes named measures:

| Measure | What it returns |
|---|---|
| `total_calls` | Call volume |
| `avg_wait_seconds` | ASA in seconds |
| `avg_handle_seconds` | AHT in seconds |
| `fcr_pct` | First Call Resolution percentage |

---

## 4. Cross-room investigative patterns

When using both rooms together, these are the canonical leading-indicator patterns:

| Signal in **call center room** | Likely root cause in **pipeline room** |
|---|---|
| Sentiment trajectory `declining` for a specific call type (e.g., "prior auth status") | Match-rate or auth-match degradation in the corresponding document type |
| Compliance streak break for a single agency | Volume spike or channel-quality issue routing more calls to that agency |
| Sudden uptick in calls about "missing eligibility" | EDI channel quality degradation in `genie_data_quality_daily` |
| Sustained increase in average handle time | Document processing backlog → providers calling for status |

When using both rooms together, these are the canonical reverse patterns (from pipeline → call center):

| Signal in **pipeline room** | Expected effect in **call center room** |
|---|---|
| `is_volume_spike = true` on intake | Call volume rises ~12–24 hrs later for the affected payer |
| `dq_degradation_flag = true` (high `pct_unreadable`) | Sentiment dip on calls referencing the same document type |
| `match_class = 'non_match'` increases | Agency compliance for "rejection-handling" calls degrades |

---

## 5. Multi-payer scope

Both rooms are designed to serve every payer in the HIN's network from one data model. The mechanism:

- A single Unity Catalog row-filter function (`payer_access_filter`, defined in `sql/03_payer_access_filter.sql`) evaluates `current_user()` against `payer_access_mapping`.
- The filter is intended to be applied to any view that carries a `payer_id` column.
- Adding a payer = inserting rows in `payer_access_mapping`. No new room. No new views. No copy of any report.

> **Status note:** the filter function and mapping table exist in `sql/03_payer_access_filter.sql`. Wiring the filter onto the Genie views requires extending the views to project a `payer_id` column from the underlying source tables — a one-line change per view that has been left out of this iteration to keep the demo's first-run minimal.

---

## 6. Iteration discipline (how to keep accuracy high over time)

Per Databricks Genie best practices, accuracy improves with curated examples — not with bigger spaces. Each time a real user asks a question and Genie returns an unsatisfactory answer:

1. Capture the question and the desired SQL.
2. Add it to the room's `example_question_sqls` (sample queries) via the UI.
3. If the question reveals missing jargon or a new abbreviation, append to `text_instructions`.
4. Re-export curation to JSON: `genie_config/room1_curation.json` / `room2_curation.json` (use the export script in `genie_config/`).
5. Commit.

Keep both rooms ≤ 7 tables. If the surface grows beyond that, split into a third room with a fresh, focused topic — do not expand an existing room past the recommended scope.
