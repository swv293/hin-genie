-- ============================================================
-- Clinical Document Intelligence — Extended Views (payer + PA + call ops)
-- ============================================================
-- Builds on 01_create_genie_views.sql by adding payer-aware versions of
-- existing views and surfacing three new question categories:
--
--   ROOM 1 — Document Processing
--   - genie_doc_intake_daily, genie_doc_match_detail get payer_code passthrough
--   - NEW genie_pa_decisions_daily       (PA TAT, SLA compliance per 2026 CMS rule)
--   - NEW genie_payer_mix                (volume + match quality per payer)
--   - NEW mv_pa_metrics                  (approval_rate, sla_compliance_rate, avg_days_to_decision)
--   - NEW mv_payer_mix_metrics           (per-payer volume + quality measures)
--
--   ROOM 2 — Provider Support & Calls
--   - NEW genie_call_ops_daily           (FCR, AHT, ASA, abandonment timings per day)
--   - NEW mv_call_ops_metrics            (avg_wait_sec, avg_handle_sec, fcr_pct)
--
-- All views read from sources updated in 04_add_payer_pa_callops.sql.
-- ============================================================

-- ============================================================
-- ROOM 1: enrich existing views with payer_code
-- ============================================================

-- ------------------------------------------------------------
-- genie_doc_intake_daily (refreshed) — adds payer dimension
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW serverless_stable_swv01_catalog.genie_availity_ops.genie_doc_intake_daily (
  intake_date          COMMENT 'Calendar date documents were received',
  payer_code           COMMENT 'Payer the document belongs to — FK to ref.payer_dim. NULL for unassigned',
  total_docs           COMMENT 'Total documents received that day across all channels for the payer',
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
  rolling_7d_avg       COMMENT '7-day trailing average of total docs for this payer',
  rolling_30d_avg      COMMENT '30-day trailing average of total docs for this payer',
  is_volume_spike      COMMENT 'True if total_docs exceeds 2x the 7-day rolling average for this payer'
) COMMENT 'Daily document intake by payer with trend metrics. Includes day-over-day change, rolling averages, and spike detection. No PHI/PII.'
AS
WITH base AS (
  SELECT
    DATE(ingestion_timestamp) AS intake_date,
    payer_code,
    COUNT(*) AS total_docs,
    SUM(CASE WHEN document_type = 'prior_auth_form'   THEN 1 ELSE 0 END) AS prior_auth_forms,
    SUM(CASE WHEN document_type = 'clinical_note'     THEN 1 ELSE 0 END) AS clinical_notes,
    SUM(CASE WHEN document_type = 'lab_result'        THEN 1 ELSE 0 END) AS lab_results,
    SUM(CASE WHEN document_type = 'imaging_report'    THEN 1 ELSE 0 END) AS imaging_reports,
    SUM(CASE WHEN document_type = 'discharge_summary' THEN 1 ELSE 0 END) AS discharge_summaries,
    SUM(CASE WHEN is_readable = false                 THEN 1 ELSE 0 END) AS unreadable_docs,
    ROUND(AVG(quality_score), 3) AS avg_quality_score,
    SUM(CASE WHEN source_channel = 'fax'        THEN 1 ELSE 0 END) AS via_fax,
    SUM(CASE WHEN source_channel = 'electronic' THEN 1 ELSE 0 END) AS via_electronic,
    SUM(CASE WHEN source_channel = 'upload'     THEN 1 ELSE 0 END) AS via_upload,
    SUM(CASE WHEN source_channel = 'mail'       THEN 1 ELSE 0 END) AS via_mail
  FROM serverless_stable_swv01_catalog.raw.clinical_document
  GROUP BY DATE(ingestion_timestamp), payer_code
)
SELECT
  *,
  ROUND(AVG(total_docs) OVER (
    PARTITION BY payer_code ORDER BY intake_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 1) AS rolling_7d_avg,
  ROUND(AVG(total_docs) OVER (
    PARTITION BY payer_code ORDER BY intake_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ), 1) AS rolling_30d_avg,
  total_docs > 2.0 * AVG(total_docs) OVER (
    PARTITION BY payer_code ORDER BY intake_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS is_volume_spike
FROM base;

-- ------------------------------------------------------------
-- genie_doc_match_detail (refreshed) — adds payer_code passthrough
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW serverless_stable_swv01_catalog.genie_availity_ops.genie_doc_match_detail (
  doc_id               COMMENT 'Document identifier (non-PII surrogate UUID)',
  payer_code           COMMENT 'Payer the document belongs to — FK to ref.payer_dim',
  document_type        COMMENT 'Type — prior_auth_form, clinical_note, lab_result, imaging_report, discharge_summary',
  source_channel       COMMENT 'Intake channel — fax, electronic, upload, mail',
  intake_date          COMMENT 'Date document was received',
  match_class          COMMENT 'Fellegi-Sunter classification — match, possible_match, non_match',
  match_weight         COMMENT 'Fellegi-Sunter total weight score',
  doc_risk_tier        COMMENT 'Risk tier — Unreadable, High Risk - No Anchors, Medium Risk - One Anchor, Low Risk - Both Anchors',
  unreadable_flag      COMMENT 'True if OCR could not parse the document',
  missing_dob          COMMENT 'True if date of birth could not be extracted',
  missing_ssn4         COMMENT 'True if last-4 SSN could not be extracted',
  has_auth_id          COMMENT 'True if an authorization ID was extracted',
  has_member_id        COMMENT 'True if a member ID was extracted'
) COMMENT 'Per-document match status by payer with risk tier and extraction flags. No PHI/PII.'
AS
SELECT
  m.doc_id,
  d.payer_code,
  d.document_type,
  d.source_channel,
  DATE(d.ingestion_timestamp) AS intake_date,
  m.match_class,
  m.total_weight AS match_weight,
  CASE
    WHEN p.unreadable_flag THEN 'Unreadable'
    WHEN COALESCE(s.missing_dob, true) AND COALESCE(s.missing_ssn4, true) THEN 'High Risk - No Anchors'
    WHEN COALESCE(s.missing_dob, true) OR  COALESCE(s.missing_ssn4, true) THEN 'Medium Risk - One Anchor'
    ELSE 'Low Risk - Both Anchors'
  END AS doc_risk_tier,
  p.unreadable_flag,
  COALESCE(s.missing_dob, true)  AS missing_dob,
  COALESCE(s.missing_ssn4, true) AS missing_ssn4,
  s.auth_id           IS NOT NULL AS has_auth_id,
  s.member_id_on_form IS NOT NULL AS has_member_id
FROM serverless_stable_swv01_catalog.pipeline_prd.doc_member_match_candidates m
LEFT JOIN serverless_stable_swv01_catalog.pipeline_prd.clinical_doc_parsed     p ON m.doc_id = p.doc_id
LEFT JOIN serverless_stable_swv01_catalog.pipeline_prd.clinical_doc_structured s ON m.doc_id = s.doc_id
LEFT JOIN serverless_stable_swv01_catalog.raw.clinical_document                d ON m.doc_id = d.doc_id;

-- ------------------------------------------------------------
-- genie_pa_decisions_daily — NEW
-- Prior-auth decision metrics per day per payer, with CMS 2026 SLA flags.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW serverless_stable_swv01_catalog.genie_availity_ops.genie_pa_decisions_daily (
  decision_date        COMMENT 'Date the authorization decision was made',
  payer_code           COMMENT 'Payer that issued the decision — FK to ref.payer_dim',
  urgency              COMMENT 'urgent — 72-hour CMS SLA; standard — 7-day CMS SLA',
  total_decisions      COMMENT 'Count of PA decisions made that day for this payer + urgency',
  approved             COMMENT 'Count of Approved decisions',
  denied               COMMENT 'Count of Denied decisions',
  partial              COMMENT 'Count of Partial-approval decisions',
  cancelled            COMMENT 'Count of Cancelled requests',
  approval_rate_pct    COMMENT 'Percentage of decisions that were Approved (excludes Cancelled)',
  denial_rate_pct      COMMENT 'Percentage of decisions that were Denied (excludes Cancelled)',
  avg_days_to_decision COMMENT 'Average days from auth_requested_date to auth_decision_date',
  sla_target_hours     COMMENT '72 for urgent, 168 for standard — per CMS-0057-F 2026',
  within_sla_count     COMMENT 'Decisions where avg_days_to_decision met the SLA',
  sla_compliance_pct   COMMENT 'Percentage of decisions completed within the CMS SLA window'
) COMMENT 'Daily PA decision metrics per payer + urgency. Tracks the CMS 2026 prior-authorization reporting requirement (72-hr urgent / 7-day standard SLA). No PHI/PII.'
AS
WITH base AS (
  SELECT
    auth_decision_date AS decision_date,
    payer_code,
    urgency,
    status,
    DATEDIFF(auth_decision_date, auth_requested_date) AS days_to_decision,
    CASE
      WHEN urgency = 'urgent'   THEN 72   -- hours
      WHEN urgency = 'standard' THEN 168  -- 7 days × 24 hrs
    END AS sla_target_hours
  FROM serverless_stable_swv01_catalog.raw.authorization
  WHERE auth_decision_date IS NOT NULL
    AND auth_requested_date IS NOT NULL
)
SELECT
  decision_date,
  payer_code,
  urgency,
  COUNT(*) AS total_decisions,
  SUM(CASE WHEN status = 'Approved'  THEN 1 ELSE 0 END) AS approved,
  SUM(CASE WHEN status = 'Denied'    THEN 1 ELSE 0 END) AS denied,
  SUM(CASE WHEN status = 'Partial'   THEN 1 ELSE 0 END) AS partial,
  SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled,
  ROUND(100.0 * SUM(CASE WHEN status = 'Approved' THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN status <> 'Cancelled' THEN 1 ELSE 0 END), 0), 2) AS approval_rate_pct,
  ROUND(100.0 * SUM(CASE WHEN status = 'Denied' THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN status <> 'Cancelled' THEN 1 ELSE 0 END), 0), 2) AS denial_rate_pct,
  ROUND(AVG(days_to_decision), 2) AS avg_days_to_decision,
  MAX(sla_target_hours) AS sla_target_hours,
  SUM(CASE WHEN days_to_decision * 24 <= sla_target_hours THEN 1 ELSE 0 END) AS within_sla_count,
  ROUND(100.0 * SUM(CASE WHEN days_to_decision * 24 <= sla_target_hours THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0), 2) AS sla_compliance_pct
FROM base
GROUP BY decision_date, payer_code, urgency;

-- ------------------------------------------------------------
-- genie_payer_mix — NEW
-- Per-payer aggregate: document volume, match quality, PA decision volume.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW serverless_stable_swv01_catalog.genie_availity_ops.genie_payer_mix (
  payer_code              COMMENT 'Payer identifier — FK to ref.payer_dim',
  payer_name              COMMENT 'Payer marketing name',
  payer_type              COMMENT 'commercial / medicare_advantage / medicaid',
  total_documents         COMMENT 'Lifetime documents received for this payer',
  total_prior_auth_forms  COMMENT 'Prior-auth-form documents only',
  unreadable_pct          COMMENT 'Percent of documents that failed OCR',
  total_pa_decisions      COMMENT 'PA decisions issued for this payer',
  pa_approval_rate_pct    COMMENT 'Approval rate across all PA decisions for this payer',
  pa_urgent_count         COMMENT 'PA decisions marked urgent',
  pa_avg_days_to_decision COMMENT 'Mean days from auth_requested_date to auth_decision_date'
) COMMENT 'Per-payer mix snapshot — volume, quality, and PA outcomes. No PHI/PII.'
AS
SELECT
  d.payer_code,
  pd.payer_name,
  pd.payer_type,
  COUNT(DISTINCT d.doc_id) AS total_documents,
  SUM(CASE WHEN d.document_type = 'prior_auth_form' THEN 1 ELSE 0 END) AS total_prior_auth_forms,
  ROUND(100.0 * SUM(CASE WHEN d.is_readable = false THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS unreadable_pct,
  COALESCE(a.total_pa_decisions, 0)      AS total_pa_decisions,
  COALESCE(a.approval_rate_pct, 0)       AS pa_approval_rate_pct,
  COALESCE(a.urgent_count, 0)            AS pa_urgent_count,
  COALESCE(a.avg_days_to_decision, 0)    AS pa_avg_days_to_decision
FROM serverless_stable_swv01_catalog.raw.clinical_document d
LEFT JOIN serverless_stable_swv01_catalog.ref.payer_dim pd ON pd.payer_code = d.payer_code
LEFT JOIN (
  SELECT
    payer_code,
    COUNT(*)                                                                 AS total_pa_decisions,
    ROUND(100.0 * SUM(CASE WHEN status = 'Approved' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS approval_rate_pct,
    SUM(CASE WHEN urgency = 'urgent' THEN 1 ELSE 0 END)                      AS urgent_count,
    ROUND(AVG(DATEDIFF(auth_decision_date, auth_requested_date)), 2)         AS avg_days_to_decision
  FROM serverless_stable_swv01_catalog.raw.authorization
  WHERE auth_decision_date IS NOT NULL
  GROUP BY payer_code
) a ON a.payer_code = d.payer_code
GROUP BY d.payer_code, pd.payer_name, pd.payer_type,
         a.total_pa_decisions, a.approval_rate_pct, a.urgent_count, a.avg_days_to_decision;

-- ------------------------------------------------------------
-- mv_pa_metrics — NEW metric view
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW serverless_stable_swv01_catalog.genie_availity_ops.mv_pa_metrics
  WITH METRICS
  LANGUAGE YAML
AS $$
version: 0.1
source: serverless_stable_swv01_catalog.genie_availity_ops.genie_pa_decisions_daily

dimensions:
  - name: decision_date
    expr: decision_date
  - name: payer_code
    expr: payer_code
  - name: urgency
    expr: urgency

measures:
  - name: pa_volume
    expr: SUM(total_decisions)
  - name: approval_rate_pct
    expr: ROUND(100.0 * SUM(approved) / NULLIF(SUM(approved + denied + partial), 0), 2)
  - name: denial_rate_pct
    expr: ROUND(100.0 * SUM(denied) / NULLIF(SUM(approved + denied + partial), 0), 2)
  - name: sla_compliance_pct
    expr: ROUND(100.0 * SUM(within_sla_count) / NULLIF(SUM(total_decisions), 0), 2)
  - name: avg_days_to_decision
    expr: ROUND(AVG(avg_days_to_decision), 2)
$$;

-- ------------------------------------------------------------
-- mv_payer_mix_metrics — NEW metric view
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW serverless_stable_swv01_catalog.genie_availity_ops.mv_payer_mix_metrics
  WITH METRICS
  LANGUAGE YAML
AS $$
version: 0.1
source: serverless_stable_swv01_catalog.genie_availity_ops.genie_payer_mix

dimensions:
  - name: payer_code
    expr: payer_code
  - name: payer_type
    expr: payer_type

measures:
  - name: documents
    expr: SUM(total_documents)
  - name: pa_volume
    expr: SUM(total_pa_decisions)
  - name: pa_approval_rate_pct
    expr: ROUND(AVG(pa_approval_rate_pct), 2)
$$;

-- ============================================================
-- ROOM 2: new call-operations view + metric view
-- ============================================================

-- ------------------------------------------------------------
-- genie_call_ops_daily — NEW
-- Per-day per-agency call operations timings (FCR, AHT, ASA).
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW serverless_stable_swv01_catalog.genie_availity_ops.genie_call_ops_daily (
  ops_date              COMMENT 'Day the call occurred',
  agency_name           COMMENT 'Outsourced agency that handled the call',
  call_type             COMMENT 'Call type taxonomy',
  total_calls           COMMENT 'Total calls handled that day for this agency + type',
  avg_wait_seconds      COMMENT 'Average queue wait time in seconds — ASA',
  avg_handle_seconds    COMMENT 'Average active call handle time — AHT',
  fcr_resolved_count    COMMENT 'Count of calls resolved on first contact (no callback within 7 days)',
  fcr_pct               COMMENT 'First Call Resolution percentage',
  abandoned_proxy_count COMMENT 'Calls where wait_seconds exceeded 600 (10 min) — proxy for abandonment risk'
) COMMENT 'Per-day per-agency call operations timings — FCR, AHT, ASA. Distinct from genie_call_scores which is QA scoring only. No PHI/PII.'
AS
SELECT
  DATE(c.created_ts)                                              AS ops_date,
  c.agency_name,
  c.call_type,
  COUNT(*)                                                        AS total_calls,
  ROUND(AVG(o.wait_seconds), 0)                                   AS avg_wait_seconds,
  ROUND(AVG(o.handle_seconds), 0)                                 AS avg_handle_seconds,
  SUM(CASE WHEN o.fcr_flag THEN 1 ELSE 0 END)                     AS fcr_resolved_count,
  ROUND(100.0 * SUM(CASE WHEN o.fcr_flag THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS fcr_pct,
  SUM(CASE WHEN o.wait_seconds > 600 THEN 1 ELSE 0 END)           AS abandoned_proxy_count
FROM serverless_stable_swv01_catalog.transcript_intel_sdp.mv_call_scores c
JOIN serverless_stable_swv01_catalog.transcript_intel_sdp.call_ops_supplemental o ON c.call_id = o.call_id
GROUP BY DATE(c.created_ts), c.agency_name, c.call_type;

-- ------------------------------------------------------------
-- mv_call_ops_metrics — NEW metric view
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW serverless_stable_swv01_catalog.genie_availity_ops.mv_call_ops_metrics
  WITH METRICS
  LANGUAGE YAML
AS $$
version: 0.1
source: serverless_stable_swv01_catalog.genie_availity_ops.genie_call_ops_daily

dimensions:
  - name: ops_date
    expr: ops_date
  - name: agency_name
    expr: agency_name
  - name: call_type
    expr: call_type

measures:
  - name: total_calls
    expr: SUM(total_calls)
  - name: avg_wait_seconds
    expr: ROUND(AVG(avg_wait_seconds), 0)
  - name: avg_handle_seconds
    expr: ROUND(AVG(avg_handle_seconds), 0)
  - name: fcr_pct
    expr: ROUND(100.0 * SUM(fcr_resolved_count) / NULLIF(SUM(total_calls), 0), 2)
$$;
