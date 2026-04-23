-- ============================================================
-- Clinical Document Intelligence — Genie Room Views
-- ============================================================
-- All 8 views for the two Genie Rooms.
-- Schema: serverless_stable_swv01_catalog.genie_availity_ops
-- No PHI/PII exposed. Read-only overlays on source tables.
-- ============================================================

-- ============================================================
-- ROOM 1: Document Processing & Authorization Intelligence
-- ============================================================

-- ------------------------------------------------------------
-- 1. genie_doc_intake_daily
--    Daily document intake with trend metrics, rolling averages,
--    and spike detection.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW genie_availity_ops.genie_doc_intake_daily (
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
  FROM serverless_stable_swv01_catalog.raw.clinical_document
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
FROM base;

-- ------------------------------------------------------------
-- 2. genie_doc_match_detail
--    Per-document match status with risk tier, extraction flags,
--    and contextual rankings.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW genie_availity_ops.genie_doc_match_detail (
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
FROM serverless_stable_swv01_catalog.pipeline_prd.doc_member_match_candidates m
LEFT JOIN serverless_stable_swv01_catalog.pipeline_prd.clinical_doc_parsed p ON m.doc_id = p.doc_id
LEFT JOIN serverless_stable_swv01_catalog.pipeline_prd.clinical_doc_structured s ON m.doc_id = s.doc_id
LEFT JOIN serverless_stable_swv01_catalog.raw.clinical_document d ON m.doc_id = d.doc_id;

-- ------------------------------------------------------------
-- 3. genie_auth_match_daily
--    Daily authorization match volumes with week-over-week trends
--    and class distribution.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW genie_availity_ops.genie_auth_match_daily (
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
  FROM serverless_stable_swv01_catalog.pipeline_prd.doc_auth_match_candidates
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
FROM base;

-- ------------------------------------------------------------
-- 4. genie_data_quality_daily
--    Daily parsing data quality with rolling trends and
--    degradation detection.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW genie_availity_ops.genie_data_quality_daily (
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
  FROM serverless_stable_swv01_catalog.pipeline_prd.clinical_doc_parsed p
  LEFT JOIN serverless_stable_swv01_catalog.pipeline_prd.clinical_doc_structured s ON p.doc_id = s.doc_id
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
FROM base;

-- ------------------------------------------------------------
-- 5. genie_pipeline_snapshot
--    Current-state pipeline KPIs in long format for easy
--    Genie querying.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW genie_availity_ops.genie_pipeline_snapshot (
  metric_name   COMMENT 'Pipeline KPI metric name',
  metric_value  COMMENT 'Current value',
  metric_unit   COMMENT 'Unit: count, percentage, or score'
) COMMENT 'Current-state pipeline KPIs in long format. No PHI/PII.'
AS
SELECT metric_name, metric_value, metric_unit FROM (
  SELECT 'Total Members' AS metric_name, CAST(total_members AS DOUBLE) AS metric_value, 'count' AS metric_unit FROM serverless_stable_swv01_catalog.dashboard_prd.v_pipeline_kpis
  UNION ALL SELECT 'Total Documents', CAST(total_documents AS DOUBLE), 'count' FROM serverless_stable_swv01_catalog.dashboard_prd.v_pipeline_kpis
  UNION ALL SELECT 'Total Parsed', CAST(total_parsed AS DOUBLE), 'count' FROM serverless_stable_swv01_catalog.dashboard_prd.v_pipeline_kpis
  UNION ALL SELECT 'Unreadable Documents', CAST(unreadable_docs AS DOUBLE), 'count' FROM serverless_stable_swv01_catalog.dashboard_prd.v_pipeline_kpis
  UNION ALL SELECT 'High Confidence Matches', CAST(high_confidence_matches AS DOUBLE), 'count' FROM serverless_stable_swv01_catalog.dashboard_prd.v_pipeline_kpis
  UNION ALL SELECT 'Possible Matches', CAST(possible_matches AS DOUBLE), 'count' FROM serverless_stable_swv01_catalog.dashboard_prd.v_pipeline_kpis
  UNION ALL SELECT 'Non Matches', CAST(non_matches AS DOUBLE), 'count' FROM serverless_stable_swv01_catalog.dashboard_prd.v_pipeline_kpis
  UNION ALL SELECT 'Auth Matches', CAST(auth_matches AS DOUBLE), 'count' FROM serverless_stable_swv01_catalog.dashboard_prd.v_pipeline_kpis
);

-- ============================================================
-- ROOM 2: Provider Support & Call Intelligence
-- ============================================================

-- ------------------------------------------------------------
-- 6. genie_call_scores
--    Call quality scores with agent rankings and contextual
--    benchmarks.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW genie_availity_ops.genie_call_scores (
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
FROM serverless_stable_swv01_catalog.transcript_intel_sdp.mv_call_scores;

-- ------------------------------------------------------------
-- 7. genie_call_sentiment
--    AI-generated call summaries with sentiment analysis.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW genie_availity_ops.genie_call_sentiment (
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
FROM serverless_stable_swv01_catalog.transcript_intel_sdp.gold_call_summaries_sentiment;

-- ------------------------------------------------------------
-- 8. genie_compliance_daily
--    Daily compliance with rolling trends, WoW comparison,
--    agency rankings, and consecutive-day streaks.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW genie_availity_ops.genie_compliance_daily (
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
  FROM serverless_stable_swv01_catalog.transcript_intel_sdp.mv_compliance_outcomes
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
FROM streaks;


-- ============================================================
-- METRIC VIEWS (Databricks AI/BI)
-- ============================================================
-- Metric views expose pre-defined measures and dimensions so
-- Genie can answer aggregate questions without writing SQL.
-- Syntax: CREATE VIEW ... WITH METRICS LANGUAGE YAML AS$ ... $
-- ============================================================

-- ------------------------------------------------------------
-- 9. mv_doc_intake_metrics
--    Measures over genie_doc_intake_daily for document volume
--    and channel analysis.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW genie_availity_ops.mv_doc_intake_metrics
  WITH METRICS
  LANGUAGE YAML
AS $
source: serverless_stable_swv01_catalog.genie_availity_ops.genie_doc_intake_daily

dimensions:
  - name: intake_date
    expr: intake_date
    type: DATE
    description: "Calendar date documents were received"
  - name: is_volume_spike
    expr: is_volume_spike
    type: BOOLEAN
    description: "True if total_docs exceeds 2x the 7-day rolling average"

measures:
  - name: total_documents
    expr: SUM(total_docs)
    type: INT
    description: "Total documents received"
  - name: unreadable_documents
    expr: SUM(unreadable_docs)
    type: INT
    description: "Documents flagged unreadable by OCR"
  - name: avg_ocr_quality
    expr: ROUND(AVG(avg_quality_score), 3)
    type: DOUBLE
    description: "Average OCR quality score (0.0 to 1.0)"
  - name: fax_volume
    expr: SUM(via_fax)
    type: INT
    description: "Documents received via fax"
  - name: electronic_volume
    expr: SUM(via_electronic)
    type: INT
    description: "Documents received via electronic/EDI"
  - name: upload_volume
    expr: SUM(via_upload)
    type: INT
    description: "Documents received via portal upload"
  - name: mail_volume
    expr: SUM(via_mail)
    type: INT
    description: "Documents received via physical mail/scan"
  - name: spike_day_count
    expr: SUM(CASE WHEN is_volume_spike THEN 1 ELSE 0 END)
    type: INT
    description: "Number of days flagged as volume spikes"
  - name: unreadable_rate_pct
    expr: ROUND(100.0 * SUM(unreadable_docs) / NULLIF(SUM(total_docs), 0), 2)
    type: DOUBLE
    description: "Percentage of documents that are unreadable"
$;

-- ------------------------------------------------------------
-- 10. mv_doc_match_metrics
--     Measures over genie_doc_match_detail for match quality
--     and risk analysis.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW genie_availity_ops.mv_doc_match_metrics
  WITH METRICS
  LANGUAGE YAML
AS $
source: serverless_stable_swv01_catalog.genie_availity_ops.genie_doc_match_detail

dimensions:
  - name: document_type
    expr: document_type
    type: STRING
    description: "Document type: prior_auth_form, clinical_note, lab_result, imaging_report, discharge_summary"
  - name: source_channel
    expr: source_channel
    type: STRING
    description: "Intake channel: fax, electronic, upload, mail"
  - name: match_class
    expr: match_class
    type: STRING
    description: "Fellegi-Sunter classification: match, possible_match, non_match"
  - name: doc_risk_tier
    expr: doc_risk_tier
    type: STRING
    description: "Risk tier based on extraction completeness"
  - name: intake_date
    expr: intake_date
    type: DATE
    description: "Date document was received"

measures:
  - name: document_count
    expr: COUNT(*)
    type: INT
    description: "Total documents"
  - name: match_rate_pct
    expr: ROUND(100.0 * SUM(CASE WHEN match_class = 'match' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
    type: DOUBLE
    description: "Percentage of documents that matched"
  - name: avg_match_weight
    expr: ROUND(AVG(match_weight), 4)
    type: DOUBLE
    description: "Average Fellegi-Sunter match weight"
  - name: high_risk_pct
    expr: ROUND(100.0 * SUM(CASE WHEN doc_risk_tier LIKE 'High%' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
    type: DOUBLE
    description: "Percentage of documents in High Risk tier"
  - name: extraction_completeness_pct
    expr: ROUND(100.0 * SUM(CASE WHEN NOT missing_dob AND NOT missing_ssn4 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
    type: DOUBLE
    description: "Percentage with both DOB and SSN4 extracted"
  - name: non_match_count
    expr: SUM(CASE WHEN match_class = 'non_match' THEN 1 ELSE 0 END)
    type: INT
    description: "Documents that did not match any member"
$;

-- ------------------------------------------------------------
-- 11. mv_call_quality_metrics
--     Measures over genie_call_scores for call center performance.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW genie_availity_ops.mv_call_quality_metrics
  WITH METRICS
  LANGUAGE YAML
AS $
source: serverless_stable_swv01_catalog.genie_availity_ops.genie_call_scores

dimensions:
  - name: agency_name
    expr: agency_name
    type: STRING
    description: "Partner call center agency"
  - name: call_type
    expr: call_type
    type: STRING
    description: "Call type taxonomy"
  - name: disposition
    expr: disposition
    type: STRING
    description: "Call outcome: resolved, pending, escalated, complaint, appeal_opened"
  - name: outcome_bucket
    expr: outcome_bucket
    type: STRING
    description: "Triage bucket: compliant, needs_review, high_risk"
  - name: scored_date
    expr: scored_date
    type: DATE
    description: "Date the call was scored"

measures:
  - name: total_calls
    expr: COUNT(*)
    type: INT
    description: "Total calls scored"
  - name: avg_call_score
    expr: ROUND(AVG(call_score), 1)
    type: DOUBLE
    description: "Average quality score (0-100)"
  - name: compliance_rate_pct
    expr: ROUND(100.0 * SUM(CASE WHEN outcome_bucket = 'compliant' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
    type: DOUBLE
    description: "Percentage of calls that are compliant (score >= 85)"
  - name: high_risk_pct
    expr: ROUND(100.0 * SUM(CASE WHEN outcome_bucket = 'high_risk' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
    type: DOUBLE
    description: "Percentage of calls that are high risk (score < 70)"
  - name: escalation_count
    expr: SUM(CASE WHEN disposition = 'escalated' THEN 1 ELSE 0 END)
    type: INT
    description: "Number of escalated calls"
  - name: escalation_rate_pct
    expr: ROUND(100.0 * SUM(CASE WHEN disposition = 'escalated' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
    type: DOUBLE
    description: "Percentage of calls that were escalated"
$;
