-- ============================================================
-- Spread synthetic timestamps over the last ~90 days
-- ============================================================
-- The original data generator wrote every document with a single
-- ingestion_timestamp (2026-03-25) and every call with a single
-- created_ts (2026-04-20). This makes any "last 7 days" / "this
-- month" / "30-day trend" question return thin or empty results.
--
-- This script spreads the doc/auth timestamps deterministically
-- across a 90-day window ending today, preserving the
-- (auth_requested_date → auth_decision_date) interval so SLA
-- compliance % stays the same per payer + urgency.
--
-- Call data: mv_call_scores is a VIEW over the materialized view
-- gold_call_scores (Lakeflow-managed, owned by a different demo).
-- We add a synthetic_call_ts column to call_ops_supplemental and
-- modify genie_call_ops_daily to date by that column instead.
--
-- Idempotent: WHERE clauses guard against double-shifts.
-- ============================================================

-- ------------------------------------------------------------
-- 1. raw.clinical_document — spread ingestion_timestamp
--    Targets: only rows still on the original 2026-03-25 batch.
-- ------------------------------------------------------------
UPDATE serverless_stable_swv01_catalog.raw.clinical_document
SET ingestion_timestamp =
  CAST(date_sub(CURRENT_DATE(), CAST(abs(hash(doc_id)) % 90 AS INT)) AS TIMESTAMP)
  + make_interval(0, 0, 0, 0,
      CAST(abs(hash(concat(doc_id, 'h'))) % 24 AS INT),
      CAST(abs(hash(concat(doc_id, 'm'))) % 60 AS INT), 0),
    received_timestamp =
  CAST(date_sub(CURRENT_DATE(), CAST(abs(hash(doc_id)) % 90 AS INT)) AS TIMESTAMP)
  + make_interval(0, 0, 0, 0,
      CAST(abs(hash(concat(doc_id, 'h'))) % 24 AS INT),
      CAST(abs(hash(concat(doc_id, 'm'))) % 60 AS INT), 0)
WHERE DATE(ingestion_timestamp) = DATE('2026-03-25');

-- ------------------------------------------------------------
-- 2. pipeline_prd.clinical_doc_parsed.ingest_ts — cascade
-- ------------------------------------------------------------
MERGE INTO serverless_stable_swv01_catalog.pipeline_prd.clinical_doc_parsed t
USING serverless_stable_swv01_catalog.raw.clinical_document s ON s.doc_id = t.doc_id
WHEN MATCHED AND DATE(t.ingest_ts) = DATE('2026-03-25')
  THEN UPDATE SET t.ingest_ts = s.ingestion_timestamp + INTERVAL 5 MINUTES;

-- ------------------------------------------------------------
-- 3. pipeline_prd.clinical_doc_structured.extraction_ts — cascade
-- ------------------------------------------------------------
MERGE INTO serverless_stable_swv01_catalog.pipeline_prd.clinical_doc_structured t
USING serverless_stable_swv01_catalog.raw.clinical_document s ON s.doc_id = t.doc_id
WHEN MATCHED AND DATE(t.extraction_ts) = DATE('2026-03-25')
  THEN UPDATE SET t.extraction_ts = s.ingestion_timestamp + INTERVAL 15 MINUTES;

-- ------------------------------------------------------------
-- 4. pipeline_prd.doc_member_match_candidates.pipeline_run_ts — cascade
-- ------------------------------------------------------------
MERGE INTO serverless_stable_swv01_catalog.pipeline_prd.doc_member_match_candidates t
USING serverless_stable_swv01_catalog.raw.clinical_document s ON s.doc_id = t.doc_id
WHEN MATCHED AND DATE(t.pipeline_run_ts) = DATE('2026-03-25')
  THEN UPDATE SET t.pipeline_run_ts = s.ingestion_timestamp + INTERVAL 30 MINUTES;

-- ------------------------------------------------------------
-- 5. pipeline_prd.doc_auth_match_candidates.pipeline_run_ts — cascade
-- ------------------------------------------------------------
MERGE INTO serverless_stable_swv01_catalog.pipeline_prd.doc_auth_match_candidates t
USING serverless_stable_swv01_catalog.raw.clinical_document s ON s.doc_id = t.doc_id
WHEN MATCHED AND DATE(t.pipeline_run_ts) = DATE('2026-03-25')
  THEN UPDATE SET t.pipeline_run_ts = s.ingestion_timestamp + INTERVAL 30 MINUTES;

-- ------------------------------------------------------------
-- 6. raw.authorization — shift both auth_requested + auth_decision
--    to land within the last 90 days, preserving days_to_decision.
--    Only rows that already have a decision (NULL = Pending; leave alone).
-- ------------------------------------------------------------
UPDATE serverless_stable_swv01_catalog.raw.authorization
SET
  auth_decision_date  = date_sub(CURRENT_DATE(), CAST(abs(hash(auth_id)) % 90 AS INT)),
  auth_requested_date = date_sub(
                          date_sub(CURRENT_DATE(), CAST(abs(hash(auth_id)) % 90 AS INT)),
                          GREATEST(0, DATEDIFF(auth_decision_date, auth_requested_date))
                        )
WHERE auth_decision_date IS NOT NULL
  AND auth_decision_date < date_sub(CURRENT_DATE(), 89);

-- ------------------------------------------------------------
-- 7. call_ops_supplemental — add synthetic_call_ts spread over 30 days
-- ------------------------------------------------------------
ALTER TABLE serverless_stable_swv01_catalog.transcript_intel_sdp.call_ops_supplemental
  ADD COLUMNS (synthetic_call_ts TIMESTAMP COMMENT 'Synthetic call timestamp spread over the last 30 days — overrides mv_call_scores.created_ts in genie_call_ops_daily');

UPDATE serverless_stable_swv01_catalog.transcript_intel_sdp.call_ops_supplemental
SET synthetic_call_ts =
  CAST(date_sub(CURRENT_DATE(), CAST(abs(hash(call_id)) % 30 AS INT)) AS TIMESTAMP)
  + make_interval(0, 0, 0, 0,
      CAST(abs(hash(concat(call_id, 'h'))) % 24 AS INT),
      CAST(abs(hash(concat(call_id, 'm'))) % 60 AS INT), 0)
WHERE synthetic_call_ts IS NULL;

-- ------------------------------------------------------------
-- 8. Replace genie_call_ops_daily to use synthetic_call_ts
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW serverless_stable_swv01_catalog.genie_availity_ops.genie_call_ops_daily (
  ops_date              COMMENT 'Day the call occurred (synthetic spread over last 30 days)',
  agency_name           COMMENT 'Outsourced agency that handled the call',
  call_type             COMMENT 'Call type taxonomy',
  total_calls           COMMENT 'Total calls handled that day for this agency + type',
  avg_wait_seconds      COMMENT 'Average queue wait time in seconds — ASA',
  avg_handle_seconds    COMMENT 'Average active call handle time — AHT',
  fcr_resolved_count    COMMENT 'Count of calls resolved on first contact (no callback within 7 days)',
  fcr_pct               COMMENT 'First Call Resolution percentage',
  abandoned_proxy_count COMMENT 'Calls where wait_seconds exceeded 600 (10 min) — proxy for abandonment risk'
) COMMENT 'Per-day per-agency call operations timings — FCR, AHT, ASA. Dates come from call_ops_supplemental.synthetic_call_ts (spread over the last 30 days). No PHI/PII.'
AS
SELECT
  DATE(o.synthetic_call_ts)                                       AS ops_date,
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
GROUP BY DATE(o.synthetic_call_ts), c.agency_name, c.call_type;
