-- ============================================================
-- Clinical Document Intelligence — Multi-Payer + PA Decision + Call-Ops Extension
-- ============================================================
-- Schema changes that make the multi-payer claim operational and surface
-- two new HIN-critical question categories:
--   1. Prior-authorization decision metrics (TAT, SLA compliance, approval rate)
--   2. Call-operations timings (FCR, AHT, ASA — distinct from QA scoring)
--
-- Strategy:
--   - Create ref.payer_dim with 5 demo payers (Aetna, UHC, BCBS, Cigna, Humana).
--   - Add payer_code to raw.clinical_document, raw.authorization, and the
--     pipeline_prd Delta tables. Backfill with a deterministic hash so the
--     same doc lands on the same payer every run.
--   - Add urgency to raw.authorization (urgent / standard).
--   - Create a supplementary transcript_intel_sdp.call_ops_supplemental table
--     for wait_seconds / handle_seconds / fcr_flag, joined to call_id.
--     (Source mv_call_scores is a materialized view; we don't ALTER it.)
--
-- Idempotent: safe to re-run. CREATE IF NOT EXISTS + UPDATEs filtered to NULL.
-- ============================================================

-- ------------------------------------------------------------
-- 1. Payer dimension — fictional payers only, no real-world company names
-- ------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS serverless_stable_swv01_catalog.ref;

CREATE TABLE IF NOT EXISTS serverless_stable_swv01_catalog.ref.payer_dim (
  payer_code  STRING NOT NULL COMMENT 'Short payer identifier — e.g., NORTHWAVE, METRIDIAN, BLUEHARBOR, VERITAS, SILVERPEAK',
  payer_name  STRING NOT NULL COMMENT 'Marketing name for the payer',
  payer_type  STRING          COMMENT 'commercial / medicare_advantage / medicaid / medigap',
  active      BOOLEAN         COMMENT 'Whether the payer is currently in the network',
  created_at  TIMESTAMP       COMMENT 'When this payer was added to the dimension'
)
COMMENT 'Payer dimension. Five fictional demo payers seeded — extend by inserting more rows.';

INSERT INTO serverless_stable_swv01_catalog.ref.payer_dim (payer_code, payer_name, payer_type, active, created_at)
SELECT p.* FROM (VALUES
  ('NORTHWAVE',  'Northwave Health',         'commercial',         true, current_timestamp()),
  ('METRIDIAN',  'Metridian',                'commercial',         true, current_timestamp()),
  ('BLUEHARBOR', 'BlueHarbor',               'commercial',         true, current_timestamp()),
  ('VERITAS',    'Veritas',                  'commercial',         true, current_timestamp()),
  ('SILVERPEAK', 'SilverPeak Senior Health', 'medicare_advantage', true, current_timestamp())
) p(payer_code, payer_name, payer_type, active, created_at)
WHERE NOT EXISTS (
  SELECT 1 FROM serverless_stable_swv01_catalog.ref.payer_dim d WHERE d.payer_code = p.payer_code
);

-- ------------------------------------------------------------
-- 2. Add payer_code to source + pipeline tables
-- ------------------------------------------------------------
ALTER TABLE serverless_stable_swv01_catalog.raw.clinical_document       ADD COLUMNS IF NOT EXISTS (payer_code STRING COMMENT 'Payer this document belongs to — FK to ref.payer_dim');
ALTER TABLE serverless_stable_swv01_catalog.raw.authorization           ADD COLUMNS IF NOT EXISTS (payer_code STRING COMMENT 'Payer that owns this authorization — FK to ref.payer_dim');
ALTER TABLE serverless_stable_swv01_catalog.raw.authorization           ADD COLUMNS IF NOT EXISTS (urgency    STRING COMMENT 'urgent (72-hr CMS SLA) or standard (7-day CMS SLA)');
ALTER TABLE serverless_stable_swv01_catalog.pipeline_prd.clinical_doc_parsed            ADD COLUMNS IF NOT EXISTS (payer_code STRING COMMENT 'Payer propagated from raw.clinical_document');
ALTER TABLE serverless_stable_swv01_catalog.pipeline_prd.clinical_doc_structured        ADD COLUMNS IF NOT EXISTS (payer_code STRING COMMENT 'Payer propagated from raw.clinical_document');
ALTER TABLE serverless_stable_swv01_catalog.pipeline_prd.doc_member_match_candidates    ADD COLUMNS IF NOT EXISTS (payer_code STRING COMMENT 'Payer propagated from raw.clinical_document');
ALTER TABLE serverless_stable_swv01_catalog.pipeline_prd.doc_auth_match_candidates      ADD COLUMNS IF NOT EXISTS (payer_code STRING COMMENT 'Payer propagated from raw.clinical_document');

-- ------------------------------------------------------------
-- 3. Deterministic backfill — same doc → same payer every run
-- ------------------------------------------------------------
-- Hash doc_id into a five-element bucket and map to payer_code.
UPDATE serverless_stable_swv01_catalog.raw.clinical_document
SET payer_code = element_at(array('NORTHWAVE','METRIDIAN','BLUEHARBOR','VERITAS','SILVERPEAK'), (abs(hash(doc_id)) % 5) + 1)
WHERE payer_code IS NULL;

UPDATE serverless_stable_swv01_catalog.raw.authorization
SET payer_code = element_at(array('NORTHWAVE','METRIDIAN','BLUEHARBOR','VERITAS','SILVERPEAK'), (abs(hash(auth_id)) % 5) + 1)
WHERE payer_code IS NULL;

-- Roughly 30% urgent / 70% standard — matches industry mix for PA volume.
UPDATE serverless_stable_swv01_catalog.raw.authorization
SET urgency = CASE WHEN (abs(hash(auth_id)) % 10) < 3 THEN 'urgent' ELSE 'standard' END
WHERE urgency IS NULL;

-- Cascade payer_code into the pipeline tables via MERGE (correlated UPDATE not supported).
MERGE INTO serverless_stable_swv01_catalog.pipeline_prd.clinical_doc_parsed t
USING serverless_stable_swv01_catalog.raw.clinical_document s ON s.doc_id = t.doc_id
WHEN MATCHED AND t.payer_code IS NULL THEN UPDATE SET t.payer_code = s.payer_code;

MERGE INTO serverless_stable_swv01_catalog.pipeline_prd.clinical_doc_structured t
USING serverless_stable_swv01_catalog.raw.clinical_document s ON s.doc_id = t.doc_id
WHEN MATCHED AND t.payer_code IS NULL THEN UPDATE SET t.payer_code = s.payer_code;

MERGE INTO serverless_stable_swv01_catalog.pipeline_prd.doc_member_match_candidates t
USING serverless_stable_swv01_catalog.raw.clinical_document s ON s.doc_id = t.doc_id
WHEN MATCHED AND t.payer_code IS NULL THEN UPDATE SET t.payer_code = s.payer_code;

MERGE INTO serverless_stable_swv01_catalog.pipeline_prd.doc_auth_match_candidates t
USING serverless_stable_swv01_catalog.raw.clinical_document s ON s.doc_id = t.doc_id
WHEN MATCHED AND t.payer_code IS NULL THEN UPDATE SET t.payer_code = s.payer_code;

-- ------------------------------------------------------------
-- 4. Call-operations supplemental table (joins to mv_call_scores.call_id)
--    Distinct from QA scoring: tracks queue wait, handle time, FCR.
--    Created as a Delta table because mv_call_scores is a materialized view
--    and cannot be ALTERed in-place.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS serverless_stable_swv01_catalog.transcript_intel_sdp.call_ops_supplemental (
  call_id        STRING NOT NULL COMMENT 'FK to transcript_intel_sdp.mv_call_scores.call_id',
  wait_seconds   INT             COMMENT 'Time in queue before agent pickup',
  handle_seconds INT             COMMENT 'Active call duration with the agent',
  fcr_flag       BOOLEAN         COMMENT 'True if the call was resolved without a callback within 7 days',
  generated_at   TIMESTAMP       COMMENT 'When this supplemental row was generated'
)
COMMENT 'Per-call operations timings (wait / handle / FCR) — complements mv_call_scores which carries QA scoring only.';

-- Backfill from mv_call_scores. Wait & handle drawn from deterministic hash so reruns are stable.
-- Healthcare benchmarks (CloudTalk / Five9): avg wait ~4.4 min (264s), avg handle ~12 min (720s), FCR ~70%.
INSERT INTO serverless_stable_swv01_catalog.transcript_intel_sdp.call_ops_supplemental
SELECT
  m.call_id,
  CAST(60 + abs(hash(concat(m.call_id, 'wait')))   % 540 AS INT) AS wait_seconds,
  CAST(180 + abs(hash(concat(m.call_id, 'handle')))% 1620 AS INT) AS handle_seconds,
  (abs(hash(concat(m.call_id, 'fcr'))) % 100) < 70 AS fcr_flag,
  current_timestamp() AS generated_at
FROM serverless_stable_swv01_catalog.transcript_intel_sdp.mv_call_scores m
WHERE NOT EXISTS (
  SELECT 1 FROM serverless_stable_swv01_catalog.transcript_intel_sdp.call_ops_supplemental s
  WHERE s.call_id = m.call_id
);
