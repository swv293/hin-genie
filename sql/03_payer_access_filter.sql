-- ============================================================
-- Clinical Document Intelligence — Multi-Payer Row-Level Security
-- ============================================================
-- Implements the multi-payer access pattern described in the
-- README and the LinkedIn article: a single mapping table +
-- a UC row-filter function applied to the Genie views.
--
--   payer_access_mapping  (user_email, payer_id)
--   payer_access_filter() — row-filter function
--   ALTER VIEW ... SET ROW FILTER for the multi-payer-aware views
--
-- Schema: serverless_stable_swv01_catalog.genie_availity_ops
-- ============================================================

-- ------------------------------------------------------------
-- 1. Mapping table — which user can see which payers.
--    Adding a payer = inserting rows here. No view duplication.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS serverless_stable_swv01_catalog.genie_availity_ops.payer_access_mapping (
  user_email STRING NOT NULL COMMENT 'User principal — email or service-principal application ID',
  payer_id   STRING NOT NULL COMMENT 'Payer the user is authorized to see — e.g., PAYER_A, PAYER_B',
  granted_at TIMESTAMP COMMENT 'When this grant was created',
  granted_by STRING COMMENT 'Who created this grant'
)
COMMENT 'Multi-payer access control list. One row per (user, payer) grant. Drives payer_access_filter().';

-- Seed a sentinel super-user grant for demo purposes — grants the demo
-- owner access to every payer in ref.payer_dim. Replace with real grants
-- in production, or remove and rely on application-level provisioning.
INSERT INTO serverless_stable_swv01_catalog.genie_availity_ops.payer_access_mapping (user_email, payer_id, granted_at, granted_by)
SELECT 'swami.venkatesh@databricks.com', payer_code, current_timestamp(), 'bootstrap'
FROM serverless_stable_swv01_catalog.ref.payer_dim
WHERE NOT EXISTS (
  SELECT 1 FROM serverless_stable_swv01_catalog.genie_availity_ops.payer_access_mapping m
  WHERE m.user_email = 'swami.venkatesh@databricks.com' AND m.payer_id = ref.payer_dim.payer_code
);

-- ------------------------------------------------------------
-- 2. Row-filter function.
--    Returns TRUE when the current user has a grant for this row's payer_id,
--    OR when the row has no payer_id (NULL passthrough — never blocks).
--    The function is referenced by ALTER ... SET ROW FILTER below.
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION serverless_stable_swv01_catalog.genie_availity_ops.payer_access_filter(payer_id STRING)
RETURNS BOOLEAN
COMMENT 'Row filter: TRUE if current_user() has an entry in payer_access_mapping for the row''s payer_id, or if payer_id is NULL.'
RETURN
  payer_id IS NULL
  OR EXISTS (
    SELECT 1
    FROM serverless_stable_swv01_catalog.genie_availity_ops.payer_access_mapping m
    WHERE m.user_email = current_user()
      AND m.payer_id = payer_access_filter.payer_id
  );

-- ------------------------------------------------------------
-- 3. Apply the row filter to the Genie views that carry a payer_id.
--    NOTE: views in 01_create_genie_views.sql do not currently project
--    a payer_id column — extend them to include payer_id from the
--    underlying source tables before uncommenting these statements.
--    Until then, this file documents the pattern and the function is
--    available for use on any future payer-scoped table.
-- ------------------------------------------------------------
-- ALTER VIEW serverless_stable_swv01_catalog.genie_availity_ops.genie_doc_intake_daily   SET ROW FILTER serverless_stable_swv01_catalog.genie_availity_ops.payer_access_filter ON (payer_id);
-- ALTER VIEW serverless_stable_swv01_catalog.genie_availity_ops.genie_doc_match_detail   SET ROW FILTER serverless_stable_swv01_catalog.genie_availity_ops.payer_access_filter ON (payer_id);
-- (etc for the remaining views once payer_id is projected)
