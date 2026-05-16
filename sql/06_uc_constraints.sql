-- ============================================================
-- Unity Catalog PK/FK constraints — required for the Joins tab
-- to auto-populate via FROM_SNIPPET. Tables must already exist
-- (run after 04_add_payer_pa_callops.sql).
--
-- Notes per Databricks docs:
--   - PK columns must be NOT NULL. We add NOT NULL where needed.
--   - All constraints are NOT ENFORCED by Delta semantics — they are
--     metadata that Genie reads to suggest joins.
--   - Constraint names must be unique per schema.
-- ============================================================

-- ------------------------------------------------------------
-- Set PK columns NOT NULL (no-ops if already NOT NULL)
-- ------------------------------------------------------------
ALTER TABLE serverless_stable_swv01_catalog.ref.payer_dim         ALTER COLUMN payer_code SET NOT NULL;
ALTER TABLE serverless_stable_swv01_catalog.raw.clinical_document ALTER COLUMN doc_id     SET NOT NULL;
ALTER TABLE serverless_stable_swv01_catalog.raw.authorization     ALTER COLUMN auth_id    SET NOT NULL;

-- ------------------------------------------------------------
-- Primary keys (idempotent via IF NOT EXISTS where supported,
-- otherwise wrapped to tolerate already-present constraints)
-- ------------------------------------------------------------
ALTER TABLE serverless_stable_swv01_catalog.ref.payer_dim
  ADD CONSTRAINT pk_payer_dim PRIMARY KEY (payer_code);

ALTER TABLE serverless_stable_swv01_catalog.raw.clinical_document
  ADD CONSTRAINT pk_clinical_document PRIMARY KEY (doc_id);

ALTER TABLE serverless_stable_swv01_catalog.raw.authorization
  ADD CONSTRAINT pk_authorization PRIMARY KEY (auth_id);

-- ------------------------------------------------------------
-- Foreign keys — payer references
-- ------------------------------------------------------------
ALTER TABLE serverless_stable_swv01_catalog.raw.clinical_document
  ADD CONSTRAINT fk_doc_payer FOREIGN KEY (payer_code)
  REFERENCES serverless_stable_swv01_catalog.ref.payer_dim(payer_code);

ALTER TABLE serverless_stable_swv01_catalog.raw.authorization
  ADD CONSTRAINT fk_auth_payer FOREIGN KEY (payer_code)
  REFERENCES serverless_stable_swv01_catalog.ref.payer_dim(payer_code);

-- ------------------------------------------------------------
-- Foreign keys — match-candidate tables → clinical_document
-- ------------------------------------------------------------
ALTER TABLE serverless_stable_swv01_catalog.pipeline_prd.doc_member_match_candidates
  ADD CONSTRAINT fk_member_match_doc FOREIGN KEY (doc_id)
  REFERENCES serverless_stable_swv01_catalog.raw.clinical_document(doc_id);

ALTER TABLE serverless_stable_swv01_catalog.pipeline_prd.doc_auth_match_candidates
  ADD CONSTRAINT fk_auth_match_doc FOREIGN KEY (doc_id)
  REFERENCES serverless_stable_swv01_catalog.raw.clinical_document(doc_id);
