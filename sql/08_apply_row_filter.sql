-- ============================================================
-- Multi-payer row-level security — wired
-- ============================================================
-- Apply the payer_access_filter row filter (defined in 03) on the
-- source tables. Because the filter is on the SOURCE table, it
-- cascades through every view, every metric view, and every Genie
-- conversation built over those tables — no per-view changes needed.
--
-- Prerequisite: rows in serverless_stable_swv01_catalog.genie_availity_ops.payer_access_mapping
-- for each user_email that should see data.
--
-- Per Databricks docs: ALTER TABLE ... SET ROW FILTER works on TABLES
-- only — not views or metric views. The cascade happens automatically.
-- ============================================================

ALTER TABLE serverless_stable_swv01_catalog.raw.clinical_document
  SET ROW FILTER serverless_stable_swv01_catalog.genie_availity_ops.payer_access_filter ON (payer_code);

ALTER TABLE serverless_stable_swv01_catalog.raw.authorization
  SET ROW FILTER serverless_stable_swv01_catalog.genie_availity_ops.payer_access_filter ON (payer_code);

ALTER TABLE serverless_stable_swv01_catalog.pipeline_prd.clinical_doc_parsed
  SET ROW FILTER serverless_stable_swv01_catalog.genie_availity_ops.payer_access_filter ON (payer_code);

ALTER TABLE serverless_stable_swv01_catalog.pipeline_prd.clinical_doc_structured
  SET ROW FILTER serverless_stable_swv01_catalog.genie_availity_ops.payer_access_filter ON (payer_code);

ALTER TABLE serverless_stable_swv01_catalog.pipeline_prd.doc_member_match_candidates
  SET ROW FILTER serverless_stable_swv01_catalog.genie_availity_ops.payer_access_filter ON (payer_code);

ALTER TABLE serverless_stable_swv01_catalog.pipeline_prd.doc_auth_match_candidates
  SET ROW FILTER serverless_stable_swv01_catalog.genie_availity_ops.payer_access_filter ON (payer_code);
