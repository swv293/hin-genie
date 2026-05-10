-- ============================================================
-- Clinical Document Intelligence — Source Table Column Comments
-- ============================================================
-- Adds column-level comments to the pipeline_prd source tables.
-- The Genie views in 01_create_genie_views.sql already carry rich
-- column COMMENTs; this file propagates the same context to the
-- underlying tables so lineage, downstream tooling, and any future
-- view that joins these tables benefits from the same metadata.
--
-- Schema: serverless_stable_swv01_catalog.pipeline_prd
-- ============================================================

USE CATALOG serverless_stable_swv01_catalog;

-- ------------------------------------------------------------
-- clinical_doc_parsed
-- ------------------------------------------------------------
ALTER TABLE pipeline_prd.clinical_doc_parsed ALTER COLUMN doc_id              COMMENT 'UUID document identifier (FK to raw.clinical_document)';
ALTER TABLE pipeline_prd.clinical_doc_parsed ALTER COLUMN unreadable_flag     COMMENT 'True if OCR could not parse the document';
ALTER TABLE pipeline_prd.clinical_doc_parsed ALTER COLUMN page_count_detected COMMENT 'Pages detected during OCR (independent of original metadata)';
ALTER TABLE pipeline_prd.clinical_doc_parsed ALTER COLUMN parse_error_status  COMMENT 'OCR error category — null on success';
ALTER TABLE pipeline_prd.clinical_doc_parsed ALTER COLUMN ingest_ts           COMMENT 'When document entered the parsing stage';

-- ------------------------------------------------------------
-- clinical_doc_structured
-- ------------------------------------------------------------
ALTER TABLE pipeline_prd.clinical_doc_structured ALTER COLUMN doc_id            COMMENT 'UUID document identifier (FK to raw.clinical_document)';
ALTER TABLE pipeline_prd.clinical_doc_structured ALTER COLUMN member_id_on_form COMMENT 'Member ID extracted from the document, if any';
ALTER TABLE pipeline_prd.clinical_doc_structured ALTER COLUMN auth_id           COMMENT 'Authorization ID extracted from the document, if any';
ALTER TABLE pipeline_prd.clinical_doc_structured ALTER COLUMN dob_extracted     COMMENT 'Date of birth as extracted from form (string, may be null/garbled)';
ALTER TABLE pipeline_prd.clinical_doc_structured ALTER COLUMN ssn4_extracted    COMMENT 'Last four of SSN as extracted from form (string, may be null)';
ALTER TABLE pipeline_prd.clinical_doc_structured ALTER COLUMN missing_dob       COMMENT 'True if DOB anchor not extractable — used in risk tiering';
ALTER TABLE pipeline_prd.clinical_doc_structured ALTER COLUMN missing_ssn4      COMMENT 'True if SSN4 anchor not extractable — used in risk tiering';
ALTER TABLE pipeline_prd.clinical_doc_structured ALTER COLUMN extraction_ts     COMMENT 'When the structured extraction completed';

-- ------------------------------------------------------------
-- doc_member_match_candidates
-- ------------------------------------------------------------
ALTER TABLE pipeline_prd.doc_member_match_candidates ALTER COLUMN doc_id              COMMENT 'UUID document identifier';
ALTER TABLE pipeline_prd.doc_member_match_candidates ALTER COLUMN candidate_member_id COMMENT 'Member ID being scored against this document';
ALTER TABLE pipeline_prd.doc_member_match_candidates ALTER COLUMN name_weight         COMMENT 'Fellegi-Sunter weight contribution from name comparison';
ALTER TABLE pipeline_prd.doc_member_match_candidates ALTER COLUMN dob_weight          COMMENT 'Fellegi-Sunter weight contribution from DOB comparison';
ALTER TABLE pipeline_prd.doc_member_match_candidates ALTER COLUMN ssn4_weight         COMMENT 'Fellegi-Sunter weight contribution from SSN-last-4 comparison';
ALTER TABLE pipeline_prd.doc_member_match_candidates ALTER COLUMN total_weight        COMMENT 'Sum of field weights — final Fellegi-Sunter score';
ALTER TABLE pipeline_prd.doc_member_match_candidates ALTER COLUMN match_class         COMMENT 'Classification: match / possible_match / non_match';
ALTER TABLE pipeline_prd.doc_member_match_candidates ALTER COLUMN pipeline_run_ts     COMMENT 'When the matching pipeline produced this candidate';

-- ------------------------------------------------------------
-- doc_auth_match_candidates
-- ------------------------------------------------------------
ALTER TABLE pipeline_prd.doc_auth_match_candidates ALTER COLUMN doc_id              COMMENT 'UUID document identifier';
ALTER TABLE pipeline_prd.doc_auth_match_candidates ALTER COLUMN candidate_auth_id   COMMENT 'Authorization ID being scored against this document';
ALTER TABLE pipeline_prd.doc_auth_match_candidates ALTER COLUMN auth_id_weight      COMMENT 'Fellegi-Sunter weight contribution from auth ID comparison';
ALTER TABLE pipeline_prd.doc_auth_match_candidates ALTER COLUMN service_date_weight COMMENT 'Fellegi-Sunter weight contribution from service-date comparison';
ALTER TABLE pipeline_prd.doc_auth_match_candidates ALTER COLUMN total_weight        COMMENT 'Sum of field weights — final Fellegi-Sunter score';
ALTER TABLE pipeline_prd.doc_auth_match_candidates ALTER COLUMN match_class         COMMENT 'Classification: match / possible_match / non_match';
ALTER TABLE pipeline_prd.doc_auth_match_candidates ALTER COLUMN pipeline_run_ts     COMMENT 'When the matching pipeline produced this candidate';

-- ------------------------------------------------------------
-- dq_run_results
-- ------------------------------------------------------------
ALTER TABLE pipeline_prd.dq_run_results ALTER COLUMN batch_id           COMMENT 'Batch identifier (timestamp-based)';
ALTER TABLE pipeline_prd.dq_run_results ALTER COLUMN run_ts             COMMENT 'When the data-quality run executed';
ALTER TABLE pipeline_prd.dq_run_results ALTER COLUMN total_docs         COMMENT 'Total documents evaluated in this batch';
ALTER TABLE pipeline_prd.dq_run_results ALTER COLUMN pct_unreadable     COMMENT 'Percent of documents that failed OCR (0–100)';
ALTER TABLE pipeline_prd.dq_run_results ALTER COLUMN pct_missing_dob    COMMENT 'Percent of documents missing DOB anchor';
ALTER TABLE pipeline_prd.dq_run_results ALTER COLUMN pct_missing_ssn4   COMMENT 'Percent of documents missing SSN-last-4 anchor';
ALTER TABLE pipeline_prd.dq_run_results ALTER COLUMN parse_errors       COMMENT 'Count of documents with parse errors in this batch';
ALTER TABLE pipeline_prd.dq_run_results ALTER COLUMN pct_match          COMMENT 'Percent of candidates classified as match';
ALTER TABLE pipeline_prd.dq_run_results ALTER COLUMN pct_possible_match COMMENT 'Percent of candidates classified as possible_match (review queue)';
ALTER TABLE pipeline_prd.dq_run_results ALTER COLUMN pct_non_match      COMMENT 'Percent of candidates classified as non_match';

-- ------------------------------------------------------------
-- match_events
-- ------------------------------------------------------------
ALTER TABLE pipeline_prd.match_events ALTER COLUMN event_id          COMMENT 'Event UUID';
ALTER TABLE pipeline_prd.match_events ALTER COLUMN doc_id            COMMENT 'Document the event refers to';
ALTER TABLE pipeline_prd.match_events ALTER COLUMN event_type        COMMENT 'Type of match event (system, manual_review, override)';
ALTER TABLE pipeline_prd.match_events ALTER COLUMN matched_entity_id COMMENT 'Entity (member or authorization) the document was matched to';
ALTER TABLE pipeline_prd.match_events ALTER COLUMN confidence        COMMENT 'Confidence score for this match decision';
ALTER TABLE pipeline_prd.match_events ALTER COLUMN event_ts          COMMENT 'When the event was recorded';

-- ------------------------------------------------------------
-- pipeline_run_log
-- ------------------------------------------------------------
ALTER TABLE pipeline_prd.pipeline_run_log ALTER COLUMN run_ts COMMENT 'When the pipeline run started';
ALTER TABLE pipeline_prd.pipeline_run_log ALTER COLUMN status COMMENT 'Run status (success, failed, partial)';
ALTER TABLE pipeline_prd.pipeline_run_log ALTER COLUMN run_by COMMENT 'Identity that triggered the run (service principal or user)';

-- ------------------------------------------------------------
-- Table-level comments
-- ------------------------------------------------------------
COMMENT ON TABLE pipeline_prd.clinical_doc_parsed              IS 'OCR parse results for clinical documents — one row per parsed document';
COMMENT ON TABLE pipeline_prd.clinical_doc_structured          IS 'Structured fields extracted from each clinical document — DOB, SSN4, member ID, auth ID';
COMMENT ON TABLE pipeline_prd.doc_member_match_candidates      IS 'Fellegi-Sunter member-match candidate scores — one row per (doc, candidate_member) pair';
COMMENT ON TABLE pipeline_prd.doc_auth_match_candidates        IS 'Fellegi-Sunter authorization-match candidate scores — one row per (doc, candidate_auth) pair';
COMMENT ON TABLE pipeline_prd.dq_run_results                   IS 'Per-batch data-quality summary metrics emitted by the DQ check stage';
COMMENT ON TABLE pipeline_prd.match_events                     IS 'Audit log of match decisions (system, manual_review, override) per document';
COMMENT ON TABLE pipeline_prd.pipeline_run_log                 IS 'Run-level audit trail for the document processing pipeline';
COMMENT ON TABLE pipeline_prd.fellegi_sunter_parameters        IS 'Estimated m/u probabilities per matching field — drives the agreement / disagreement weights';
