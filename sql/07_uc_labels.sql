-- ============================================================
-- Column TAGS / synonyms — helps Genie disambiguate user terms
-- by attaching alternate names to columns. Genie surfaces these
-- when interpreting prompts.
--
-- Reference: ALTER TABLE ... ALTER COLUMN ... SET TAGS (...)
-- The synonym list is keyed by 'synonyms' tag (whitespace-separated)
-- per Databricks' Genie metadata convention.
-- ============================================================

-- raw.clinical_document
ALTER TABLE serverless_stable_swv01_catalog.raw.clinical_document
  ALTER COLUMN payer_code     SET TAGS ('synonyms' = 'payer carrier insurer plan');
ALTER TABLE serverless_stable_swv01_catalog.raw.clinical_document
  ALTER COLUMN source_channel SET TAGS ('synonyms' = 'channel intake_channel how_received submission_method');
ALTER TABLE serverless_stable_swv01_catalog.raw.clinical_document
  ALTER COLUMN document_type  SET TAGS ('synonyms' = 'doc_type document type form_type doc_category');
ALTER TABLE serverless_stable_swv01_catalog.raw.clinical_document
  ALTER COLUMN is_readable    SET TAGS ('synonyms' = 'readable parseable ocr_ok');

-- raw.authorization
ALTER TABLE serverless_stable_swv01_catalog.raw.authorization
  ALTER COLUMN urgency        SET TAGS ('synonyms' = 'priority urgent_flag pa_priority decision_priority');
ALTER TABLE serverless_stable_swv01_catalog.raw.authorization
  ALTER COLUMN status         SET TAGS ('synonyms' = 'auth_status decision pa_status determination');
ALTER TABLE serverless_stable_swv01_catalog.raw.authorization
  ALTER COLUMN payer_code     SET TAGS ('synonyms' = 'payer carrier insurer plan');

-- transcript_intel_sdp.gold_call_summaries_sentiment
ALTER TABLE serverless_stable_swv01_catalog.transcript_intel_sdp.gold_call_summaries_sentiment
  ALTER COLUMN sentiment_trajectory SET TAGS ('synonyms' = 'trajectory sentiment_arc emotional_arc trend_in_call');
ALTER TABLE serverless_stable_swv01_catalog.transcript_intel_sdp.gold_call_summaries_sentiment
  ALTER COLUMN sentiment_overall    SET TAGS ('synonyms' = 'overall_sentiment tone overall_tone customer_sentiment');

-- transcript_intel_sdp.call_ops_supplemental
ALTER TABLE serverless_stable_swv01_catalog.transcript_intel_sdp.call_ops_supplemental
  ALTER COLUMN wait_seconds   SET TAGS ('synonyms' = 'asa average_speed_to_answer queue_wait wait_time');
ALTER TABLE serverless_stable_swv01_catalog.transcript_intel_sdp.call_ops_supplemental
  ALTER COLUMN handle_seconds SET TAGS ('synonyms' = 'aht average_handle_time talk_time call_duration handle_time');
ALTER TABLE serverless_stable_swv01_catalog.transcript_intel_sdp.call_ops_supplemental
  ALTER COLUMN fcr_flag       SET TAGS ('synonyms' = 'fcr first_call_resolution resolved_on_first_contact');
