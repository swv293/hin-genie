-- ============================================================
-- Clinical Document Intelligence — Schema Setup
-- ============================================================
-- Run this against your Databricks SQL warehouse.
-- Adjust the catalog name if using a different workspace.
-- ============================================================

-- Reference data (member golden master)
CREATE SCHEMA IF NOT EXISTS serverless_stable_swv01_catalog.ref
COMMENT 'Reference/master data. Contains member golden records.';

-- Raw ingested data (clinical documents, authorizations)
CREATE SCHEMA IF NOT EXISTS serverless_stable_swv01_catalog.raw
COMMENT 'Raw ingested data. Clinical documents and authorization records as received from source systems.';

-- Pipeline production tables (parsed, structured, match candidates)
CREATE SCHEMA IF NOT EXISTS serverless_stable_swv01_catalog.pipeline_prd
COMMENT 'Production pipeline streaming tables. OCR parsing, structured extraction, and Fellegi-Sunter match scoring.';

-- Dashboard production views (KPI aggregates)
CREATE SCHEMA IF NOT EXISTS serverless_stable_swv01_catalog.dashboard_prd
COMMENT 'Production dashboard views. Pre-aggregated KPIs for operational dashboards.';

-- Genie Room curated views
CREATE SCHEMA IF NOT EXISTS serverless_stable_swv01_catalog.genie_availity_ops
COMMENT 'Curated views for Clinical Document Intelligence Genie Rooms. Read-only overlays on pipeline and call center data. Safe to drop without affecting source tables.';
