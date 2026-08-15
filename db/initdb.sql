-- =============================================================================
-- LeadWorkers — Database Initialization Script
-- Version: 1.0.0
-- =============================================================================
--
-- HOW TO RUN:
--   Option A (CLI, recommended):
--     mysql -h <host> -u <user> -p < db/initdb.sql
--
--   Option B (GUI tools — DBeaver, TablePlus, MySQL Workbench):
--     Run db/create_db.sql first, then run db/initdb.sql
--     with the leads_automation database selected.
--
--   Option C (pipe directly into the DB, skip CREATE DATABASE):
--     mysql -h <host> -u <user> -p leads_automation < db/initdb.sql
--
-- Safe to re-run: all statements use IF NOT EXISTS / INSERT IGNORE.
-- New schema changes go at the bottom under MIGRATIONS — never edit above.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS leads_automation
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE leads_automation;

-- =============================================================================
-- TABLE: customers
-- Canonical record for a known person.
-- Unique by email OR phone — either alone is enough to identify someone.
-- =============================================================================
CREATE TABLE IF NOT EXISTS customers (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  email      VARCHAR(255)    NULL,
  phone      VARCHAR(50)     NULL,
  name       VARCHAR(255)    NULL,
  created_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_customers_email (email),
  UNIQUE KEY uq_customers_phone (phone)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- TABLE: anonymous_profiles
-- One row per (anonymous_id, source) pair.
-- customer_id is NULL while the visitor is anonymous.
-- identified_at is set exactly ONCE on first identity link —
-- this is the backfill gate that prevents double-scoring.
-- =============================================================================
CREATE TABLE IF NOT EXISTS anonymous_profiles (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  anonymous_id  VARCHAR(255)    NOT NULL,
  customer_id   BIGINT UNSIGNED NULL,
  source        VARCHAR(100)    NOT NULL,
  first_seen_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  identified_at DATETIME        NULL,
  created_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_anon_profiles_anon_source (anonymous_id, source),
  KEY idx_anon_profiles_customer (customer_id),
  CONSTRAINT fk_anon_profiles_customer
    FOREIGN KEY (customer_id) REFERENCES customers (id)
    ON UPDATE CASCADE ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- TABLE: customer_scores
-- Running score total — one row per (customer, source).
-- Updated in-place on each qualifying event.
-- =============================================================================
CREATE TABLE IF NOT EXISTS customer_scores (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  customer_id BIGINT UNSIGNED NOT NULL,
  source      VARCHAR(100)    NOT NULL,
  score       INT             NOT NULL DEFAULT 0,
  created_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_customer_scores_customer_source (customer_id, source),
  CONSTRAINT fk_customer_scores_customer
    FOREIGN KEY (customer_id) REFERENCES customers (id)
    ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- TABLE: score_events
-- Immutable audit log of every point change.
-- event_id is the ClickHouse rev_events.event_id for traceability.
-- is_backfill = 1 marks the one-time historical catch-up row.
-- =============================================================================
CREATE TABLE IF NOT EXISTS score_events (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  customer_id  BIGINT UNSIGNED NOT NULL,
  anonymous_id VARCHAR(255)    NOT NULL,
  source       VARCHAR(100)    NOT NULL,
  event_name   VARCHAR(255)    NOT NULL,
  event_id     VARCHAR(255)    NULL,
  score_delta  INT             NOT NULL,
  is_backfill  TINYINT(1)      NOT NULL DEFAULT 0,
  event_time   DATETIME        NOT NULL,
  created_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_score_events_customer (customer_id),
  KEY idx_score_events_anon     (anonymous_id),
  KEY idx_score_events_event_id (event_id),
  CONSTRAINT fk_score_events_customer
    FOREIGN KEY (customer_id) REFERENCES customers (id)
    ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- TABLE: source_event_scores
-- Config table — maps (source, event_name) to point value.
-- Nothing is hardcoded in application code; all lookups read this table.
-- is_active = 0 disables scoring for an event without losing history.
-- =============================================================================
CREATE TABLE IF NOT EXISTS source_event_scores (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  source     VARCHAR(100)    NOT NULL,
  event_name VARCHAR(255)    NOT NULL,
  score      INT             NOT NULL DEFAULT 0,
  is_active  TINYINT(1)      NOT NULL DEFAULT 1,
  created_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_source_event_scores_source_event (source, event_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- TABLE: identity_merges
-- Audit trail for every time an anonymous_id gets linked or re-linked
-- to a customer, including cross-source merges.
-- =============================================================================
CREATE TABLE IF NOT EXISTS identity_merges (
  id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  primary_customer_id BIGINT UNSIGNED NOT NULL,
  anonymous_id        VARCHAR(255)    NOT NULL,
  source              VARCHAR(100)    NOT NULL,
  reason              VARCHAR(255)    NOT NULL,
  created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_identity_merges_customer (primary_customer_id),
  KEY idx_identity_merges_anon     (anonymous_id),
  CONSTRAINT fk_identity_merges_customer
    FOREIGN KEY (primary_customer_id) REFERENCES customers (id)
    ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- SEED DATA — source_event_scores
-- =============================================================================
INSERT IGNORE INTO source_event_scores (source, event_name, score) VALUES
  -- Passive / tracking events
  ('website', 'page_view',               1),
  ('website', 'product_view',            3),
  ('website', 'login',                  10),

  -- Form submissions (also trigger identity resolution)
  ('website', 'contact_form',           15),
  ('website', 'test_ride_booking',      20),
  ('website', 'book_bike',              30),

  -- High-intent signals — instant Hottest flag
  ('website', 'booking_page_viewed',    22),
  ('website', 'test_ride_page_viewed',  18),
  ('website', 'emi_calculator_used',    20),
  ('website', 'savings_calculator_used',12),

  -- Warm-to-hot promoters
  ('website', 'dealer_locator_opened',  15),
  ('website', 'return_visit',           15),
  ('website', 'product_page_3rd_view',  15),

  -- Content signals — trigger targeted email sequences
  ('website', 'financing_page_view',     5),
  ('website', 'warranty_page_view',      5),

  -- Re-engagement signal
  ('website', 'renewed_site_activity',  10);

-- =============================================================================
-- MIGRATIONS
-- =============================================================================
-- Add new statements here. Never modify the sections above.
-- Format:
--   -- [MIGRATION v1.x.x] Short description  YYYY-MM-DD
--   ALTER TABLE ...;
-- =============================================================================

-- (no migrations yet — v1.0.0 is the initial schema)
