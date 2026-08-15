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

-- [MIGRATION v1.1.0] Score decay rules, segments, instant flags, history  2026-08-16

-- score_decay_rules
-- Defines how score decays based on days of inactivity per source.
-- decay_type: 'percent' (reduce by X%) | 'reset' (set to 0)
-- days_to NULL means "and beyond" (open-ended upper bound)
CREATE TABLE IF NOT EXISTS score_decay_rules (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  source      VARCHAR(100)    NOT NULL,
  days_from   INT             NOT NULL,
  days_to     INT             NULL,
  decay_type  ENUM('percent','reset') NOT NULL,
  decay_value INT             NOT NULL DEFAULT 0,
  min_score   INT             NOT NULL DEFAULT 0,
  is_active   TINYINT(1)      NOT NULL DEFAULT 1,
  created_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_decay_rules_source (source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- segment_rules
-- Maps score range → segment name per source.
-- Segment transitions are one-directional up (never down from score alone).
-- daily_touch_cap is read by the messaging layer for frequency capping.
CREATE TABLE IF NOT EXISTS segment_rules (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  source          VARCHAR(100)    NOT NULL,
  segment_name    VARCHAR(50)     NOT NULL,
  min_score       INT             NOT NULL,
  max_score       INT             NULL,
  daily_touch_cap INT             NOT NULL DEFAULT 0,
  is_active       TINYINT(1)      NOT NULL DEFAULT 1,
  created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_segment_rules_source_segment (source, segment_name),
  KEY idx_segment_rules_source (source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- instant_flag_events
-- Events that immediately force a customer to a target segment,
-- bypassing the score threshold entirely.
CREATE TABLE IF NOT EXISTS instant_flag_events (
  id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  source           VARCHAR(100)    NOT NULL,
  event_name       VARCHAR(255)    NOT NULL,
  target_segment   VARCHAR(50)     NOT NULL,
  is_active        TINYINT(1)      NOT NULL DEFAULT 1,
  created_at       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_instant_flag_source_event (source, event_name),
  KEY idx_instant_flag_source (source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- segment_history
-- Immutable audit log of every segment change per customer.
-- is_paused tracks messaging pause state (e.g. after test ride booked).
-- changed_by: 'event', 'decay', 'instant_flag', 'journey_stop'
ALTER TABLE customer_scores
  ADD COLUMN segment      VARCHAR(50)  NULL     AFTER score,
  ADD COLUMN is_paused    TINYINT(1)   NOT NULL DEFAULT 0 AFTER segment,
  ADD COLUMN paused_at    DATETIME     NULL     AFTER is_paused,
  ADD COLUMN last_seen_at DATETIME     NULL     AFTER paused_at;

CREATE TABLE IF NOT EXISTS segment_history (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  customer_id  BIGINT UNSIGNED NOT NULL,
  source       VARCHAR(100)    NOT NULL,
  from_segment VARCHAR(50)     NULL,
  to_segment   VARCHAR(50)     NOT NULL,
  changed_by   VARCHAR(50)     NOT NULL,
  event_name   VARCHAR(255)    NULL,
  created_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_seg_history_customer (customer_id),
  CONSTRAINT fk_seg_history_customer
    FOREIGN KEY (customer_id) REFERENCES customers (id)
    ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ── Seed: score_decay_rules (website) ──────────────────────────────────────
INSERT IGNORE INTO score_decay_rules (source, days_from, days_to, decay_type, decay_value, min_score) VALUES
  ('website', 0,  20, 'percent',  0, 0),   -- 0-20 days: no decay
  ('website', 21, 45, 'percent', 30, 0),   -- 21-45 days: drop 30%
  ('website', 46, NULL, 'reset',  0, 0);   -- 46+ days: reset to 0

-- ── Seed: segment_rules (website) ──────────────────────────────────────────
INSERT IGNORE INTO segment_rules (source, segment_name, min_score, max_score, daily_touch_cap) VALUES
  ('website', 'WARM',    1,   49, 3),
  ('website', 'HOT',    50,   99, 4),
  ('website', 'HOTTEST',100, NULL, 5);

-- ── Seed: instant_flag_events (website) ────────────────────────────────────
INSERT IGNORE INTO instant_flag_events (source, event_name, target_segment) VALUES
  ('website', 'booking_page_viewed',     'HOTTEST'),
  ('website', 'emi_calculator_used',     'HOTTEST'),
  ('website', 'savings_calculator_used', 'HOTTEST'),
  ('website', 'test_ride_page_viewed',   'HOTTEST');

-- ── Seed: journey_stop event in source_event_scores (0 pts, triggers demotion)
INSERT IGNORE INTO source_event_scores (source, event_name, score) VALUES
  ('website', 'journey_stop', 0);

-- =============================================================================
-- MIGRATIONS
-- =============================================================================
-- Add new statements here. Never modify the sections above.
-- Format:
--   -- [MIGRATION v1.x.x] Short description  YYYY-MM-DD
--   ALTER TABLE ...;
-- =============================================================================

-- [MIGRATION v1.1.0] Score decay, segments, instant flags, segment history  2026-08-16

CREATE TABLE IF NOT EXISTS score_decay_rules (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  source      VARCHAR(100)    NOT NULL,
  days_from   INT             NOT NULL,
  days_to     INT             NULL,
  decay_type  ENUM('percent','reset') NOT NULL,
  decay_value INT             NOT NULL DEFAULT 0,
  min_score   INT             NOT NULL DEFAULT 0,
  is_active   TINYINT(1)      NOT NULL DEFAULT 1,
  created_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_decay_rules_source (source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS segment_rules (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  source          VARCHAR(100)    NOT NULL,
  segment_name    VARCHAR(50)     NOT NULL,
  min_score       INT             NOT NULL,
  max_score       INT             NULL,
  daily_touch_cap INT             NOT NULL DEFAULT 0,
  is_active       TINYINT(1)      NOT NULL DEFAULT 1,
  created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_segment_rules_source_segment (source, segment_name),
  KEY idx_segment_rules_source (source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS instant_flag_events (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  source         VARCHAR(100)    NOT NULL,
  event_name     VARCHAR(255)    NOT NULL,
  target_segment VARCHAR(50)     NOT NULL,
  is_active      TINYINT(1)      NOT NULL DEFAULT 1,
  created_at     DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at     DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_instant_flag_source_event (source, event_name),
  KEY idx_instant_flag_source (source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- NOTE: MySQL 8.0 does not support ADD COLUMN IF NOT EXISTS.
-- These columns are added here for fresh installs only.
-- If running on an existing DB, check if columns exist first.
ALTER TABLE customer_scores
  ADD COLUMN segment      VARCHAR(50)  NULL     AFTER score,
  ADD COLUMN is_paused    TINYINT(1)   NOT NULL DEFAULT 0 AFTER segment,
  ADD COLUMN paused_at    DATETIME     NULL     AFTER is_paused,
  ADD COLUMN last_seen_at DATETIME     NULL     AFTER paused_at;

CREATE TABLE IF NOT EXISTS segment_history (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  customer_id  BIGINT UNSIGNED NOT NULL,
  source       VARCHAR(100)    NOT NULL,
  from_segment VARCHAR(50)     NULL,
  to_segment   VARCHAR(50)     NOT NULL,
  changed_by   VARCHAR(50)     NOT NULL,
  event_name   VARCHAR(255)    NULL,
  created_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_seg_history_customer (customer_id),
  CONSTRAINT fk_seg_history_customer
    FOREIGN KEY (customer_id) REFERENCES customers (id)
    ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO score_decay_rules (source, days_from, days_to, decay_type, decay_value, min_score) VALUES
  ('website', 0,  20, 'percent',  0, 0),
  ('website', 21, 45, 'percent', 30, 0),
  ('website', 46, NULL, 'reset',  0, 0);

INSERT IGNORE INTO segment_rules (source, segment_name, min_score, max_score, daily_touch_cap) VALUES
  ('website', 'WARM',     1,   49, 3),
  ('website', 'HOT',     50,   99, 4),
  ('website', 'HOTTEST', 100, NULL, 5);

INSERT IGNORE INTO instant_flag_events (source, event_name, target_segment) VALUES
  ('website', 'booking_page_viewed',     'HOTTEST'),
  ('website', 'emi_calculator_used',     'HOTTEST'),
  ('website', 'savings_calculator_used', 'HOTTEST'),
  ('website', 'test_ride_page_viewed',   'HOTTEST');


-- [MIGRATION v1.2.0] Lifecycle funnel tracking  2026-08-16

-- lifecycle_stage_triggers
-- Maps (source, event_name) to a lifecycle stage transition.
-- is_regression_allowed = 1 means the customer can move to this stage even if
-- they are currently at a later stage (e.g. NO_SHOW after TEST_RIDE_COMPLETED).
CREATE TABLE IF NOT EXISTS lifecycle_stage_triggers (
  id                   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  source               VARCHAR(100)    NOT NULL,
  event_name           VARCHAR(255)    NOT NULL,
  to_stage             VARCHAR(50)     NOT NULL,
  is_regression_allowed TINYINT(1)     NOT NULL DEFAULT 0,
  is_active            TINYINT(1)      NOT NULL DEFAULT 1,
  created_at           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_lifecycle_triggers_source_event (source, event_name),
  KEY idx_lifecycle_triggers_source (source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- customer_lifecycle
-- Current lifecycle stage per (customer, source). One row, updated in-place.
CREATE TABLE IF NOT EXISTS customer_lifecycle (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  customer_id     BIGINT UNSIGNED NOT NULL,
  source          VARCHAR(100)    NOT NULL,
  current_stage   VARCHAR(50)     NOT NULL,
  previous_stage  VARCHAR(50)     NULL,
  entered_at      DATETIME        NOT NULL,
  created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_customer_lifecycle_customer_source (customer_id, source),
  KEY idx_customer_lifecycle_stage (current_stage),
  CONSTRAINT fk_customer_lifecycle_customer
    FOREIGN KEY (customer_id) REFERENCES customers (id)
    ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- lifecycle_history
-- Immutable log of every stage a customer has passed through.
-- exited_at is NULL while the customer is still in this stage.
CREATE TABLE IF NOT EXISTS lifecycle_history (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  customer_id BIGINT UNSIGNED NOT NULL,
  source      VARCHAR(100)    NOT NULL,
  stage       VARCHAR(50)     NOT NULL,
  event_name  VARCHAR(255)    NULL,
  entered_at  DATETIME        NOT NULL,
  exited_at   DATETIME        NULL,
  created_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_lifecycle_history_customer (customer_id),
  KEY idx_lifecycle_history_stage    (stage),
  CONSTRAINT fk_lifecycle_history_customer
    FOREIGN KEY (customer_id) REFERENCES customers (id)
    ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Seed: lifecycle triggers for website source
INSERT IGNORE INTO lifecycle_stage_triggers (source, event_name, to_stage, is_regression_allowed) VALUES
  -- LEAD_FILLED: any form submission that creates a known customer
  ('website', 'contact_form',          'LEAD_FILLED',           0),
  ('website', 'login',                 'LEAD_FILLED',           0),
  -- LEAD_SCORED is set programmatically by SegmentService on first segment assignment
  -- Test ride stages
  ('website', 'test_ride_booking',     'TEST_RIDE_BOOKED',      0),
  ('website', 'test_ride_scheduled',   'TEST_RIDE_SCHEDULED',   0),
  ('website', 'test_ride_completed',   'TEST_RIDE_COMPLETED',   0),
  -- NO_SHOW: regression allowed — can re-enter funnel after a no-show
  ('website', 'no_show_marked',        'NO_SHOW',               1),
  -- Booking stages
  ('website', 'booking_started',       'BOOKING_STARTED',       0),
  ('website', 'book_bike',             'BOOKING_CREATED',       0),
  -- Final conversion
  ('website', 'retail_completed',      'RETAIL_COMPLETED',      0);

-- Add lifecycle events to source_event_scores (0 pts — they trigger lifecycle, not scoring)
INSERT IGNORE INTO source_event_scores (source, event_name, score) VALUES
  ('website', 'test_ride_scheduled',   0),
  ('website', 'test_ride_completed',   0),
  ('website', 'no_show_marked',        0),
  ('website', 'booking_started',       0),
  ('website', 'retail_completed',      0);
