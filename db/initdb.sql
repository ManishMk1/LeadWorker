-- =============================================================================
-- LeadWorkers — Database Initialization Script
-- =============================================================================
--
-- HOW TO RUN:
--   mysql -h <host> -u <user> -p < db/initdb.sql
--
-- Safe to re-run on a FRESH database (uses IF NOT EXISTS / INSERT IGNORE).
-- For existing databases, run only the new MIGRATION section at the bottom.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS leads_automation
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE leads_automation;

-- =============================================================================
-- CORE TABLES
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

CREATE TABLE IF NOT EXISTS customer_scores (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  customer_id  BIGINT UNSIGNED NOT NULL,
  source       VARCHAR(100)    NOT NULL,
  score        INT             NOT NULL DEFAULT 0,
  segment      VARCHAR(50)     NULL,
  is_paused    TINYINT(1)      NOT NULL DEFAULT 0,
  paused_at    DATETIME        NULL,
  last_seen_at DATETIME        NULL,
  created_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_customer_scores_customer_source (customer_id, source),
  CONSTRAINT fk_customer_scores_customer
    FOREIGN KEY (customer_id) REFERENCES customers (id)
    ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

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

CREATE TABLE IF NOT EXISTS lifecycle_stage_triggers (
  id                    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  source                VARCHAR(100)    NOT NULL,
  event_name            VARCHAR(255)    NOT NULL,
  to_stage              VARCHAR(50)     NOT NULL,
  is_regression_allowed TINYINT(1)      NOT NULL DEFAULT 0,
  is_active             TINYINT(1)      NOT NULL DEFAULT 1,
  created_at            DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at            DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_lifecycle_triggers_source_event (source, event_name),
  KEY idx_lifecycle_triggers_source (source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS customer_lifecycle (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  customer_id    BIGINT UNSIGNED NOT NULL,
  source         VARCHAR(100)    NOT NULL,
  current_stage  VARCHAR(50)     NOT NULL,
  previous_stage VARCHAR(50)     NULL,
  entered_at     DATETIME        NOT NULL,
  created_at     DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at     DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_customer_lifecycle_customer_source (customer_id, source),
  KEY idx_customer_lifecycle_stage (current_stage),
  CONSTRAINT fk_customer_lifecycle_customer
    FOREIGN KEY (customer_id) REFERENCES customers (id)
    ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

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

-- =============================================================================
-- SEED DATA
-- =============================================================================

INSERT IGNORE INTO source_event_scores (source, event_name, score) VALUES
  ('website', 'page_view',               1),
  ('website', 'product_view',            3),
  ('website', 'login',                  10),
  ('website', 'contact_form',           15),
  ('website', 'test_ride_booking',      20),
  ('website', 'book_bike',              30),
  ('website', 'booking_page_viewed',    22),
  ('website', 'test_ride_page_viewed',  18),
  ('website', 'emi_calculator_used',    20),
  ('website', 'savings_calculator_used',12),
  ('website', 'dealer_locator_opened',  15),
  ('website', 'return_visit',           15),
  ('website', 'product_page_3rd_view',  15),
  ('website', 'financing_page_view',     5),
  ('website', 'warranty_page_view',      5),
  ('website', 'renewed_site_activity',  10),
  ('website', 'journey_stop',            0),
  ('website', 'test_ride_scheduled',     0),
  ('website', 'test_ride_completed',     0),
  ('website', 'no_show_marked',          0),
  ('website', 'booking_started',         0),
  ('website', 'retail_completed',        0);

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

INSERT IGNORE INTO lifecycle_stage_triggers (source, event_name, to_stage, is_regression_allowed) VALUES
  ('website', 'contact_form',          'LEAD_FILLED',           0),
  ('website', 'login',                 'LEAD_FILLED',           0),
  ('website', 'test_ride_booking',     'TEST_RIDE_BOOKED',      0),
  ('website', 'test_ride_scheduled',   'TEST_RIDE_SCHEDULED',   0),
  ('website', 'test_ride_completed',   'TEST_RIDE_COMPLETED',   0),
  ('website', 'no_show_marked',        'NO_SHOW',               1),
  ('website', 'booking_started',       'BOOKING_STARTED',       0),
  ('website', 'book_bike',             'BOOKING_CREATED',       0),
  ('website', 'retail_completed',      'RETAIL_COMPLETED',      0);
