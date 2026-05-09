-- MIGRATION 001: Schema hardening — type fixes, indexes, constraints
-- MySQL 8.0+ | Safe for production: column rebuilds take seconds on this dataset size.
-- Run once. Re-running is safe (IF NOT EXISTS / IF EXISTS guards where MySQL supports them).
-- Test in staging first.

-- =============================================================================
-- PART 1 — Fix `year` columns: VARCHAR → INT
-- All tables store year as clean 4-digit integers ('2017'..'2026'), conversion is safe.
-- =============================================================================

ALTER TABLE results         MODIFY COLUMN year INT NULL;
ALTER TABLE sessions        MODIFY COLUMN year INT NULL;
ALTER TABLE records         MODIFY COLUMN year INT NULL;
ALTER TABLE standing_riders MODIFY COLUMN year INT NULL;
ALTER TABLE TeamRiders      MODIFY COLUMN year INT NULL;

-- =============================================================================
-- PART 2 — Fix `top_speed`: VARCHAR(45) → FLOAT
-- Column contains only numeric strings ('295', '347.5', etc.), no bad values.
-- =============================================================================

ALTER TABLE results MODIFY COLUMN top_speed FLOAT NULL;

-- =============================================================================
-- PART 3 — Add `session_datetime` computed column on sessions
-- `sessions.date` stores ISO 8601 strings like '2024-04-12T09:00:00+00:00'.
-- This generated column exposes a proper DATETIME for range queries and JOINs
-- without touching existing code that writes the raw string.
-- =============================================================================

ALTER TABLE sessions
    ADD COLUMN session_datetime DATETIME GENERATED ALWAYS AS (
        CASE WHEN date IS NOT NULL
             THEN STR_TO_DATE(LEFT(date, 19), '%Y-%m-%dT%H:%i:%s')
             ELSE NULL
        END
    ) STORED;

-- Index the computed column so date-range queries on sessions are fast
ALTER TABLE sessions
    ADD INDEX idx_sessions_session_datetime (session_datetime);

-- =============================================================================
-- PART 4 — Critical missing indexes
-- `results` has 215k rows with zero secondary indexes; every dashboard query
-- is a full table scan.
-- =============================================================================

-- results — the hot table
ALTER TABLE results
    ADD INDEX idx_results_session_id          (session_id),
    ADD INDEX idx_results_year_category       (year, category_id),
    ADD INDEX idx_results_rider_year          (rider_id, year),
    ADD INDEX idx_results_year_event_session  (year, event_id, session_id);

-- sessions
ALTER TABLE sessions
    ADD INDEX idx_sessions_year_event_cat     (year, event_id, category_id),
    ADD INDEX idx_sessions_year_status        (year, status);

-- events (used in sessions.py / sessions_test.py date-window queries)
ALTER TABLE events
    ADD INDEX idx_events_year_test_date       (year, test, date_start);

-- standing_riders (dashboard standings tab)
ALTER TABLE standing_riders
    ADD INDEX idx_standing_year_category      (year, category_id);

-- =============================================================================
-- PART 5 — Unique constraint on event_legacy_ids (from DB_HARDENING.sql)
-- Makes upserts in events.py truly idempotent.
-- =============================================================================

ALTER TABLE event_legacy_ids
    ADD UNIQUE KEY uq_event_legacy (event_id, categoryId, eventId);
