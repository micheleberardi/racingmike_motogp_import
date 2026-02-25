-- Optional schema hardening for production reliability.
-- Review and run in staging first.

-- 1) Make event legacy upserts truly idempotent.
ALTER TABLE event_legacy_ids
ADD UNIQUE KEY uq_event_legacy (event_id, categoryId, eventId);

-- 2) Helpful indexes for frequent filters.
ALTER TABLE events
ADD INDEX idx_events_year_test_date (year, test, date_start);

ALTER TABLE sessions
ADD INDEX idx_sessions_year_date (year, date);

ALTER TABLE results
ADD INDEX idx_results_year_event_session (year, event_id, session_id);

ALTER TABLE standing_riders
ADD INDEX idx_standing_year_category (year, category_id);

-- 3) Optional type alignment (run only if application and historical data are ready).
-- ALTER TABLE sessions MODIFY COLUMN year INT NULL;
-- ALTER TABLE results MODIFY COLUMN year INT NULL;
-- ALTER TABLE records MODIFY COLUMN year INT NULL;
-- ALTER TABLE standing_riders MODIFY COLUMN year INT NULL;
-- ALTER TABLE TeamRiders MODIFY COLUMN year INT NULL;
