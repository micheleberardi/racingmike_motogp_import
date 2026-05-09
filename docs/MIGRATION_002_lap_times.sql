-- MIGRATION 002: Add lap_times table
-- MySQL 8.0+ | Run once.
--
-- Stores per-lap data parsed from the official Analysis PDFs (analysis_url
-- column in sessions).  Provides sector times (T1-T4), lap time in
-- milliseconds for arithmetic, and top speed for each rider per lap.

CREATE TABLE IF NOT EXISTS lap_times (
    id              BIGINT          NOT NULL AUTO_INCREMENT,
    session_id      VARCHAR(50)     NOT NULL,
    rider_id        VARCHAR(50)     NULL,
    rider_number    INT             NULL,
    rider_name      VARCHAR(255)    NULL,
    lap_number      INT             NOT NULL,

    -- Raw string as it appears in the PDF (e.g. "1'30.124") — used for display
    lap_time        VARCHAR(15)     NULL,
    -- Lap time converted to milliseconds — used for sorting / arithmetic
    lap_time_ms     INT             NULL,

    -- Sector times in seconds as strings (e.g. "20.212")
    t1              VARCHAR(10)     NULL,
    t2              VARCHAR(10)     NULL,
    t3              VARCHAR(10)     NULL,
    t4              VARCHAR(10)     NULL,

    -- Sector times in milliseconds — for arithmetic
    t1_ms           INT             NULL,
    t2_ms           INT             NULL,
    t3_ms           INT             NULL,
    t4_ms           INT             NULL,

    speed           FLOAT           NULL,       -- km/h at end of lap
    is_best_lap     TINYINT(1)      NOT NULL DEFAULT 0,

    -- Denormalised context (from sessions row) for standalone queries
    session_type    VARCHAR(45)     NULL,
    year            INT             NULL,
    event_id        VARCHAR(50)     NULL,
    category_id     VARCHAR(50)     NULL,
    category_name   VARCHAR(100)    NULL,
    event_name      VARCHAR(255)    NULL,
    circuit_name    VARCHAR(255)    NULL,

    md5             VARCHAR(64)     NOT NULL,

    PRIMARY KEY (id),
    UNIQUE KEY uq_lap_times (session_id, rider_id, lap_number),
    UNIQUE KEY uq_lap_times_md5 (md5),

    INDEX idx_lt_session    (session_id),
    INDEX idx_lt_rider_year (rider_id, year),
    INDEX idx_lt_year_cat   (year, category_id),
    INDEX idx_lt_lap_ms     (lap_time_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
