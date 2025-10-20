-- Rerunnable Oracle SQL script to prepare a single table with RANGE partitioning by committed date and time
-- This script ONLY prepares the structure (DDL). It does not load test data.
-- Run this first, then use
--   scripts/partition_by_time_test_new.sql
-- to insert sample rows and test partition cleanup.
--
-- Default object names (adjust by search/replace as needed):
--   TABLE NAME   : TIME_PARTITIONED_DATA
--   PARTITION BY : RANGE on COMMITTED_AT (DATE column)
--
-- PARTITION STRATEGY:
-- This example uses INTERVAL partitioning by day on a DATE column:
--   - DBA creates the table with one initial partition (ONE-TIME SETUP)
--   - New partitions are created automatically per day as data is inserted
--   - Granularity: 1 day (INTERVAL '1' DAY)
--   - COMMITTED_AT uses Oracle DATE (stores date and time to seconds)
--
-- INDEX INFORMATION:
--   - Primary key on ID column
--   - Local index on COMMITTED_AT (partition key) for efficient time-based queries
--   - Additional indexes for common query patterns
--
-- Note: In SQLcl/SQL*Plus, use '/' only to execute PL/SQL blocks; do not place it after DDL terminated by ';'.
--
PROMPT === Setup: Drop old table if it exists ===
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE TIME_PARTITIONED_DATA PURGE';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/

PROMPT === Create interval-partitioned table by committed date and time (day-level granularity) ===
CREATE TABLE TIME_PARTITIONED_DATA (
  ID            NUMBER PRIMARY KEY,
  PID           VARCHAR2(100) NOT NULL,  -- Add PID column
  DATA_VALUE    VARCHAR2(200),
  DESCRIPTION   VARCHAR2(500),
  STATUS        VARCHAR2(50),
  COMMITTED_AT  DATE NOT NULL,  -- Oracle DATE includes date and time to seconds
  CONSTRAINT TIME_PART_DATA_PID_TIME_UQ UNIQUE (PID, COMMITTED_AT)  -- Add compound unique constraint
)
PARTITION BY RANGE (COMMITTED_AT)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
(
  -- Initial partition (boundary before first day)
  PARTITION P_INITIAL VALUES LESS THAN (DATE '2024-01-01')
);

PROMPT === Create indexes on partitioned table ===

-- Primary key already creates an index on ID

-- Local index on COMMITTED_AT (partition key) for efficient time-based queries
CREATE INDEX TIME_PART_DATA_COMMITTED_IDX ON TIME_PARTITIONED_DATA (COMMITTED_AT) LOCAL;

-- Index on STATUS for filtering queries
CREATE INDEX TIME_PART_DATA_STATUS_IDX ON TIME_PARTITIONED_DATA (STATUS);

-- Composite index for status and time queries
CREATE INDEX TIME_PART_DATA_STATUS_TIME_IDX ON TIME_PARTITIONED_DATA (STATUS, COMMITTED_AT) LOCAL;

PROMPT === Query partition information ===
SELECT table_name, partition_name, high_value, num_rows
FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA'
ORDER BY partition_position;

PROMPT === Setup complete. Next, run scripts/partition_by_time_test_new.sql to load sample data and test partition cleanup. ===
-- EXIT command removed to prevent issues in SQLcl
-- Uncomment the line below if running in SQL*Plus and you want to exit
-- EXIT
