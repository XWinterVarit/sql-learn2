-- Rerunnable Oracle SQL script to prepare a single table with RANGE partitioning by committed date and time
-- This script ONLY prepares the structure (DDL). It does not load test data.
-- Run this first, then use
--   scripts/partition_by_time_test.sql
-- to insert sample rows and test partition cleanup.
--
-- Default object names (adjust by search/replace as needed):
--   TABLE NAME   : TIME_PARTITIONED_DATA
--   PARTITION BY : RANGE on COMMITTED_AT (timestamp column)
--
-- PARTITION STRATEGY:
-- This example uses INTERVAL partitioning for automatic partition creation:
--   - DBA creates the table with one initial partition (ONE-TIME SETUP)
--   - Oracle automatically creates new monthly partitions as data arrives
--   - No need to manually create partitions for future dates
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

PROMPT === Create interval-partitioned table by committed date and time ===
CREATE TABLE TIME_PARTITIONED_DATA (
  ID            NUMBER PRIMARY KEY,
  DATA_VALUE    VARCHAR2(200),
  DESCRIPTION   VARCHAR2(500),
  STATUS        VARCHAR2(50),
  COMMITTED_AT  TIMESTAMP NOT NULL
)
-- Using INTERVAL partitioning on COMMITTED_AT timestamp
-- Oracle automatically creates new monthly partitions as data arrives
-- DBA only needs to create this table ONCE - no future partition management needed!
PARTITION BY RANGE (COMMITTED_AT)
INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(
  -- Initial partition - Oracle will create new partitions automatically for later dates
  PARTITION P_INITIAL VALUES LESS THAN (TIMESTAMP '2024-01-01 00:00:00')
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

PROMPT === Setup complete. Next, run scripts/partition_by_time_test.sql to load sample data and test partition cleanup. ===
-- EXIT command removed to prevent issues in SQLcl
-- Uncomment the line below if running in SQL*Plus and you want to exit
EXIT
