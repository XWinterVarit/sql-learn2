-- Rerunnable Oracle SQL script to prepare tables for EXCHANGE PARTITION testing
-- This script ONLY prepares the structures (DDL). It does not perform the
-- exchange or load test data. Run this first, then use
--   scripts/partition_exchange_test_exchange.sql
-- to insert sample rows and execute the exchange.
--
-- Default object names (adjust by search/replace as needed):
--   MASTER TABLE : EXAMPLE_MASTER (partitioned by LIST on ID with a DEFAULT partition)
--   STAGING TABLE: EXAMPLE_STAGING (non-partitioned)
--   PARTITION    : PDATA - single partition for exchange only (explicitly named)
-- 
-- INDEX INFORMATION:
-- This example demonstrates realistic index usage with partition exchange:
--   - Primary key indexes on ID column in both tables
--   - indexes in the master table (automatically maintained during exchange)
--   - Regular indexes in the staging table (must match master table's structure)
--   - Global index on AGE (requires UPDATE GLOBAL INDEXES clause during exchange)
--
-- Note: In SQLcl/SQL*Plus, use '/' only to execute PL/SQL blocks; do not place it after DDL terminated by ';'.
--
PROMPT === Setup: Drop old tables if they exist ===
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE EXAMPLE_MASTER PURGE';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE EXAMPLE_STAGING PURGE';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/

PROMPT === Create partitioned master table ===
CREATE TABLE EXAMPLE_MASTER (
  ID          NUMBER PRIMARY KEY,
  FIRST_NAME  VARCHAR2(100),
  LAST_NAME   VARCHAR2(100),
  AGE         NUMBER,
  SALARY      NUMBER
)
-- Using LIST partitioning with DEFAULT partition named PDATA
-- This ensures the partition has the explicit name referenced in the exchange script
PARTITION BY LIST (ID) (
  PARTITION PDATA VALUES (DEFAULT)
);

PROMPT === Create indexes on master table ===

-- Primary key already creates an index on ID

-- index on LAST_NAME for searching by surname
CREATE INDEX EXAMPLE_MASTER_LN_IDX ON EXAMPLE_MASTER (LAST_NAME);

-- Composite  index for name-based queries (first+last name searches)
CREATE INDEX EXAMPLE_MASTER_NAME_IDX ON EXAMPLE_MASTER (FIRST_NAME, LAST_NAME);

-- index on SALARY for range queries (e.g., salary bands)
CREATE INDEX EXAMPLE_MASTER_SALARY_IDX ON EXAMPLE_MASTER (SALARY);

-- Global index on AGE (requires special handling during partition exchange)
CREATE INDEX EXAMPLE_MASTER_AGE_IDX ON EXAMPLE_MASTER (AGE);

PROMPT === Create compatible non-partitioned staging table ===
CREATE TABLE EXAMPLE_STAGING (
  ID          NUMBER PRIMARY KEY,
  FIRST_NAME  VARCHAR2(100),
  LAST_NAME   VARCHAR2(100),
  AGE         NUMBER,
  SALARY      NUMBER
);

PROMPT === Create indexes on staging table ===
-- For partition exchange, indexes on staging table should match master table's indexes
-- Primary key already creates an index on ID

-- Regular index on LAST_NAME (matches local index in master)
CREATE INDEX EXAMPLE_STAGING_LN_IDX ON EXAMPLE_STAGING (LAST_NAME);

-- Composite index for name-based queries
CREATE INDEX EXAMPLE_STAGING_NAME_IDX ON EXAMPLE_STAGING (FIRST_NAME, LAST_NAME);

-- Index on SALARY for range queries
CREATE INDEX EXAMPLE_STAGING_SALARY_IDX ON EXAMPLE_STAGING (SALARY);

-- Index on AGE (matches global index in master)
CREATE INDEX EXAMPLE_STAGING_AGE_IDX ON EXAMPLE_STAGING (AGE);

PROMPT === Setup complete. Next, run scripts/partition_exchange_test_exchange.sql to load sample data and perform the exchange. ===
-- EXIT command removed to prevent issues in SQLcl
-- Uncomment the line below if running in SQL*Plus and you want to exit
EXIT