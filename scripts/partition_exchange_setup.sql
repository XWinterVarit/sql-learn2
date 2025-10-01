-- Rerunnable Oracle SQL script to prepare tables for EXCHANGE PARTITION testing
-- This script ONLY prepares the structures (DDL). It does not perform the
-- exchange or load test data. Run this first, then use
--   scripts/partition_exchange_test_exchange.sql
-- to insert sample rows and execute the exchange.
--
-- Default object names (adjust by search/replace as needed):
--   MASTER TABLE : EXAMPLE_MASTER (partitioned by LIST on PART_KEY)
--   STAGING TABLE: EXAMPLE_STAGING (non-partitioned)
--   PARTITIONS   : P202501 (values 202501), P202502 (values 202502)
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
  ID          NUMBER,
  PART_KEY    NUMBER NOT NULL,
  FIRST_NAME  VARCHAR2(100),
  LAST_NAME   VARCHAR2(100),
  AGE         NUMBER,
  SALARY      NUMBER
)
PARTITION BY LIST (PART_KEY) (
  PARTITION P202501 VALUES (202501),
  PARTITION P202502 VALUES (202502)
);

-- Optional example local index (not required for the exchange)
-- CREATE INDEX EXAMPLE_MASTER_LN_IDX ON EXAMPLE_MASTER (LAST_NAME) LOCAL;
-- /

PROMPT === Create compatible non-partitioned staging table ===
CREATE TABLE EXAMPLE_STAGING (
  ID          NUMBER,
  PART_KEY    NUMBER NOT NULL,
  FIRST_NAME  VARCHAR2(100),
  LAST_NAME   VARCHAR2(100),
  AGE         NUMBER,
  SALARY      NUMBER
);

PROMPT === Setup complete. Next, run scripts/partition_exchange_test_exchange.sql to load sample data and perform the exchange. ===
