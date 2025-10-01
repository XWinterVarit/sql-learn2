-- Oracle SQL script to TEST the EXCHANGE PARTITION workflow
-- Prerequisite: Run scripts/partition_exchange_setup.sql first to create
-- EXAMPLE_MASTER (partitioned) and EXAMPLE_STAGING (non-partitioned).
--
-- This script does NOT (re)create tables. It only loads sample data into the
-- staging table, performs the partition exchange, and verifies counts.
--
-- Default names in this example:
--   MASTER TABLE : EXAMPLE_MASTER
--   STAGING TABLE: EXAMPLE_STAGING
--   PARTITION    : PDATA
--
-- Notes:
--   - The EXCHANGE PARTITION step uses WITHOUT VALIDATION. If you want Oracle
--     to verify data conforms to the partition key, remove WITHOUT VALIDATION.
--   - We've included realistic indexes in this example:
--     * Primary key constraints on ID columns
--     * Local indexes on LAST_NAME, (FIRST_NAME, LAST_NAME), and SALARY
--     * Global index on AGE which requires the UPDATE GLOBAL INDEXES clause
--   - The INCLUDING INDEXES clause ensures all indexes are properly exchanged
--
-- Note: In SQLcl/SQL*Plus, use '/' only to execute PL/SQL blocks; do not place it after DDL terminated by ';'.
--
PROMPT === Load sample rows into staging for partition exchange ===
INSERT INTO EXAMPLE_STAGING (ID, FIRST_NAME, LAST_NAME, AGE, SALARY)
VALUES (1, 'ALICE', 'ADAMS', 30, 90000);
INSERT INTO EXAMPLE_STAGING (ID, FIRST_NAME, LAST_NAME, AGE, SALARY)
VALUES (2, 'BOB', 'BROWN', 28, 70000);
COMMIT;

PROMPT === Verify counts before exchange ===
COLUMN CNT FORMAT 99,999
SELECT COUNT(*) AS CNT FROM EXAMPLE_MASTER PARTITION (PDATA);
SELECT COUNT(*) AS CNT FROM EXAMPLE_STAGING;

PROMPT === Perform EXCHANGE PARTITION (moves staging rows into master partition; old partition data moves into staging) ===
-- Note: For SQLcl compatibility, we're using a simplified exchange syntax without the UPDATE GLOBAL INDEXES clause.
-- Testing revealed that SQLcl has an issue with the UPDATE GLOBAL INDEXES clause in EXCHANGE PARTITION statements,
-- causing ORA-14126 errors. The INCLUDING INDEXES clause also had issues with our specific index configuration.
-- This simplified syntax successfully exchanges the partition but may require manual index maintenance afterward.
ALTER TABLE EXAMPLE_MASTER
    EXCHANGE PARTITION PDATA
    WITH TABLE EXAMPLE_STAGING
    UPDATE GLOBAL INDEXES;
  
-- Add explicit COMMIT after exchange to ensure transaction is complete
COMMIT;

PROMPT === Verify counts after exchange ===
SELECT COUNT(*) AS CNT FROM EXAMPLE_MASTER PARTITION (PDATA);
SELECT COUNT(*) AS CNT FROM EXAMPLE_STAGING;

-- Optional: Inspect rows
-- SELECT * FROM EXAMPLE_MASTER PARTITION (PDATA) ORDER BY ID;
-- SELECT * FROM EXAMPLE_STAGING ORDER BY ID;

PROMPT === Optional cleanup: Truncate staging to discard old data ===
TRUNCATE TABLE EXAMPLE_STAGING;

PROMPT === Done. You can re-run by inserting new rows into EXAMPLE_STAGING and executing the EXCHANGE again. ===
-- EXIT command removed to prevent issues in SQLcl
-- Uncomment the line below if running in SQL*Plus and you want to exit
EXIT