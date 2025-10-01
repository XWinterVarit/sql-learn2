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
--   PARTITION    : P202501
--
-- Notes:
--   - The EXCHANGE PARTITION step uses WITHOUT VALIDATION. If you want Oracle
--     to verify data conforms to the partition key, remove WITHOUT VALIDATION.
--   - If you have global indexes or constraints, additional clauses/steps may
--     be needed. This example avoids global index complexity.
--
-- Note: In SQLcl/SQL*Plus, use '/' only to execute PL/SQL blocks; do not place it after DDL terminated by ';'.
--
PROMPT === Load sample rows into staging for partition P202501 ===
INSERT INTO EXAMPLE_STAGING (ID, PART_KEY, FIRST_NAME, LAST_NAME, AGE, SALARY)
VALUES (1, 202501, 'ALICE', 'ADAMS', 30, 90000);
INSERT INTO EXAMPLE_STAGING (ID, PART_KEY, FIRST_NAME, LAST_NAME, AGE, SALARY)
VALUES (2, 202501, 'BOB', 'BROWN', 28, 70000);
COMMIT;

PROMPT === Verify counts before exchange ===
COLUMN CNT FORMAT 99,999
SELECT COUNT(*) AS CNT FROM EXAMPLE_MASTER PARTITION (P202501);
SELECT COUNT(*) AS CNT FROM EXAMPLE_STAGING;

PROMPT === Perform EXCHANGE PARTITION (moves staging rows into master partition; old partition data moves into staging) ===
ALTER TABLE EXAMPLE_MASTER
  EXCHANGE PARTITION P202501
  WITH TABLE EXAMPLE_STAGING
  WITHOUT VALIDATION;

PROMPT === Verify counts after exchange ===
SELECT COUNT(*) AS CNT FROM EXAMPLE_MASTER PARTITION (P202501);
SELECT COUNT(*) AS CNT FROM EXAMPLE_STAGING;

-- Optional: Inspect rows
-- SELECT * FROM EXAMPLE_MASTER PARTITION (P202501) ORDER BY ID;
-- SELECT * FROM EXAMPLE_STAGING ORDER BY ID;

PROMPT === Optional cleanup: Truncate staging to discard old data ===
TRUNCATE TABLE EXAMPLE_STAGING;

PROMPT === Done. You can re-run by inserting new rows into EXAMPLE_STAGING and executing the EXCHANGE again. ===
