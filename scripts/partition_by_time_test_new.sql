-- Oracle SQL script to TEST time-based partition management with INTERVAL partitioning per DAY
-- Prerequisite: Run scripts/partition_by_time_setup.sql first to create
-- TIME_PARTITIONED_DATA (interval partitioned by COMMITTED_AT DATE with 1-day granularity).
--
-- This script does NOT (re)create the table. It:
--   1. Inserts new data with the current date/time (Oracle DATE)
--   2. Keeps the table fully available during the insert process
--   3. Cleans up old data ONLY AFTER new data is safely inserted
--   4. Verifies the cleanup while maintaining zero downtime
--   5. Optimized for large batch inserts (5+ million rows per job)
--
-- Default names in this example:
--   TABLE NAME: TIME_PARTITIONED_DATA
--
-- Note: With INTERVAL partitioning, Oracle will auto-create daily partitions as needed
-- Note: In SQLcl/SQL*Plus, use '/' only to execute PL/SQL blocks; do not place it after DDL terminated by ';'.
--
-- No deletion here to maintain data availability
-- Old data will be removed after new data is safely inserted

-- First, check the table structure 
PROMPT === Verifying table structure ===
SELECT table_name, partitioned FROM user_tables WHERE table_name = 'TIME_PARTITIONED_DATA';

-- Check partitioning key column
PROMPT === Verifying partition key column ===
SELECT column_name, data_type, data_length FROM user_tab_columns 
WHERE table_name = 'TIME_PARTITIONED_DATA' AND column_name = 'COMMITTED_AT';

-- Check existing partitions
PROMPT === Checking existing partitions ===
SELECT partition_name, high_value FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA'
ORDER BY partition_position;

PROMPT === Interval partitioning note ===
BEGIN
  DBMS_OUTPUT.PUT_LINE('Interval partitioning is enabled (1-day granularity).');
  DBMS_OUTPUT.PUT_LINE('Oracle will automatically create the necessary daily partition upon insert.');
END;
/

PROMPT === Insert new batch of data with current timestamp ===
-- Interval partitioning will auto-create the daily partition upon insert
-- Run this script multiple times to create multiple batches on different days
-- In production, each insert job handles ~5 million rows in a single daily partition
DECLARE
  v_commit_time DATE := SYSDATE;
  v_next_id NUMBER;
  v_batch_size NUMBER := 4; -- In production, this would be ~5 million
BEGIN
  -- Get the next available ID (max ID + 1) to avoid conflicts with existing data
  SELECT NVL(MAX(ID), 0) + 1 INTO v_next_id FROM TIME_PARTITIONED_DATA;
  
  DBMS_OUTPUT.PUT_LINE('Starting batch insert with COMMITTED_AT (DATE): ' || TO_CHAR(v_commit_time, 'YYYY-MM-DD HH24:MI:SS'));
  DBMS_OUTPUT.PUT_LINE('In production, this would insert ~5 million rows into a single daily partition');
  
  -- For testing purposes, we only insert 4 records
  INSERT INTO TIME_PARTITIONED_DATA (ID, PID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT)
  VALUES (v_next_id, 'PID-' || v_next_id, 'BATCH_DATA_1', 'Batch record 1', 'ACTIVE', v_commit_time);

  INSERT INTO TIME_PARTITIONED_DATA (ID, PID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT)
  VALUES (v_next_id + 1, 'PID-' || (v_next_id + 1), 'BATCH_DATA_2', 'Batch record 2', 'ACTIVE', v_commit_time);

  INSERT INTO TIME_PARTITIONED_DATA (ID, PID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT)
  VALUES (v_next_id + 2, 'PID-' || (v_next_id + 2), 'BATCH_DATA_3', 'Batch record 3', 'ACTIVE', v_commit_time);

  INSERT INTO TIME_PARTITIONED_DATA (ID, PID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT)
  VALUES (v_next_id + 3, 'PID-' || (v_next_id + 3), 'BATCH_DATA_4', 'Batch record 4', 'ACTIVE', v_commit_time);

  COMMIT;
  
  -- Now that new data is safely inserted, remove data from previous days (keep today)
  DBMS_OUTPUT.PUT_LINE('Removing data from previous days (keeping today)...');
  DELETE FROM TIME_PARTITIONED_DATA
  WHERE COMMITTED_AT < TRUNC(v_commit_time);
  DBMS_OUTPUT.PUT_LINE('Rows deleted from previous days: ' || SQL%ROWCOUNT);
  COMMIT;

  -- Display the timestamp used for this batch
  DBMS_OUTPUT.PUT_LINE('Batch committed at: ' || TO_CHAR(v_commit_time, 'YYYY-MM-DD HH24:MI:SS'));
  DBMS_OUTPUT.PUT_LINE('Records inserted: ' || v_next_id || ' to ' || (v_next_id + 3));
END;
/

PROMPT === View all partitions and record counts ===
SELECT partition_name,
       high_value,
       num_rows
FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA'
ORDER BY partition_position;

PROMPT === Total record count ===
SELECT COUNT(*) AS total_records FROM TIME_PARTITIONED_DATA;

PROMPT === Clear old data (after new data was inserted) ===
-- Truncate old data after inserting new batch to ensure table remains queryable during insertion
DECLARE
  v_latest_partition VARCHAR2(128);
BEGIN
  -- Find the latest partition (highest partition position)
  SELECT partition_name
  INTO v_latest_partition
  FROM user_tab_partitions
  WHERE table_name = 'TIME_PARTITIONED_DATA'
  ORDER BY partition_position DESC
  FETCH FIRST 1 ROWS ONLY;

  DBMS_OUTPUT.PUT_LINE('Latest partition to keep: ' || v_latest_partition);
  
  -- Truncate all partitions except the latest one (where we just inserted data)
  -- and except P_INITIAL (which is the base partition)
  FOR rec IN (
    SELECT partition_name
    FROM user_tab_partitions
    WHERE table_name = 'TIME_PARTITIONED_DATA'
      AND partition_name != v_latest_partition
      AND partition_name != 'P_INITIAL'  -- Always keep the initial partition
  ) LOOP
    BEGIN
      EXECUTE IMMEDIATE 'ALTER TABLE TIME_PARTITIONED_DATA TRUNCATE PARTITION ' || rec.partition_name;
      DBMS_OUTPUT.PUT_LINE('Truncated partition: ' || rec.partition_name);
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error truncating partition ' || rec.partition_name || ': ' || SQLERRM);
    END;
  END LOOP;
  
  COMMIT;
END;
/

PROMPT === Drop all old partitions (keeping only the latest daily partition) ===
-- With interval partitioning, partitions are system-named; we'll drop by position
-- This PL/SQL block drops all partitions except the most recent one
DECLARE
  v_partition_count NUMBER := 0;
  v_latest_partition VARCHAR2(128);
  v_max_position NUMBER;
BEGIN
  -- Find the latest partition (highest partition position)
  SELECT partition_name, partition_position
  INTO v_latest_partition, v_max_position
  FROM user_tab_partitions
  WHERE table_name = 'TIME_PARTITIONED_DATA'
  ORDER BY partition_position DESC
  FETCH FIRST 1 ROWS ONLY;

  DBMS_OUTPUT.PUT_LINE('Latest partition to keep: ' || v_latest_partition);

  -- Drop all partitions except the latest partition and P_INITIAL
  -- This handles second-level granularity partitioning
  FOR rec IN (
    SELECT partition_name, partition_position
    FROM user_tab_partitions
    WHERE table_name = 'TIME_PARTITIONED_DATA'
      AND partition_name != v_latest_partition
      AND partition_name != 'P_INITIAL'  -- Always keep the initial partition
    ORDER BY partition_position
  ) LOOP
    BEGIN
      EXECUTE IMMEDIATE 'ALTER TABLE TIME_PARTITIONED_DATA DROP PARTITION ' || rec.partition_name;
      v_partition_count := v_partition_count + 1;
      DBMS_OUTPUT.PUT_LINE('Dropped partition: ' || rec.partition_name);
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error dropping partition ' || rec.partition_name || ': ' || SQLERRM);
    END;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('Total partitions dropped: ' || v_partition_count);
  COMMIT;
END;
/

PROMPT === Verify remaining partitions ===
SELECT table_name, partition_name, high_value, num_rows
FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA'
ORDER BY partition_position;

PROMPT === Total count after cleanup (should only show latest data) ===
SELECT COUNT(*) AS total_records_after_cleanup FROM TIME_PARTITIONED_DATA;

PROMPT === Display remaining data ===
COLUMN ID FORMAT 999
COLUMN PID FORMAT A15
COLUMN DATA_VALUE FORMAT A20
COLUMN DESCRIPTION FORMAT A40
COLUMN STATUS FORMAT A10
COLUMN COMMITTED_AT FORMAT A30
SELECT ID, PID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT
FROM TIME_PARTITIONED_DATA
ORDER BY COMMITTED_AT, ID;

PROMPT === Done. Table now contains only the latest daily batch. ===
-- EXIT command removed to prevent issues in SQLcl
-- Uncomment the line below if running in SQL*Plus and you want to exit
EXIT