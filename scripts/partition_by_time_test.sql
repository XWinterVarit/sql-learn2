-- Oracle SQL script to TEST time-based interval partition management with cleanup
-- Prerequisite: Run scripts/partition_by_time_setup.sql first to create
-- TIME_PARTITIONED_DATA (interval-partitioned by COMMITTED_AT).
--
-- This script does NOT (re)create the table. It:
--   1. Inserts test data across multiple time periods (Oracle auto-creates partitions)
--   2. Inserts new data with the latest committed date and time
--   3. Drops all old partitions, keeping only the latest data
--   4. Verifies the cleanup
--
-- Default names in this example:
--   TABLE NAME: TIME_PARTITIONED_DATA
--
-- Note: With INTERVAL partitioning, partition names are system-generated (e.g., SYS_P12345)
-- Note: In SQLcl/SQL*Plus, use '/' only to execute PL/SQL blocks; do not place it after DDL terminated by ';'.
--
PROMPT === Insert new batch of data with current timestamp ===
-- All records in this batch share the SAME current timestamp
-- Run this script multiple times to create multiple batches with different timestamps
DECLARE
  v_commit_time TIMESTAMP := SYSTIMESTAMP;
  v_next_id NUMBER;
BEGIN
  -- Get the next available ID (max ID + 1)
  SELECT NVL(MAX(ID), 0) + 1 INTO v_next_id FROM TIME_PARTITIONED_DATA;

  -- Insert batch of records with same timestamp
  INSERT INTO TIME_PARTITIONED_DATA (ID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT)
  VALUES (v_next_id, 'BATCH_DATA_1', 'Batch record 1', 'ACTIVE', v_commit_time);

  INSERT INTO TIME_PARTITIONED_DATA (ID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT)
  VALUES (v_next_id + 1, 'BATCH_DATA_2', 'Batch record 2', 'ACTIVE', v_commit_time);

  INSERT INTO TIME_PARTITIONED_DATA (ID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT)
  VALUES (v_next_id + 2, 'BATCH_DATA_3', 'Batch record 3', 'ACTIVE', v_commit_time);

  INSERT INTO TIME_PARTITIONED_DATA (ID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT)
  VALUES (v_next_id + 3, 'BATCH_DATA_4', 'Batch record 4', 'ACTIVE', v_commit_time);

  COMMIT;

  -- Display the timestamp used for this batch
  DBMS_OUTPUT.PUT_LINE('Batch committed at: ' || TO_CHAR(v_commit_time, 'YYYY-MM-DD HH24:MI:SS.FF6'));
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

PROMPT === Drop all old partitions (keeping only the latest batch with current timestamp) ===
-- With interval partitioning, we need to dynamically drop partitions by name
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

  -- Drop all partitions except P_INITIAL and the latest partition
  FOR rec IN (
    SELECT partition_name, partition_position, high_value
    FROM user_tab_partitions
    WHERE table_name = 'TIME_PARTITIONED_DATA'
      AND partition_name NOT IN (v_latest_partition, 'P_INITIAL')
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
COLUMN DATA_VALUE FORMAT A20
COLUMN DESCRIPTION FORMAT A40
COLUMN STATUS FORMAT A10
COLUMN COMMITTED_AT FORMAT A30
SELECT ID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT
FROM TIME_PARTITIONED_DATA
ORDER BY COMMITTED_AT, ID;

PROMPT === Done. Table now contains only the latest batch with current timestamp. ===
-- EXIT command removed to prevent issues in SQLcl
-- Uncomment the line below if running in SQL*Plus and you want to exit
EXIT
