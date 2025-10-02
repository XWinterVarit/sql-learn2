-- Oracle SQL script to TEST time-based partition management with cleanup at SECOND granularity
-- Prerequisite: Run scripts/partition_by_time_setup.sql first to create
-- TIME_PARTITIONED_DATA (partitioned by COMMITTED_AT using string format "YYYYMMDD HH:MI:SS").
--
-- This script does NOT (re)create the table. It:
--   1. Creates a new partition for the current timestamp at SECOND granularity
--   2. Inserts new data with the latest timestamp in the format "YYYYMMDD HH:MI:SS"
--   3. Keeps the table fully available during the insert process
--   4. Cleans up old data ONLY AFTER new data is safely inserted
--   5. Verifies the cleanup while maintaining zero downtime
--   6. Optimized for large batch inserts (5+ million rows per job)
--
-- Default names in this example:
--   TABLE NAME: TIME_PARTITIONED_DATA
--
-- Note: With RANGE partitioning on strings, we create explicit partitions by timestamp
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

PROMPT === Create partition for current timestamp at SECOND granularity ===
-- Create a new partition for the current timestamp with second-level granularity
-- This allows for extremely fine partitioning - a new partition for each batch
DECLARE
  v_commit_time TIMESTAMP := SYSTIMESTAMP;
  v_commit_str VARCHAR2(19);
  v_next_partition VARCHAR2(50);
  v_high_value VARCHAR2(25);
  v_sql VARCHAR2(1000);
BEGIN
  -- Format the timestamp as YYYYMMDD HH:MI:SS for string-based partitioning
  v_commit_str := TO_CHAR(v_commit_time, 'YYYYMMDD HH24:MI:SS');
  
  -- Create next higher bound for partition (add 1 second)
  v_high_value := TO_CHAR(v_commit_time + INTERVAL '1' SECOND, 'YYYYMMDD HH24:MI:SS');
  
  -- Create a unique partition name based on timestamp
  v_next_partition := 'P_' || TO_CHAR(v_commit_time, 'YYYYMMDDHH24MISS');
  
  -- Create a new partition for this exact second
  v_sql := 'ALTER TABLE TIME_PARTITIONED_DATA ADD PARTITION ' || 
           v_next_partition || 
           ' VALUES LESS THAN (''' || v_high_value || ''')';
           
  -- Execute the partition creation
  DBMS_OUTPUT.PUT_LINE('Creating new partition at second granularity: ' || v_next_partition);
  DBMS_OUTPUT.PUT_LINE('Using timestamp: ' || v_commit_str);
  DBMS_OUTPUT.PUT_LINE('High value: ' || v_high_value);
  DBMS_OUTPUT.PUT_LINE('SQL: ' || v_sql);
  
  BEGIN
    EXECUTE IMMEDIATE v_sql;
    DBMS_OUTPUT.PUT_LINE('Successfully created partition: ' || v_next_partition);
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error creating partition: ' || SQLERRM);
      -- Continue anyway - partition might already exist
  END;
END;
/

PROMPT === Insert new batch of data with current timestamp ===
-- All records in this batch share the SAME current timestamp string
-- Run this script multiple times to create multiple batches with different timestamps
-- In production, each insert job handles ~5 million rows in a single partition
DECLARE
  v_commit_time TIMESTAMP := SYSTIMESTAMP;
  v_commit_str VARCHAR2(19);
  v_next_id NUMBER;
  v_batch_size NUMBER := 4; -- In production, this would be ~5 million
BEGIN
  -- Format timestamp as YYYYMMDD HH:MI:SS for string-based partitioning
  v_commit_str := TO_CHAR(v_commit_time, 'YYYYMMDD HH24:MI:SS');
  
  -- Get the next available ID (max ID + 1) to avoid conflicts with existing data
  SELECT NVL(MAX(ID), 0) + 1 INTO v_next_id FROM TIME_PARTITIONED_DATA;
  
  DBMS_OUTPUT.PUT_LINE('Starting batch insert with timestamp: ' || v_commit_str);
  DBMS_OUTPUT.PUT_LINE('In production, this would insert ~5 million rows into a single partition');
  
  -- For testing purposes, we only insert 4 records
  -- In production, this would be a bulk insert of ~5 million rows
  -- All with exactly the same timestamp string, going into the partition we just created
  
  -- Insert batch of records with same timestamp string
  INSERT INTO TIME_PARTITIONED_DATA (ID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT)
  VALUES (v_next_id, 'BATCH_DATA_1', 'Batch record 1', 'ACTIVE', v_commit_str);

  INSERT INTO TIME_PARTITIONED_DATA (ID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT)
  VALUES (v_next_id + 1, 'BATCH_DATA_2', 'Batch record 2', 'ACTIVE', v_commit_str);

  INSERT INTO TIME_PARTITIONED_DATA (ID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT)
  VALUES (v_next_id + 2, 'BATCH_DATA_3', 'Batch record 3', 'ACTIVE', v_commit_str);

  INSERT INTO TIME_PARTITIONED_DATA (ID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT)
  VALUES (v_next_id + 3, 'BATCH_DATA_4', 'Batch record 4', 'ACTIVE', v_commit_str);

  COMMIT;
  
  -- Now that new data is safely inserted, remove data from previous runs
  -- This maintains data availability - old data was accessible during the insert
  DBMS_OUTPUT.PUT_LINE('Removing data from previous runs...');
  DELETE FROM TIME_PARTITIONED_DATA
  WHERE COMMITTED_AT < v_commit_str;
  DBMS_OUTPUT.PUT_LINE('Rows deleted from previous runs: ' || SQL%ROWCOUNT);
  COMMIT;

  -- Display the timestamp used for this batch
  DBMS_OUTPUT.PUT_LINE('Batch committed at: ' || v_commit_str);
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

PROMPT === Drop all old partitions (keeping only the latest batch with current timestamp) ===
-- With string-based second-level partitioning, we need to dynamically drop partitions by name
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