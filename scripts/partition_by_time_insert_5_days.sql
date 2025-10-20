-- Oracle SQL script to INSERT 5 batches across 5 committed days (1 batch per day)
-- Prerequisite: Run scripts/partition_by_time_setup.sql first to create
--   TIME_PARTITIONED_DATA (interval partitioned by COMMITTED_AT DATE, 1-day granularity)
--
-- Purpose:
--   - Insert 5 batches of data, one batch per day for today and the previous 4 days
--   - Let INTERVAL partitioning auto-create daily partitions as needed
--   - No cleanup here; use scripts/partition_by_time_drop_old_partitions.sql to drop older days
--
-- Notes:
--   - Designed for SQL*Plus/SQLcl
--   - Do NOT use '/' after non-PL/SQL statements terminated by ';'
--   - Adjust v_rows_per_batch if you want a larger test volume
--
PROMPT === Verifying table structure ===
SELECT table_name, partitioned FROM user_tables WHERE table_name = 'TIME_PARTITIONED_DATA';

PROMPT === Verifying partition key column ===
SELECT column_name, data_type, data_length FROM user_tab_columns 
WHERE table_name = 'TIME_PARTITIONED_DATA' AND column_name = 'COMMITTED_AT';

PROMPT === Checking existing partitions BEFORE inserts ===
SELECT partition_name, high_value
FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA'
ORDER BY partition_position;

PROMPT === Inserting 5 daily batches (today and previous 4 days) ===
DECLARE
  v_rows_per_batch CONSTANT PLS_INTEGER := 4; -- keep small for demo; raise as needed
  v_day_offset      PLS_INTEGER;
  v_commit_time     DATE;
  v_next_id         NUMBER;
BEGIN
  FOR v_day_offset IN 0..4 LOOP
    -- Commit time set to TRUNC(SYSDATE) - v_day_offset at 10:00:00 for determinism
    v_commit_time := TRUNC(SYSDATE) - v_day_offset + (10/24);

    -- Compute next starting ID to avoid conflicts
    SELECT NVL(MAX(ID), 0) + 1 INTO v_next_id FROM TIME_PARTITIONED_DATA;

    DBMS_OUTPUT.PUT_LINE('Inserting batch for day offset ' || v_day_offset ||
                         ' at ' || TO_CHAR(v_commit_time, 'YYYY-MM-DD HH24:MI:SS'));

    -- Insert v_rows_per_batch rows
    FOR i IN 0..(v_rows_per_batch - 1) LOOP
      INSERT INTO TIME_PARTITIONED_DATA (ID, PID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT)
      VALUES (
        v_next_id + i,
        'PID-D' || TO_CHAR(v_day_offset) || '-' || TO_CHAR(v_next_id + i),
        'BATCH_D' || TO_CHAR(v_day_offset) || '_' || TO_CHAR(i+1),
        'Batch record ' || TO_CHAR(i+1) || ' for day offset ' || TO_CHAR(v_day_offset),
        'ACTIVE',
        v_commit_time
      );
    END LOOP;

    COMMIT; -- commit each day's batch
    DBMS_OUTPUT.PUT_LINE('Committed rows: ' || v_rows_per_batch || ' for ' || TO_CHAR(v_commit_time, 'YYYY-MM-DD'));
  END LOOP;
END;
/

PROMPT === Checking partitions AFTER inserts ===
SELECT partition_name, high_value, num_rows
FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA'
ORDER BY partition_position;

PROMPT === Total record count after inserts ===
SELECT COUNT(*) AS total_records FROM TIME_PARTITIONED_DATA;

-- EXIT is commented for SQLcl safety. Uncomment if needed.
-- EXIT
