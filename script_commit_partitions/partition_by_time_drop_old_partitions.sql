-- Oracle SQL script to DROP older daily partitions, keeping only the latest day
-- Prerequisite: Run scripts/partition_by_time_setup.sql first to create
--   TIME_PARTITIONED_DATA (interval partitioned by COMMITTED_AT DATE, 1-day granularity)
-- Optional prerequisite: Run scripts/partition_by_time_insert_5_days.sql to populate 5 days
--
-- Purpose:
--   - Identify the newest daily partition
--   - Drop all other daily partitions except the newest and P_INITIAL
--   - Effectively removes older days (e.g., the earlier 4 days if 5 exist)
--
-- Notes:
--   - With INTERVAL partitioning, Oracle system-generates partition names; we drop by listing positions
--   - Designed for SQL*Plus/SQLcl
--
PROMPT === Partitions BEFORE dropping old days ===
SELECT partition_name, high_value, partition_position
FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA'
ORDER BY partition_position;

PROMPT === Dropping old daily partitions (keeping latest and P_INITIAL) ===
DECLARE
  v_latest_partition   VARCHAR2(128);
  v_max_position       NUMBER;
  v_dropped_count      NUMBER := 0;
BEGIN
  -- Determine the latest partition by highest position
  SELECT partition_name, partition_position
  INTO v_latest_partition, v_max_position
  FROM (
    SELECT partition_name, partition_position
    FROM user_tab_partitions
    WHERE table_name = 'TIME_PARTITIONED_DATA'
    ORDER BY partition_position DESC
  )
  WHERE ROWNUM = 1;

  DBMS_OUTPUT.PUT_LINE('Latest partition to keep: ' || v_latest_partition || ' (position ' || v_max_position || ')');

  -- Drop all other partitions except the latest and P_INITIAL
  FOR rec IN (
    SELECT partition_name, partition_position
    FROM user_tab_partitions
    WHERE table_name = 'TIME_PARTITIONED_DATA'
      AND partition_name != v_latest_partition
      AND partition_name != 'P_INITIAL'
    ORDER BY partition_position
  ) LOOP
    BEGIN
      EXECUTE IMMEDIATE 'ALTER TABLE TIME_PARTITIONED_DATA DROP PARTITION ' || rec.partition_name;
      v_dropped_count := v_dropped_count + 1;
      DBMS_OUTPUT.PUT_LINE('Dropped partition: ' || rec.partition_name || ' (position ' || rec.partition_position || ')');
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error dropping partition ' || rec.partition_name || ': ' || SQLERRM);
    END;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('Total partitions dropped: ' || v_dropped_count);
  COMMIT;
END;
/

PROMPT === Partitions AFTER dropping old days ===
SELECT table_name, partition_name, high_value, partition_position
FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA'
ORDER BY partition_position;

PROMPT === Total count after cleanup (only latest day should remain, plus P_INITIAL base) ===
SELECT COUNT(*) AS total_records_after_cleanup FROM TIME_PARTITIONED_DATA;

-- EXIT is commented for SQLcl safety. Uncomment if needed.
-- EXIT
