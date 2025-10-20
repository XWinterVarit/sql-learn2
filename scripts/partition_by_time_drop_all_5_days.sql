-- Oracle SQL script to DROP ALL daily partitions (reset last 5 days or more)
-- Prerequisite: Run scripts/partition_by_time_setup.sql first to create
--   TIME_PARTITIONED_DATA (interval partitioned by COMMITTED_AT DATE, 1-day granularity)
-- Optional prerequisite: Run scripts/partition_by_time_insert_5_days.sql to populate 5 days
--
-- Purpose:
--   - Manual reset utility: drop every auto-created daily partition
--   - Keeps only the base partition P_INITIAL
--   - Useful when you want to remove all 5 daily partitions inserted by the demo
--
-- Notes:
--   - With INTERVAL partitioning, Oracle system-generates partition names; we iterate and drop them all
--   - Designed for SQL*Plus/SQLcl
--   - Do NOT place '/' after DDL; only use '/' to run PL/SQL blocks
--
PROMPT === Partitions BEFORE full reset ===
SELECT partition_name, high_value, partition_position
FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA'
ORDER BY partition_position;

PROMPT === Dropping ALL daily partitions (keeping only P_INITIAL) ===
DECLARE
  v_dropped_count NUMBER := 0;
BEGIN
  -- Drop all partitions except the initial base partition
  FOR rec IN (
    SELECT partition_name, partition_position
    FROM user_tab_partitions
    WHERE table_name = 'TIME_PARTITIONED_DATA'
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

PROMPT === Partitions AFTER full reset ===
SELECT table_name, partition_name, high_value, partition_position
FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA'
ORDER BY partition_position;

PROMPT === Total rows after full reset (should be 0 if data only existed in daily partitions) ===
SELECT COUNT(*) AS total_records_after_reset FROM TIME_PARTITIONED_DATA;

-- Optional: If you ever inserted rows into P_INITIAL (older than its boundary),
-- you can also truncate that partition. This is normally unnecessary in the demo.
-- Uncomment the following block only if you know what you're doing:
--
-- BEGIN
--   EXECUTE IMMEDIATE 'ALTER TABLE TIME_PARTITIONED_DATA TRUNCATE PARTITION P_INITIAL';
--   DBMS_OUTPUT.PUT_LINE('Truncated P_INITIAL partition.');
-- END;
-- /
--
-- EXIT is commented for SQLcl safety. Uncomment if needed.
-- EXIT
