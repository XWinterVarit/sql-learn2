-- Oracle SQL script to gather statistics on the TIME_PARTITIONED_DATA table
-- This will update the NUM_ROWS column in user_tab_partitions view
--
-- The DBMS_STATS package is used to gather statistics for the optimizer
-- but also populates metadata views like user_tab_partitions with counts

PROMPT === Checking partitions BEFORE gathering statistics ===
SELECT table_name, partition_name, high_value, num_rows
FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA'
ORDER BY partition_position;

PROMPT === Gathering statistics on TIME_PARTITIONED_DATA table ===
BEGIN
  -- Gather statistics for the whole table including all partitions
  DBMS_STATS.GATHER_TABLE_STATS(
    ownname    => 'LEARN1',                -- Schema name
    tabname    => 'TIME_PARTITIONED_DATA', -- Table name
    estimate_percent => 100,               -- Analyze 100% of rows (full analysis)
    cascade    => TRUE,                    -- Include all indexes
    granularity => 'ALL',                  -- Gather stats for table, partitions, and subpartitions
    degree     => NULL                     -- Use default degree of parallelism
  );
  
  DBMS_OUTPUT.PUT_LINE('Table statistics gathered successfully.');
END;
/

PROMPT === Checking partitions AFTER gathering statistics ===
SELECT table_name, partition_name, high_value, num_rows
FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA'
ORDER BY partition_position;

PROMPT === Verify total row count matches statistics ===
SELECT COUNT(*) AS actual_count FROM TIME_PARTITIONED_DATA;

-- EXIT is commented out to prevent issues in SQLcl
-- Uncomment if running in SQL*Plus and you want to exit
-- EXIT