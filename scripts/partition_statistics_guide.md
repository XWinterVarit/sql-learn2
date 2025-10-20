# Understanding Oracle Partition Statistics

## Problem

When querying the `user_tab_partitions` view to check partition information, the `NUM_ROWS` column may appear empty:

```sql
SELECT TABLE_NAME, PARTITION_NAME, HIGH_VALUE, NUM_ROWS
FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA';
```

Even though data exists in the table, the `NUM_ROWS` column might not show any values, making it difficult to monitor partition sizes and distribution.

## Solution

Oracle requires explicit statistics gathering to populate metadata views like `user_tab_partitions`. The solution is to use the `DBMS_STATS` package to gather statistics on the partitioned table:

```sql
BEGIN
  DBMS_STATS.GATHER_TABLE_STATS(
    ownname    => 'LEARN1',                -- Schema name
    tabname    => 'TIME_PARTITIONED_DATA', -- Table name
    estimate_percent => 100,               -- Analyze 100% of rows
    cascade    => TRUE,                    -- Include indexes
    granularity => 'ALL',                  -- Gather stats for table, partitions, and subpartitions
    degree     => NULL                     -- Use default degree of parallelism
  );
END;
/
```

After running this, the `NUM_ROWS` column will be populated with the correct row counts for each partition.

## Why Statistics Gathering Is Needed

1. **Metadata Views**: Oracle doesn't automatically update statistics in metadata views like `user_tab_partitions` when data is inserted, updated, or deleted.

2. **Query Optimizer**: Statistics are primarily used by Oracle's query optimizer to determine the most efficient execution plan for SQL queries.

3. **Manual vs. Automatic**: While Oracle can be configured for automatic statistics gathering (typically during maintenance windows), for immediate visibility, manual statistics gathering is required.

4. **Performance Impact**: Gathering statistics has some performance overhead, so Oracle doesn't do it automatically after every DML operation.

## Best Practices

1. **After Bulk Operations**: Gather statistics after large data loads or significant changes to table data.

2. **Before Important Queries**: Ensure statistics are up-to-date before running critical reports or queries.

3. **Granularity Options**:
   - `'ALL'`: Statistics for the table, all partitions, and all subpartitions
   - `'AUTO'`: Oracle determines the granularity
   - `'GLOBAL'`: Table-level statistics only
   - `'PARTITION'`: Partition-level statistics only

4. **Method Options**:
   - `estimate_percent => 100`: Analyzes all rows (most accurate, but slower)
   - `estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE`: Oracle determines the appropriate sampling

## Script Reference

A script named `gather_table_stats.sql` has been created in the scripts directory that:
1. Checks partition statistics before gathering
2. Runs the DBMS_STATS procedure to gather statistics
3. Verifies the updated partition statistics
4. Confirms the total row count matches

Run it with:

```
sql LEARN1/Welcome@localhost:1521/XE @/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/gather_table_stats.sql
```