# Oracle Time-Based Partition Scripts: Command Reference

This document provides detailed explanations of how each command works in the Oracle partition management scripts.

## Table of Contents

1. [Setup Script](#setup-script)
2. [Insert 5 Days Script](#insert-5-days-script)
3. [Drop Old Partitions Script](#drop-old-partitions-script)
4. [Drop All 5 Days Script](#drop-all-5-days-script)
5. [Wrapper Scripts](#wrapper-scripts)

## Setup Script

**File: partition_by_time_setup.sql**

This script creates the initial table structure with interval partitioning by date.

### Drop Existing Table (If Any)

```sql
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE TIME_PARTITIONED_DATA PURGE';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
```

**How it works:**
- `BEGIN`/`END` - Defines a PL/SQL anonymous block
- `EXECUTE IMMEDIATE` - Executes dynamic SQL (a SQL statement stored in a string)
- `DROP TABLE ... PURGE` - Removes the table and all its data completely (PURGE skips the recycle bin)
- `EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;` - Error handling that:
  - Catches all errors (`WHEN OTHERS`)
  - Checks if the error is anything other than "table doesn't exist" (ORA-00942)
  - Re-throws any other errors (`RAISE`)
- `/` - Executes the PL/SQL block (required after PL/SQL blocks in SQL*Plus/SQLcl)

### Create Partitioned Table

```sql
CREATE TABLE TIME_PARTITIONED_DATA (
  ID            NUMBER PRIMARY KEY,
  DATA_VALUE    VARCHAR2(200),
  DESCRIPTION   VARCHAR2(500),
  STATUS        VARCHAR2(50),
  COMMITTED_AT  DATE NOT NULL  -- Oracle DATE includes date and time to seconds
)
PARTITION BY RANGE (COMMITTED_AT)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
(
  -- Initial partition (boundary before first day)
  PARTITION P_INITIAL VALUES LESS THAN (DATE '2024-01-01')
);
```

**How it works:**
- `CREATE TABLE` - Standard DDL command to create a table
- Column definitions with data types:
  - `NUMBER` - Oracle numeric type for exact numeric values
  - `VARCHAR2` - Variable-length character string
  - `DATE` - Oracle date type (includes both date and time components)
  - `PRIMARY KEY` - Constraint that uniquely identifies each row and creates an index
  - `NOT NULL` - Constraint requiring a value
- `PARTITION BY RANGE (COMMITTED_AT)` - Specifies range partitioning on the COMMITTED_AT column
- `INTERVAL (NUMTODSINTERVAL(1, 'DAY'))` - Enables automatic creation of partitions for each new day
  - `NUMTODSINTERVAL(1, 'DAY')` - Converts the number 1 to a DAY interval (1 day)
- `PARTITION P_INITIAL VALUES LESS THAN (DATE '2024-01-01')` - Creates the initial partition
  - `P_INITIAL` - Name of the initial partition
  - `VALUES LESS THAN` - Upper boundary for the range partition
  - `DATE '2024-01-01'` - Literal date value defining the boundary

### Create Indexes

```sql
-- Local index on COMMITTED_AT (partition key) for efficient time-based queries
CREATE INDEX TIME_PART_DATA_COMMITTED_IDX ON TIME_PARTITIONED_DATA (COMMITTED_AT) LOCAL;

-- Index on STATUS for filtering queries
CREATE INDEX TIME_PART_DATA_STATUS_IDX ON TIME_PARTITIONED_DATA (STATUS);

-- Composite index for status and time queries
CREATE INDEX TIME_PART_DATA_STATUS_TIME_IDX ON TIME_PARTITIONED_DATA (STATUS, COMMITTED_AT) LOCAL;
```

**How it works:**
- `CREATE INDEX` - Creates a database index for faster queries
- `TIME_PART_DATA_COMMITTED_IDX` - Name of the index
- `ON TIME_PARTITIONED_DATA` - The table being indexed
- `(COMMITTED_AT)` - The column(s) being indexed
- `LOCAL` - Creates a local index, meaning each partition has its own index segment
  - Without `LOCAL`, an index would be global and span across all partitions
- `(STATUS, COMMITTED_AT)` - Composite index on multiple columns, ordered as listed

### Query Partition Information

```sql
SELECT table_name, partition_name, high_value, num_rows
FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA'
ORDER BY partition_position;
```

**How it works:**
- `SELECT` - Standard query command
- `user_tab_partitions` - Oracle data dictionary view with information about all partitioned tables
- Columns selected:
  - `table_name` - Name of the table
  - `partition_name` - Name of the partition
  - `high_value` - Upper boundary of the partition range
  - `num_rows` - Estimated number of rows in the partition
- `WHERE table_name = 'TIME_PARTITIONED_DATA'` - Filters to only our table
- `ORDER BY partition_position` - Sorts results by the partition's position in the table

## Insert 5 Days Script

**File: partition_by_time_insert_5_days.sql**

This script inserts 5 batches of data, one batch per day for the current day and the previous 4 days.

### Verify Table Structure

```sql
SELECT table_name, partitioned FROM user_tables WHERE table_name = 'TIME_PARTITIONED_DATA';
```

**How it works:**
- `SELECT` - Standard query command
- `user_tables` - Oracle data dictionary view with information about all user tables
- `partitioned` - Column indicating if the table is partitioned (YES/NO)
- `WHERE table_name = 'TIME_PARTITIONED_DATA'` - Filters to only our table

### PL/SQL Block for Inserting 5 Days of Data

```sql
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
      INSERT INTO TIME_PARTITIONED_DATA (ID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT)
      VALUES (
        v_next_id + i,
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
```

**How it works:**
- `DECLARE` - Begins the declaration section of a PL/SQL block
- Variable declarations:
  - `CONSTANT PLS_INTEGER` - Integer constant that cannot be changed
  - `v_day_offset` - Loop counter for days (0 to 4)
  - `v_commit_time` - The date/time to use for each batch
  - `v_next_id` - Starting ID for each batch
- `BEGIN`/`END` - Defines the executable section of a PL/SQL block
- `FOR v_day_offset IN 0..4 LOOP` - Loop 5 times (0,1,2,3,4)
- `TRUNC(SYSDATE)` - Truncates current date to midnight (removes time component)
- `- v_day_offset` - Subtracts days (0,1,2,3,4) to get today and 4 previous days
- `+ (10/24)` - Adds 10 hours (10/24 of a day) to set time to 10:00 AM
- `SELECT NVL(MAX(ID), 0) + 1 INTO v_next_id` - Gets next available ID:
  - `MAX(ID)` - Finds highest existing ID
  - `NVL(..., 0)` - Returns 0 if no IDs exist
  - `+ 1` - Adds 1 for the next ID
  - `INTO v_next_id` - Stores result in the variable
- `DBMS_OUTPUT.PUT_LINE` - Outputs text messages to the console
- Nested `FOR i IN 0..(v_rows_per_batch - 1) LOOP` - Loops for each row in the batch
- `INSERT INTO ... VALUES` - Standard SQL insert command
- `COMMIT` - Saves the transaction to the database
- `/` - Executes the PL/SQL block

## Drop Old Partitions Script

**File: partition_by_time_drop_old_partitions.sql**

This script identifies and drops all partitions except the newest daily partition and the initial partition.

### Find Latest Partition

```sql
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
```

**How it works:**
- Variable declarations:
  - `v_latest_partition` - Will store the name of the latest partition
  - `v_max_position` - Will store the position number of the latest partition
  - `v_dropped_count` - Counter for dropped partitions, initialized to 0
- Subquery to find latest partition:
  - Inner query sorts partitions in descending order by position
  - `WHERE ROWNUM = 1` - Outer query limits to just the first row (highest position)
  - `INTO v_latest_partition, v_max_position` - Stores results in variables

### Drop All Partitions Except Latest and Initial

```sql
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
```

**How it works:**
- `FOR rec IN (SELECT ...)` - Cursor FOR loop that automatically:
  - Declares a record variable (`rec`)
  - Opens a cursor for the query
  - Fetches each row into `rec`
  - Closes cursor when done
- Query filters:
  - `partition_name != v_latest_partition` - Excludes the latest partition
  - `partition_name != 'P_INITIAL'` - Excludes the initial partition
- `EXECUTE IMMEDIATE` - Executes dynamic SQL for each partition
- `'ALTER TABLE ... DROP PARTITION ' || rec.partition_name` - Dynamic SQL to drop the partition
- `v_dropped_count := v_dropped_count + 1` - Increments the counter
- Nested `BEGIN`/`EXCEPTION`/`END` - Exception handling for each iteration
- `WHEN OTHERS THEN` - Catches any error
- `SQLERRM` - Gets the error message text
- `COMMIT` - Finalizes all changes

## Drop All 5 Days Script

**File: partition_by_time_drop_all_5_days.sql**

This script drops all daily partitions, keeping only the initial partition P_INITIAL.

### Main PL/SQL Block

```sql
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
```

**How it works:**
- Similar to the drop_old_partitions script, except:
  - No variable to track the latest partition
  - Simpler filter condition: only excludes P_INITIAL
  - Will drop ALL other partitions regardless of position or date

### Optional Truncate Initial Partition

```sql
-- Optional: If you ever inserted rows into P_INITIAL (older than its boundary),
-- you can also truncate that partition. This is normally unnecessary in the demo.
-- Uncomment the following block only if you know what you're doing:
--
-- BEGIN
--   EXECUTE IMMEDIATE 'ALTER TABLE TIME_PARTITIONED_DATA TRUNCATE PARTITION P_INITIAL';
--   DBMS_OUTPUT.PUT_LINE('Truncated P_INITIAL partition.');
-- END;
-- /
```

**How it works:**
- Commented-out code that can be uncommented if needed
- `TRUNCATE PARTITION` - Quickly removes all rows from a partition (faster than DELETE)
- This is optional and only needed if data was inserted into the initial partition

## Wrapper Scripts

### Run All Tests

**File: run_partition_time_test.sql**

This script runs the setup and insert scripts in sequence.

```sql
PROMPT === Running partition_by_time_setup.sql ===
@/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/partition_by_time_setup.sql

PROMPT === Running partition_by_time_insert_5_days.sql ===
@/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/partition_by_time_insert_5_days.sql

-- Uncomment the following lines if you want to also run these scripts:

-- PROMPT === Running partition_by_time_drop_old_partitions.sql ===
-- @/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/partition_by_time_drop_old_partitions.sql

-- PROMPT === Running partition_by_time_drop_all_5_days.sql ===
-- @/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/partition_by_time_drop_all_5_days.sql
```

**How it works:**
- `PROMPT` - Displays a message in the console
- `@/path/to/script.sql` - Executes another SQL script from the specified path
  - The `@` symbol is SQL*Plus/SQLcl syntax for running a script
- Commented-out sections can be uncommented to also run those scripts

### Drop Old Partitions Test

**File: run_drop_old_partitions_test.sql**

This script runs the setup, insert, and drop_old_partitions scripts in sequence.

```sql
PROMPT === Running partition_by_time_setup.sql ===
@/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/partition_by_time_setup.sql

PROMPT === Running partition_by_time_insert_5_days.sql ===
@/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/partition_by_time_insert_5_days.sql

PROMPT === Running partition_by_time_drop_old_partitions.sql ===
@/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/partition_by_time_drop_old_partitions.sql
```

**How it works:**
- Similar to the run_partition_time_test.sql script
- Runs the scripts in sequence to:
  1. Set up the table structure
  2. Insert 5 days of test data
  3. Drop all partitions except the latest day and P_INITIAL

## Running the Scripts

To execute any of these scripts against an Oracle database, use SQL*Plus or SQLcl with the following syntax:

```
sql username/password@hostname:port/service @/path/to/script.sql
```

For example:

```
sql LEARN1/Welcome@localhost:1521/XE @/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/run_partition_time_test.sql
```

This command connects to the Oracle database and runs the specified script.