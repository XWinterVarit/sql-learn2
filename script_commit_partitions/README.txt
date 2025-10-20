# Oracle Partition By Time Testing Instructions

## Setup and Test Execution

To run the partition by time tests, use the following command:

```
sql LEARN1/Welcome@localhost:1521/XE @/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/run_partition_time_test.sql
```

This will:
1. Set up the table structure (partition_by_time_setup.sql)
2. Insert test data with single commit datetime (partition_by_time_insert_single_commit.sql)

## Additional Operations

The wrapper script also includes commented-out code to:
- Drop old partitions (keeping only the most recent day)
- Drop all 5 days of partitions (full reset)

To enable these operations, edit run_partition_time_test.sql and uncomment the relevant sections.

## Script Files

- `partition_by_time_setup.sql`: Creates the partitioned table structure
- `partition_by_time_insert_single_commit.sql`: Inserts multiple rows with single commit datetime
- `partition_by_time_drop_old_partitions.sql`: Drops older partitions, keeping only the latest
- `partition_by_time_drop_all_5_days.sql`: Drops ALL daily partitions (reset)
- `run_partition_time_test.sql`: Wrapper script that executes the above scripts

## Connection Details

These scripts connect to the Oracle database with:
- Username: LEARN1
- Password: Welcome
- Host: localhost
- Port: 1521
- Service: XE