# SQL Scripts Documentation

This document explains the purpose and usage of each SQL script in the repository.

## Table of Contents
1. [partition_by_time_setup.sql](#partition_by_time_setupsql)
2. [partition_by_time_test.sql](#partition_by_time_testsql)
3. [partition_by_time_test_new.sql](#partition_by_time_test_newsql)
4. [partition_test_debug.sql](#partition_test_debugsql)
5. [partition_exchange_setup.sql](#partition_exchange_setupsql)
6. [partition_exchange_test_exchange.sql](#partition_exchange_test_exchangesql)
7. [synonym_swap_setup.sql](#synonym_swap_setupsql)
8. [check_data.sql](#check_datasql)

---

## partition_by_time_setup.sql

**Purpose**: Sets up a table with RANGE partitioning by time at second-level granularity using string timestamps.

**Key Features**:
- Creates `TIME_PARTITIONED_DATA` table with VARCHAR2(19) column for timestamp
- Implements RANGE partitioning using string-based timestamps in "YYYYMMDD HH:MI:SS" format
- Creates appropriate indexes for efficient time-based queries
- Establishes an initial partition (P_INITIAL)

**How to Run**:
```
sql USERNAME/PASSWORD@HOST:PORT/SERVICE @partition_by_time_setup.sql
```

**Expected Output**:
- Table structure created
- Initial partition created
- Indexes created
- Partition information displayed

**Prerequisites**:
- Oracle database connection
- Sufficient privileges to create tables and indexes

**Related Scripts**:
- `partition_by_time_test.sql` or `partition_by_time_test_new.sql` for testing

---

## partition_by_time_test.sql

**Purpose**: Tests time-based partition management with second-level granularity and cleanup of old data.

**Key Features**:
- Creates a partition for the current timestamp at second granularity
- Inserts test data with the latest timestamp
- Maintains table availability during data insertion
- Cleans up old data only after new data is safely inserted
- Drops old partitions to maintain only the latest data

**How to Run**:
```
sql USERNAME/PASSWORD@HOST:PORT/SERVICE @partition_by_time_test.sql
```

**Expected Output**:
- New partition created for current timestamp
- 4 records inserted into the latest partition
- Old data removed
- Verification of data and partitions after cleanup

**Prerequisites**:
- Run `partition_by_time_setup.sql` first to create the table structure

---

## partition_by_time_test_new.sql

**Purpose**: Improved version of the time partitioning test script with enhanced error handling and maintenance.

**Key Features**:
- Similar to partition_by_time_test.sql but with improved logic
- More robust partition creation and management
- Better error handling for production scenarios
- Properly formats timestamps for second-level granularity

**How to Run**:
```
sql USERNAME/PASSWORD@HOST:PORT/SERVICE @partition_by_time_test_new.sql
```

**Expected Output**:
- Similar to partition_by_time_test.sql
- Creates a partition, inserts data, removes old data, and maintains only latest batch

**Prerequisites**:
- Run `partition_by_time_setup.sql` first

---

## partition_test_debug.sql

**Purpose**: Diagnostic script for testing string-based partitioning with detailed output for debugging.

**Key Features**:
- Simplified version of partition_by_time_test.sql for diagnostics
- Displays detailed information about the partition creation process
- Attempts to insert a single row with explicit timestamp string
- Shows which partition contains the data after insertion
- Provides detailed error messages for troubleshooting

**How to Run**:
```
sql USERNAME/PASSWORD@HOST:PORT/SERVICE @partition_test_debug.sql
```

**Expected Output**:
- Detailed information about table structure
- SQL statements for partition creation
- Results of single row insertion
- Data distribution across partitions

**Prerequisites**:
- Run `partition_by_time_setup.sql` first

---

## partition_exchange_setup.sql

**Purpose**: Prepares tables for testing the EXCHANGE PARTITION pattern.

**Key Features**:
- Creates a partitioned master table (EXAMPLE_MASTER) with LIST partitioning
- Creates a compatible non-partitioned staging table (EXAMPLE_STAGING)
- Sets up realistic indexes on both tables to demonstrate proper exchange
- Configures a DEFAULT partition named PDATA for the exchange

**How to Run**:
```
sql USERNAME/PASSWORD@HOST:PORT/SERVICE @partition_exchange_setup.sql
```

**Expected Output**:
- Master and staging tables created with compatible structures
- Appropriate indexes created on both tables
- Setup verification messages

**Prerequisites**:
- Oracle database connection with privileges to create tables and indexes

**Related Scripts**:
- `partition_exchange_test_exchange.sql` for testing the exchange

---

## partition_exchange_test_exchange.sql

**Purpose**: Tests the partition exchange operation between master and staging tables.

**Key Features**:
- Rebuilds indexes on the staging table to ensure they are usable
- Loads sample data into the staging table
- Performs the actual EXCHANGE PARTITION operation
- Verifies counts before and after exchange
- Includes optional cleanup of the staging table

**How to Run**:
```
sql USERNAME/PASSWORD@HOST:PORT/SERVICE @partition_exchange_test_exchange.sql
```

**Expected Output**:
- Indexes rebuilt
- Sample rows inserted into staging
- Row counts displayed before and after exchange
- Data verification after exchange
- Optional cleanup of staging table

**Prerequisites**:
- Run `partition_exchange_setup.sql` first to create the table structures

---

## synonym_swap_setup.sql

**Purpose**: Creates two identical tables with the same structure and a synonym pointing to one of them, enabling table-swap pattern.

**Key Features**:
- Creates two tables (EXAMPLE_A, EXAMPLE_B) with identical structures
- Sets up a synonym (EXAMPLE) initially pointing to EXAMPLE_A
- Provides a foundation for zero-downtime data refreshes
- Includes optional index creation for both tables

**How to Run**:
```
sql USERNAME/PASSWORD@HOST:PORT/SERVICE @synonym_swap_setup.sql
```

**Expected Output**:
- Two identical tables created
- Synonym created pointing to EXAMPLE_A
- Optional indexes created

**Prerequisites**:
- Oracle database connection with sufficient privileges
- Being connected as the owner schema for the tables and synonym

**Usage Notes**:
- To switch active table, use: `CREATE OR REPLACE SYNONYM EXAMPLE FOR EXAMPLE_B;`
- For multi-schema setups, qualify object names with schema

---

## check_data.sql

**Purpose**: Simple utility script to check data in the TIME_PARTITIONED_DATA table.

**Key Features**:
- Basic SELECT query to verify data in the time-partitioned table

**How to Run**:
```
sql USERNAME/PASSWORD@HOST:PORT/SERVICE @check_data.sql
```

**Expected Output**:
- All rows from TIME_PARTITIONED_DATA displayed

**Prerequisites**:
- Table TIME_PARTITIONED_DATA must exist
- Run after other scripts that populate the table

---

This documentation provides an overview of all SQL scripts in the repository, explaining their purpose, features, and usage patterns. Each script is designed to demonstrate specific Oracle database techniques for data management with a focus on partitioning, data exchanges, and synonym swapping for zero-downtime operations.