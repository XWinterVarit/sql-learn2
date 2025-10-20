-- Oracle SQL script wrapper for running all partition by time tests with a single command
-- Execute with:
-- sql LEARN1/Welcome@localhost:1521/XE @/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/run_partition_time_test.sql
--
-- This script will:
-- 1. Set up the table structure (partition_by_time_setup.sql)
-- 2. Insert test data for 5 days (partition_by_time_insert_5_days.sql)
--
-- Note: You can uncomment the additional operations if needed

PROMPT === Running partition_by_time_setup.sql ===
@/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/partition_by_time_setup.sql

PROMPT === Running partition_by_time_insert_5_days.sql ===
@/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/partition_by_time_insert_5_days.sql

-- Uncomment the following lines if you want to also run these scripts:

-- PROMPT === Running partition_by_time_drop_old_partitions.sql ===
-- @/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/partition_by_time_drop_old_partitions.sql

-- PROMPT === Running partition_by_time_drop_all_5_days.sql ===
-- @/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/partition_by_time_drop_all_5_days.sql

PROMPT === All scripts completed successfully ===
-- EXIT command is commented out to prevent issues in SQLcl
-- Uncomment if running in SQL*Plus and you want to exit
-- EXIT