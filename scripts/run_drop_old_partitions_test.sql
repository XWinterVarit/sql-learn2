-- Oracle SQL script wrapper for testing the drop old partitions functionality
-- Execute with:
-- sql LEARN1/Welcome@localhost:1521/XE @/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/run_drop_old_partitions_test.sql
--
-- This script will:
-- 1. Set up the table structure (partition_by_time_setup.sql)
-- 2. Insert test data with single commit datetime (partition_by_time_insert_single_commit.sql)
-- 3. Drop older partitions, keeping only the latest day (partition_by_time_drop_old_partitions.sql)

PROMPT === Running partition_by_time_setup.sql ===
@/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/partition_by_time_setup.sql

PROMPT === Running partition_by_time_insert_single_commit.sql ===
@/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/partition_by_time_insert_single_commit.sql

PROMPT === Running partition_by_time_drop_old_partitions.sql ===
@/Users/cheevaritrodnuson/GolandProjects/sql-learn2/scripts/partition_by_time_drop_old_partitions.sql

PROMPT === All scripts completed successfully ===
-- EXIT command is commented out to prevent issues in SQLcl
-- Uncomment if running in SQL*Plus and you want to exit
-- EXIT