-- Rerunnable setup script to prepare a base table and a materialized view for uninterrupted reads during bulk reloads
--
-- Scenario:
-- - Clients always query the materialized view (MV)
-- - A daily bulk process truncates and reloads the base table
-- - After load completes, MV is refreshed (COMPLETE, ATOMIC) so clients only see new data after refresh commits
--
-- How it guarantees no interruption:
-- - During table TRUNCATE/INSERT, MV still holds the previous data set
-- - During atomic COMPLETE refresh, readers see the old MV data until the refresh transaction commits
--
-- Object names (adjust if needed):
--   Base table            : BULK_DATA
--   Materialized View     : MV_BULK_DATA
--
-- Notes:
-- - Designed for Oracle Database (SQL*Plus/SQLcl compatible)
-- - This script is safe to run multiple times
--
PROMPT === Drop materialized view if it exists ===
BEGIN
  EXECUTE IMMEDIATE 'DROP MATERIALIZED VIEW MV_BULK_DATA';
EXCEPTION
  WHEN OTHERS THEN
    -- ORA-12003: materialized view does not exist
    IF SQLCODE NOT IN (-12003, -942) THEN RAISE; END IF;
END;
/

PROMPT === Drop base table if it exists ===
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE BULK_DATA PURGE';
EXCEPTION
  WHEN OTHERS THEN
    -- ORA-00942: table or view does not exist
    IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

PROMPT === Create base table ===
CREATE TABLE BULK_DATA (
  ID            NUMBER PRIMARY KEY,
  DATA_VALUE    VARCHAR2(200),
  DESCRIPTION   VARCHAR2(500),
  STATUS        VARCHAR2(50),
  CREATED_AT    DATE
);

PROMPT === Create materialized view (read target for clients) ===
CREATE MATERIALIZED VIEW MV_BULK_DATA
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT
  ID,
  DATA_VALUE,
  DESCRIPTION,
  STATUS,
  CREATED_AT
FROM BULK_DATA;

PROMPT === Objects created ===
PROMPT - Table             : BULK_DATA
PROMPT - Materialized View : MV_BULK_DATA

-- Optional quick sanity check
SELECT COUNT(*) AS BASE_TABLE_COUNT FROM BULK_DATA;
SELECT COUNT(*) AS MV_COUNT FROM MV_BULK_DATA;
