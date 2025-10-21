-- Simulate a daily bulk reload with uninterrupted reads via a materialized view
--
-- Steps performed:
-- 1) TRUNCATE base table BULK_DATA
-- 2) INSERT a new batch of rows with a single, consistent CREATED_AT timestamp
-- 3) COMMIT the load
-- 4) Atomically COMPLETE refresh the MV (MV_BULK_DATA) so readers only see the new data after commit
--
-- Designed for Oracle Database; SQL*Plus/SQLcl compatible.
-- Safe to run multiple times.
--
SET SERVEROUTPUT ON
DECLARE
  l_bulk_count  PLS_INTEGER := 1000000;  -- Adjust batch size as needed
  -- Use Thailand time (Asia/Bangkok) for CREATED_AT; apply the same timestamp to the entire batch
  l_created_at  DATE := TO_DATE(TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS');
  -- Timing markers
  l_ins_start      TIMESTAMP;
  l_ins_end        TIMESTAMP;
  l_commit_start   TIMESTAMP;
  l_commit_end     TIMESTAMP;
  l_refresh_start  TIMESTAMP;
  l_refresh_end    TIMESTAMP;

  -- Helper to convert a TIMESTAMP difference to total seconds (with fractional part)
  FUNCTION seconds_between(p_start IN TIMESTAMP, p_end IN TIMESTAMP) RETURN NUMBER IS
    l_diff INTERVAL DAY TO SECOND;
  BEGIN
    l_diff := p_end - p_start;
    RETURN (EXTRACT(DAY    FROM l_diff) * 86400)
         + (EXTRACT(HOUR   FROM l_diff) * 3600)
         + (EXTRACT(MINUTE FROM l_diff) * 60)
         +  EXTRACT(SECOND FROM l_diff);
  END;
BEGIN
  DBMS_OUTPUT.PUT_LINE('Truncating BULK_DATA ...');
  EXECUTE IMMEDIATE 'TRUNCATE TABLE BULK_DATA';

  DBMS_OUTPUT.PUT_LINE('Inserting ' || l_bulk_count || ' rows with CREATED_AT = ' || TO_CHAR(l_created_at, 'YYYY-MM-DD HH24:MI:SS'));
  l_ins_start := SYSTIMESTAMP;
  INSERT /*+ APPEND */ INTO BULK_DATA (ID, DATA_VALUE, DESCRIPTION, STATUS, CREATED_AT)
  SELECT
    LEVEL AS ID,
    'VAL_' || TO_CHAR(LEVEL) AS DATA_VALUE,
    'Generated row #' || TO_CHAR(LEVEL) AS DESCRIPTION,
    CASE WHEN MOD(LEVEL, 10) = 0 THEN 'INACTIVE' ELSE 'ACTIVE' END AS STATUS,
    l_created_at AS CREATED_AT
  FROM dual
  CONNECT BY LEVEL <= l_bulk_count;
  l_ins_end := SYSTIMESTAMP;

  l_commit_start := SYSTIMESTAMP;
  COMMIT;
  l_commit_end := SYSTIMESTAMP;
  DBMS_OUTPUT.PUT_LINE('Insert committed. Refreshing MV_BULK_DATA (COMPLETE, ATOMIC) ...');

  -- Atomic COMPLETE refresh ensures readers see the old data until the refresh commits
  l_refresh_start := SYSTIMESTAMP;
  DBMS_MVIEW.REFRESH(
    list           => 'MV_BULK_DATA',
    method         => 'C',            -- COMPLETE
    atomic_refresh => TRUE
  );
  l_refresh_end := SYSTIMESTAMP;

  DBMS_OUTPUT.PUT_LINE('Refresh complete.');

  DBMS_OUTPUT.PUT_LINE('=== Timing report (seconds) ===');
  DBMS_OUTPUT.PUT_LINE('Insert duration (s): ' || TO_CHAR(seconds_between(l_ins_start, l_ins_end), 'FM9999990D999999'));
  DBMS_OUTPUT.PUT_LINE('Commit duration (s): ' || TO_CHAR(seconds_between(l_commit_start, l_commit_end), 'FM9999990D999999'));
  DBMS_OUTPUT.PUT_LINE('Refresh duration (s): ' || TO_CHAR(seconds_between(l_refresh_start, l_refresh_end), 'FM9999990D999999'));
  DBMS_OUTPUT.PUT_LINE('Total [insert start -> refresh end] (s): ' || TO_CHAR(seconds_between(l_ins_start, l_refresh_end), 'FM9999990D999999'));
END;
/

PROMPT === Post-refresh checks ===
SELECT COUNT(*) AS BASE_TABLE_COUNT, TO_CHAR(MAX(CREATED_AT), 'YYYY-MM-DD HH24:MI:SS') AS BASE_MAX_CREATED_AT FROM BULK_DATA;
SELECT COUNT(*) AS MV_COUNT, TO_CHAR(MAX(CREATED_AT), 'YYYY-MM-DD HH24:MI:SS') AS MV_MAX_CREATED_AT FROM MV_BULK_DATA;