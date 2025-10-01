-- Rerunnable Oracle SQL script to (re)create two tables (BASE_A, BASE_B)
-- with the same example fields and indexes, and a synonym BASE that points
-- to the active table. Adjust variables by search/replace if needed.
--
-- Usage:
--   - Connect as the owner schema (the synonym will be created in current schema)
--   - Change BASE_NAME if desired (default EXAMPLE)
--   - Run multiple times safely; it recreates tables and synonym.
--
-- Example schema based on example.csv:
--   ID         VARCHAR2(100)   NOT NULL
--   FIRST_NAME VARCHAR2(100)
--   LAST_NAME  VARCHAR2(100)
--   AGE        NUMBER
--   SALARY     NUMBER
--   PK on (ID)

-- === Configuration ===
-- Change BASE_NAME to your base table name (no _A/_B suffix). Keep it UPPERCASE.
-- Note: This is a simple script without bind variables; use replace-in-editor.
-- Example: BASE_NAME = 'EXAMPLE'

-- Set base name
-- Replace the next line to your base name; ensure uppercase and valid identifier
-- define BASE_NAME = EXAMPLE

-- For SQL*Plus users uncomment the following two lines and set variable
-- DEFINE BASE_NAME = EXAMPLE
-- COLUMN BASE_NAME NEW_VALUE BASE_NAME

-- We'll just use EXAMPLE as default here. To change, global replace EXAMPLE with your base name.

-- Drop and recreate the two tables with the same structure
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE EXAMPLE_A PURGE';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE EXAMPLE_B PURGE';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/

CREATE TABLE EXAMPLE_A (
  ID         VARCHAR2(100) NOT NULL,
  FIRST_NAME VARCHAR2(100),
  LAST_NAME  VARCHAR2(100),
  AGE        NUMBER,
  SALARY     NUMBER,
  CONSTRAINT EXAMPLE_A_PK PRIMARY KEY (ID)
);
/

CREATE TABLE EXAMPLE_B (
  ID         VARCHAR2(100) NOT NULL,
  FIRST_NAME VARCHAR2(100),
  LAST_NAME  VARCHAR2(100),
  AGE        NUMBER,
  SALARY     NUMBER,
  CONSTRAINT EXAMPLE_B_PK PRIMARY KEY (ID)
);
/

-- Optional additional indexes (example on LAST_NAME)
BEGIN
  EXECUTE IMMEDIATE 'CREATE INDEX EXAMPLE_A_LAST_NAME_IDX ON EXAMPLE_A(LAST_NAME)';
EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN
  EXECUTE IMMEDIATE 'CREATE INDEX EXAMPLE_B_LAST_NAME_IDX ON EXAMPLE_B(LAST_NAME)';
EXCEPTION WHEN OTHERS THEN NULL; END;
/

-- Create or replace synonym to point to the active table. Default active: EXAMPLE_A
CREATE OR REPLACE SYNONYM EXAMPLE FOR EXAMPLE_A;
/

-- To switch active manually later, you can run:
--   CREATE OR REPLACE SYNONYM EXAMPLE FOR EXAMPLE_A;  -- or EXAMPLE_B;

-- Notes:
-- - This script is owner-schema scoped. If using separate schema for synonym vs tables,
--   qualify with schema names and use CREATE OR REPLACE SYNONYM <schema>.EXAMPLE FOR <schema>.EXAMPLE_A;
-- - For public synonyms, use CREATE OR REPLACE PUBLIC SYNONYM EXAMPLE FOR <schema>.EXAMPLE_A;