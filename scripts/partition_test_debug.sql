-- Debug script to test partition by time with string key
-- This simplified script helps diagnose partition key mapping issues

-- First, check the table structure
PROMPT === Checking table structure ===
SELECT table_name, partitioned FROM user_tables WHERE table_name = 'TIME_PARTITIONED_DATA';

-- Check partitioning key column
PROMPT === Checking partition key column ===
SELECT column_name, data_type, data_length FROM user_tab_columns 
WHERE table_name = 'TIME_PARTITIONED_DATA' AND column_name = 'COMMITTED_AT';

-- Check existing partitions
PROMPT === Checking existing partitions ===
SELECT partition_name, high_value FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA'
ORDER BY partition_position;

-- Create a test partition with current timestamp + 1 second
PROMPT === Creating a test partition ===
DECLARE
  v_commit_time TIMESTAMP := SYSTIMESTAMP;
  v_commit_str VARCHAR2(19);
  v_next_partition VARCHAR2(50);
  v_high_value VARCHAR2(25);
  v_sql VARCHAR2(1000);
BEGIN
  -- Format the timestamp as YYYYMMDD HH:MI:SS for string-based partitioning
  v_commit_str := TO_CHAR(v_commit_time, 'YYYYMMDD HH24:MI:SS');
  
  -- Create next higher bound for partition (add 1 second)
  v_high_value := TO_CHAR(v_commit_time + INTERVAL '1' SECOND, 'YYYYMMDD HH24:MI:SS');
  
  -- Create a unique partition name based on timestamp
  v_next_partition := 'P_' || TO_CHAR(v_commit_time, 'YYYYMMDDHH24MISS');
  
  -- Create a new partition for this exact second
  v_sql := 'ALTER TABLE TIME_PARTITIONED_DATA ADD PARTITION ' || 
           v_next_partition || 
           ' VALUES LESS THAN (''' || v_high_value || ''')';
           
  -- Display the SQL and values for debugging
  DBMS_OUTPUT.PUT_LINE('Creating partition with:');
  DBMS_OUTPUT.PUT_LINE('Current timestamp string: ' || v_commit_str);
  DBMS_OUTPUT.PUT_LINE('Partition name: ' || v_next_partition);
  DBMS_OUTPUT.PUT_LINE('High value: ' || v_high_value);
  DBMS_OUTPUT.PUT_LINE('SQL: ' || v_sql);
  
  -- Execute the partition creation
  BEGIN
    EXECUTE IMMEDIATE v_sql;
    DBMS_OUTPUT.PUT_LINE('Successfully created partition: ' || v_next_partition);
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error creating partition: ' || SQLERRM);
      -- Continue anyway
  END;
END;
/

-- Check partitions after creation 
PROMPT === Checking partitions after creation ===
SELECT partition_name, high_value FROM user_tab_partitions
WHERE table_name = 'TIME_PARTITIONED_DATA'
ORDER BY partition_position;

-- Try inserting a single row with explicit timestamp string
PROMPT === Attempting to insert a single row ===
DECLARE
  v_commit_time TIMESTAMP := SYSTIMESTAMP;
  v_commit_str VARCHAR2(19);
BEGIN
  -- Format timestamp as YYYYMMDD HH:MI:SS
  v_commit_str := TO_CHAR(v_commit_time, 'YYYYMMDD HH24:MI:SS');
  
  -- Display the exact string we're using
  DBMS_OUTPUT.PUT_LINE('Inserting with timestamp string: ' || v_commit_str);
  
  -- Insert one record
  BEGIN
    INSERT INTO TIME_PARTITIONED_DATA (ID, DATA_VALUE, DESCRIPTION, STATUS, COMMITTED_AT)
    VALUES (1, 'TEST_DATA', 'Test record', 'ACTIVE', v_commit_str);
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Insert successful');
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error on insert: ' || SQLERRM);
      ROLLBACK;
  END;
END;
/

-- Check which partition contains the data
PROMPT === Checking data distribution ===
SELECT p.partition_name, COUNT(*) as row_count
FROM TIME_PARTITIONED_DATA t
JOIN user_tab_partitions p ON p.table_name = 'TIME_PARTITIONED_DATA'
GROUP BY p.partition_name
ORDER BY p.partition_name;

-- Display the data
PROMPT === Display data ===
SELECT ID, DATA_VALUE, COMMITTED_AT FROM TIME_PARTITIONED_DATA;