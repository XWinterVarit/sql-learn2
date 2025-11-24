# POC_Lock - Oracle Transaction Locking Demo

This directory contains both SQL and Go implementations demonstrating Oracle's row-level locking behavior across related tables.

## Scenario

Two concurrent flows demonstrate transaction and locking behavior:

### CHAIN Flow
1. SELECT ... FOR UPDATE on table A (acquires row lock)
2. Wait 10 seconds
3. UPDATE table B
4. Wait 10 seconds  
5. UPDATE table C
6. COMMIT

### EARLY Flow
1. Wait 2 seconds
2. UPDATE table C
3. COMMIT

## Expected Behavior

In Oracle, row-level locks are per row, per table. **Locking a row in table A does not block updates on unrelated rows in table C.**

Therefore:
- The EARLY flow updates C at t≈2s and commits immediately (not blocked by the lock on A)
- The CHAIN flow updates C again at t≈20s (overwrites EARLY's update)
- Final value in C: `C1_UPDATED_BY_CHAIN` (last update wins)

---

## Go Implementation

### Files

- **main.go** - Entry point, connection handling, goroutine orchestration
- **setup.go** - Table creation, cleanup, and display functions
- **logger.go** - Event logging with autonomous transaction semantics
- **chain_flow.go** - CHAIN goroutine implementation
- **early_flow.go** - EARLY goroutine implementation

### Prerequisites

- Go 1.16 or later
- Oracle database accessible
- Oracle credentials

### Build

```bash
cd POC_Lock
go build -o lockdemo .
```

### Run

#### Using default connection (LEARN1/Welcome@localhost:1521/XE)
```bash
./lockdemo
```

#### Using command-line flags
```bash
./lockdemo -user LEARN1 -pass Welcome -host localhost -port 1521 -service XE
```

#### Using environment variables
```bash
export ORA_USER=LEARN1
export ORA_PASS=Welcome
export ORA_HOST=localhost
export ORA_PORT=1521
export ORA_SERVICE=XE
./lockdemo
```

### Expected Output

```
✓ Connected to Oracle
Step 1: Cleaning up and creating tables A, B, C, EVENT_LOG...
✓ Tables created and sample data inserted
Step 2: Launching CHAIN and EARLY goroutines...
✓ Both flows completed

=== Event Log (ordered by time) ===
  2025-11-24 19:07:00.123  CHAIN     BEGIN: select for update on A
  2025-11-24 19:07:00.125  CHAIN     Locked A.id=1; sleeping 10s
  2025-11-24 19:07:00.126  EARLY     BEGIN: sleeping 2s before updating C
  2025-11-24 19:07:02.128  EARLY     Updating C.id=1
  2025-11-24 19:07:02.130  EARLY     Committing
  2025-11-24 19:07:02.132  EARLY     DONE
  2025-11-24 19:07:10.127  CHAIN     Updating B.id=1
  2025-11-24 19:07:10.129  CHAIN     B updated; sleeping 10s
  2025-11-24 19:07:20.131  CHAIN     Updating C.id=1
  2025-11-24 19:07:20.133  CHAIN     Committing
  2025-11-24 19:07:20.135  CHAIN     DONE

=== Final rows in table C ===
  ID: 1  B_ID: 1  DATA: C1_UPDATED_BY_CHAIN

✓ Demo completed successfully
```

### Key Points

1. **Goroutines replace DBMS_SCHEDULER** - Two goroutines run concurrently, simulating separate database sessions
2. **Autonomous logging** - EventLogger uses separate transactions to log events without affecting main flows
3. **Transaction isolation** - CHAIN holds a lock on row A within its transaction; EARLY updates C independently
4. **No special privileges** - Unlike the SQL script, this Go version requires only standard SQL (no DBMS_LOCK, DBMS_SCHEDULER, or CREATE JOB privileges)
5. **Portable and readable** - Multiple files in a single package for better code organization

---

## SQL Implementation

### File

- **lock_demo.sql** - Complete Oracle SQL/PLSQL script

### Prerequisites

- Oracle database accessible
- User privileges: `EXECUTE` on `DBMS_LOCK`, `EXECUTE` on `DBMS_SCHEDULER`, and `CREATE JOB`

### Grant Required Privileges

Connect as DBA and run:
```sql
GRANT EXECUTE ON DBMS_LOCK TO LEARN1;
GRANT EXECUTE ON DBMS_SCHEDULER TO LEARN1;
GRANT CREATE JOB TO LEARN1;
```

Or as a one-liner:
```bash
echo "GRANT EXECUTE ON DBMS_LOCK TO LEARN1; GRANT EXECUTE ON DBMS_SCHEDULER TO LEARN1; GRANT CREATE JOB TO LEARN1; EXIT;" | sql -S SYSTEM/<system_password>@localhost:1521/XE
```

### Run

Using SQLcl:
```bash
sql -S LEARN1/Welcome@localhost:1521/XE @/path/to/POC_Lock/lock_demo.sql
```

Using SQL*Plus:
```bash
sqlplus -s LEARN1/Welcome@localhost:1521/XE @/path/to/POC_Lock/lock_demo.sql
```

### What the Script Does

1. Drops and recreates tables A, B, C, EVENT_LOG
2. Creates autonomous logging procedure LOG_EVENT
3. Creates procedures P_LOCK_CHAIN and P_UPDATE_C_EARLY
4. Creates and runs two DBMS_SCHEDULER jobs concurrently
5. Waits ~25 seconds for completion
6. Displays event log and final table C contents

---

## Comparison

| Aspect | SQL Script | Go Implementation |
|--------|------------|-------------------|
| **Concurrency** | DBMS_SCHEDULER jobs | Goroutines |
| **Timing** | DBMS_LOCK.SLEEP() | time.Sleep() |
| **Logging** | PRAGMA AUTONOMOUS_TRANSACTION | Separate BeginTx() per log |
| **Privileges** | Requires EXECUTE on DBMS_LOCK, DBMS_SCHEDULER, CREATE JOB | Only standard SQL |
| **Portability** | Oracle-specific | Portable Go code |
| **Code organization** | Single SQL script | Multiple Go files |

---

## Tables Created

### Table A
```sql
CREATE TABLE A (
  id   NUMBER PRIMARY KEY,
  data VARCHAR2(50)
);
```

### Table B
```sql
CREATE TABLE B (
  id   NUMBER PRIMARY KEY,
  a_id NUMBER REFERENCES A(id),
  data VARCHAR2(50)
);
```

### Table C
```sql
CREATE TABLE C (
  id   NUMBER PRIMARY KEY,
  b_id NUMBER REFERENCES B(id),
  data VARCHAR2(50)
);
```

### Table EVENT_LOG
```sql
CREATE TABLE EVENT_LOG (
  ts  TIMESTAMP(3) DEFAULT SYSTIMESTAMP,
  who VARCHAR2(50),
  msg VARCHAR2(4000)
);
```

---

## License

This is a proof-of-concept demonstration for educational purposes.
