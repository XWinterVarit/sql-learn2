#!/usr/bin/env bash
# Run the materialized view bulk-refresh test using provided Oracle connection info.
# Usage:
#   ORA_USER=LEARN1 ORA_PASS=Welcome ORA_HOST=localhost ORA_PORT=1521 ORA_SERVICE=XE ./scripts/run_mv_refresh_test.sh
# Notes:
# - Tries SQLcl (sql) first, then SQL*Plus (sqlplus). If neither is found, exits non‑zero.
# - Spools output to logs/mv_refresh_test_YYYYmmdd_HHMMSS.log
set -euo pipefail

# Resolve connection params with defaults (match existing conventions)
ORA_USER="${ORA_USER:-LEARN1}"
ORA_PASS="${ORA_PASS:-Welcome}"
ORA_HOST="${ORA_HOST:-localhost}"
ORA_PORT="${ORA_PORT:-1521}"
ORA_SERVICE="${ORA_SERVICE:-XE}"

ROOT_DIR="$(cd "$(dirname "$0")"/.. && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"
TS="$(date '+%Y%m%d_%H%M%S')"
LOG_FILE="${LOG_DIR}/mv_refresh_test_${TS}.log"
TMP_SQL="${LOG_DIR}/_mv_refresh_tmp_${TS}.sql"

SQL_SETUP="${ROOT_DIR}/script_material_view_refresh/setup_materialized_view.sql"
SQL_SIMULATE="${ROOT_DIR}/script_material_view_refresh/simulate_bulk_load_and_refresh.sql"

if [[ ! -f "$SQL_SETUP" || ! -f "$SQL_SIMULATE" ]]; then
  echo "ERROR: Required SQL scripts not found." >&2
  exit 2
fi

# Build a temporary SQL driver script that spools output and runs the test scripts
cat >"${TMP_SQL}" <<EOSQL
SET ECHO ON FEEDBACK ON SERVEROUTPUT ON LINESIZE 200 PAGESIZE 100 TRIMSPOOL ON VERIFY OFF
SPOOL ${LOG_FILE}
PROMPT === Running setup_materialized_view.sql ===
@${SQL_SETUP}
PROMPT === Running simulate_bulk_load_and_refresh.sql ===
@${SQL_SIMULATE}
SPOOL OFF
EXIT
EOSQL

CONN_STRING="${ORA_USER}/${ORA_PASS}@${ORA_HOST}:${ORA_PORT}/${ORA_SERVICE}"

run_with_sqlcl() {
  echo "[INFO] Using SQLcl (sql)"
  sql -S "$CONN_STRING" @"${TMP_SQL}"
}

run_with_sqlplus() {
  echo "[INFO] Using SQL*Plus (sqlplus)"
  sqlplus -s "$CONN_STRING" @"${TMP_SQL}"
}

if command -v sql >/dev/null 2>&1; then
  run_with_sqlcl || { echo "[WARN] SQLcl failed, trying SQL*Plus"; command -v sqlplus >/dev/null 2>&1 && run_with_sqlplus; }
elif command -v sqlplus >/dev/null 2>&1; then
  run_with_sqlplus
else
  echo "ERROR: Neither SQLcl (sql) nor SQL*Plus (sqlplus) found in PATH." >&2
  echo "Generated the SQL driver at: ${TMP_SQL}" >&2
  echo "You can run it manually with your Oracle client." >&2
  exit 3
fi

echo "[OK] Test completed. Log saved to: ${LOG_FILE}"
