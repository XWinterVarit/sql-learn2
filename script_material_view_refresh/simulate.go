package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	"sql-learn2/bulkinsert"
)

// TableStats represents row count and max created timestamp for a table.
type TableStats struct {
	Count      int            `db:"CNT"`
	MaxCreated sql.NullString `db:"MAX_CREATED"`
}

// timingReport holds duration measurements for different operations.
type timingReport struct {
	InsertDuration  time.Duration
	CommitDuration  time.Duration
	RefreshDuration time.Duration
	TotalDuration   time.Duration
}

// truncateTable truncates the BULK_DATA table.
func truncateTable(ctx context.Context, db *sqlx.DB) error {
	log.Println("Truncating BULK_DATA ...")
	_, err := db.ExecContext(ctx, "TRUNCATE TABLE BULK_DATA")
	if err != nil {
		return fmt.Errorf("truncate failed: %w", err)
	}
	return nil
}

// generateBatchData creates a batch of test data rows with separate column names and data.
// Returns column names and a 2D array where each inner array represents one row's values.
func generateBatchData(batchStart, batchCount int, createdAtStr string) ([]string, [][]interface{}) {
	// Define column names once
	columnNames := []string{"ID", "DATA_VALUE", "DESCRIPTION", "STATUS", "CREATED_AT"}

	// Create data rows
	rows := make([][]interface{}, batchCount)

	for i := 0; i < batchCount; i++ {
		rowNum := batchStart + i
		status := "ACTIVE"
		if rowNum%10 == 0 {
			status = "INACTIVE"
		}

		// Each row is a slice of values matching the column order
		rows[i] = []interface{}{
			rowNum,                                   // ID
			fmt.Sprintf("VAL_%d", rowNum),            // DATA_VALUE
			fmt.Sprintf("Generated row #%d", rowNum), // DESCRIPTION
			status,                                   // STATUS
			createdAtStr,                             // CREATED_AT
		}
	}

	return columnNames, rows
}

// insertBulkData inserts bulk data using the simplest bulkinsert API.
func insertBulkData(ctx context.Context, db *sqlx.DB, bulkCount int, createdAtStr string) (time.Duration, error) {
	log.Printf("Inserting %d rows with CREATED_AT = %s", bulkCount, createdAtStr)

	// Generate column names and row-oriented data
	columnNames, rows := generateBatchData(1, bulkCount, createdAtStr)

	// Use bulkinsert.InsertStructs which accepts row-oriented data directly (no transposition needed)
	insDuration, err := bulkinsert.InsertStructs(ctx, db, "BULK_DATA", columnNames, rows)
	if err != nil {
		return 0, err
	}

	return insDuration, nil
}

// commitTransaction commits the given transaction and returns the duration.
func commitTransaction(tx *sql.Tx) (time.Duration, error) {
	log.Println("Committing transaction...")
	commitStart := time.Now()
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit failed: %w", err)
	}
	return time.Since(commitStart), nil
}

// refreshMaterializedView refreshes the MV_BULK_DATA materialized view.
func refreshMaterializedView(ctx context.Context, db *sqlx.DB) (time.Duration, error) {
	log.Println("Insert committed. Refreshing MV_BULK_DATA (COMPLETE, ATOMIC) ...")
	refreshStart := time.Now()

	refreshSQL := `
BEGIN
  DBMS_MVIEW.REFRESH(
    list           => 'MV_BULK_DATA',
    method         => 'C',
    atomic_refresh => TRUE
  );
END;`

	result, err := db.ExecContext(ctx, refreshSQL)
	if err != nil {
		return 0, fmt.Errorf("refresh materialized view failed: %w", err)
	}
	// Check if any rows were affected
	if result != nil {
		rowsAffected, _ := result.RowsAffected()
		log.Printf("Refresh result - rows affected: %d", rowsAffected)
	}

	log.Println("Refresh complete.")
	return time.Since(refreshStart), nil
}

// queryTableStats queries and returns statistics for a given table.
func queryTableStats(ctx context.Context, db *sqlx.DB, tableName string) (*TableStats, error) {
	var stats TableStats
	query := fmt.Sprintf("SELECT COUNT(*) AS CNT, TO_CHAR(MAX(CREATED_AT), 'YYYY-MM-DD HH24:MI:SS') AS MAX_CREATED FROM %s", tableName)
	err := db.GetContext(ctx, &stats, query)
	if err != nil {
		return nil, fmt.Errorf("query %s failed: %w", tableName, err)
	}
	return &stats, nil
}

// logTimingReport logs the timing report for all operations.
func logTimingReport(report *timingReport) {
	log.Println("=== Timing report (seconds) ===")
	log.Printf("Insert duration (s):  %.6f", report.InsertDuration.Seconds())
	log.Printf("Commit duration (s):  %.6f", report.CommitDuration.Seconds())
	log.Printf("Refresh duration (s): %.6f", report.RefreshDuration.Seconds())
	log.Printf("Total [insert start -> refresh end] (s): %.6f", report.TotalDuration.Seconds())
}

// logTableStats logs the statistics for a table.
func logTableStats(stats *TableStats, tableName string) {
	maxCreated := ""
	if stats.MaxCreated.Valid {
		maxCreated = stats.MaxCreated.String
	}
	log.Printf("%s_COUNT: %d, %s_MAX_CREATED_AT: %s", tableName, stats.Count, tableName, maxCreated)
}

// validatePostRefresh performs post-refresh validation checks.
func validatePostRefresh(ctx context.Context, db *sqlx.DB) error {
	log.Println("=== Post-refresh checks ===")

	baseStats, err := queryTableStats(ctx, db, "BULK_DATA")
	if err != nil {
		return err
	}
	logTableStats(baseStats, "BASE_TABLE")

	mvStats, err := queryTableStats(ctx, db, "MV_BULK_DATA")
	if err != nil {
		return err
	}
	logTableStats(mvStats, "MV")

	return nil
}

// RunBulkLoadSimulation performs the bulk load and materialized view refresh simulation.
// It replicates the functionality of simulate_bulk_load_and_refresh.sql in Go code.
//
// Steps performed:
// 1. TRUNCATE base table BULK_DATA
// 2. INSERT a batch of rows with a single, consistent CREATED_AT timestamp (Thailand timezone)
// 3. COMMIT the load
// 4. Atomically COMPLETE refresh the MV (MV_BULK_DATA) so readers only see the new data after commit
// 5. Validate results by checking row counts and timestamps
//
// Parameters:
//   - ctx: context for database operations
//   - db: sqlx database connection
//   - bulkCount: number of rows to insert
//
// Returns error if any step fails.
func RunBulkLoadSimulation(ctx context.Context, db *sqlx.DB, bulkCount int) error {
	// Get Thailand time for CREATED_AT
	loc, err := time.LoadLocation("Asia/Bangkok")
	if err != nil {
		return fmt.Errorf("failed to load timezone: %w", err)
	}
	createdAt := time.Now().In(loc)
	createdAtStr := createdAt.Format("2006-01-02 15:04:05")

	log.Println("=== Starting bulk load simulation ===")

	// Step 1: Truncate BULK_DATA table
	if err := truncateTable(ctx, db); err != nil {
		return err
	}

	// Step 2: Insert bulk data and commit
	operationStart := time.Now()
	insDuration, err := insertBulkData(ctx, db, bulkCount, createdAtStr)
	if err != nil {
		return err
	}
	commitDuration := time.Since(operationStart) - insDuration

	// Step 3: Refresh materialized view
	refreshDuration, err := refreshMaterializedView(ctx, db)
	if err != nil {
		return err
	}

	// Step 4: Print timing report
	totalDuration := time.Since(operationStart)
	report := &timingReport{
		InsertDuration:  insDuration,
		CommitDuration:  commitDuration,
		RefreshDuration: refreshDuration,
		TotalDuration:   totalDuration,
	}
	logTimingReport(report)

	// Step 5: Post-refresh validation
	if err := validatePostRefresh(ctx, db); err != nil {
		return err
	}

	log.Println("=== Simulation completed successfully ===")
	return nil
}
