package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	"sql-learn2/bulkload"
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

// commitTransaction commits the given transaction and returns the duration.
func commitTransaction(tx *sql.Tx) (time.Duration, error) {
	log.Println("Committing transaction...")
	commitStart := time.Now()
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit failed: %w", err)
	}
	return time.Since(commitStart), nil
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
//   - batchSize: rows per insert batch (if <= 0, inserts in a single batch)
//
// Returns error if any step fails.
func RunBulkLoadSimulation(ctx context.Context, db *sqlx.DB, bulkCount int, batchSize int) error {
	// Get Thailand time for CREATED_AT
	loc, err := time.LoadLocation("Asia/Bangkok")
	if err != nil {
		return fmt.Errorf("failed to load timezone: %w", err)
	}
	createdAt := time.Now().In(loc)

	log.Println("=== Starting bulk load simulation ===")

	// Execute the complete bulk load operation (truncate, insert, refresh)
	timing, err := bulkload.ExecuteBulkLoad(ctx, db, bulkCount, batchSize, createdAt)
	if err != nil {
		return err
	}

	// Print timing report
	report := &timingReport{
		InsertDuration:  timing.InsertDuration,
		CommitDuration:  timing.CommitDuration,
		RefreshDuration: timing.RefreshDuration,
		TotalDuration:   timing.TotalDuration,
	}
	logTimingReport(report)

	// Step 5: Post-refresh validation
	if err := validatePostRefresh(ctx, db); err != nil {
		return err
	}

	log.Println("=== Simulation completed successfully ===")
	return nil
}
