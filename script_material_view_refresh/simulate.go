package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

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
//   - db: database connection
//   - bulkCount: number of rows to insert
//
// Returns error if any step fails.
func RunBulkLoadSimulation(ctx context.Context, db *sql.DB, bulkCount int) error {
	// Get Thailand time for CREATED_AT
	loc, err := time.LoadLocation("Asia/Bangkok")
	if err != nil {
		return fmt.Errorf("failed to load timezone: %w", err)
	}
	createdAt := time.Now().In(loc)
	createdAtStr := createdAt.Format("2006-01-02 15:04:05")

	log.Println("=== Starting bulk load simulation ===")

	// Step 1: Truncate BULK_DATA table
	log.Println("Truncating BULK_DATA ...")
	if _, err := db.ExecContext(ctx, "TRUNCATE TABLE BULK_DATA"); err != nil {
		return fmt.Errorf("truncate failed: %w", err)
	}

	// Step 2: Insert bulk data using batch approach
	log.Printf("Inserting %d rows with CREATED_AT = %s", bulkCount, createdAtStr)
	insStart := time.Now()

	// Batch size for insertion - balance between memory usage and performance
	const batchSize = 10000

	// Prepare insert statement
	insertSQL := `INSERT INTO BULK_DATA (ID, DATA_VALUE, DESCRIPTION, STATUS, CREATED_AT)
		VALUES (:1, :2, :3, :4, TO_DATE(:5, 'YYYY-MM-DD HH24:MI:SS'))`

	// Use a transaction for better performance
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback() // Will be no-op if committed

	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("prepare insert statement failed: %w", err)
	}
	defer stmt.Close()

	// Process data in batches
	for batchStart := 1; batchStart <= bulkCount; batchStart += batchSize {
		batchEnd := batchStart + batchSize - 1
		if batchEnd > bulkCount {
			batchEnd = bulkCount
		}
		batchCount := batchEnd - batchStart + 1

		// Pre-generate batch data in memory (preparing for future file loading)
		ids := make([]int, batchCount)
		dataValues := make([]string, batchCount)
		descriptions := make([]string, batchCount)
		statuses := make([]string, batchCount)
		createdAts := make([]string, batchCount)

		// Generate data for this batch
		for i := 0; i < batchCount; i++ {
			rowNum := batchStart + i
			ids[i] = rowNum
			dataValues[i] = fmt.Sprintf("VAL_%d", rowNum)
			descriptions[i] = fmt.Sprintf("Generated row #%d", rowNum)
			if rowNum%10 == 0 {
				statuses[i] = "INACTIVE"
			} else {
				statuses[i] = "ACTIVE"
			}
			createdAts[i] = createdAtStr
		}

		// Insert batch data using array binding (go-ora v2.8+)
		// When all parameters are arrays of the same size, go-ora automatically uses bulk binding
		_, err := stmt.ExecContext(ctx, ids, dataValues, descriptions, statuses, createdAts)
		if err != nil {
			return fmt.Errorf("insert batch starting at row %d failed: %w", batchStart, err)
		}

		// Progress logging
		log.Printf("  Inserted %d / %d rows...", batchEnd, bulkCount)
	}

	insEnd := time.Now()
	insDuration := insEnd.Sub(insStart)

	// Step 3: Commit the transaction
	log.Println("Committing transaction...")
	commitStart := time.Now()
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}
	commitEnd := time.Now()
	commitDuration := commitEnd.Sub(commitStart)

	log.Println("Insert committed. Refreshing MV_BULK_DATA (COMPLETE, ATOMIC) ...")

	// Step 4: Refresh materialized view
	refreshStart := time.Now()

	// Execute DBMS_MVIEW.REFRESH using an anonymous PL/SQL block
	refreshSQL := `
BEGIN
  DBMS_MVIEW.REFRESH(
    list           => 'MV_BULK_DATA',
    method         => 'C',
    atomic_refresh => TRUE
  );
END;`

	if _, err := db.ExecContext(ctx, refreshSQL); err != nil {
		return fmt.Errorf("refresh materialized view failed: %w", err)
	}

	refreshEnd := time.Now()
	refreshDuration := refreshEnd.Sub(refreshStart)

	log.Println("Refresh complete.")

	// Print timing report
	totalDuration := refreshEnd.Sub(insStart)
	log.Println("=== Timing report (seconds) ===")
	log.Printf("Insert duration (s):  %.6f", insDuration.Seconds())
	log.Printf("Commit duration (s):  %.6f", commitDuration.Seconds())
	log.Printf("Refresh duration (s): %.6f", refreshDuration.Seconds())
	log.Printf("Total [insert start -> refresh end] (s): %.6f", totalDuration.Seconds())

	// Step 5: Post-refresh validation
	log.Println("=== Post-refresh checks ===")

	// Check base table
	var baseCount int
	var baseMaxCreated sql.NullString
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*), TO_CHAR(MAX(CREATED_AT), 'YYYY-MM-DD HH24:MI:SS') FROM BULK_DATA").
		Scan(&baseCount, &baseMaxCreated)
	if err != nil {
		return fmt.Errorf("query base table failed: %w", err)
	}

	maxCreatedBase := ""
	if baseMaxCreated.Valid {
		maxCreatedBase = baseMaxCreated.String
	}
	log.Printf("BASE_TABLE_COUNT: %d, BASE_MAX_CREATED_AT: %s", baseCount, maxCreatedBase)

	// Check materialized view
	var mvCount int
	var mvMaxCreated sql.NullString
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*), TO_CHAR(MAX(CREATED_AT), 'YYYY-MM-DD HH24:MI:SS') FROM MV_BULK_DATA").
		Scan(&mvCount, &mvMaxCreated)
	if err != nil {
		return fmt.Errorf("query materialized view failed: %w", err)
	}

	maxCreatedMV := ""
	if mvMaxCreated.Valid {
		maxCreatedMV = mvMaxCreated.String
	}
	log.Printf("MV_COUNT: %d, MV_MAX_CREATED_AT: %s", mvCount, maxCreatedMV)

	log.Println("=== Simulation completed successfully ===")
	return nil
}
