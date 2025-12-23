package bulkinsert

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
)

// executeInsertBatch executes the bulk insert within a transaction.
// Returns the insert duration (excluding commit time) and any error encountered.
func executeInsertBatch(ctx context.Context, db *sqlx.DB, insertSQL string, columnData []interface{}) (time.Duration, error) {
	insStart := time.Now()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return 0, fmt.Errorf("prepare insert statement failed: %w", err)
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, columnData...)
	if err != nil {
		return 0, fmt.Errorf("insert batch failed: %w", err)
	}

	log.Println("Committing transaction...")
	commitStart := time.Now()
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit failed: %w", err)
	}
	commitDuration := time.Since(commitStart)

	insDuration := time.Since(insStart) - commitDuration
	return insDuration, nil
}
