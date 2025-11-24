package main

import (
	"context"
	"database/sql"
	"time"
)

// RunEarlyFlow simulates:
//  1. wait 2 seconds
//  2. UPDATE C
//  3. COMMIT
func RunEarlyFlow(ctx context.Context, db *sql.DB, logger *EventLogger, startTime time.Time) error {
	logger.Log(ctx, "EARLY", "BEGIN: sleeping 2s before updating C")

	// Wait 2 seconds
	time.Sleep(2 * time.Second)

	// Start a transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		logger.Log(ctx, "EARLY", "ERROR: failed to begin transaction: "+err.Error())
		return err
	}
	defer tx.Rollback()

	// Update C
	logger.Log(ctx, "EARLY", "Updating C.id=1 (early_data column)")
	_, err = tx.ExecContext(ctx, "UPDATE C SET early_data = 'UPDATED_EARLY' WHERE id = 1")
	if err != nil {
		logger.Log(ctx, "EARLY", "ERROR: failed to update C: "+err.Error())
		return err
	}

	// Commit
	logger.Log(ctx, "EARLY", "Committing")
	if err := tx.Commit(); err != nil {
		logger.Log(ctx, "EARLY", "ERROR: commit failed: "+err.Error())
		return err
	}

	logger.Log(ctx, "EARLY", "DONE")
	return nil
}
