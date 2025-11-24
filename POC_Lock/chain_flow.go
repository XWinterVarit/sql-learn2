package main

import (
	"context"
	"database/sql"
	"time"
)

// RunChainFlow simulates:
//  1. SELECT ... FOR UPDATE on A (lock a row)
//  2. wait 10 seconds
//  3. UPDATE B
//  4. wait 10 seconds
//  5. UPDATE C
//  6. COMMIT
func RunChainFlow(ctx context.Context, db *sql.DB, logger *EventLogger, timeline *TimelineTracker, startTime time.Time) error {
	// Launch shadow timeline for expectations (Ideal Schedule)
	go func() {
		timeline.RecordExpected("CHAIN", "A")
		time.Sleep(3 * time.Second)
		timeline.RecordExpected("CHAIN", "B")
		time.Sleep(2 * time.Second)
		timeline.RecordExpected("CHAIN", "C")
	}()

	logger.Log(ctx, "CHAIN", "BEGIN: select for update on A")

	// Start a transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		logger.Log(ctx, "CHAIN", "ERROR: failed to begin transaction: "+err.Error())
		return err
	}
	defer tx.Rollback() // rollback if not committed

	// SELECT FOR UPDATE on A (locks row id=1)
	timeline.RecordStart("CHAIN", "A")
	var aID int
	err = tx.QueryRowContext(ctx, "SELECT id FROM A WHERE id = 1 FOR UPDATE").Scan(&aID)
	timeline.RecordEnd("CHAIN", "A")
	if err != nil {
		logger.Log(ctx, "CHAIN", "ERROR: failed to lock A: "+err.Error())
		return err
	}
	logger.Log(ctx, "CHAIN", "Locked A.id=1; sleeping 10s")

	// Wait 10 seconds
	time.Sleep(3 * time.Second)

	// Update B
	timeline.RecordStart("CHAIN", "B")
	logger.Log(ctx, "CHAIN", "Updating B.id=1")
	_, err = tx.ExecContext(ctx, "UPDATE B SET data = 'B1_UPDATED_BY_CHAIN' WHERE id = 1")
	timeline.RecordEnd("CHAIN", "B")
	if err != nil {
		logger.Log(ctx, "CHAIN", "ERROR: failed to update B: "+err.Error())
		return err
	}

	logger.Log(ctx, "CHAIN", "B updated; sleeping 10s")
	time.Sleep(2 * time.Second)

	// Update C
	timeline.RecordStart("CHAIN", "C")
	logger.Log(ctx, "CHAIN", "Updating C.id=1 (chain_data column)")
	_, err = tx.ExecContext(ctx, "UPDATE C SET chain_data = 'UPDATED_BY_CHAIN' WHERE id = 1")
	timeline.RecordEnd("CHAIN", "C")

	if err != nil {
		logger.Log(ctx, "CHAIN", "ERROR: failed to update C: "+err.Error())
		return err
	}
	//return errors.New("dummy for rollback")

	logger.Log(ctx, "CHAIN", "C updated; sleeping 2s")
	time.Sleep(2 * time.Second)

	// Commit
	logger.Log(ctx, "CHAIN", "Committing")
	if err := tx.Commit(); err != nil {
		logger.Log(ctx, "CHAIN", "ERROR: commit failed: "+err.Error())
		return err
	}
	timeline.RecordCommit("CHAIN")

	logger.Log(ctx, "CHAIN", "DONE")
	return nil
}
