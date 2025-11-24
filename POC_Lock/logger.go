package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// EventLogger logs events to EVENT_LOG table
type EventLogger struct {
	db *sql.DB
}

func NewEventLogger(db *sql.DB) *EventLogger {
	return &EventLogger{db: db}
}

// Log inserts an event into EVENT_LOG with autonomous transaction semantics
// (In Go, we use a separate connection/transaction to mimic Oracle's PRAGMA AUTONOMOUS_TRANSACTION)
func (l *EventLogger) Log(ctx context.Context, who, msg string) {
	// Use a separate transaction that commits immediately
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("[%s] Logger error: failed to begin tx: %v", who, err)
		return
	}
	defer tx.Rollback() // safety rollback if commit fails

	_, err = tx.ExecContext(ctx, "INSERT INTO EVENT_LOG (who, msg) VALUES (:1, :2)", who, msg)
	if err != nil {
		log.Printf("[%s] Logger error: insert failed: %v", who, err)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[%s] Logger error: commit failed: %v", who, err)
	}
}

// DisplayEventLog prints all events from EVENT_LOG ordered by timestamp
func DisplayEventLog(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "SELECT TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS.FF3'), who, msg FROM EVENT_LOG ORDER BY ts")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var ts, who, msg string
		if err := rows.Scan(&ts, &who, &msg); err != nil {
			return err
		}
		fmt.Printf("  %s  %-8s  %s\n", ts, who, msg)
	}
	return rows.Err()
}
