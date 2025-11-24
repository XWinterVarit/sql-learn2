package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"
)

// EventLogger logs events to EVENT_LOG table asynchronously
type EventLogger struct {
	db       *sql.DB
	logQueue chan logEntry
	wg       sync.WaitGroup
}

type logEntry struct {
	ts  time.Time
	who string
	msg string
}

func NewEventLogger(db *sql.DB) *EventLogger {
	l := &EventLogger{
		db:       db,
		logQueue: make(chan logEntry, 100), // buffered channel
	}

	l.wg.Add(1)
	go l.worker()
	return l
}

func (l *EventLogger) worker() {
	defer l.wg.Done()

	for entry := range l.logQueue {
		l.persist(entry)
	}
}

func (l *EventLogger) persist(entry logEntry) {
	ctx := context.Background()
	// Use a separate transaction that commits immediately
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("[%s] Logger error: failed to begin tx: %v", entry.who, err)
		return
	}
	defer tx.Rollback() // safety rollback if commit fails

	// Insert with explicit timestamp to preserve ordering
	_, err = tx.ExecContext(ctx, "INSERT INTO EVENT_LOG (ts, who, msg) VALUES (:1, :2, :3)", entry.ts, entry.who, entry.msg)
	if err != nil {
		log.Printf("[%s] Logger error: insert failed: %v", entry.who, err)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[%s] Logger error: commit failed: %v", entry.who, err)
	}
}

// Log queues an event to be logged to EVENT_LOG table
func (l *EventLogger) Log(ctx context.Context, who, msg string) {
	// Capture timestamp immediately
	entry := logEntry{
		ts:  time.Now(),
		who: who,
		msg: msg,
	}

	// Send to worker (non-blocking unless queue is full)
	select {
	case l.logQueue <- entry:
	default:
		log.Printf("[%s] Logger warning: queue full, dropping log: %s", who, msg)
	}
}

// Close closes the log queue and waits for all pending logs to be written
func (l *EventLogger) Close() {
	close(l.logQueue)
	l.wg.Wait()
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
