package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type StepType int

const (
	StepSQL StepType = iota
	StepWait
)

type Step struct {
	Type     StepType
	Table    string
	Label    string
	SQL      string
	Duration time.Duration
}

// TxFlow represents a transaction flow with ordered steps
type TxFlow struct {
	Name     string
	Steps    []Step
	db       *sql.DB
	logger   *EventLogger
	timeline *TimelineTracker
}

// NewTxFlow creates a new flow builder
func NewTxFlow(name string, db *sql.DB, logger *EventLogger, timeline *TimelineTracker) *TxFlow {
	return &TxFlow{
		Name:     name,
		db:       db,
		logger:   logger,
		timeline: timeline,
		Steps:    make([]Step, 0),
	}
}

// AddQuery adds a SELECT (or SELECT FOR UPDATE) step
func (f *TxFlow) AddQuery(table, label, sqlQuery string) *TxFlow {
	f.Steps = append(f.Steps, Step{
		Type:  StepSQL,
		Table: table,
		Label: label,
		SQL:   sqlQuery,
	})
	return f
}

// AddUpdate adds an UPDATE step
func (f *TxFlow) AddUpdate(table, label, sqlUpdate string) *TxFlow {
	f.Steps = append(f.Steps, Step{
		Type:  StepSQL,
		Table: table,
		Label: label,
		SQL:   sqlUpdate,
	})
	return f
}

// AddWait adds a sleep step
func (f *TxFlow) AddWait(duration time.Duration) *TxFlow {
	f.Steps = append(f.Steps, Step{
		Type:     StepWait,
		Duration: duration,
		Label:    fmt.Sprintf("Sleeping %v", duration),
	})
	return f
}

// Execute runs the flow:
// 1. Starts the shadow "Expected" timeline generator.
// 2. Executes the actual steps in a transaction.
func (f *TxFlow) Execute(ctx context.Context) error {
	// 2. Run Actual Flow
	f.logger.Log(ctx, f.Name, "BEGIN")

	// Start Transaction
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		f.logger.Log(ctx, f.Name, "ERROR: failed to begin transaction: "+err.Error())
		return err
	}
	defer tx.Rollback()

	// 1. Launch Shadow Timeline (Expected) - Start after Tx begins to align T=0
	go f.runExpected()

	// Execute Steps
	for _, step := range f.Steps {
		if step.Type == StepWait {
			f.logger.Log(ctx, f.Name, step.Label)
			time.Sleep(step.Duration)
		} else if step.Type == StepSQL {
			f.timeline.RecordStart(f.Name, step.Table)
			f.logger.Log(ctx, f.Name, step.Label)

			// Execute SQL
			if err := f.execSQL(ctx, tx, step.SQL); err != nil {
				f.logger.Log(ctx, f.Name, fmt.Sprintf("ERROR: %v: %v", step.Label, err))
				return err
			}

			// Record End *after* the operation
			f.timeline.RecordEnd(f.Name, step.Table)
		}
	}

	// Commit
	f.logger.Log(ctx, f.Name, "Committing")
	if err := tx.Commit(); err != nil {
		f.logger.Log(ctx, f.Name, "ERROR: commit failed: "+err.Error())
		return err
	}
	f.timeline.RecordCommit(f.Name)
	f.logger.Log(ctx, f.Name, "DONE")
	return nil
}

func (f *TxFlow) execSQL(ctx context.Context, tx *sql.Tx, sqlStmt string) error {
	// Simple heuristic to detect SELECT queries
	// If it starts with SELECT, use Query to ensure proper lock waiting/scanning
	trimmed := trimLeft(sqlStmt)
	if len(trimmed) > 6 && (trimmed[:6] == "SELECT" || trimmed[:6] == "select") {
		rows, err := tx.QueryContext(ctx, sqlStmt)
		if err != nil {
			return err
		}
		defer rows.Close()
		// Consume rows to ensure execution completes
		for rows.Next() {
		}
		return rows.Err()
	}

	// For UPDATE/INSERT/DELETE
	_, err := tx.ExecContext(ctx, sqlStmt)
	return err
}

func (f *TxFlow) runExpected() {
	for _, step := range f.Steps {
		if step.Type == StepWait {
			time.Sleep(step.Duration)
		} else if step.Type == StepSQL {
			f.timeline.RecordExpected(f.Name, step.Table)
			// Simulate execution time to align with Actual timeline
			time.Sleep(30 * time.Millisecond)
		}
	}
	// Record expected commit time
	f.timeline.RecordCommit(f.Name + " EXPECTED")
}

func trimLeft(s string) string {
	// Basic trim for heuristic
	start := 0
	for start < len(s) && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n') {
		start++
	}
	return s[start:]
}
