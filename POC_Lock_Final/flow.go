package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
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
	Timeout  time.Duration
}

// TxFlow represents a transaction flow with ordered steps
type TxFlow struct {
	Name      string
	Steps     []Step
	db        *sql.DB
	logger    *EventLogger
	timeline  *TimelineTracker
	TxTimeout time.Duration
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

// SetTxTimeout sets the overall transaction timeout
func (f *TxFlow) SetTxTimeout(d time.Duration) *TxFlow {
	f.TxTimeout = d
	return f
}

// AddQuery adds a SELECT (or SELECT FOR UPDATE) step
func (f *TxFlow) AddQuery(table, label, sqlQuery string, options ...time.Duration) *TxFlow {
	var timeout time.Duration
	if len(options) > 0 {
		timeout = options[0]
	}
	f.Steps = append(f.Steps, Step{
		Type:    StepSQL,
		Table:   table,
		Label:   label,
		SQL:     sqlQuery,
		Timeout: timeout,
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

	txCtx := ctx
	if f.TxTimeout > 0 {
		var cancel context.CancelFunc
		txCtx, cancel = context.WithTimeout(ctx, f.TxTimeout)
		defer cancel()
	}

	// Start Transaction
	tx, err := f.db.BeginTx(txCtx, nil)
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
			stepCtx := txCtx
			if step.Timeout > 0 {
				var cancel context.CancelFunc
				stepCtx, cancel = context.WithTimeout(txCtx, step.Timeout)
				defer cancel()
			}

			if err := f.execSQL(stepCtx, tx, step.SQL); err != nil {
				f.logger.Log(ctx, f.Name, fmt.Sprintf("ERROR: %v: %v", step.Label, err))
				f.timeline.RecordRollback(f.Name)
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
		f.timeline.RecordRollback(f.Name)
		return err
	}
	f.timeline.RecordCommit(f.Name)
	f.logger.Log(ctx, f.Name, "DONE")
	return nil
}

func (f *TxFlow) execSQL(ctx context.Context, tx *sql.Tx, sqlStmt string) error {
	// Simple heuristic to detect SELECT queries
	trimmed := trimLeft(sqlStmt)
	if len(trimmed) > 6 && (strings.EqualFold(trimmed[:6], "SELECT")) {
		rows, err := tx.QueryContext(ctx, sqlStmt)
		if err != nil {
			return err
		}
		defer rows.Close()

		if err := ctx.Err(); err != nil {
			return err
		}

		results, err := processRows(rows)
		if err != nil {
			return err
		}
		if len(results) > 0 {
			for _, res := range results {
				f.logger.Log(ctx, f.Name, "Result: "+res)
			}
		} else {
			f.logger.Log(ctx, f.Name, "Result: <no rows>")
		}
		return nil
	}

	// For UPDATE/INSERT/DELETE
	res, err := tx.ExecContext(ctx, sqlStmt)
	if err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err == nil {
		f.logger.Log(ctx, f.Name, fmt.Sprintf("Result: %d rows affected", affected))
	}
	return nil
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

// NonTxFlow represents a flow without a transaction
type NonTxFlow struct {
	Name     string
	Steps    []Step
	db       *sql.DB
	logger   *EventLogger
	timeline *TimelineTracker
}

// NewNonTxFlow creates a new non-transaction flow builder
func NewNonTxFlow(name string, db *sql.DB, logger *EventLogger, timeline *TimelineTracker) *NonTxFlow {
	return &NonTxFlow{
		Name:     name,
		db:       db,
		logger:   logger,
		timeline: timeline,
		Steps:    make([]Step, 0),
	}
}

// AddQuery adds a SELECT step
func (f *NonTxFlow) AddQuery(table, label, sqlQuery string, options ...time.Duration) *NonTxFlow {
	var timeout time.Duration
	if len(options) > 0 {
		timeout = options[0]
	}
	f.Steps = append(f.Steps, Step{
		Type:    StepSQL,
		Table:   table,
		Label:   label,
		SQL:     sqlQuery,
		Timeout: timeout,
	})
	return f
}

// AddUpdate adds an UPDATE step
func (f *NonTxFlow) AddUpdate(table, label, sqlUpdate string) *NonTxFlow {
	f.Steps = append(f.Steps, Step{
		Type:  StepSQL,
		Table: table,
		Label: label,
		SQL:   sqlUpdate,
	})
	return f
}

// AddWait adds a sleep step
func (f *NonTxFlow) AddWait(duration time.Duration) *NonTxFlow {
	f.Steps = append(f.Steps, Step{
		Type:     StepWait,
		Duration: duration,
		Label:    fmt.Sprintf("Sleeping %v", duration),
	})
	return f
}

// Execute runs the flow without a transaction
func (f *NonTxFlow) Execute(ctx context.Context) error {
	f.logger.Log(ctx, f.Name, "BEGIN (Non-Tx)")

	// 1. Launch Shadow Timeline (Expected)
	go f.runExpected()

	// Execute Steps
	for _, step := range f.Steps {
		if step.Type == StepWait {
			f.logger.Log(ctx, f.Name, step.Label)
			time.Sleep(step.Duration)
		} else if step.Type == StepSQL {
			f.timeline.RecordStart(f.Name, step.Table)
			f.logger.Log(ctx, f.Name, step.Label)

			// Execute SQL directly on DB
			stepCtx := ctx
			if step.Timeout > 0 {
				var cancel context.CancelFunc
				stepCtx, cancel = context.WithTimeout(ctx, step.Timeout)
				defer cancel()
			}

			if err := f.execSQL(stepCtx, step.SQL); err != nil {
				f.logger.Log(ctx, f.Name, fmt.Sprintf("ERROR: %v: %v", step.Label, err))
				return err
			}

			f.timeline.RecordEnd(f.Name, step.Table)
		}
	}

	f.timeline.RecordCommit(f.Name) // Mark end
	f.logger.Log(ctx, f.Name, "DONE")
	return nil
}

func (f *NonTxFlow) execSQL(ctx context.Context, sqlStmt string) error {
	trimmed := trimLeft(sqlStmt)
	if len(trimmed) > 6 && (strings.EqualFold(trimmed[:6], "SELECT")) {
		rows, err := f.db.QueryContext(ctx, sqlStmt)
		if err != nil {
			return err
		}
		defer rows.Close()

		if err := ctx.Err(); err != nil {
			return err
		}

		results, err := processRows(rows)
		if err != nil {
			return err
		}
		if len(results) > 0 {
			for _, res := range results {
				f.logger.Log(ctx, f.Name, "Result: "+res)
			}
		} else {
			f.logger.Log(ctx, f.Name, "Result: <no rows>")
		}
		return nil
	}

	res, err := f.db.ExecContext(ctx, sqlStmt)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err == nil {
		f.logger.Log(ctx, f.Name, fmt.Sprintf("Result: %d rows affected", affected))
	}
	return nil
}

func (f *NonTxFlow) runExpected() {
	for _, step := range f.Steps {
		if step.Type == StepWait {
			time.Sleep(step.Duration)
		} else if step.Type == StepSQL {
			f.timeline.RecordExpected(f.Name, step.Table)
			time.Sleep(30 * time.Millisecond)
		}
	}
	f.timeline.RecordCommit(f.Name + " EXPECTED")
}

func processRows(rows *sql.Rows) ([]string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []string
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		var parts []string
		for i, col := range cols {
			val := values[i]
			var valStr string
			if b, ok := val.([]byte); ok {
				valStr = string(b)
			} else {
				valStr = fmt.Sprintf("%v", val)
			}
			parts = append(parts, fmt.Sprintf("%s=%s", col, valStr))
		}
		results = append(results, fmt.Sprintf("[%s]", strings.Join(parts, ", ")))
	}
	return results, rows.Err()
}
