package bulkloadv3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"sql-learn2/bulk_load_v3/rp_dynamic"
)

// --- Mocks ---

type MockRepo struct {
	TruncateFunc                func(ctx context.Context, tableName string) error
	BulkInsertFunc              func(ctx context.Context, builder *rp_dynamic.BulkInsertBuilder) error
	RefreshMaterializedViewFunc func(ctx context.Context, name string) (time.Duration, error)
}

func (m *MockRepo) Truncate(ctx context.Context, tableName string) error {
	if m.TruncateFunc != nil {
		return m.TruncateFunc(ctx, tableName)
	}
	return nil
}

func (m *MockRepo) BulkInsert(ctx context.Context, builder *rp_dynamic.BulkInsertBuilder) error {
	if m.BulkInsertFunc != nil {
		return m.BulkInsertFunc(ctx, builder)
	}
	return nil
}

func (m *MockRepo) RefreshMaterializedView(ctx context.Context, name string) (time.Duration, error) {
	if m.RefreshMaterializedViewFunc != nil {
		return m.RefreshMaterializedViewFunc(ctx, name)
	}
	return 0, nil
}

type MockSource struct {
	ValidateFunc func(ctx context.Context) error
	NextFunc     func(ctx context.Context) (interface{}, error)
	ConvertFunc  func(rawRow interface{}) ([]interface{}, error)
}

func (m *MockSource) Validate(ctx context.Context) error {
	if m.ValidateFunc != nil {
		return m.ValidateFunc(ctx)
	}
	return nil
}

func (m *MockSource) Next(ctx context.Context) (interface{}, error) {
	if m.NextFunc != nil {
		return m.NextFunc(ctx)
	}
	return nil, io.EOF
}

func (m *MockSource) Convert(rawRow interface{}) ([]interface{}, error) {
	if m.ConvertFunc != nil {
		return m.ConvertFunc(rawRow)
	}
	return []interface{}{rawRow}, nil
}

// --- Helper to create a basic valid config ---
func createValidConfig(repo rp_dynamic.Repository) Config {
	return Config{
		Repo:      repo,
		TableName: "TEST_TABLE",
		Columns:   []string{"COL1"},
		BatchSize: 10,
		MVName:    "MV_TEST",
	}
}

// --- Tests ---

// 1. Basic Cases

func TestRun_Success_NoRows(t *testing.T) {
	repo := &MockRepo{
		TruncateFunc: func(ctx context.Context, tableName string) error {
			if tableName != "TEST_TABLE" {
				return fmt.Errorf("unexpected table: %s", tableName)
			}
			return nil
		},
		BulkInsertFunc: func(ctx context.Context, builder *rp_dynamic.BulkInsertBuilder) error {
			return errors.New("should not be called for empty source")
		},
		RefreshMaterializedViewFunc: func(ctx context.Context, name string) (time.Duration, error) {
			return 1 * time.Millisecond, nil
		},
	}

	src := &MockSource{
		NextFunc: func(ctx context.Context) (interface{}, error) {
			return nil, io.EOF
		},
	}

	cfg := createValidConfig(repo)
	err := Run(context.Background(), cfg, src)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
}

func TestRun_Success_WithRows(t *testing.T) {
	insertCount := 0
	repo := &MockRepo{
		BulkInsertFunc: func(ctx context.Context, builder *rp_dynamic.BulkInsertBuilder) error {
			insertCount++
			args := builder.GetArgs()
			// We expect 1 column
			if len(args) != 1 {
				return fmt.Errorf("expected 1 column, got %d", len(args))
			}
			return nil
		},
	}

	rows := []string{"row1", "row2", "row3"}
	idx := 0
	src := &MockSource{
		NextFunc: func(ctx context.Context) (interface{}, error) {
			if idx >= len(rows) {
				return nil, io.EOF
			}
			val := rows[idx]
			idx++
			return val, nil
		},
		ConvertFunc: func(rawRow interface{}) ([]interface{}, error) {
			return []interface{}{rawRow}, nil
		},
	}

	cfg := createValidConfig(repo)
	cfg.BatchSize = 100 // Large batch, single insert expected
	err := Run(context.Background(), cfg, src)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if insertCount != 1 {
		t.Errorf("Expected 1 insert call, got %d", insertCount)
	}
}

// 2. Complex Cases (Batching)

func TestRun_BatchingLogic(t *testing.T) {
	// Scenario: BatchSize = 2, Total Rows = 5.
	// Expected: Insert(2), Insert(2), Insert(1) -> 3 calls.

	batches := []int{}
	repo := &MockRepo{
		BulkInsertFunc: func(ctx context.Context, builder *rp_dynamic.BulkInsertBuilder) error {
			// Count rows in this batch.
			// GetArgs returns []interface{}, where each element is a column slice.
			// args[0] is the slice for first column.
			args := builder.GetArgs()
			colData := args[0].([]interface{})
			batches = append(batches, len(colData))
			return nil
		},
	}

	rowCount := 5
	curr := 0
	src := &MockSource{
		NextFunc: func(ctx context.Context) (interface{}, error) {
			if curr >= rowCount {
				return nil, io.EOF
			}
			curr++
			return curr, nil
		},
		ConvertFunc: func(rawRow interface{}) ([]interface{}, error) {
			return []interface{}{rawRow}, nil
		},
	}

	cfg := createValidConfig(repo)
	cfg.BatchSize = 2

	err := Run(context.Background(), cfg, src)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if len(batches) != 3 {
		t.Fatalf("Expected 3 batch inserts, got %d", len(batches))
	}
	if batches[0] != 2 || batches[1] != 2 || batches[2] != 1 {
		t.Errorf("Unexpected batch sizes: %v", batches)
	}
}

func TestRun_BatchingExactMultiple(t *testing.T) {
	// Scenario: BatchSize = 2, Total Rows = 4.
	// Flow: Read 1, 2 -> Full -> Flush(2) -> Read 3, 4 -> Full -> Flush(2) -> EOF -> Final Flush(0) skipped.
	// Wait, the Lazy Flush logic:
	// Loop:
	//  Read 1. Count=0. < BatchSize. Convert. Add. Count=1.
	//  Read 2. Count=1. < BatchSize. Convert. Add. Count=2.
	//  Read 3. Count=2. >= BatchSize! Flush(2). Reset. Convert(3). Add(3). Count=1.
	//  Read 4. Count=1. < BatchSize. Convert. Add. Count=2.
	//  Read EOF.
	// Final: Count=2 > 0. Flush(2).
	// Total Flushes: 2 (size 2, size 2).

	batches := []int{}
	repo := &MockRepo{
		BulkInsertFunc: func(ctx context.Context, builder *rp_dynamic.BulkInsertBuilder) error {
			args := builder.GetArgs()
			colData := args[0].([]interface{})
			batches = append(batches, len(colData))
			return nil
		},
	}

	rowCount := 4
	curr := 0
	src := &MockSource{
		NextFunc: func(ctx context.Context) (interface{}, error) {
			if curr >= rowCount {
				return nil, io.EOF
			}
			curr++
			return curr, nil
		},
		ConvertFunc: func(rawRow interface{}) ([]interface{}, error) {
			return []interface{}{rawRow}, nil
		},
	}

	cfg := createValidConfig(repo)
	cfg.BatchSize = 2

	err := Run(context.Background(), cfg, src)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if len(batches) != 2 {
		t.Fatalf("Expected 2 batch inserts, got %d: %v", len(batches), batches)
	}
	if batches[0] != 2 || batches[1] != 2 {
		t.Errorf("Unexpected batch sizes: %v", batches)
	}
}

// 3. Exceptional Cases

func TestRun_ValidationErrors(t *testing.T) {
	repo := &MockRepo{}
	src := &MockSource{}

	tests := []struct {
		name      string
		config    Config
		expectErr string
	}{
		{
			name: "Missing Repo",
			config: Config{
				Repo:      nil,
				TableName: "T",
				Columns:   []string{"C"},
			},
			expectErr: "repository (Repo) is required",
		},
		{
			name: "Missing TableName",
			config: Config{
				Repo:      repo,
				TableName: "",
				Columns:   []string{"C"},
			},
			expectErr: "table name is required",
		},
		{
			name: "Missing Columns",
			config: Config{
				Repo:      repo,
				TableName: "T",
				Columns:   nil,
			},
			expectErr: "target columns are required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Run(context.Background(), tt.config, src)
			if err == nil {
				t.Error("Expected error, got nil")
			} else if err.Error() != tt.expectErr {
				t.Errorf("Expected error %q, got %q", tt.expectErr, err.Error())
			}
		})
	}
}

func TestRun_SourceFailures(t *testing.T) {
	repo := &MockRepo{}

	// Case 1: Validate Fails
	srcValFail := &MockSource{
		ValidateFunc: func(ctx context.Context) error {
			return errors.New("validate boom")
		},
	}
	err := Run(context.Background(), createValidConfig(repo), srcValFail)
	if err == nil || err.Error() != "source validation failed: validate boom" {
		t.Errorf("Expected validate error, got %v", err)
	}

	// Case 2: Next Fails
	srcNextFail := &MockSource{
		NextFunc: func(ctx context.Context) (interface{}, error) {
			return nil, errors.New("read boom")
		},
	}
	err = Run(context.Background(), createValidConfig(repo), srcNextFail)
	if err == nil || err.Error() != "read line failed: read boom" {
		t.Errorf("Expected read error, got %v", err)
	}

	// Case 3: Convert Fails
	srcConvFail := &MockSource{
		NextFunc: func(ctx context.Context) (interface{}, error) {
			return "row", nil
		},
		ConvertFunc: func(rawRow interface{}) ([]interface{}, error) {
			return nil, errors.New("convert boom")
		},
	}
	err = Run(context.Background(), createValidConfig(repo), srcConvFail)
	if err == nil || err.Error() != "row conversion failed: convert boom" {
		t.Errorf("Expected convert error, got %v", err)
	}
}

func TestRun_RepoFailures(t *testing.T) {
	src := &MockSource{
		NextFunc: func(ctx context.Context) (interface{}, error) {
			return nil, io.EOF
		},
	}

	// Case 1: Truncate Fails
	repoTruncFail := &MockRepo{
		TruncateFunc: func(ctx context.Context, tableName string) error {
			return errors.New("truncate boom")
		},
	}
	err := Run(context.Background(), createValidConfig(repoTruncFail), src)
	// Error message format: "truncate table %s failed: %w"
	if err == nil || err.Error() != "truncate table TEST_TABLE failed: truncate boom" {
		t.Errorf("Expected truncate error, got %v", err)
	}

	// Case 2: Flush Fails (using a source that yields rows)
	repoFlushFail := &MockRepo{
		BulkInsertFunc: func(ctx context.Context, builder *rp_dynamic.BulkInsertBuilder) error {
			return errors.New("insert boom")
		},
	}

	// We need source to stop or fail fast.
	// If BatchSize=1, Next -> Convert -> Add -> Next -> Full -> Flush -> Fail.
	// Actually:
	// Read 1. Add.
	// Read 2. Full -> Flush -> Fail.

	// Let's control source to return 1 row then EOF.
	// Then Flush is called at the end (Finalize).
	srcOneRow := &MockSource{
		NextFunc: func(ctx context.Context) (interface{}, error) {
			return nil, io.EOF // Wait, need one row first
		},
	}
	// Redefine source for this test
	iter := 0
	srcOneRow.NextFunc = func(ctx context.Context) (interface{}, error) {
		if iter == 0 {
			iter++
			return "row", nil
		}
		return nil, io.EOF
	}

	err = Run(context.Background(), createValidConfig(repoFlushFail), srcOneRow)
	if err == nil || err.Error() != "final bulk insert failed: bulk insert failed: insert boom" {
		t.Errorf("Expected flush error, got %v", err)
	}
}

func TestRun_Recovery(t *testing.T) {
	// Test that it recovers from panic in the main loop
	repo := &MockRepo{}
	srcPanic := &MockSource{
		ValidateFunc: func(ctx context.Context) error {
			panic("unexpected panic")
		},
	}

	err := Run(context.Background(), createValidConfig(repo), srcPanic)
	if err == nil {
		t.Fatal("Expected error from panic recovery, got nil")
	}
	// Error message format: "panic in bulk load run: %v\nstack: %s"
	if len(err.Error()) < 20 || err.Error()[:22] != "panic in bulk load run" {
		t.Errorf("Unexpected error format: %v", err)
	}
}
