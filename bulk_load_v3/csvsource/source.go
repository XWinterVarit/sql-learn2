package csvsource

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime/debug"

	"sql-learn2/bulk_load_v3"
	"sql-learn2/bulk_load_v3/rp_dynamic"

	"github.com/jmoiron/sqlx"
)

// Config holds configuration for the CSV source.
type Config struct {
	FilePath            string
	ExpectedHeaderCount int
	ExpectedHeaders     map[int]string
	Delimiter           rune // Custom delimiter (default is comma)
	// ConvertFunc is called to convert a CSV record to database values.
	ConvertFunc func(row []string) ([]interface{}, error)

	// Bulk Load settings
	DB        *sqlx.DB
	TableName string
	Columns   []string
	BatchSize int
	MVName    string
}

// CsvSource implements bulkloadv3.Source using the native encoding/csv package.
type CsvSource struct {
	cfg Config

	file   *os.File
	reader *csv.Reader
}

// New creates a new CsvSource.
func New(cfg Config) (*CsvSource, func() error) {
	src := &CsvSource{
		cfg: cfg,
	}
	return src, src.Close
}

// sourceAdapter adapts CsvSource to the bulkloadv3.Source interface.
// It directly implements the logic for Validate, Next, and Convert,
// operating on the underlying CsvSource state.
type sourceAdapter struct {
	*CsvSource
}

// Validate opens the CSV file and validates the header count and names.
func (a *sourceAdapter) Validate(ctx context.Context) error {
	slog.Info("Opening CSV for validation", bulkloadv3.LogFieldFile, a.cfg.FilePath, bulkloadv3.LogFieldTable, a.cfg.TableName)

	if a.file != nil {
		_ = a.file.Close()
		a.file = nil
	}

	f, err := os.Open(a.cfg.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", a.cfg.FilePath, err)
	}
	a.file = f

	a.reader = csv.NewReader(f)
	if a.cfg.Delimiter != 0 {
		a.reader.Comma = a.cfg.Delimiter
	}
	// Enforce that all records have the same number of fields as the first record (header).
	a.reader.FieldsPerRecord = 0

	// 1. Read and Validate Header
	header, err := a.reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read header from %s: %w", a.cfg.FilePath, err)
	}

	if a.cfg.ExpectedHeaderCount > 0 {
		if len(header) != a.cfg.ExpectedHeaderCount {
			return fmt.Errorf("header count mismatch: got %d, want %d", len(header), a.cfg.ExpectedHeaderCount)
		}
	}

	if len(a.cfg.ExpectedHeaders) > 0 {
		for index, expectedName := range a.cfg.ExpectedHeaders {
			if index < 0 || index >= len(header) {
				return fmt.Errorf("expected header index %d is out of bounds (header length: %d)", index, len(header))
			}
			if header[index] != expectedName {
				return fmt.Errorf("header name mismatch at index %d: got '%s', want '%s'", index, header[index], expectedName)
			}
		}
	}

	slog.Info("CSV validation successful", bulkloadv3.LogFieldFile, a.cfg.FilePath, bulkloadv3.LogFieldTable, a.cfg.TableName)
	return nil
}

// Next reads the next data row from the CSV.
func (a *sourceAdapter) Next(ctx context.Context) (interface{}, error) {
	if a.reader == nil {
		return nil, fmt.Errorf("reader not initialized (call Validate first)")
	}
	// Read the next record
	record, err := a.reader.Read()
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("read csv %s failed: %w", a.cfg.FilePath, err)
	}

	return record, nil
}

// Convert transforms the raw CSV record ([]string) into DB values.
func (a *sourceAdapter) Convert(rawRow interface{}) ([]interface{}, error) {
	row, ok := rawRow.([]string)
	if !ok {
		return nil, fmt.Errorf("expected []string, got %T", rawRow)
	}

	return a.cfg.ConvertFunc(row)
}

// Run executes the bulk load process.
func (s *CsvSource) Run(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in csv source run: %v\nstack: %s", r, debug.Stack())
		}
	}()

	if s.cfg.DB == nil {
		return fmt.Errorf("database connection (DB) is required")
	}
	if s.cfg.TableName == "" {
		return fmt.Errorf("table name is required")
	}
	if len(s.cfg.Columns) == 0 {
		return fmt.Errorf("target columns are required")
	}
	if s.cfg.ConvertFunc == nil {
		return fmt.Errorf("ConvertFunc is required")
	}

	repo := rp_dynamic.NewRepo(s.cfg.DB)

	cfg := bulkloadv3.Config{
		Repo:      repo,
		TableName: s.cfg.TableName,
		Columns:   s.cfg.Columns,
		BatchSize: s.cfg.BatchSize,
		MVName:    s.cfg.MVName,
	}
	// Use the adapter to expose the interface methods to bulkloadv3
	loader := bulkloadv3.NewLoader(cfg, &sourceAdapter{CsvSource: s})
	return loader.Run(ctx)
}

// Close closes the underlying file handle.
func (s *CsvSource) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}
