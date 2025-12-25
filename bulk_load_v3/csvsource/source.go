package csvsource

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"

	"sql-learn2/bulk_load_v3"
	"sql-learn2/bulk_load_v3/rp_dynamic"

	"github.com/jmoiron/sqlx"
)

// Config holds configuration for the CSV source.
type Config struct {
	FilePath            string
	ExpectedHeaderCount int
	ExpectedHeaders     map[int]string
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
	s *CsvSource
}

// Validate opens the CSV file and validates the header count and names.
func (a *sourceAdapter) Validate(ctx context.Context) error {
	slog.Info("Opening CSV for validation", bulkloadv3.LogFieldFile, a.s.cfg.FilePath, bulkloadv3.LogFieldTable, a.s.cfg.TableName)

	f, err := os.Open(a.s.cfg.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", a.s.cfg.FilePath, err)
	}
	a.s.file = f

	a.s.reader = csv.NewReader(f)
	// Enforce that all records have the same number of fields as the first record (header).
	a.s.reader.FieldsPerRecord = 0

	// 1. Read and Validate Header
	header, err := a.s.reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read header from %s: %w", a.s.cfg.FilePath, err)
	}

	if a.s.cfg.ExpectedHeaderCount > 0 {
		if len(header) != a.s.cfg.ExpectedHeaderCount {
			return fmt.Errorf("header count mismatch: got %d, want %d", len(header), a.s.cfg.ExpectedHeaderCount)
		}
	}

	if len(a.s.cfg.ExpectedHeaders) > 0 {
		for index, expectedName := range a.s.cfg.ExpectedHeaders {
			if index < 0 || index >= len(header) {
				return fmt.Errorf("expected header index %d is out of bounds (header length: %d)", index, len(header))
			}
			if header[index] != expectedName {
				return fmt.Errorf("header name mismatch at index %d: got '%s', want '%s'", index, header[index], expectedName)
			}
		}
	}

	slog.Info("CSV validation successful", bulkloadv3.LogFieldFile, a.s.cfg.FilePath, bulkloadv3.LogFieldTable, a.s.cfg.TableName)
	return nil
}

// Next reads the next data row from the CSV.
func (a *sourceAdapter) Next(ctx context.Context) (interface{}, error) {
	if a.s.reader == nil {
		return nil, fmt.Errorf("reader not initialized (call Validate first)")
	}
	// Read the next record
	record, err := a.s.reader.Read()
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("read csv %s failed: %w", a.s.cfg.FilePath, err)
	}

	return record, nil
}

// Convert transforms the raw CSV record ([]string) into DB values.
func (a *sourceAdapter) Convert(rawRow interface{}) ([]interface{}, error) {
	row, ok := rawRow.([]string)
	if !ok {
		return nil, fmt.Errorf("expected []string, got %T", rawRow)
	}

	if a.s.cfg.ConvertFunc == nil {
		return nil, fmt.Errorf("ConvertFunc is required")
	}

	return a.s.cfg.ConvertFunc(row)
}

// Run executes the bulk load process.
func (s *CsvSource) Run(ctx context.Context) error {
	repo := rp_dynamic.NewRepo(s.cfg.DB)

	cfg := bulkloadv3.Config{
		Repo:      repo,
		TableName: s.cfg.TableName,
		Columns:   s.cfg.Columns,
		BatchSize: s.cfg.BatchSize,
		MVName:    s.cfg.MVName,
	}
	// Use the adapter to expose the interface methods to bulkloadv3
	return bulkloadv3.Run(ctx, cfg, &sourceAdapter{s: s})
}

// Close closes the underlying file handle.
func (s *CsvSource) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}
