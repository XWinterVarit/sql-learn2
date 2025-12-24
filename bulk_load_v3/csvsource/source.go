package csvsource

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"

	"sql-learn2/bulk_load_v3"

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

// Validate opens the CSV file and validates the header count and names.
func (s *CsvSource) Validate(ctx context.Context) error {
	slog.Info("Opening CSV for validation", "file", s.cfg.FilePath, bulkloadv3.LogFieldTable, s.cfg.TableName)

	f, err := os.Open(s.cfg.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", s.cfg.FilePath, err)
	}
	s.file = f

	s.reader = csv.NewReader(f)
	// Enforce that all records have the same number of fields as the first record (header).
	s.reader.FieldsPerRecord = 0

	// 1. Read and Validate Header
	header, err := s.reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read header from %s: %w", s.cfg.FilePath, err)
	}

	if s.cfg.ExpectedHeaderCount > 0 {
		if len(header) != s.cfg.ExpectedHeaderCount {
			return fmt.Errorf("header count mismatch: got %d, want %d", len(header), s.cfg.ExpectedHeaderCount)
		}
	}

	if len(s.cfg.ExpectedHeaders) > 0 {
		for index, expectedName := range s.cfg.ExpectedHeaders {
			if index < 0 || index >= len(header) {
				return fmt.Errorf("expected header index %d is out of bounds (header length: %d)", index, len(header))
			}
			if header[index] != expectedName {
				return fmt.Errorf("header name mismatch at index %d: got '%s', want '%s'", index, header[index], expectedName)
			}
		}
	}

	slog.Info("CSV validation successful", "file", s.cfg.FilePath, bulkloadv3.LogFieldTable, s.cfg.TableName)
	return nil
}

// Next reads the next data row from the CSV.
func (s *CsvSource) Next(ctx context.Context) (interface{}, error) {
	if s.reader == nil {
		return nil, fmt.Errorf("reader not initialized (call Validate first)")
	}
	// Read the next record
	record, err := s.reader.Read()
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("read csv %s failed: %w", s.cfg.FilePath, err)
	}

	return record, nil
}

// Convert transforms the raw CSV record ([]string) into DB values.
func (s *CsvSource) Convert(rawRow interface{}) ([]interface{}, error) {
	row, ok := rawRow.([]string)
	if !ok {
		return nil, fmt.Errorf("expected []string, got %T", rawRow)
	}

	if s.cfg.ConvertFunc == nil {
		return nil, fmt.Errorf("ConvertFunc is required")
	}

	return s.cfg.ConvertFunc(row)
}

// Run executes the bulk load process.
func (s *CsvSource) Run(ctx context.Context) error {
	cfg := bulkloadv3.Config{
		DB:        s.cfg.DB,
		TableName: s.cfg.TableName,
		Columns:   s.cfg.Columns,
		BatchSize: s.cfg.BatchSize,
	}
	return bulkloadv3.Run(ctx, cfg, s)
}

// Close closes the underlying file handle.
func (s *CsvSource) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}
