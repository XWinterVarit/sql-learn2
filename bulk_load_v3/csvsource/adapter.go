package csvsource

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"

	"sql-learn2/bulk_load_v3"
)

// sourceAdapter adapts CsvSource to the bulkloadv3.Source interface.
// It directly implements the logic for Validate, Next, and Convert,
// operating on the underlying CsvSource state.
type sourceAdapter struct {
	*CsvSource
}

// Validate opens the CSV file, validates that all required headers exist,
// and prepares the column mapping.
func (a *sourceAdapter) Validate(ctx context.Context) error {
	slog.Info("Opening CSV for validation", bulkloadv3.LogFieldFile, a.cfg.FilePath, bulkloadv3.LogFieldTable, a.cfg.TableName)

	if err := a.openFile(); err != nil {
		return err
	}

	header, err := a.validateHeader()
	if err != nil {
		return err
	}

	if err := a.mapColumns(header); err != nil {
		return err
	}

	slog.Info("CSV validation successful", bulkloadv3.LogFieldFile, a.cfg.FilePath, bulkloadv3.LogFieldTable, a.cfg.TableName)
	return nil
}

func (a *sourceAdapter) openFile() error {
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
	return nil
}

func (a *sourceAdapter) validateHeader() ([]string, error) {
	header, err := a.reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read header from %s: %w", a.cfg.FilePath, err)
	}

	if a.cfg.ExpectedHeaderCount > 0 {
		if len(header) != a.cfg.ExpectedHeaderCount {
			return nil, fmt.Errorf("header count mismatch: got %d, want %d", len(header), a.cfg.ExpectedHeaderCount)
		}
	}
	return header, nil
}

func (a *sourceAdapter) mapColumns(header []string) error {
	headerMap := make(map[string]int)
	for i, name := range header {
		headerMap[name] = i
	}

	if len(a.cfg.Parsers) == 0 {
		return fmt.Errorf("no parsers defined")
	}

	a.columnIndices = make([]int, len(a.cfg.Parsers))
	for i, p := range a.cfg.Parsers {
		if p.CSVHeader == "" {
			// Special case: No CSV header required (e.g., fixed value).
			// Use -1 to indicate no CSV column mapping.
			a.columnIndices[i] = -1
			continue
		}
		idx, ok := headerMap[p.CSVHeader]
		if !ok {
			return fmt.Errorf("csv header '%s' not found in file", p.CSVHeader)
		}
		a.columnIndices[i] = idx
	}
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

// Convert transforms the raw CSV record ([]string) into DB values using the configured Parsers.
func (a *sourceAdapter) Convert(rawRow interface{}) ([]interface{}, error) {
	row, ok := rawRow.([]string)
	if !ok {
		return nil, fmt.Errorf("expected []string, got %T", rawRow)
	}

	values := make([]interface{}, len(a.cfg.Parsers))
	for i, parser := range a.cfg.Parsers {
		val, err := a.parseField(i, parser, row)
		if err != nil {
			return nil, err
		}
		values[i] = val
	}

	return values, nil
}

func (a *sourceAdapter) parseField(index int, parser Parser, row []string) (interface{}, error) {
	csvIdx := a.columnIndices[index]
	var csvVal string

	if csvIdx != -1 {
		// csv.Reader ensures rows have enough fields, but a safety check is cheap
		if csvIdx >= len(row) {
			return nil, fmt.Errorf("csv index %d out of bounds for row with length %d", csvIdx, len(row))
		}
		csvVal = row[csvIdx]
	}
	// Else: csvIdx == -1, csvVal remains "" (empty string)

	if parser.ParserFunc != nil {
		val, err := parser.ParserFunc(csvVal)
		if err != nil {
			return nil, fmt.Errorf("parse error for column '%s' (csv header '%s') value '%s': %w", parser.DBColumn, parser.CSVHeader, csvVal, err)
		}
		return val, nil
	}
	// Default to string if no parser provided
	return csvVal, nil
}
