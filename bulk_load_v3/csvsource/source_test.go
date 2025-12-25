package csvsource

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Helper to create a temp CSV file
func createTempCSV(t *testing.T, content [][]string) string {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.csv")
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if err := w.WriteAll(content); err != nil {
		t.Fatalf("failed to write csv content: %v", err)
	}
	w.Flush()
	return filePath
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name            string
		content         [][]string
		expectedCount   int
		expectedHeaders map[int]string
		expectError     bool
		errorContains   string
	}{
		{
			name: "Success Basic",
			content: [][]string{
				{"ID", "NAME"},
				{"1", "Alice"},
			},
			expectedCount:   2,
			expectedHeaders: map[int]string{0: "ID", 1: "NAME"},
			expectError:     false,
		},
		{
			name: "Success No Header Check",
			content: [][]string{
				{"ID", "NAME"},
			},
			expectedCount:   0, // 0 means ignored
			expectedHeaders: nil,
			expectError:     false,
		},
		{
			name: "Fail Header Count",
			content: [][]string{
				{"ID", "NAME", "EXTRA"},
			},
			expectedCount: 2,
			expectError:   true,
			errorContains: "header count mismatch",
		},
		{
			name: "Fail Header Name",
			content: [][]string{
				{"ID", "WRONG"},
			},
			expectedCount:   2,
			expectedHeaders: map[int]string{1: "NAME"},
			expectError:     true,
			errorContains:   "header name mismatch",
		},
		{
			name: "Fail Header Index Out of Bounds",
			content: [][]string{
				{"ID"},
			},
			expectedHeaders: map[int]string{5: "NAME"},
			expectError:     true,
			errorContains:   "out of bounds",
		},
		{
			name:          "Fail File Not Found",
			content:       nil, // Special case handled in loop
			expectError:   true,
			errorContains: "failed to open file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var filePath string
			if tt.content != nil {
				filePath = createTempCSV(t, tt.content)
			} else {
				filePath = "non_existent_file.csv"
			}

			cfg := Config{
				FilePath:            filePath,
				ExpectedHeaderCount: tt.expectedCount,
				ExpectedHeaders:     tt.expectedHeaders,
				TableName:           "TEST_TABLE", // Required for logging
			}
			src, closer := New(cfg)
			defer closer()
			adapter := &sourceAdapter{CsvSource: src}

			err := adapter.Validate(context.Background())

			if tt.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errorContains != "" {
					if !contains(err.Error(), tt.errorContains) {
						t.Errorf("error %q does not contain %q", err.Error(), tt.errorContains)
					}
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestNext(t *testing.T) {
	content := [][]string{
		{"ID", "NAME"},
		{"1", "Alice"},
		{"2", "Bob"},
	}
	filePath := createTempCSV(t, content)

	cfg := Config{
		FilePath:  filePath,
		TableName: "TEST_TABLE",
	}
	src, closer := New(cfg)
	defer closer()
	adapter := &sourceAdapter{CsvSource: src}

	// Must validate first to init reader
	if err := adapter.Validate(context.Background()); err != nil {
		t.Fatalf("Validate failed: %v", err)
	}

	// Read Row 1
	row1, err := adapter.Next(context.Background())
	if err != nil {
		t.Fatalf("Next (1) failed: %v", err)
	}
	// encoding/csv returns []string
	r1, ok := row1.([]string)
	if !ok {
		t.Fatalf("expected []string, got %T", row1)
	}
	if !reflect.DeepEqual(r1, []string{"1", "Alice"}) {
		t.Errorf("row 1 mismatch: got %v", r1)
	}

	// Read Row 2
	row2, err := adapter.Next(context.Background())
	if err != nil {
		t.Fatalf("Next (2) failed: %v", err)
	}
	r2 := row2.([]string)
	if !reflect.DeepEqual(r2, []string{"2", "Bob"}) {
		t.Errorf("row 2 mismatch: got %v", r2)
	}

	// EOF
	_, err = adapter.Next(context.Background())
	if err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestNext_WithoutValidate(t *testing.T) {
	src, closer := New(Config{})
	defer closer()
	adapter := &sourceAdapter{CsvSource: src}

	_, err := adapter.Next(context.Background())
	if err == nil {
		t.Error("expected error calling Next without Validate, got nil")
	}
}

func TestNext_VariableFieldsError(t *testing.T) {
	// csv.Reader.FieldsPerRecord = 0 (set in Validate) enforces row length matches header
	dir := t.TempDir()
	filePath := filepath.Join(dir, "bad_rows.csv")
	f, _ := os.Create(filePath)
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write([]string{"H1", "H2"})
	w.Write([]string{"D1"}) // Too few fields
	w.Flush()

	cfg := Config{FilePath: filePath, TableName: "TEST"}
	src, closer := New(cfg)
	defer closer()
	adapter := &sourceAdapter{CsvSource: src}

	adapter.Validate(context.Background())

	_, err := adapter.Next(context.Background())
	if err == nil {
		t.Error("expected error for row length mismatch, got nil")
	}
}

func TestConvert(t *testing.T) {
	convertCalled := false
	mockConvert := func(row []string) ([]interface{}, error) {
		convertCalled = true
		if row[0] == "fail" {
			return nil, fmt.Errorf("mock error")
		}
		return []interface{}{"converted"}, nil
	}

	cfg := Config{ConvertFunc: mockConvert}
	src, closer := New(cfg)
	defer closer()
	adapter := &sourceAdapter{CsvSource: src}

	// Success case
	res, err := adapter.Convert([]string{"ok"})
	if err != nil {
		t.Errorf("Convert failed: %v", err)
	}
	if !convertCalled {
		t.Error("ConvertFunc was not called")
	}
	if len(res) != 1 || res[0] != "converted" {
		t.Errorf("unexpected result: %v", res)
	}

	// Error case
	_, err = adapter.Convert([]string{"fail"})
	if err == nil {
		t.Error("expected error from Convert, got nil")
	}

	// Invalid input type
	_, err = adapter.Convert(123) // Not []string
	if err == nil {
		t.Error("expected error for invalid input type, got nil")
	}
}

func TestRun_Validation(t *testing.T) {
	// Test fail-fast validation in Run
	src, closer := New(Config{}) // Empty config
	defer closer()

	err := src.Run(context.Background())
	if err == nil {
		t.Error("expected error for empty config, got nil")
	}
	// We expect "DB is required" or similar
}

// Simple helper for string containment
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	// Basic implementation since strings.Contains is not available?
	// strings package is standard. I should use import strings.
	// But I didn't import it. I'll check imports.
	// Ah, I missed importing "strings" in the file content above.
	// I'll stick to a simple loop or re-create file with strings.
	// Let's use fmt.Sprintf and strict equality or just allow panic if I messed up imports?
	// No, I can implement a simple check.
	// actually I can just use strict check for now or rely on the error message structure.
	// Or I can rewrite the file with "strings" import.
	// I'll rewrite the file in a second step if needed, but wait.
	// I will just implement a naive contains check.

	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
