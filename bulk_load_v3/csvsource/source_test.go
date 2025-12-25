package csvsource

import (
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
)

// Helper to create a temp CSV file with specific delimiter
func createTempCSVWithDelimiter(t *testing.T, content [][]string, delimiter rune) string {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.csv")
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if delimiter != 0 {
		w.Comma = delimiter
	}
	if err := w.WriteAll(content); err != nil {
		t.Fatalf("failed to write csv content: %v", err)
	}
	w.Flush()
	return filePath
}

// Helper to create a temp CSV file (default comma)
func createTempCSV(t *testing.T, content [][]string) string {
	return createTempCSVWithDelimiter(t, content, 0)
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name          string
		content       [][]string
		parsers       []Parser
		expectError   bool
		errorContains string
	}{
		{
			name: "Success Basic",
			content: [][]string{
				{"ID", "NAME"},
				{"1", "Alice"},
			},
			parsers: []Parser{
				{CSVHeader: "ID", DBColumn: "USER_ID", ParserFunc: ParseInt},
				{CSVHeader: "NAME", DBColumn: "USER_NAME", ParserFunc: ParseString},
			},
			expectError: false,
		},
		{
			name: "Success Empty CSVHeader",
			content: [][]string{
				{"ID"},
				{"1"},
			},
			parsers: []Parser{
				{CSVHeader: "ID", DBColumn: "USER_ID", ParserFunc: ParseInt},
				{CSVHeader: "", DBColumn: "CREATED_AT", ParserFunc: func(s string) (interface{}, error) { return "NOW", nil }},
			},
			expectError: false,
		},
		{
			name: "Fail Missing Header",
			content: [][]string{
				{"ID"},
			},
			parsers: []Parser{
				{CSVHeader: "NAME", DBColumn: "USER_NAME"},
			},
			expectError:   true,
			errorContains: "not found",
		},
		{
			name:          "Fail No Parsers",
			content:       [][]string{{"ID"}},
			parsers:       nil,
			expectError:   true,
			errorContains: "no parsers defined",
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
				FilePath:  filePath,
				Parsers:   tt.parsers,
				TableName: "TEST_TABLE",
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
	// Use custom delimiter to test that configuration as well
	filePath := createTempCSVWithDelimiter(t, content, ';')

	cfg := Config{
		FilePath:  filePath,
		Delimiter: ';',
		TableName: "TEST_TABLE",
		Parsers: []Parser{
			{CSVHeader: "ID", DBColumn: "USER_ID", ParserFunc: ParseInt},
		},
	}
	src, closer := New(cfg)
	defer closer()
	adapter := &sourceAdapter{CsvSource: src}

	if err := adapter.Validate(context.Background()); err != nil {
		t.Fatalf("Validate failed: %v", err)
	}

	// Read Row 1
	row1, err := adapter.Next(context.Background())
	if err != nil {
		t.Fatalf("Next (1) failed: %v", err)
	}
	rec1, ok := row1.([]string)
	if !ok {
		t.Fatalf("expected []string, got %T", row1)
	}
	if rec1[0] != "1" || rec1[1] != "Alice" {
		t.Errorf("unexpected row 1 content: %v", rec1)
	}

	// Read Row 2
	row2, err := adapter.Next(context.Background())
	if err != nil {
		t.Fatalf("Next (2) failed: %v", err)
	}
	rec2, ok := row2.([]string)
	if !ok {
		t.Fatalf("expected []string, got %T", row2)
	}
	if rec2[0] != "2" || rec2[1] != "Bob" {
		t.Errorf("unexpected row 2 content: %v", rec2)
	}

	// Read EOF
	_, err = adapter.Next(context.Background())
	if err == nil {
		t.Error("expected EOF, got nil")
	} else if err.Error() != "EOF" { // io.EOF error string is "EOF"
		// Better check: err == io.EOF
		// But here we rely on standard library behavior
	}
}

func TestConvert(t *testing.T) {
	content := [][]string{
		{"ID", "NAME"},
		{"1", "Alice"},
	}
	filePath := createTempCSV(t, content)

	cfg := Config{
		FilePath:  filePath,
		TableName: "TEST_TABLE",
		Parsers: []Parser{
			{CSVHeader: "ID", DBColumn: "USER_ID", ParserFunc: ParseInt},
			{CSVHeader: "", DBColumn: "FIXED", ParserFunc: func(_ string) (interface{}, error) { return "fixed", nil }},
		},
	}
	src, closer := New(cfg)
	defer closer()
	adapter := &sourceAdapter{CsvSource: src}

	if err := adapter.Validate(context.Background()); err != nil {
		t.Fatalf("Validate failed: %v", err)
	}

	// Test Convert
	rawRow := []string{"1", "Alice"}
	res, err := adapter.Convert(rawRow)
	if err != nil {
		t.Fatalf("Convert failed: %v", err)
	}

	if len(res) != 2 {
		t.Fatalf("expected 2 values, got %d", len(res))
	}
	if res[0] != 1 {
		t.Errorf("expected 1, got %v", res[0])
	}
	if res[1] != "fixed" {
		t.Errorf("expected 'fixed', got %v", res[1])
	}
}

func TestConvert_ParserError(t *testing.T) {
	content := [][]string{
		{"ID"},
		{"abc"}, // Invalid int
	}
	filePath := createTempCSV(t, content)

	cfg := Config{
		FilePath:  filePath,
		TableName: "TEST_TABLE",
		Parsers: []Parser{
			{CSVHeader: "ID", DBColumn: "USER_ID", ParserFunc: ParseInt},
		},
	}
	src, closer := New(cfg)
	defer closer()
	adapter := &sourceAdapter{CsvSource: src}

	if err := adapter.Validate(context.Background()); err != nil {
		t.Fatalf("Validate failed: %v", err)
	}

	rawRow := []string{"abc"}
	_, err := adapter.Convert(rawRow)
	if err == nil {
		t.Error("expected error for invalid int parsing, got nil")
	} else if !contains(err.Error(), "invalid syntax") && !contains(err.Error(), "parse error") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestConvert_IndexOutOfBounds(t *testing.T) {
	// This simulates a row that is shorter than expected (though csv reader normally handles this if FieldsPerRecord is set)
	content := [][]string{
		{"ID", "NAME"},
	}
	filePath := createTempCSV(t, content)

	cfg := Config{
		FilePath:  filePath,
		TableName: "TEST_TABLE",
		Parsers: []Parser{
			{CSVHeader: "NAME", DBColumn: "USER_NAME", ParserFunc: ParseString},
		},
	}
	src, closer := New(cfg)
	defer closer()
	adapter := &sourceAdapter{CsvSource: src}

	if err := adapter.Validate(context.Background()); err != nil {
		t.Fatalf("Validate failed: %v", err)
	}

	// The parser expects NAME at index 1.
	// If we provide a row with only 1 element, it should fail.
	rawRow := []string{"1"}
	_, err := adapter.Convert(rawRow)
	if err == nil {
		t.Error("expected error for out of bounds, got nil")
	} else if !contains(err.Error(), "out of bounds") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRun_Validation(t *testing.T) {
	// Test basic Run validation logic (without mocking the whole loader/repo which is complex here)
	// We just check that Run fails fast if config is invalid.
	src, closer := New(Config{}) // Empty config
	defer closer()

	err := src.Run(context.Background())
	if err == nil {
		t.Error("Run expected error for empty config, got nil")
	} else if !contains(err.Error(), "database connection (DB) is required") {
		t.Errorf("unexpected error: %v", err)
	}
}

// Simple helper for string containment
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
