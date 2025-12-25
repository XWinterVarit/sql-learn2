package rp_dynamic

import (
	"reflect"
	"testing"
	"time"
)

func TestNewBulkInsertBuilder(t *testing.T) {
	tableName := "TEST_TABLE"
	columns := []string{"COL1", "COL2", "COL3"}
	builder := NewBulkInsertBuilder(tableName, columns...)

	if builder.tableName != tableName {
		t.Errorf("expected tableName %s, got %s", tableName, builder.tableName)
	}

	if len(builder.columns) != len(columns) {
		t.Errorf("expected %d columns, got %d", len(columns), len(builder.columns))
	}

	if len(builder.data) != len(columns) {
		t.Errorf("expected data slice length %d, got %d", len(columns), len(builder.data))
	}

	for i, colData := range builder.data {
		if len(colData) != 0 {
			t.Errorf("expected empty data slice for column %d, got length %d", i, len(colData))
		}
	}
}

func TestAddRow(t *testing.T) {
	builder := NewBulkInsertBuilder("TEST_TABLE", "ID", "NAME")

	// Test case 1: Add valid row
	err := builder.AddRow(1, "Alice")
	if err != nil {
		t.Errorf("unexpected error adding valid row: %v", err)
	}

	// Verify data storage
	if len(builder.data[0]) != 1 || builder.data[0][0] != 1 {
		t.Errorf("expected data[0][0] to be 1")
	}
	if len(builder.data[1]) != 1 || builder.data[1][0] != "Alice" {
		t.Errorf("expected data[1][0] to be 'Alice'")
	}

	// Test case 2: Add row with incorrect number of values (too few)
	err = builder.AddRow(2)
	if err == nil {
		t.Error("expected error adding row with too few values, got nil")
	}

	// Test case 3: Add row with incorrect number of values (too many)
	err = builder.AddRow(3, "Bob", "Extra")
	if err == nil {
		t.Error("expected error adding row with too many values, got nil")
	}

	// Test case 4: Add another valid row
	err = builder.AddRow(2, "Bob")
	if err != nil {
		t.Errorf("unexpected error adding second valid row: %v", err)
	}

	// Verify data storage for second row
	if len(builder.data[0]) != 2 || builder.data[0][1] != 2 {
		t.Errorf("expected data[0][1] to be 2")
	}
	if len(builder.data[1]) != 2 || builder.data[1][1] != "Bob" {
		t.Errorf("expected data[1][1] to be 'Bob'")
	}
}

func TestGetSQL(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		columns   []string
		expected  string
	}{
		{
			name:      "Single Column",
			tableName: "USERS",
			columns:   []string{"ID"},
			expected:  "INSERT INTO USERS (ID) VALUES (:1)",
		},
		{
			name:      "Multiple Columns",
			tableName: "PRODUCTS",
			columns:   []string{"ID", "CODE", "PRICE"},
			expected:  "INSERT INTO PRODUCTS (ID, CODE, PRICE) VALUES (:1, :2, :3)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewBulkInsertBuilder(tt.tableName, tt.columns...)
			got := builder.GetSQL()
			if got != tt.expected {
				t.Errorf("GetSQL() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestGetArgs(t *testing.T) {
	builder := NewBulkInsertBuilder("TEST_TABLE", "ID", "NAME")

	_ = builder.AddRow(1, "Alice")
	_ = builder.AddRow(2, "Bob")
	_ = builder.AddRow(3, "Charlie")

	args := builder.GetArgs()

	// We expect args to be a slice of slices: [[1, 2, 3], ["Alice", "Bob", "Charlie"]]
	if len(args) != 2 {
		t.Fatalf("expected 2 columns of args, got %d", len(args))
	}

	// Verify Column 1 (ID)
	col1, ok := args[0].([]interface{})
	if !ok {
		t.Fatalf("expected args[0] to be []interface{}, got %T", args[0])
	}
	expectedCol1 := []interface{}{1, 2, 3}
	if !reflect.DeepEqual(col1, expectedCol1) {
		t.Errorf("expected col1 data %v, got %v", expectedCol1, col1)
	}

	// Verify Column 2 (NAME)
	col2, ok := args[1].([]interface{})
	if !ok {
		t.Fatalf("expected args[1] to be []interface{}, got %T", args[1])
	}
	expectedCol2 := []interface{}{"Alice", "Bob", "Charlie"}
	if !reflect.DeepEqual(col2, expectedCol2) {
		t.Errorf("expected col2 data %v, got %v", expectedCol2, col2)
	}
}

func TestBuilder_ComplexTypes(t *testing.T) {
	builder := NewBulkInsertBuilder("COMPLEX_TABLE", "INT_COL", "STR_COL", "FLOAT_COL", "BOOL_COL", "TIME_COL", "NIL_COL")

	now := time.Now()
	err := builder.AddRow(123, "text", 3.14, true, now, nil)
	if err != nil {
		t.Fatalf("AddRow failed: %v", err)
	}

	args := builder.GetArgs()

	// Helper to check value
	check := func(colIndex int, expected interface{}, name string) {
		colData := args[colIndex].([]interface{})
		if len(colData) != 1 {
			t.Errorf("%s: expected 1 row, got %d", name, len(colData))
			return
		}
		if !reflect.DeepEqual(colData[0], expected) {
			t.Errorf("%s: expected %v, got %v", name, expected, colData[0])
		}
	}

	check(0, 123, "INT_COL")
	check(1, "text", "STR_COL")
	check(2, 3.14, "FLOAT_COL")
	check(3, true, "BOOL_COL")
	check(4, now, "TIME_COL")
	check(5, nil, "NIL_COL")
}

func TestBuilder_SpecialCharacters(t *testing.T) {
	builder := NewBulkInsertBuilder("SPECIAL_CHARS", "DATA")

	specialStrings := []string{
		"Simple string",
		"String with 'quotes'",
		`String with "double quotes"`,
		"String with \n newline",
		"Emoji 🚀",
		"SQL Injection attempt: '; DROP TABLE USERS; --",
		"Japanese: こんにちは",
	}

	for _, s := range specialStrings {
		if err := builder.AddRow(s); err != nil {
			t.Errorf("failed to add special string '%s': %v", s, err)
		}
	}

	args := builder.GetArgs()
	colData := args[0].([]interface{})

	if len(colData) != len(specialStrings) {
		t.Fatalf("expected %d rows, got %d", len(specialStrings), len(colData))
	}

	for i, s := range specialStrings {
		if colData[i] != s {
			t.Errorf("row %d: expected '%s', got '%s'", i, s, colData[i])
		}
	}
}

func TestBuilder_LargeBatch(t *testing.T) {
	builder := NewBulkInsertBuilder("LARGE_BATCH", "ID", "VALUE")
	rowCount := 10000

	for i := 0; i < rowCount; i++ {
		if err := builder.AddRow(i, float64(i)*1.5); err != nil {
			t.Fatalf("failed to add row %d: %v", i, err)
		}
	}

	args := builder.GetArgs()

	if len(args) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(args))
	}

	col1 := args[0].([]interface{})
	col2 := args[1].([]interface{})

	if len(col1) != rowCount {
		t.Errorf("expected %d rows in col1, got %d", rowCount, len(col1))
	}
	if len(col2) != rowCount {
		t.Errorf("expected %d rows in col2, got %d", rowCount, len(col2))
	}

	// Verify a few random samples
	if col1[0] != 0 || col2[0] != 0.0 {
		t.Error("row 0 data mismatch")
	}
	lastIdx := rowCount - 1
	if col1[lastIdx] != lastIdx || col2[lastIdx] != float64(lastIdx)*1.5 {
		t.Error("last row data mismatch")
	}
}

func TestBuilder_EdgeCases(t *testing.T) {
	t.Run("No Columns", func(t *testing.T) {
		builder := NewBulkInsertBuilder("NO_COLS")
		err := builder.AddRow() // Should accept 0 args
		if err != nil {
			t.Errorf("AddRow() failed for no-columns builder: %v", err)
		}

		sql := builder.GetSQL()
		expectedSQL := "INSERT INTO NO_COLS () VALUES ()"
		if sql != expectedSQL {
			t.Errorf("GetSQL() = %q, want %q", sql, expectedSQL)
		}

		args := builder.GetArgs()
		if len(args) != 0 {
			t.Errorf("expected 0 args columns, got %d", len(args))
		}
	})

	t.Run("Empty Builder GetArgs", func(t *testing.T) {
		builder := NewBulkInsertBuilder("EMPTY_BUILDER", "COL1")
		args := builder.GetArgs()
		if len(args) != 1 {
			t.Fatalf("expected 1 column, got %d", len(args))
		}
		col1 := args[0].([]interface{})
		if len(col1) != 0 {
			t.Errorf("expected 0 rows, got %d", len(col1))
		}
	})
}
