package rp_dynamic

import (
	"fmt"
	"strings"
)

// BulkInsertBuilder helps construct bulk insert statements and data for go-ora.
type BulkInsertBuilder struct {
	tableName string
	columns   []string
	// data holds the data in column-oriented format: data[colIndex][rowIndex]
	data [][]interface{}
}

// NewBulkInsertBuilder creates a new builder instance.
func NewBulkInsertBuilder(tableName string, columns ...string) *BulkInsertBuilder {
	// Initialize columnData slices for each column
	columnData := make([][]interface{}, len(columns))
	for i := range columnData {
		columnData[i] = make([]interface{}, 0)
	}

	return &BulkInsertBuilder{
		tableName: tableName,
		columns:   columns,
		data:      columnData,
	}
}

// AddRow adds a single row of values to the builder.
// The order of values must match the order of columns defined in NewBulkInsertBuilder.
func (b *BulkInsertBuilder) AddRow(values ...interface{}) error {
	if len(values) != len(b.columns) {
		return fmt.Errorf("bulk insert error for table '%s': expected %d values for columns %v, got %d values", b.tableName, len(b.columns), b.columns, len(values))
	}

	for i, val := range values {
		b.data[i] = append(b.data[i], val)
	}
	return nil
}

// GetSQL generates the INSERT statement with Oracle placeholders (:1, :2, etc.).
func (b *BulkInsertBuilder) GetSQL() string {
	placeholders := make([]string, len(b.columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		b.tableName,
		strings.Join(b.columns, ", "),
		strings.Join(placeholders, ", "))
}

// GetArgs returns the arguments to be passed to stmt.Exec.
// It returns a slice of slices, where each inner slice represents a column of data.
func (b *BulkInsertBuilder) GetArgs() []interface{} {
	args := make([]interface{}, len(b.data))
	for i, colData := range b.data {
		args[i] = colData
	}
	return args
}
