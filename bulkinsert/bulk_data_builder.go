package bulkinsert

import (
	"fmt"
)

// BulkDataBuilder provides an efficient way to build bulk insert data
// that is optimized for go-ora's column-oriented bulk insert format.
// It allows human-readable row-by-row data entry while internally
// storing data in column-oriented format to avoid transposition overhead.
type BulkDataBuilder struct {
	columnNames []string
	columnData  [][]interface{}
	numRows     int
	capacity    int
}

// NewBulkDataBuilder creates a new builder with the specified columns and initial capacity.
// Providing an accurate capacity avoids reallocation as rows are added.
//
// Parameters:
//   - columnNames: slice of column names in order
//   - capacity: expected number of rows (for pre-allocation)
func NewBulkDataBuilder(columnNames []string, capacity int) *BulkDataBuilder {
	if capacity <= 0 {
		capacity = 100 // default capacity
	}

	numCols := len(columnNames)
	columnData := make([][]interface{}, numCols)

	// Pre-allocate each column slice with the specified capacity
	for i := 0; i < numCols; i++ {
		columnData[i] = make([]interface{}, 0, capacity)
	}

	return &BulkDataBuilder{
		columnNames: columnNames,
		columnData:  columnData,
		numRows:     0,
		capacity:    capacity,
	}
}

// addRowInternal adds a row of values to the builder (internal helper).
// Values must be provided in the same order as column names.
// This method maintains human-readable row-by-row data entry
// while internally organizing data by columns for efficiency.
//
// Parameters:
//   - values: slice of values matching the column order
//
// Returns error if the number of values doesn't match the number of columns.
func (b *BulkDataBuilder) addRowInternal(values []interface{}) error {
	if len(values) != len(b.columnNames) {
		return fmt.Errorf("expected %d values but got %d", len(b.columnNames), len(values))
	}

	// Append each value to its corresponding column slice
	for i, value := range values {
		b.columnData[i] = append(b.columnData[i], value)
	}

	b.numRows++
	return nil
}

// AddRow adds a row using the Row type from data_generator.go.
// This provides an array-like interface similar to data_generator.go
// while maintaining optimized internal column-oriented storage.
//
// Parameters:
//   - row: Row containing Column structs with Name and value
//
// Returns error if column names don't match or are missing.
func (b *BulkDataBuilder) AddRow(row Row) error {
	// If builder has no column names yet, use names from the first row
	if len(b.columnNames) == 0 {
		// Initialize column names from row order
		b.columnNames = make([]string, len(row))
		for i, col := range row {
			b.columnNames[i] = col.Name
		}
		// Initialize columnData slices based on detected column count
		numCols := len(b.columnNames)
		b.columnData = make([][]interface{}, numCols)
		for i := 0; i < numCols; i++ {
			b.columnData[i] = make([]interface{}, 0, b.capacity)
		}
	}

	if len(row) != len(b.columnNames) {
		return fmt.Errorf("expected %d columns but got %d", len(b.columnNames), len(row))
	}

	// Ignore provided column names for all rows (including first after captured)
	// and use positional values only
	values := make([]interface{}, len(b.columnNames))
	for i := range b.columnNames {
		values[i] = row[i].value
	}

	return b.addRowInternal(values)
}

// AddRows adds multiple rows using the Rows type from data_generator.go.
// This provides an array-like interface similar to data_generator.go
// while maintaining optimized internal column-oriented storage.
//
// Parameters:
//   - rows: Rows containing multiple Row structs
//
// Returns error if any row is invalid.
func (b *BulkDataBuilder) AddRows(rows Rows) error {
	for i, row := range rows {
		if err := b.AddRow(row); err != nil {
			return fmt.Errorf("error adding row %d: %w", i, err)
		}
	}
	return nil
}

// GetColumnNames returns the column names in order.
func (b *BulkDataBuilder) GetColumnNames() []string {
	return b.columnNames
}

// GetColumnData returns the column-oriented data ready for go-ora bulk insert.
// This method returns []interface{} where each element is a []interface{}
// containing all values for that column. This format is directly compatible
// with go-ora's bulk insert without any copying or transposition.
//
// Returns the column data as []interface{} for use with InsertBatched.
func (b *BulkDataBuilder) GetColumnData() []interface{} {
	result := make([]interface{}, len(b.columnData))
	for i, colSlice := range b.columnData {
		result[i] = colSlice
	}
	return result
}

// GetNumRows returns the number of rows currently in the builder.
func (b *BulkDataBuilder) GetNumRows() int {
	return b.numRows
}

// Reset clears all data from the builder while preserving column names and capacity.
// This is useful for reusing the builder for another batch.
func (b *BulkDataBuilder) Reset() {
	numCols := len(b.columnNames)
	b.columnData = make([][]interface{}, numCols)

	// Re-allocate each column slice with the original capacity
	for i := 0; i < numCols; i++ {
		b.columnData[i] = make([]interface{}, 0, b.capacity)
	}

	b.numRows = 0
}
