package bulkinsert

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
)

// InsertBatched performs bulk insert operations with any array of column values.
// It accepts slices of column values where each slice represents one column's values for all rows.
// The function inserts all provided rows in a single batch within a transaction.
// The caller is responsible for dividing data into appropriate batch sizes.
//
// Parameters:
//   - ctx: context for database operations
//   - db: sqlx database connection
//   - tableName: name of the database table to insert into
//   - columnNames: slice of column names for the insert operation (order must match columnData)
//   - columnData: variable number of slices, each representing values for one column
//
// Returns the insert duration (excluding commit time) and any error encountered.
func InsertBatched(ctx context.Context, db *sqlx.DB, tableName string, columnNames []string, columnData ...interface{}) (time.Duration, error) {
	if len(columnNames) == 0 {
		return 0, fmt.Errorf("no column names provided")
	}
	if len(columnData) == 0 {
		return 0, fmt.Errorf("no column data provided")
	}
	if len(columnData) != len(columnNames) {
		return 0, fmt.Errorf("mismatched columns: got %d data slices for %d columns", len(columnData), len(columnNames))
	}

	insertSQL := buildInsertSQL(tableName, columnNames)
	log.Println("Starting bulk insert...")

	insDuration, err := executeInsertBatch(ctx, db, insertSQL, columnData)
	if err != nil {
		return 0, err
	}

	log.Println("Bulk insert completed successfully")
	return insDuration, nil
}

// InsertStructs performs bulk insert operations with separate column names and data arrays.
// Column names are provided once, and each row is represented as a slice of values in the same order.
// The caller only needs to provide the table name, column names, and array of row data - no SQL knowledge required.
//
// Parameters:
//   - ctx: context for database operations
//   - db: sqlx database connection
//   - tableName: name of the database table to insert into
//   - columnNames: slice of column names for the insert operation
//   - rows: slice of rows, where each row is a slice of values matching the column order
//
// Returns the insert duration (excluding commit time) and any error encountered.
func InsertStructs(ctx context.Context, db *sqlx.DB, tableName string, columnNames []string, rows [][]interface{}) (time.Duration, error) {
	if len(columnNames) == 0 {
		return 0, fmt.Errorf("no column names provided")
	}
	if len(rows) == 0 {
		return 0, fmt.Errorf("no rows provided")
	}

	// Validate that all rows have the correct number of columns
	if err := validateRowDimensions(rows, len(columnNames)); err != nil {
		return 0, err
	}

	insertSQL := buildInsertSQL(tableName, columnNames)
	log.Printf("Generated SQL: %s", insertSQL)
	log.Printf("Starting bulk insert of %d rows...", len(rows))

	// Convert row-oriented data to column-oriented typed arrays
	columnData, err := transposeRowsToColumns(rows, columnNames)
	if err != nil {
		return 0, err
	}

	// Execute the batch insert
	insDuration, err := executeInsertBatch(ctx, db, insertSQL, columnData)
	if err != nil {
		return 0, err
	}

	log.Println("Bulk insert completed successfully")
	return insDuration, nil
}
