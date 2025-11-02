package bulkinsert

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// BatchInsertParams holds the parameters for batch insert operations.
type BatchInsertParams struct {
	// InsertSQL is the SQL statement for insertion (with parameter placeholders)
	InsertSQL string
}

// InsertBatched performs bulk insert operations with any array of column values.
// It accepts slices of column values where each slice represents one column's values for all rows.
// The function inserts all provided rows in a single batch within a transaction.
// The caller is responsible for dividing data into appropriate batch sizes.
//
// Parameters:
//   - ctx: context for database operations
//   - db: sqlx database connection
//   - params: batch insert parameters (SQL statement)
//   - columnData: variable number of slices, each representing values for one column
//
// Returns the insert duration (excluding commit time) and any error encountered.
func InsertBatched(ctx context.Context, db *sqlx.DB, params BatchInsertParams, columnData ...interface{}) (time.Duration, error) {
	if len(columnData) == 0 {
		return 0, fmt.Errorf("no column data provided")
	}

	log.Println("Starting bulk insert...")
	insStart := time.Now()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, params.InsertSQL)
	if err != nil {
		return 0, fmt.Errorf("prepare insert statement failed: %w", err)
	}
	defer stmt.Close()

	// Insert all provided data in a single batch
	_, err = stmt.ExecContext(ctx, columnData...)
	if err != nil {
		return 0, fmt.Errorf("insert batch failed: %w", err)
	}

	log.Println("Committing transaction...")
	commitStart := time.Now()
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit failed: %w", err)
	}
	commitDuration := time.Since(commitStart)

	insDuration := time.Since(insStart) - commitDuration
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

	// Build INSERT SQL statement
	placeholders := make([]string, len(columnNames))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "))

	log.Printf("Generated SQL: %s", insertSQL)
	log.Printf("Starting bulk insert of %d rows...", len(rows))
	insStart := time.Now()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return 0, fmt.Errorf("prepare insert statement failed: %w", err)
	}
	defer stmt.Close()

	// Organize data by column (transpose from row-oriented to column-oriented)
	numRows := len(rows)
	numCols := len(columnNames)
	columnData := make([]interface{}, numCols)

	for colIdx := 0; colIdx < numCols; colIdx++ {
		// Create a slice for this column's values
		columnSlice := make([]interface{}, numRows)

		for rowIdx, row := range rows {
			if len(row) != numCols {
				return 0, fmt.Errorf("row %d has %d values but expected %d columns", rowIdx, len(row), numCols)
			}
			columnSlice[rowIdx] = row[colIdx]
		}

		columnData[colIdx] = columnSlice
	}

	// Insert all data in a single batch
	_, err = stmt.ExecContext(ctx, columnData...)
	if err != nil {
		return 0, fmt.Errorf("insert batch failed: %w", err)
	}

	log.Println("Committing transaction...")
	commitStart := time.Now()
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit failed: %w", err)
	}
	commitDuration := time.Since(commitStart)

	insDuration := time.Since(insStart) - commitDuration
	log.Println("Bulk insert completed successfully")
	return insDuration, nil
}
