package bulkinsert

import (
	"context"
	"fmt"
	"log"
	"strings"
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

	// Build INSERT SQL statement
	placeholders := make([]string, len(columnNames))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "))

	log.Println("Starting bulk insert...")
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

	// Build typed slices per column for go-ora array binding. The driver expects
	// concrete typed slices (e.g., []int64, []string, []time.Time), not []interface{}.
	columnData := make([]interface{}, numCols)

	for colIdx := 0; colIdx < numCols; colIdx++ {
		// Validate row widths and find a sample non-nil value to determine type
		var sample interface{}
		for rowIdx, row := range rows {
			if len(row) != numCols {
				return 0, fmt.Errorf("row %d has %d values but expected %d columns", rowIdx, len(row), numCols)
			}
			if row[colIdx] != nil {
				sample = row[colIdx]
				break
			}
		}

		switch v := sample.(type) {
		case int64, int, int32, uint, uint32, uint64:
			arr := make([]int64, numRows)
			for i, row := range rows {
				val := row[colIdx]
				switch vv := val.(type) {
				case int64:
					arr[i] = vv
				case int:
					arr[i] = int64(vv)
				case int32:
					arr[i] = int64(vv)
				case uint:
					arr[i] = int64(vv)
				case uint32:
					arr[i] = int64(vv)
				case uint64:
					arr[i] = int64(vv)
				default:
					return 0, fmt.Errorf("column %d type mismatch: expected integer-like, got %T at row %d", colIdx, val, i)
				}
			}
			columnData[colIdx] = arr
		case float64, float32:
			arr := make([]float64, numRows)
			for i, row := range rows {
				val := row[colIdx]
				switch vv := val.(type) {
				case float64:
					arr[i] = vv
				case float32:
					arr[i] = float64(vv)
				default:
					return 0, fmt.Errorf("column %d type mismatch: expected float-like, got %T at row %d", colIdx, val, i)
				}
			}
			columnData[colIdx] = arr
		case bool:
			arr := make([]bool, numRows)
			for i, row := range rows {
				val := row[colIdx]
				vb, ok := val.(bool)
				if !ok {
					return 0, fmt.Errorf("column %d type mismatch: expected bool, got %T at row %d", colIdx, val, i)
				}
				arr[i] = vb
			}
			columnData[colIdx] = arr
		case time.Time:
			arr := make([]time.Time, numRows)
			for i, row := range rows {
				val := row[colIdx]
				vt, ok := val.(time.Time)
				if !ok {
					return 0, fmt.Errorf("column %d type mismatch: expected time.Time, got %T at row %d", colIdx, val, i)
				}
				arr[i] = vt
			}
			columnData[colIdx] = arr
		case string:
			arr := make([]string, numRows)
			for i, row := range rows {
				val := row[colIdx]
				vs, ok := val.(string)
				if !ok {
					return 0, fmt.Errorf("column %d type mismatch: expected string, got %T at row %d", colIdx, val, i)
				}
				arr[i] = vs
			}
			columnData[colIdx] = arr
		default:
			// Fallback: use []interface{} as-is. This is less efficient and may not be supported by all drivers.
			arr := make([]interface{}, numRows)
			for i, row := range rows {
				if len(row) != numCols {
					return 0, fmt.Errorf("row %d has %d values but expected %d columns", i, len(row), numCols)
				}
				arr[i] = row[colIdx]
			}
			log.Printf("Warning: binding column %s with generic []interface{} (type %T)", columnNames[colIdx], v)
			columnData[colIdx] = arr
		}

		// Optional: log chosen binding type to aid troubleshooting
		log.Printf("Binding column %s as %T (rows=%d)", columnNames[colIdx], columnData[colIdx], numRows)
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
