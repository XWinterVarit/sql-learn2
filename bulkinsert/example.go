package bulkinsert

import (
	"context"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
)

// ExampleBasicUsage demonstrates the basic usage of BulkDataBuilder
// with Row/Rows types and the convenience Insert() method.
func ExampleBasicUsage(ctx context.Context, db *sqlx.DB) error {
	// Step 1: Define column names as variables (for pointer references)
	colID := ColID
	colName := ColName
	colLastName := ColLastName
	colBalance := ColBalance

	// Step 2: Create a BulkDataBuilder with column names and capacity (no table name needed)
	columnNames := []string{ColID, ColName, ColLastName, ColBalance}
	builder := NewBulkDataBuilder(columnNames, 100)

	// Step 3: Add rows using the Row type (array-like interface)
	// This style is easy to read and maintain with low error prone arrangement
	err := builder.AddRow(Row{
		Column{Name: colID, value: 1},
		Column{Name: colName, value: "Alice"},
		Column{Name: colLastName, value: "Anderson"},
		Column{Name: colBalance, value: 5000.00},
	})
	if err != nil {
		return fmt.Errorf("failed to add row: %w", err)
	}

	err = builder.AddRow(Row{
		Column{Name: colID, value: 2},
		Column{Name: colName, value: "Bob"},
		Column{Name: colLastName, value: "Brown"},
		Column{Name: colBalance, value: 3500.50},
	})
	if err != nil {
		return fmt.Errorf("failed to add row: %w", err)
	}

	// Step 4: Add multiple rows at once using AddRows
	moreRows := Rows{
		Row{
			Column{Name: colID, value: 3},
			Column{Name: colName, value: "Charlie"},
			Column{Name: colLastName, value: "Clark"},
			Column{Name: colBalance, value: 7200.75},
		},
		Row{
			Column{Name: colID, value: 4},
			Column{Name: colName, value: "Diana"},
			Column{Name: colLastName, value: "Davis"},
			Column{Name: colBalance, value: 4800.25},
		},
		Row{
			Column{Name: colID, value: 5},
			Column{Name: colName, value: "Edward"},
			Column{Name: colLastName, value: "Evans"},
			Column{Name: colBalance, value: 6100.00},
		},
	}

	err = builder.AddRows(moreRows)
	if err != nil {
		return fmt.Errorf("failed to add rows: %w", err)
	}

	// Step 5: Get SQL and column data from builder, then execute bulk insert
	// The builder provides these automatically - NO manual SQL writing required!
	columnData := builder.GetColumnData()

	duration, err := InsertBatched(ctx, db, "employees", builder.GetColumnNames(), columnData...)
	if err != nil {
		return fmt.Errorf("bulk insert failed: %w", err)
	}

	log.Printf("Successfully inserted %d rows in %v", builder.GetNumRows(), duration)
	return nil
}

// with InsertBatched for more control over the insert process.
// The builder provides the SQL statement and column data, so the caller doesn't
// need to write SQL manually, but still has full control over execution.
func ExampleManualControl(ctx context.Context, db *sqlx.DB) error {
	// Step 1: Define column names as variables (for pointer references)
	colID := ColID
	colName := ColName
	colLastName := ColLastName
	colBalance := ColBalance

	// Step 2: Create a BulkDataBuilder with column names and capacity (no table name needed)
	columnNames := []string{ColID, ColName, ColLastName, ColBalance}
	builder := NewBulkDataBuilder(columnNames, 100)

	// Step 3: Add rows using the Row type (array-like interface)
	err := builder.AddRow(Row{
		Column{Name: colID, value: 1},
		Column{Name: colName, value: "Alice"},
		Column{Name: colLastName, value: "Anderson"},
		Column{Name: colBalance, value: 5000.00},
	})
	if err != nil {
		return fmt.Errorf("failed to add row: %w", err)
	}

	err = builder.AddRow(Row{
		Column{Name: colID, value: 2},
		Column{Name: colName, value: "Bob"},
		Column{Name: colLastName, value: "Brown"},
		Column{Name: colBalance, value: 3500.50},
	})
	if err != nil {
		return fmt.Errorf("failed to add row: %w", err)
	}

	// Step 4: Get the SQL statement and column data from builder
	// The builder provides these automatically - NO manual SQL writing required!
	columnData := builder.GetColumnData() // Gets column-oriented data ready for go-ora

	// Step 5: Execute bulk insert using InsertBatched with builder-provided params
	// You have full control over when/how to execute, but no SQL writing needed!
	duration, err := InsertBatched(ctx, db, "employees", builder.GetColumnNames(), columnData...)
	if err != nil {
		return fmt.Errorf("bulk insert failed: %w", err)
	}

	log.Printf("Successfully inserted %d rows in %v using manual control", builder.GetNumRows(), duration)
	return nil
}

// ExampleWithGeneratedData demonstrates using the GenerateRow function
// from data_generator.go with BulkDataBuilder.
func ExampleWithGeneratedData(ctx context.Context, db *sqlx.DB) error {
	// Step 1: Generate rows using the GenerateRow function
	rows := GenerateRow() // Generates 100 rows with sample data

	// Step 2: Extract column names from the generated rows
	columnNames := rows.GetColumnsNames()

	// Step 3: Create a BulkDataBuilder
	builder := NewBulkDataBuilder(columnNames, len(rows))

	// Step 4: Add all generated rows to the builder
	err := builder.AddRows(rows)
	if err != nil {
		return fmt.Errorf("failed to add generated rows: %w", err)
	}

	// Step 5: Get SQL and column data from builder, then execute bulk insert
	// The builder provides these automatically - NO manual SQL writing required!
	columnData := builder.GetColumnData()

	duration, err := InsertBatched(ctx, db, "employees", builder.GetColumnNames(), columnData...)
	if err != nil {
		return fmt.Errorf("bulk insert failed: %w", err)
	}

	log.Printf("Successfully inserted %d rows in %v", builder.GetNumRows(), duration)
	return nil
}

// ExampleWithBatches demonstrates processing large datasets in batches
// to avoid memory issues and transaction timeouts.
func ExampleWithBatches(ctx context.Context, db *sqlx.DB, totalRows int, batchSize int) error {
	// Column name variables
	colID := ColID
	colName := ColName
	colLastName := ColLastName
	colBalance := ColBalance

	columnNames := []string{ColID, ColName, ColLastName, ColBalance}

	// Create a reusable builder with batch size capacity
	builder := NewBulkDataBuilder(columnNames, batchSize)

	totalInserted := 0

	for i := 0; i < totalRows; i++ {
		// Add row to builder
		err := builder.AddRow(Row{
			Column{Name: colID, value: i + 1},
			Column{Name: colName, value: fmt.Sprintf("Name%d", i+1)},
			Column{Name: colLastName, value: fmt.Sprintf("LastName%d", i+1)},
			Column{Name: colBalance, value: float64(1000 + i)},
		})
		if err != nil {
			return fmt.Errorf("failed to add row %d: %w", i, err)
		}

		// When batch is full, insert and reset
		if builder.GetNumRows() >= batchSize {
			columnData := builder.GetColumnData()

			duration, err := InsertBatched(ctx, db, "employees", builder.GetColumnNames(), columnData...)
			if err != nil {
				return fmt.Errorf("batch insert failed at row %d: %w", i, err)
			}

			totalInserted += builder.GetNumRows()
			log.Printf("Batch inserted %d rows in %v (total: %d)", builder.GetNumRows(), duration, totalInserted)

			// Reset builder for next batch
			builder.Reset()
		}
	}

	// Insert remaining rows if any
	if builder.GetNumRows() > 0 {
		columnData := builder.GetColumnData()

		duration, err := InsertBatched(ctx, db, "employees", builder.GetColumnNames(), columnData...)
		if err != nil {
			return fmt.Errorf("final batch insert failed: %w", err)
		}

		totalInserted += builder.GetNumRows()
		log.Printf("Final batch inserted %d rows in %v (total: %d)", builder.GetNumRows(), duration, totalInserted)
	}

	log.Printf("All batches completed. Total inserted: %d rows", totalInserted)
	return nil
}

// ExampleFlexibleColumnOrder demonstrates that columns can be specified
// in any order - the builder matches them by name.
func ExampleFlexibleColumnOrder(ctx context.Context, db *sqlx.DB) error {
	// Define column names
	colID := ColID
	colName := ColName
	colLastName := ColLastName
	colBalance := ColBalance

	// Builder expects columns in this order
	columnNames := []string{ColID, ColName, ColLastName, ColBalance}
	builder := NewBulkDataBuilder(columnNames, 10)

	// NOTE: Column names are captured from the first row (or provided at builder creation)
	// and subsequent rows are matched by position. Keep the same order for all rows.
	err := builder.AddRow(Row{
		Column{Name: colID, value: 1},
		Column{Name: colName, value: "Alice"},
		Column{Name: colLastName, value: "Anderson"},
		Column{Name: colBalance, value: 5000.00},
	})
	if err != nil {
		return fmt.Errorf("failed to add row: %w", err)
	}

	err = builder.AddRow(Row{
		Column{Name: colID, value: 2},
		Column{Name: colName, value: "Bob"},
		Column{Name: colLastName, value: "Brown"},
		Column{Name: colBalance, value: 3500.50},
	})
	if err != nil {
		return fmt.Errorf("failed to add row: %w", err)
	}

	// The builder internally organizes data correctly
	// Get SQL and column data, then execute bulk insert
	columnData := builder.GetColumnData()

	duration, err := InsertBatched(ctx, db, "employees", builder.GetColumnNames(), columnData...)
	if err != nil {
		return fmt.Errorf("bulk insert failed: %w", err)
	}

	log.Printf("Successfully inserted %d rows in %v with flexible column order", builder.GetNumRows(), duration)
	return nil
}
