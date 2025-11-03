package bulkinsert

import (
	"context"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
)

const (
	ColID       = "id"
	ColName     = "name"
	ColLastName = "lastname"
	ColBalance  = "balance"
)

// ExampleBasicUsage demonstrates the basic usage of BulkDataBuilder
// with Row/Rows types and the convenience Insert() method.
func ExampleBasicUsage(ctx context.Context, db *sqlx.DB) error {
	// Step 1: Define column names as variables (for pointer references)
	colID := ColID
	colName := ColName
	colLastName := ColLastName
	colBalance := ColBalance

	// Step 2: Create a BulkDataBuilder with capacity only; column names come from the first row
	builder := NewBulkDataBuilder(100)

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
