package bulkload

import (
	"context"
	"fmt"
	"log"
	"time"

	"sql-learn2/bulkinsert"

	"github.com/jmoiron/sqlx"
)

// generateBatchData creates a batch of test data rows using the Row/Column style
// demonstrated in bulkinsert.ExampleBasicUsage. It returns column names and row data
// in the legacy return format expected by callers.
func generateBatchData(batchStart, batchCount int, createdAt time.Time) ([]string, [][]interface{}) {
	// Step 1: Define column names as variables (for pointer-like references)
	colID := "ID"
	colDataValue := "DATA_VALUE"
	colDescription := "DESCRIPTION"
	colStatus := "STATUS"
	colCreatedAt := "CREATED_AT"

	// Step 2: Build rows using bulkinsert.Row/Column for readability and safety
	rowsDef := make(bulkinsert.Rows, 0, batchCount)
	for i := 0; i < batchCount; i++ {
		rowNum := batchStart + i
		status := "ACTIVE"
		if rowNum%10 == 0 {
			status = "INACTIVE"
		}

		rowsDef = append(rowsDef, bulkinsert.Row{
			bulkinsert.Column{Name: colID, Value: rowNum},
			bulkinsert.Column{Name: colDataValue, Value: fmt.Sprintf("VAL_%d", rowNum)},
			bulkinsert.Column{Name: colDescription, Value: fmt.Sprintf("Generated row #%d", rowNum)},
			bulkinsert.Column{Name: colStatus, Value: status},
			bulkinsert.Column{Name: colCreatedAt, Value: createdAt},
		})
	}

	// Step 3: Convert to the required return types
	return rowsDef.GetColumnsNames(), rowsDef.GetRows()
}

// insertBulkData inserts bulk data in batches.
// batchSize controls rows per batch; if <= 0 it falls back to a single batch of bulkCount.
func insertBulkData(ctx context.Context, db *sqlx.DB, bulkCount int, batchSize int, createdAt time.Time) (time.Duration, error) {
	if bulkCount <= 0 {
		return 0, nil
	}
	if batchSize <= 0 || batchSize > bulkCount {
		batchSize = bulkCount
	}
	log.Printf("Inserting %d rows with CREATED_AT = %s in batches of %d", bulkCount, createdAt.Format("2006-01-02 15:04:05"), batchSize)

	var totalInsert time.Duration
	startID := 1
	remaining := bulkCount
	batchNum := 0
	totalBatches := (bulkCount + batchSize - 1) / batchSize
	for remaining > 0 {
		n := batchSize
		if remaining < batchSize {
			n = remaining
		}
		batchNum++

		// Pre-batch progress log so users see ongoing work before each insert starts
		log.Printf("Batch %d/%d: starting insert of %d rows (remaining before: %d)", batchNum, totalBatches, n, remaining)

		columnNames, rows := generateBatchData(startID, n, createdAt)
		insDuration, err := bulkinsert.InsertStructs(ctx, db, "BULK_DATA", columnNames, rows)
		if err != nil {
			return totalInsert, err
		}
		totalInsert += insDuration
		startID += n
		remaining -= n

		log.Printf("Batch %d/%d: inserted %d rows (remaining: %d)", batchNum, totalBatches, n, remaining)
	}

	return totalInsert, nil
}
