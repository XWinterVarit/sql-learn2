package bulkload

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

// TimingReport holds duration measurements for the bulk load operations.
type TimingReport struct {
	InsertDuration  time.Duration
	CommitDuration  time.Duration
	RefreshDuration time.Duration
	TotalDuration   time.Duration
}

// ExecuteBulkLoad performs the complete bulk load operation in three steps:
// 1. TRUNCATE base table BULK_DATA
// 2. INSERT bulk data in batches with the given CREATED_AT timestamp
// 3. REFRESH the materialized view MV_BULK_DATA (COMPLETE, ATOMIC)
//
// Parameters:
//   - ctx: context for database operations
//   - db: sqlx database connection
//   - bulkCount: number of rows to insert
//   - batchSize: rows per insert batch (if <= 0, inserts in a single batch)
//   - createdAt: timestamp to use for all inserted rows
//
// Returns TimingReport with durations for each operation and error if any step fails.
func ExecuteBulkLoad(ctx context.Context, db *sqlx.DB, bulkCount int, batchSize int, createdAt time.Time) (*TimingReport, error) {
	// Step 1: Truncate BULK_DATA table
	if err := truncateTable(ctx, db); err != nil {
		return nil, err
	}

	// Step 2: Insert bulk data and measure total operation time
	operationStart := time.Now()
	insertDuration, err := insertBulkData(ctx, db, bulkCount, batchSize, createdAt)
	if err != nil {
		return nil, err
	}

	// Calculate commit duration (time between insert end and now)
	commitDuration := time.Since(operationStart) - insertDuration

	// Step 3: Refresh materialized view
	refreshDuration, err := refreshMaterializedView(ctx, db)
	if err != nil {
		return nil, err
	}

	// Calculate total duration
	totalDuration := time.Since(operationStart)

	return &TimingReport{
		InsertDuration:  insertDuration,
		CommitDuration:  commitDuration,
		RefreshDuration: refreshDuration,
		TotalDuration:   totalDuration,
	}, nil
}
