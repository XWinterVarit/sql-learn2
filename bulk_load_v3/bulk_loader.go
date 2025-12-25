package bulkloadv3

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"sql-learn2/bulk_load_v3/rp_dynamic"
)

const (
	LogFieldTable    = "table"
	LogFieldRowIndex = "row_index"
	LogFieldRawData  = "raw_data"
	LogFieldErr      = "error"
	LogFieldDuration = "duration"
	LogFieldRowCount = "row_count"
	LogFieldFile     = "file"
)

// Config holds configuration for the bulk load operation.
type Config struct {
	Repo      rp_dynamic.Repository
	TableName string
	Columns   []string
	BatchSize int
}

// Source defines the interface for input data handling.
// The caller implements this to provide custom logic for input validation, reading, and conversion.
type Source interface {
	// Validate performs initial checks on the source (e.g., header validation, row counting).
	// This corresponds to the "Validate CSV" step in the diagram.
	Validate(ctx context.Context) error

	// Next returns the next raw row data from the source.
	// It should return io.EOF when there are no more rows.
	// This corresponds to the "Read Line" step in the diagram.
	Next(ctx context.Context) (interface{}, error)

	// Convert transforms the raw row data into a slice of values corresponding to the target columns.
	// This corresponds to the "Parse And Validate Row" step in the diagram.
	Convert(rawRow interface{}) ([]interface{}, error)
}

// Run executes the bulk load process according to the workflow defined in the diagram.
func Run(ctx context.Context, cfg Config, src Source) error {
	runStart := time.Now()
	logger := slog.With(LogFieldTable, cfg.TableName)
	logger.Info("Starting bulk load process...")

	// 1. Validate Source
	// Diagram: Open CSV File -> Validate CSV
	logger.Info("Validating source...")
	if err := src.Validate(ctx); err != nil {
		return fmt.Errorf("source validation failed: %w", err)
	}

	// 2. Truncate Table
	// Diagram: Truncate Table
	logger.Info("Truncating table...")
	truncStart := time.Now()
	if err := cfg.Repo.Truncate(ctx, cfg.TableName); err != nil {
		return fmt.Errorf("truncate table %s failed: %w", cfg.TableName, err)
	}
	logger.Info("Truncate finished", LogFieldDuration, time.Since(truncStart))

	// 3. Process Rows (Read loop)
	logger.Info("Starting row processing...")
	builder := rp_dynamic.NewBulkInsertBuilder(cfg.TableName, cfg.Columns...)
	rowCount := 0
	totalRows := 0

	batchReadStart := time.Now()

	for {
		// Diagram: Read Line
		rawRow, err := src.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read line failed: %w", err)
		}

		// Diagram: Is Buffer Full?
		if rowCount >= cfg.BatchSize {
			// Diagram: Buffer Has Rows -> Insert Bulk
			readDuration := time.Since(batchReadStart)
			logger.Info("Buffer full. Inserting batch...", LogFieldRowCount, rowCount, LogFieldDuration, readDuration)

			flushStart := time.Now()
			if err := cfg.Repo.BulkInsert(ctx, builder); err != nil {
				logger.Error("Bulk insert failed", LogFieldErr, err)
				return fmt.Errorf("bulk insert failed: %w", err)
			}
			logger.Info("Batch inserted", LogFieldDuration, time.Since(flushStart))

			// Diagram: Reset Buffer
			builder = rp_dynamic.NewBulkInsertBuilder(cfg.TableName, cfg.Columns...)
			rowCount = 0
			batchReadStart = time.Now()
		}

		currentLine := totalRows + 1
		rowLogger := logger.With(LogFieldRowIndex, currentLine)

		// Diagram: Parse And Validate Row
		values, err := src.Convert(rawRow)
		if err != nil {
			rowLogger.Error("Row conversion failed", LogFieldRawData, rawRow, LogFieldErr, err)
			return fmt.Errorf("row conversion failed: %w", err)
		}

		// Diagram: Add Row To Buffer
		if err := builder.AddRow(values...); err != nil {
			rowLogger.Error("Add row to buffer failed", LogFieldRawData, rawRow, LogFieldErr, err)
			return fmt.Errorf("add row to buffer failed: %w", err)
		}
		rowCount++
		totalRows++
	}

	// Diagram: Done -> Buffer Has Rows? -> Insert Bulk
	if rowCount > 0 {
		readDuration := time.Since(batchReadStart)
		logger.Info("Inserting remaining rows...", LogFieldRowCount, rowCount, LogFieldDuration, readDuration)

		flushStart := time.Now()
		if err := cfg.Repo.BulkInsert(ctx, builder); err != nil {
			logger.Error("Final bulk insert failed", LogFieldErr, err)
			return fmt.Errorf("final bulk insert failed: %w", err)
		}
		logger.Info("Final batch inserted", LogFieldDuration, time.Since(flushStart))
	}

	logger.Info("Inserted total rows.", LogFieldRowCount, totalRows)

	// 4. Refresh Materialized View
	// Diagram: Refresh Material View
	refreshStart := time.Now()
	if _, err := cfg.Repo.RefreshMaterializedView(ctx); err != nil {
		logger.Error("Refresh MV failed", LogFieldErr, err)
		return err
	}
	logger.Info("MV Refreshed", LogFieldDuration, time.Since(refreshStart))

	// Diagram: Done Batch
	logger.Info("Batch Done.", LogFieldDuration, time.Since(runStart))
	return nil
}
