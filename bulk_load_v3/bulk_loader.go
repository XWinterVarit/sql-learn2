package bulkloadv3

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
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
	MVName    string
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

// Loader handles the bulk load operation.
type Loader struct {
	cfg    Config
	src    Source
	logger *slog.Logger
}

// NewLoader creates a new Loader instance.
func NewLoader(cfg Config, src Source) *Loader {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
		slog.Warn("BatchSize was <= 0, defaulting to 100")
	}

	logger := slog.With(LogFieldTable, cfg.TableName)
	return &Loader{
		cfg:    cfg,
		src:    src,
		logger: logger,
	}
}

// Run executes the bulk load process.
func (l *Loader) Run(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in bulk load run: %v\nstack: %s", r, debug.Stack())
		}
	}()

	if err := l.validateConfig(); err != nil {
		return err
	}

	runStart := time.Now()
	l.logger.Info("Starting bulk load process...")

	// 1. Preparation
	if err := l.prepare(ctx); err != nil {
		return err
	}

	// 2. Processing
	totalRows, err := l.process(ctx)
	if err != nil {
		return err
	}

	// 3. Finalization
	if err := l.refreshMatView(ctx); err != nil {
		return err
	}

	l.logger.Info("Batch Done.", LogFieldDuration, time.Since(runStart), LogFieldRowCount, totalRows)
	return nil
}

func (l *Loader) validateConfig() error {
	if l.cfg.Repo == nil {
		return fmt.Errorf("repository (Repo) is required")
	}
	if l.cfg.TableName == "" {
		return fmt.Errorf("table name is required")
	}
	if len(l.cfg.Columns) == 0 {
		return fmt.Errorf("target columns are required")
	}
	return nil
}

// prepare handles source validation and table truncation.
func (l *Loader) prepare(ctx context.Context) error {
	// Diagram: Open CSV File -> Validate CSV
	l.logger.Info("Validating source...")
	if err := l.src.Validate(ctx); err != nil {
		return fmt.Errorf("source validation failed: %w", err)
	}

	// Diagram: Truncate Table
	l.logger.Info("Truncating table...")
	truncStart := time.Now()
	if err := l.cfg.Repo.Truncate(ctx, l.cfg.TableName); err != nil {
		return fmt.Errorf("truncate table %s failed: %w", l.cfg.TableName, err)
	}
	l.logger.Info("Truncate finished", LogFieldDuration, time.Since(truncStart))
	return nil
}

// process handles reading, converting, buffering, and inserting rows.
func (l *Loader) process(ctx context.Context) (int, error) {
	l.logger.Info("Starting row processing...")
	builder := rp_dynamic.NewBulkInsertBuilder(l.cfg.TableName, l.cfg.Columns...)
	rowCount := 0
	totalRows := 0
	batchReadStart := time.Now()

	for {
		// Diagram: Read Line
		rawRow, err := l.src.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return totalRows, fmt.Errorf("read line failed: %w", err)
		}

		// Diagram: Is Buffer Full?
		if rowCount >= l.cfg.BatchSize {
			// Diagram: Buffer Has Rows -> Insert Bulk
			if err := l.flushBatch(ctx, builder, rowCount, time.Since(batchReadStart)); err != nil {
				return totalRows, err
			}
			// Diagram: Reset Buffer
			builder = rp_dynamic.NewBulkInsertBuilder(l.cfg.TableName, l.cfg.Columns...)
			rowCount = 0
			batchReadStart = time.Now()
		}

		currentLine := totalRows + 1
		rowLogger := l.logger.With(LogFieldRowIndex, currentLine)

		// Diagram: Parse And Validate Row
		values, err := l.src.Convert(rawRow)
		if err != nil {
			rowLogger.Error("Row conversion failed", LogFieldRawData, rawRow, LogFieldErr, err)
			return totalRows, fmt.Errorf("row conversion failed: %w", err)
		}

		// Diagram: Add Row To Buffer
		if err := builder.AddRow(values...); err != nil {
			rowLogger.Error("Add row to buffer failed", LogFieldRawData, rawRow, LogFieldErr, err)
			return totalRows, fmt.Errorf("add row to buffer failed: %w", err)
		}
		rowCount++
		totalRows++
	}

	// Diagram: Done -> Buffer Has Rows? -> Insert Bulk
	if rowCount > 0 {
		l.logger.Info("Inserting remaining rows...", LogFieldRowCount, rowCount, LogFieldDuration, time.Since(batchReadStart))
		if err := l.flushBatch(ctx, builder, rowCount, time.Since(batchReadStart)); err != nil {
			l.logger.Error("Final bulk insert failed", LogFieldErr, err)
			return totalRows, fmt.Errorf("final bulk insert failed: %w", err)
		}
	}

	l.logger.Info("Inserted total rows.", LogFieldRowCount, totalRows)
	return totalRows, nil
}

// flushBatch inserts the current buffer into the database.
func (l *Loader) flushBatch(ctx context.Context, builder *rp_dynamic.BulkInsertBuilder, count int, readDuration time.Duration) error {
	l.logger.Info("Inserting batch...", LogFieldRowCount, count, LogFieldDuration, readDuration)
	flushStart := time.Now()
	if err := l.cfg.Repo.BulkInsert(ctx, builder); err != nil {
		l.logger.Error("Bulk insert failed", LogFieldErr, err)
		return fmt.Errorf("bulk insert failed: %w", err)
	}
	l.logger.Info("Batch inserted", LogFieldDuration, time.Since(flushStart))
	return nil
}

// refreshMatView handles materialized view refresh.
func (l *Loader) refreshMatView(ctx context.Context) error {
	// Diagram: Refresh Material View
	if l.cfg.MVName != "" {
		l.logger.Info("Refreshing materialized view...", "mv", l.cfg.MVName)
		refreshStart := time.Now()
		if _, err := l.cfg.Repo.RefreshMaterializedView(ctx, l.cfg.MVName); err != nil {
			l.logger.Error("Refresh MV failed", LogFieldErr, err)
			return err
		}
		l.logger.Info("MV Refreshed", LogFieldDuration, time.Since(refreshStart))
	} else {
		l.logger.Info("No MV configured, skipping refresh.")
	}
	return nil
}

// Run executes the bulk load process according to the workflow defined in the diagram.
// This is a helper function that delegates to Loader.
func Run(ctx context.Context, cfg Config, src Source) error {
	loader := NewLoader(cfg, src)
	return loader.Run(ctx)
}
