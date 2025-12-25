package csvsource

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"runtime/debug"
	"sql-learn2/bulk_load_v3"
	"sql-learn2/bulk_load_v3/rp_dynamic"

	"github.com/jmoiron/sqlx"
)

// Config holds configuration for the CSV source.
type Config struct {
	FilePath  string
	Delimiter rune // Custom delimiter (default is comma)

	// ExpectedHeaderCount is the total number of columns expected in the CSV file.
	// If 0, the check is skipped.
	ExpectedHeaderCount int

	// Parsers defines the mapping from CSV Header to DB Column and the conversion logic.
	// The order of elements in this slice determines the order of columns in the DB insert.
	Parsers []Parser

	// Bulk Load settings
	DB        *sqlx.DB
	TableName string
	BatchSize int
	MVName    string
}

// CsvSource implements bulkloadv3.Source using the native encoding/csv package.
type CsvSource struct {
	cfg Config

	file   *os.File
	reader *csv.Reader

	// columnIndices maps the index in cfg.Parsers to the index in the CSV row.
	// columnIndices[i] is the CSV index for cfg.Parsers[i].
	columnIndices []int
}

// New creates a new CsvSource.
func New(cfg Config) (*CsvSource, func() error) {
	src := &CsvSource{
		cfg: cfg,
	}
	return src, src.Close
}

// Run executes the bulk load process.
func (s *CsvSource) Run(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in csv source run: %v\nstack: %s", r, debug.Stack())
		}
	}()

	if err := s.validateConfig(); err != nil {
		return err
	}

	dbColumns, err := s.extractDBColumns()
	if err != nil {
		return err
	}

	loaderCfg := s.createLoaderConfig(dbColumns)
	loader := bulkloadv3.NewLoader(loaderCfg, &sourceAdapter{CsvSource: s})
	return loader.Run(ctx)
}

func (s *CsvSource) validateConfig() error {
	if s.cfg.DB == nil {
		return fmt.Errorf("database connection (DB) is required")
	}
	if s.cfg.TableName == "" {
		return fmt.Errorf("table name is required")
	}
	if len(s.cfg.Parsers) == 0 {
		return fmt.Errorf("parsers are required")
	}
	return nil
}

func (s *CsvSource) extractDBColumns() ([]string, error) {
	dbColumns := make([]string, len(s.cfg.Parsers))
	for i, p := range s.cfg.Parsers {
		if p.DBColumn == "" {
			return nil, fmt.Errorf("DBColumn name is required for parser at index %d", i)
		}
		dbColumns[i] = p.DBColumn
	}
	return dbColumns, nil
}

func (s *CsvSource) createLoaderConfig(dbColumns []string) bulkloadv3.Config {
	repo := rp_dynamic.NewRepo(s.cfg.DB)
	return bulkloadv3.Config{
		Repo:      repo,
		TableName: s.cfg.TableName,
		Columns:   dbColumns,
		BatchSize: s.cfg.BatchSize,
		MVName:    s.cfg.MVName,
	}
}

// Close closes the underlying file handle.
func (s *CsvSource) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}
