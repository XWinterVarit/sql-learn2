package partexchange

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"sql-learn2/csvdb"
)

// Options describes inputs for the partition-exchange workflow.
//
// MasterTable: name of the partitioned master table.
// StagingTable: name of the non-partitioned staging table used for exchange.
// PartitionName: target partition in the master table to exchange.
// CSVPath: path to the CSV file to load into the staging table before exchange.
// Schema: optional schema/owner to qualify table names. If empty, current schema is used.
// DropOldData: if true, will TRUNCATE the staging table after exchange to remove old data.
// WithoutValidation: if true, use WITHOUT VALIDATION for the exchange (faster, assumes compatibility).
// IncludingIndexes: if true, add INCLUDING INDEXES clause during exchange.
// Note: Oracle requires that the staging table is structurally compatible with the partition.
//
//	This workflow will create/replace the staging table based on the CSV headers/types.
//	Ensure it matches your master partition schema.
type Options struct {
	MasterTable       string
	StagingTable      string
	PartitionName     string
	CSVPath           string
	Schema            string
	DropOldData       bool
	WithoutValidation bool
	IncludingIndexes  bool
}

// Run performs: load CSV -> exchange partition -> cleanup old data (truncate staging).
func Run(ctx context.Context, db *sql.DB, opt Options) error {
	if db == nil {
		return errors.New("db is nil")
	}
	if strings.TrimSpace(opt.MasterTable) == "" {
		return errors.New("MasterTable is required")
	}
	if strings.TrimSpace(opt.StagingTable) == "" {
		return errors.New("StagingTable is required")
	}
	if strings.TrimSpace(opt.PartitionName) == "" {
		return errors.New("PartitionName is required")
	}
	if strings.TrimSpace(opt.CSVPath) == "" {
		return errors.New("CSVPath is required")
	}

	master := normalizeIdentifierForOracle(opt.MasterTable)
	staging := normalizeIdentifierForOracle(opt.StagingTable)
	part := normalizeIdentifierForOracle(opt.PartitionName)
	if master == "" || staging == "" || part == "" {
		return fmt.Errorf("invalid identifiers: master=%q staging=%q partition=%q", opt.MasterTable, opt.StagingTable, opt.PartitionName)
	}
	qual := func(name string) string {
		if strings.TrimSpace(opt.Schema) == "" {
			return name
		}
		return normalizeIdentifierForOracle(opt.Schema) + "." + name
	}

	// 1) Load CSV into staging table (create/replace based on CSV definition)
	if err := csvdb.LoadCSVToDBAs(ctx, db, opt.CSVPath, qual(staging)); err != nil {
		return fmt.Errorf("load csv into staging %s: %w", qual(staging), err)
	}
	log.Printf("Loaded CSV %s into staging table %s", opt.CSVPath, qual(staging))

	// 2) Exchange partition
	// Build ALTER TABLE statement
	clause := ""
	if opt.IncludingIndexes {
		clause += " INCLUDING INDEXES"
	}
	if opt.WithoutValidation {
		clause += " WITHOUT VALIDATION"
	}
	stmt := fmt.Sprintf("ALTER TABLE %s EXCHANGE PARTITION %s WITH TABLE %s%s", qual(master), part, qual(staging), clause)
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("exchange partition: %w", err)
	}
	log.Printf("Exchanged partition %s of %s with table %s", part, qual(master), qual(staging))

	// 3) Delete old data: after exchange, old data moves into staging; truncate it if requested
	if opt.DropOldData {
		trunc := fmt.Sprintf("TRUNCATE TABLE %s", qual(staging))
		if _, err := db.ExecContext(ctx, trunc); err != nil {
			return fmt.Errorf("truncate staging after exchange: %w", err)
		}
		log.Printf("Truncated staging table %s to remove old data", qual(staging))
	}

	return nil
}

func normalizeIdentifierForOracle(s string) string {
	if s == "" {
		return ""
	}
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, " ", "_")
	b := make([]rune, 0, len(s))
	for _, r := range s {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b = append(b, r)
		} else {
			b = append(b, '_')
		}
	}
	upper := strings.ToUpper(string(b))
	if len(upper) == 0 {
		return ""
	}
	if !(upper[0] >= 'A' && upper[0] <= 'Z') {
		upper = "X" + upper
	}
	if len(upper) > 30 {
		upper = upper[:30]
	}
	return upper
}
