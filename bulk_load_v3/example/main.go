package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"sql-learn2/bulk_load_v3/csvsource"

	"github.com/jmoiron/sqlx"
	_ "github.com/sijms/go-ora/v2"
)

// Main function demonstrating the usage of bulk_load_v3 with the csvsource helper.
func main() {
	// Configuration
	const (
		dbConnStr = "oracle://user:password@localhost:1521/service" // Update with actual connection string
		tableName = "PRODUCT"
		batchSize = 1000
		csvFile   = "bulk_load_v3/example/product_data.csv"

		// Column constants
		colID           = "PRODUCT_ID"
		colCode         = "PRODUCT_CODE"
		colName         = "PRODUCT_NAME"
		colDesc         = "DESCRIPTION"
		colCategory     = "CATEGORY"
		colCost         = "STANDARD_COST"
		colPrice        = "LIST_PRICE"
		colReorderLevel = "REORDER_LEVEL"
		colTargetLevel  = "TARGET_LEVEL"
		colDiscontinued = "DISCONTINUED"
	)

	fmt.Println("Starting Bulk Load Example...")
	fmt.Printf("Connecting to DB: %s (masked)\n", dbConnStr)

	db, err := sqlx.Open("oracle", dbConnStr)
	if err != nil {
		log.Fatalf("Failed to open DB driver: %v", err)
	}
	defer db.Close()

	// Ensure the connection is valid (this will fail if no DB is running)
	if err := db.Ping(); err != nil {
		log.Printf("Warning: DB ping failed: %v. This is expected if no DB is running.", err)
		log.Println("Continuing to demonstrate structure, but execution will likely fail at DB operations.")
	}

	// Initialize the CSV Source using the reusable library
	src, closer := csvsource.New(csvsource.Config{
		FilePath:            csvFile,
		ExpectedHeaderCount: 20, // We generated 20 columns
		ExpectedHeaders: map[int]string{
			2:  "ID",
			4:  "CODE",
			7:  "NAME",
			1:  "DESCRIPTION",
			13: "CATEGORY",
			18: "COST",
			9:  "PRICE",
			11: "REORDER_LEVEL",
			5:  "TARGET_LEVEL",
			16: "DISCONTINUED",
		},
		BatchSize: batchSize,

		DB:        db,
		TableName: tableName,
		Columns: []string{
			colID, colCode, colName, colDesc, colCategory,
			colCost, colPrice, colReorderLevel, colTargetLevel, colDiscontinued,
		},
		ConvertFunc: func(row []string) ([]interface{}, error) {
			p := &RowParser{}
			values := []interface{}{
				p.Int(row[2], colID),
				p.String(row[4], colCode),
				p.String(row[7], colName),
				p.NullableString(row[1], colDesc),
				p.String(row[13], colCategory),
				p.Float64(row[18], colCost),
				p.Float64(row[9], colPrice),
				p.NullableInt(row[11], colReorderLevel),
				p.NullableInt(row[5], colTargetLevel),
				p.Int(row[16], colDiscontinued),
			}
			if err := p.Err(); err != nil {
				return nil, err
			}
			return values, nil
		},
	})
	defer closer()

	ctx := context.Background()
	start := time.Now()

	if err := src.Run(ctx); err != nil {
		log.Fatalf("Bulk load failed: %v", err)
	}

	log.Printf("Bulk load completed in %v", time.Since(start))
}

// RowParser helps simplify row conversion by collecting errors.
type RowParser struct {
	err error
}

func (p *RowParser) Int(s string, field string) interface{} {
	if p.err != nil {
		return nil
	}
	val, err := strconv.Atoi(s)
	if err != nil {
		p.err = fmt.Errorf("invalid %s '%s': %w", field, s, err)
		return nil
	}
	return val
}

func (p *RowParser) Float64(s string, field string) interface{} {
	if p.err != nil {
		return nil
	}
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		p.err = fmt.Errorf("invalid %s '%s': %w", field, s, err)
		return nil
	}
	return val
}

func (p *RowParser) String(s string, field string) interface{} {
	if p.err != nil {
		return nil
	}
	return s
}

func (p *RowParser) NullableString(s string, field string) interface{} {
	if p.err != nil {
		return nil
	}
	if s == "" {
		return nil
	}
	return s
}

func (p *RowParser) NullableInt(s string, field string) interface{} {
	if p.err != nil {
		return nil
	}
	if s == "" {
		return nil
	}
	val, err := strconv.Atoi(s)
	if err != nil {
		p.err = fmt.Errorf("invalid %s '%s': %w", field, s, err)
		return nil
	}
	return val
}

func (p *RowParser) Err() error {
	return p.err
}
