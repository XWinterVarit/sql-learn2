package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"sql-learn2/bulk_load_v3/csvsource"

	"github.com/jmoiron/sqlx"
	_ "github.com/sijms/go-ora/v2"
)

// Main function demonstrating the usage of bulk_load_v3 with the csvsource helper.
func main() {
	// Configuration
	user := flag.String("user", getEnv("ORA_USER", "LEARN1"), "Oracle username")
	pass := flag.String("pass", getEnv("ORA_PASS", "Welcome"), "Oracle password")
	host := flag.String("host", getEnv("ORA_HOST", "localhost"), "Oracle host")
	port := flag.String("port", getEnv("ORA_PORT", "1521"), "Oracle port")
	service := flag.String("service", getEnv("ORA_SERVICE", "XE"), "Oracle service name")
	flag.Parse()

	dbConnStr := fmt.Sprintf("oracle://%s:%s@%s:%s/%s", *user, *pass, *host, *port, *service)

	const (
		tableName = "PRODUCT"
		batchSize = 100000
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
		colUpdatedAt    = "UPDATED_AT"
	)

	loc, err := time.LoadLocation("Asia/Bangkok")
	if err != nil {
		log.Fatalf("Failed to load location: %v", err)
	}
	runTime := time.Now().In(loc)

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
			colCost, colPrice, colReorderLevel, colTargetLevel, colDiscontinued, colUpdatedAt,
		},
		MVName: "MV_PRODUCT",
		ConvertFunc: func(row []string) ([]interface{}, error) {
			p := csvsource.NewRowParser()
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
				runTime,
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

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
