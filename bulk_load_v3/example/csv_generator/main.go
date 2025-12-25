package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func main() {
	// Command line flags
	rowCount := flag.Int("rows", 1000000, "Number of rows to generate")
	outputFile := flag.String("output", "product_data.csv", "Output CSV file path")
	flag.Parse()

	log.Printf("Generating %d rows to %s...", *rowCount, *outputFile)
	start := time.Now()

	file, err := os.Create(*outputFile)
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 1. Write Header (20 fields)
	// Scattered Layout:
	// 2: ID, 4: CODE, 7: NAME, 1: DESCRIPTION, 13: CATEGORY
	// 18: COST, 9: PRICE, 11: REORDER_LEVEL, 5: TARGET_LEVEL, 16: DISCONTINUED
	// Others: JUNK
	header := make([]string, 20)
	for i := 0; i < 20; i++ {
		header[i] = fmt.Sprintf("JUNK_%d", i)
	}
	header[2] = "ID"
	header[4] = "CODE"
	header[7] = "NAME"
	header[1] = "DESCRIPTION"
	header[13] = "CATEGORY"
	header[18] = "COST"
	header[9] = "PRICE"
	header[11] = "REORDER_LEVEL"
	header[5] = "TARGET_LEVEL"
	header[16] = "DISCONTINUED"

	if err := writer.Write(header); err != nil {
		log.Fatalf("Failed to write header: %v", err)
	}

	// 2. Write Data Rows
	// Seed random for variety
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	categories := []string{"Electronics", "Clothing", "Home", "Garden", "Toys", "Books", "Tools"}

	for i := 1; i <= *rowCount; i++ {
		row := make([]string, 20)
		// Fill junk first
		for j := 0; j < 20; j++ {
			row[j] = fmt.Sprintf("junk_%d_%d", i, j)
		}

		// --- Product Fields ---
		row[2] = strconv.Itoa(i)                   // ID
		row[4] = fmt.Sprintf("PROD-%08d", i)       // CODE
		row[7] = fmt.Sprintf("Product Name %d", i) // NAME

		// Description (Nullable simulation: 20% empty)
		if rng.Float32() > 0.8 {
			row[1] = ""
		} else {
			row[1] = fmt.Sprintf("Description for product %d with some details.", i)
		}

		row[13] = categories[rng.Intn(len(categories))] // CATEGORY

		cost := 10.0 + rng.Float64()*100.0
		row[18] = fmt.Sprintf("%.2f", cost)    // COST
		row[9] = fmt.Sprintf("%.2f", cost*1.5) // PRICE (50% markup)

		// Levels (Nullable simulation)
		if rng.Float32() > 0.9 {
			row[11] = "" // REORDER_LEVEL null
		} else {
			row[11] = strconv.Itoa(rng.Intn(50))
		}

		if rng.Float32() > 0.9 {
			row[5] = "" // TARGET_LEVEL null
		} else {
			row[5] = strconv.Itoa(50 + rng.Intn(100))
		}

		// Discontinued (0 or 1)
		if rng.Float32() > 0.95 {
			row[16] = "1"
		} else {
			row[16] = "0"
		}

		if err := writer.Write(row); err != nil {
			log.Fatalf("Failed to write row %d: %v", i, err)
		}

		// Flush periodically for large files to avoid huge memory buffer usage
		if i%1000 == 0 {
			writer.Flush()
			if err := writer.Error(); err != nil {
				log.Fatalf("Flush error at row %d: %v", i, err)
			}
		}
	}

	duration := time.Since(start)
	log.Printf("Done. Generated %d rows in %v.", *rowCount, duration)
}
