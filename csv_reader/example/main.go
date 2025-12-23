package main

import (
	"fmt"
	"log"
	"os"
	"sql-learn2/csv_reader"
	"time"
)

func main() {
	fileName := "duplicates.csv"
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		fileName = "csv_reader/example/duplicates.csv"
	}
	reader := csv_reader.NewCSVReader(fileName)
	reader.HasHeader = true
	// reader.HasTail = false // Default is false, keeping it as is.

	fmt.Printf("Reading %s...\n", fileName)

	// We can use ReadAll, ReadChunk, or ReadSingleRow.
	// Let's demonstrate ReadSingleRow for streaming processing.

	count := 0
	lastTime := time.Now()
	for {
		line, isEOF, err := reader.ReadSingleRow()
		if err != nil {
			log.Fatalf("Error reading row: %v", err)
		}
		if isEOF {
			break
		}

		// Print first 5 rows
		if count < 5 {
			fmt.Printf("Row %d: ID=%s, Name=%s, Email=%s\n", count+1, line.Value(0), line.Value(1), line.Value(2))
		}

		count++

		if count%50000 == 0 {
			now := time.Now()
			fmt.Printf("Rows processed: %d. Time for last 50,000: %v\n", count, now.Sub(lastTime))
			lastTime = now
		}
	}

	fmt.Printf("Total body rows read: %d\n", count)
	fmt.Printf("Total rows reported by reader: %d\n", reader.CountBodyRow())
}
