package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
)

func main() {
	// Define the CSV file name
	fileName := "duplicates.csv"

	// Create the file
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("failed to create file: %s", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Define the header
	header := make([]string, 30)
	for i := 0; i < 30; i++ {
		header[i] = fmt.Sprintf("field_%d", i+1)
	}
	header[0] = "id"
	header[1] = "name"
	header[2] = "email"
	header[3] = "department"
	header[29] = "long_description"

	if err := writer.Write(header); err != nil {
		log.Fatalf("failed to write header: %s", err)
	}

	longValue := "This is a very long value designed to test how the system handles large amounts of data in a single CSV field. " +
		"It contains many characters and repeats itself to ensure it is significantly longer than a standard field value. "
	for i := 0; i < 5; i++ {
		longValue += longValue
	}

	// Define initial rows to be duplicated
	initialRows := [][]string{
		append([]string{"1", "John Doe", "john@example.com", "IT"}, make([]string, 26)...),
		append([]string{"2", "Jane Smith", "jane@example.com", "HR"}, make([]string, 26)...),
		append([]string{"3", "Bob Wilson", "bob@example.com", "Sales"}, make([]string, 26)...),
		append([]string{"4", "Alice Brown", "alice@example.com", "Marketing"}, make([]string, 26)...),
		append([]string{"5", "Charlie Davis", "charlie@example.com", "IT"}, make([]string, 26)...),
	}

	for i := range initialRows {
		for j := 4; j < 29; j++ {
			initialRows[i][j] = fmt.Sprintf("val_%d_%d", i+1, j+1)
		}
		initialRows[i][29] = longValue
	}

	targetRowCount := 1000000
	for i := 0; i < targetRowCount; i++ {
		row := initialRows[i%len(initialRows)]
		if err := writer.Write(row); err != nil {
			log.Fatalf("failed to write row at index %d: %s", i, err)
		}
	}

	fmt.Printf("Successfully generated %s with %d rows (duplicates of %d initial rows).\n", fileName, targetRowCount, len(initialRows))
}
