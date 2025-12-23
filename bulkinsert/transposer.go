package bulkinsert

import "log"

// transposeRowsToColumns converts row-oriented data to column-oriented typed arrays.
// This is required for go-ora array binding which expects concrete typed slices.
// Returns a slice of typed arrays (one per column) ready for batch insert.
func transposeRowsToColumns(rows [][]interface{}, columnNames []string) ([]interface{}, error) {
	numCols := len(columnNames)
	columnData := make([]interface{}, numCols)

	for colIdx := 0; colIdx < numCols; colIdx++ {
		// Find a sample non-nil value to determine the column type
		sample := findSampleValue(rows, colIdx)

		// Build typed array for this column
		typedArray, err := buildTypedColumnArray(rows, colIdx, columnNames[colIdx], sample)
		if err != nil {
			return nil, err
		}
		columnData[colIdx] = typedArray

		// Log the binding type for troubleshooting
		log.Printf("Binding column %s as %T (rows=%d)", columnNames[colIdx], typedArray, len(rows))
	}

	return columnData, nil
}
