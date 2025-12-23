package bulkinsert

import "fmt"

// validateRowDimensions checks if all rows have the expected number of columns.
// Returns an error if any row has a different number of columns.
func validateRowDimensions(rows [][]interface{}, expectedCols int) error {
	for rowIdx, row := range rows {
		if len(row) != expectedCols {
			return fmt.Errorf("row %d has %d values but expected %d columns", rowIdx, len(row), expectedCols)
		}
	}
	return nil
}

// findSampleValue finds the first non-nil value in a column to determine its type.
// Returns the sample value or nil if all values are nil.
func findSampleValue(rows [][]interface{}, colIdx int) interface{} {
	for _, row := range rows {
		if row[colIdx] != nil {
			return row[colIdx]
		}
	}
	return nil
}
