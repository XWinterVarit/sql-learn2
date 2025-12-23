package bulkinsert

import (
	"fmt"
	"log"
	"time"
)

// buildInt64Array builds a typed []int64 slice from column data.
// Supports int, int32, int64, uint, uint32, uint64 types.
func buildInt64Array(rows [][]interface{}, colIdx int, columnName string) ([]int64, error) {
	numRows := len(rows)
	arr := make([]int64, numRows)
	for i, row := range rows {
		val := row[colIdx]
		switch vv := val.(type) {
		case int64:
			arr[i] = vv
		case int:
			arr[i] = int64(vv)
		case int32:
			arr[i] = int64(vv)
		case uint:
			arr[i] = int64(vv)
		case uint32:
			arr[i] = int64(vv)
		case uint64:
			arr[i] = int64(vv)
		default:
			return nil, fmt.Errorf("column %s (index %d) type mismatch: expected integer-like, got %T at row %d", columnName, colIdx, val, i)
		}
	}
	return arr, nil
}

// buildFloat64Array builds a typed []float64 slice from column data.
// Supports float32 and float64 types.
func buildFloat64Array(rows [][]interface{}, colIdx int, columnName string) ([]float64, error) {
	numRows := len(rows)
	arr := make([]float64, numRows)
	for i, row := range rows {
		val := row[colIdx]
		switch vv := val.(type) {
		case float64:
			arr[i] = vv
		case float32:
			arr[i] = float64(vv)
		default:
			return nil, fmt.Errorf("column %s (index %d) type mismatch: expected float-like, got %T at row %d", columnName, colIdx, val, i)
		}
	}
	return arr, nil
}

// buildBoolArray builds a typed []bool slice from column data.
func buildBoolArray(rows [][]interface{}, colIdx int, columnName string) ([]bool, error) {
	numRows := len(rows)
	arr := make([]bool, numRows)
	for i, row := range rows {
		val := row[colIdx]
		vb, ok := val.(bool)
		if !ok {
			return nil, fmt.Errorf("column %s (index %d) type mismatch: expected bool, got %T at row %d", columnName, colIdx, val, i)
		}
		arr[i] = vb
	}
	return arr,
}

// buildTimeArray builds a typed []time.Time slice from column data.
func buildTimeArray(rows [][]interface{}, colIdx int, columnName string) ([]time.Time, error) {
	numRows := len(rows)
	arr := make([]time.Time, numRows)
	for i, row := range rows {
		val := row[colIdx]
		vt, ok := val.(time.Time)
		if !ok {
			return nil, fmt.Errorf("column %s (index %d) type mismatch: expected time.Time, got %T at row %d", columnName, colIdx, val, i)
		}
		arr[i] = vt
	}
	return arr, nil
}

// buildStringArray builds a typed []string slice from column data.
func buildStringArray(rows [][]interface{}, colIdx int, columnName string) ([]string, error) {
	numRows := len(rows)
	arr := make([]string, numRows)
	for i, row := range rows {
		val := row[colIdx]
		vs, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("column %s (index %d) type mismatch: expected string, got %T at row %d", columnName, colIdx, val, i)
		}
		arr[i] = vs
	}
	return arr, nil
}

// buildGenericArray builds a generic []interface{} slice from column data.
// This is a fallback for unsupported types and may not work with all drivers.
func buildGenericArray(rows [][]interface{}, colIdx int, columnName string, sampleType interface{}) []interface{} {
	numRows := len(rows)
	arr := make([]interface{}, numRows)
	for i, row := range rows {
		arr[i] = row[colIdx]
	}
	log.Printf("Warning: binding column %s with generic []interface{} (type %T)", columnName, sampleType)
	return arr
}

// buildTypedColumnArray builds a typed array for a single column based on sample value type.
// Returns the typed array as interface{} and any error encountered.
func buildTypedColumnArray(rows [][]interface{}, colIdx int, columnName string, sample interface{}) (interface{}, error) {
	switch sample.(type) {
	case int64, int, int32, uint, uint32, uint64:
		return buildInt64Array(rows, colIdx, columnName)
	case float64, float32:
		return buildFloat64Array(rows, colIdx, columnName)
	case bool:
		return buildBoolArray(rows, colIdx, columnName)
	case time.Time:
		return buildTimeArray(rows, colIdx, columnName)
	case string:
		return buildStringArray(rows, colIdx, columnName)
	default:
		// Fallback for unsupported types
		return buildGenericArray(rows, colIdx, columnName, sample), nil
	}
}
