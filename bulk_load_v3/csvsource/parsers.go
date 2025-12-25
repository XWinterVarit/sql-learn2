package csvsource

import (
	"strconv"
)

// ParserFunc defines the function signature for converting a CSV string value to a DB value.
type ParserFunc func(csvVal string) (interface{}, error)

// Parser defines the mapping and conversion logic for a single column.
type Parser struct {
	CSVHeader  string     // The name of the header in the CSV file
	DBColumn   string     // The name of the target column in the database
	ParserFunc ParserFunc // Function to convert the string value. If nil, returns string as-is.
}

// Common Parsers

// ParseInt converts a string to an int.
func ParseInt(s string) (interface{}, error) {
	return strconv.Atoi(s)
}

// ParseFloat converts a string to a float64.
func ParseFloat(s string) (interface{}, error) {
	return strconv.ParseFloat(s, 64)
}

// ParseString returns the string as-is (identity).
func ParseString(s string) (interface{}, error) {
	return s, nil
}

// ParseNullableString returns nil if the string is empty, otherwise returns the string.
func ParseNullableString(s string) (interface{}, error) {
	if s == "" {
		return nil, nil
	}
	return s, nil
}

// ParseNullableInt returns nil if the string is empty, otherwise converts to int.
func ParseNullableInt(s string) (interface{}, error) {
	if s == "" {
		return nil, nil
	}
	return strconv.Atoi(s)
}
