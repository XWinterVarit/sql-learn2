package csvsource

import (
	"fmt"
	"strconv"
)

// RowParser helps simplify row conversion by collecting errors.
// It allows declarative parsing of fields without checking error on every step.
type RowParser struct {
	err error
}

// NewRowParser creates a new RowParser.
func NewRowParser() *RowParser {
	return &RowParser{}
}

// Int parses a string as an integer.
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

// Float64 parses a string as a float64.
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

// String returns the string as is.
func (p *RowParser) String(s string, field string) interface{} {
	if p.err != nil {
		return nil
	}
	return s
}

// NullableString returns nil if string is empty, otherwise returns the string.
func (p *RowParser) NullableString(s string, field string) interface{} {
	if p.err != nil {
		return nil
	}
	if s == "" {
		return nil
	}
	return s
}

// NullableInt returns nil if string is empty, otherwise parses as int.
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

// Err returns the first error encountered during parsing.
func (p *RowParser) Err() error {
	return p.err
}
