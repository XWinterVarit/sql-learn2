package rp_dynamic

import (
	"fmt"
	"reflect"
	"strings"
)

// StructBulkInsertBuilder helps construct bulk insert statements and data for go-ora using a struct.
type StructBulkInsertBuilder[T any] struct {
	tableName string
	columns   []string
	// data holds the data in column-oriented format: data[colIndex][rowIndex]
	data [][]interface{}
	// fieldIndices maps column index to struct field index
	fieldIndices []int
}

// NewStructBulkInsertBuilder creates a new struct-based builder instance.
func NewStructBulkInsertBuilder[T any](tableName string, columns ...string) *StructBulkInsertBuilder[T] {
	// Initialize columnData slices for each column
	columnData := make([][]interface{}, len(columns))
	for i := range columnData {
		columnData[i] = make([]interface{}, 0)
	}

	// Map columns to struct fields
	var t T
	typ := reflect.TypeOf(t)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct {
		panic(fmt.Sprintf("StructBulkInsertBuilder: type %v must be a struct or pointer to struct", typ))
	}

	indices := make([]int, len(columns))
	for i, col := range columns {
		indices[i] = findFieldIndex(typ, col)
	}

	return &StructBulkInsertBuilder[T]{
		tableName:    tableName,
		columns:      columns,
		data:         columnData,
		fieldIndices: indices,
	}
}

func findFieldIndex(typ reflect.Type, colName string) int {
	// 1. Check db tag
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		tag := f.Tag.Get("db")
		parts := strings.Split(tag, ",")
		if len(parts) > 0 && parts[0] == colName {
			return i
		}
	}

	// 2. Check name (case-insensitive)
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if strings.EqualFold(f.Name, colName) {
			return i
		}
	}

	return -1
}

// AddRow adds a single row (struct) to the builder.
func (b *StructBulkInsertBuilder[T]) AddRow(row T) error {
	val := reflect.ValueOf(row)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return fmt.Errorf("bulk insert error for table '%s': nil row pointer", b.tableName)
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return fmt.Errorf("bulk insert error for table '%s': expected struct, got %v", b.tableName, val.Kind())
	}

	for i, fieldIdx := range b.fieldIndices {
		if fieldIdx == -1 {
			return fmt.Errorf("bulk insert error for table '%s': column '%s' not found in struct %v", b.tableName, b.columns[i], val.Type())
		}

		fieldVal := val.Field(fieldIdx).Interface()
		b.data[i] = append(b.data[i], fieldVal)
	}
	return nil
}

// GetSQL generates the INSERT statement with Oracle placeholders (:1, :2, etc.).
func (b *StructBulkInsertBuilder[T]) GetSQL() string {
	placeholders := make([]string, len(b.columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		b.tableName,
		strings.Join(b.columns, ", "),
		strings.Join(placeholders, ", "))
}

// GetArgs returns the arguments to be passed to stmt.Exec.
// It returns a slice of slices, where each inner slice represents a column of data.
func (b *StructBulkInsertBuilder[T]) GetArgs() []interface{} {
	args := make([]interface{}, len(b.data))
	for i, colData := range b.data {
		args[i] = colData
	}
	return args
}
