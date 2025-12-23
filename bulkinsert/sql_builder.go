package bulkinsert

import (
	"fmt"
	"strings"
)

// buildInsertSQL constructs the INSERT SQL statement with placeholders.
// Returns the SQL string with named placeholders (:1, :2, etc.).
func buildInsertSQL(tableName string, columnNames []string) string {
	placeholders := make([]string, len(columnNames))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "))
}
