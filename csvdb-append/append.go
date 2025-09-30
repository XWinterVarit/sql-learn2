package csvdbappend

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"sql-learn2/dynamic"
)

// UpsertCSVToDB reads a CSV file and upserts its data into an existing Oracle table.
//
// CSV format (same as csvdb package):
// - Row 1: column headers
// - Row 2: data types (VARCHAR2, NUMBER, DATE, TIMESTAMP, CLOB) — used for value conversion only
// - Row 3+: data rows
//
// Behavior:
//   - The target table must already exist with compatible columns.
//   - keyCols defines the natural key used to match existing rows. Matching rows are updated
//     (non-key columns only). Non-matching rows are inserted.
//   - Column and table names are normalized to Oracle unquoted identifiers (upper-case, 30-char limit, etc.).
func UpsertCSVToDB(ctx context.Context, db *sql.DB, csvPath, tableName string, keyCols []string) error {
	if db == nil {
		return errors.New("db is nil")
	}
	if csvPath == "" {
		return errors.New("csvPath is empty")
	}
	if len(keyCols) == 0 {
		return errors.New("keyCols must not be empty")
	}

	f, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("open csv: %w", err)
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1

	rows := make([][]string, 0, 128)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read csv: %w", err)
		}
		for i := range rec {
			rec[i] = strings.TrimSpace(rec[i])
		}
		// skip empty rows
		empty := true
		for _, v := range rec {
			if v != "" {
				empty = false
				break
			}
		}
		if empty {
			continue
		}
		rows = append(rows, rec)
	}
	if len(rows) < 2 {
		return errors.New("csv must have at least 2 rows: header and types")
	}

	headers := rows[0]
	typesRow := rows[1]
	if len(typesRow) < len(headers) {
		return fmt.Errorf("types row has fewer cells (%d) than headers (%d)", len(typesRow), len(headers))
	}

	// Derive table name if not provided
	if strings.TrimSpace(tableName) == "" {
		base := filepath.Base(csvPath)
		name := strings.TrimSuffix(base, filepath.Ext(base))
		tableName = normalizeIdentifierForOracle(name)
		if tableName == "" {
			return fmt.Errorf("cannot derive valid table name from file: %s", base)
		}
	} else {
		tableName = normalizeIdentifierForOracle(tableName)
		if tableName == "" {
			return fmt.Errorf("invalid table name")
		}
	}

	// Normalize headers and collect types
	oracleCols := make([]string, 0, len(headers))
	colTypes := make([]dynamic.DataType, 0, len(headers))
	for i, h := range headers {
		col := normalizeIdentifierForOracle(h)
		if col == "" {
			return fmt.Errorf("invalid column name at position %d: %q", i+1, h)
		}
		oracleCols = append(oracleCols, col)
		dtStr := strings.ToUpper(strings.TrimSpace(typesRow[i]))
		switch dtStr {
		case "VARCHAR", "VARCHAR2":
			colTypes = append(colTypes, dynamic.Varchar2)
		case "NUMBER":
			colTypes = append(colTypes, dynamic.Number)
		case "DATE":
			colTypes = append(colTypes, dynamic.Date)
		case "TIMESTAMP":
			colTypes = append(colTypes, dynamic.Timestamp)
		case "CLOB":
			colTypes = append(colTypes, dynamic.Clob)
		default:
			return fmt.Errorf("unsupported type %q for column %s", dtStr, col)
		}
	}

	// Normalize and validate key columns
	colIndex := make(map[string]int, len(oracleCols))
	for i, c := range oracleCols {
		colIndex[c] = i
	}
	keys := make([]string, 0, len(keyCols))
	for _, k := range keyCols {
		kk := normalizeIdentifierForOracle(k)
		if kk == "" {
			return fmt.Errorf("invalid key column: %q", k)
		}
		if _, ok := colIndex[kk]; !ok {
			return fmt.Errorf("key column %s not found in CSV headers", kk)
		}
		keys = append(keys, kk)
	}

	// Determine non-key columns for UPDATE
	nonKeys := make([]string, 0, len(oracleCols))
	isKey := make(map[string]bool, len(keys))
	for _, k := range keys {
		isKey[k] = true
	}
	for _, c := range oracleCols {
		if !isKey[c] {
			nonKeys = append(nonKeys, c)
		}
	}

	if len(rows) <= 2 {
		// nothing to do
		return nil
	}
	dataRows := rows[2:]

	// Build MERGE statement template
	placeholders := make([]string, len(oracleCols))
	selectItems := make([]string, len(oracleCols))
	for i := range oracleCols {
		ph := fmt.Sprintf(":%d", i+1)
		placeholders[i] = ph
		selectItems[i] = fmt.Sprintf("%s AS %s", ph, oracleCols[i])
	}

	onConds := make([]string, len(keys))
	for i, k := range keys {
		onConds[i] = fmt.Sprintf("t.%s = s.%s", k, k)
	}

	updateClause := ""
	if len(nonKeys) > 0 {
		sets := make([]string, len(nonKeys))
		for i, c := range nonKeys {
			sets[i] = fmt.Sprintf("t.%s = s.%s", c, c)
		}
		updateClause = fmt.Sprintf("WHEN MATCHED THEN UPDATE SET %s", strings.Join(sets, ", "))
	}

	insertCols := strings.Join(oracleCols, ", ")
	values := make([]string, len(oracleCols))
	for i, c := range oracleCols {
		values[i] = fmt.Sprintf("s.%s", c)
	}
	insertClause := fmt.Sprintf("WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)", insertCols, strings.Join(values, ", "))

	mergeSQL := fmt.Sprintf(
		"MERGE INTO %s t USING (SELECT %s FROM DUAL) s ON (%s) %s %s",
		tableName,
		strings.Join(selectItems, ", "),
		strings.Join(onConds, " AND "),
		updateClause,
		insertClause,
	)

	stmt, err := db.PrepareContext(ctx, mergeSQL)
	if err != nil {
		return fmt.Errorf("prepare merge: %w", err)
	}
	defer stmt.Close()

	for rIdx, rec := range dataRows {
		vals := make([]any, len(oracleCols))
		for cIdx := range oracleCols {
			cell := ""
			if cIdx < len(rec) {
				cell = strings.TrimSpace(rec[cIdx])
			}
			if cell == "" {
				vals[cIdx] = sql.NullString{Valid: false}
				continue
			}
			switch colTypes[cIdx] {
			case dynamic.Number:
				// Decide int64 vs float64
				if strings.ContainsAny(cell, ".eE") {
					if f, err := strconv.ParseFloat(cell, 64); err == nil {
						vals[cIdx] = f
					} else {
						return fmt.Errorf("row %d col %d: invalid NUMBER %q: %v", rIdx+3, cIdx+1, cell, err)
					}
				} else {
					if n, err := strconv.ParseInt(cell, 10, 64); err == nil {
						vals[cIdx] = n
					} else if f, err2 := strconv.ParseFloat(cell, 64); err2 == nil {
						vals[cIdx] = f
					} else {
						return fmt.Errorf("row %d col %d: invalid NUMBER %q", rIdx+3, cIdx+1, cell)
					}
				}
			default:
				vals[cIdx] = cell
			}
		}
		if _, err := stmt.ExecContext(ctx, vals...); err != nil {
			return fmt.Errorf("merge row %d: %w", rIdx+3, err)
		}
	}

	return nil
}

// normalizeIdentifierForOracle converts a string into a valid Oracle unquoted identifier:
// - Uppercases
// - Replaces invalid characters with underscore
// - Ensures it starts with a letter (prefixes with X if needed)
// - Truncates to 30 chars
func normalizeIdentifierForOracle(s string) string {
	if s == "" {
		return ""
	}
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, " ", "_")
	// Replace non [A-Za-z0-9_] with _
	b := make([]rune, 0, len(s))
	for _, r := range s {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b = append(b, r)
		} else {
			b = append(b, '_')
		}
	}
	upper := strings.ToUpper(string(b))
	if len(upper) == 0 {
		return ""
	}
	if !(upper[0] >= 'A' && upper[0] <= 'Z') {
		upper = "X" + upper
	}
	if len(upper) > 30 {
		upper = upper[:30]
	}
	return upper
}
