package csvdb

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
	"regexp"
	"strconv"
	"strings"

	"sql-learn2/dynamic"
)

// LoadCSVToDB reads a CSV file and creates a table based on its content, then loads data.
//
// Rules per requirements:
// - Table name = CSV file name (without extension), normalized to Oracle identifier
// - Column names = first row (header), normalized to Oracle identifiers
// - Data types = second row; supported: VARCHAR2, NUMBER, DATE, TIMESTAMP, CLOB (others error)
// - Data rows = from third row onwards
// - Uses dynamic package to create or replace the table
//
// Notes:
// - Whitespace around header/type cells is trimmed.
// - If a data row has fewer cells than columns, remaining cells are treated as NULL.
// - If a data row has more cells, extras are ignored.
// - NUMBER values are parsed into int64 or float64 when possible; empty string => NULL.
// - Other types are passed as strings; empty string => NULL.
func LoadCSVToDB(ctx context.Context, db *sql.DB, csvPath string) error {
	if db == nil {
		return errors.New("db is nil")
	}
	if csvPath == "" {
		return errors.New("csvPath is empty")
	}

	f, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("open csv: %w", err)
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1 // allow variable

	rows := make([][]string, 0, 128)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read csv: %w", err)
		}
		// Trim spaces for each cell
		for i := range rec {
			rec[i] = strings.TrimSpace(rec[i])
		}
		// Skip fully empty lines
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
	if len(rows) < 3 {
		// no data rows; we still create the table
	}

	headers := rows[0]
	typesRow := rows[1]

	if len(typesRow) < len(headers) {
		return fmt.Errorf("types row has fewer cells (%d) than headers (%d)", len(typesRow), len(headers))
	}

	// Derive table name from file name
	base := filepath.Base(csvPath)
	name := strings.TrimSuffix(base, filepath.Ext(base))
	tableName := normalizeIdentifierForOracle(name)
	if tableName == "" {
		return fmt.Errorf("cannot derive valid table name from file: %s", base)
	}

	// Build column defs
	cols := make([]dynamic.ColumnDef, 0, len(headers))
	oracleCols := make([]string, 0, len(headers))
	for i, h := range headers {
		colName := normalizeIdentifierForOracle(h)
		if colName == "" {
			return fmt.Errorf("invalid column name at position %d: %q", i+1, h)
		}
		oracleCols = append(oracleCols, colName)

		dtype := strings.ToUpper(strings.TrimSpace(typesRow[i]))
		var dt dynamic.DataType
		switch dtype {
		case "VARCHAR", "VARCHAR2":
			dt = dynamic.Varchar2
		case "NUMBER":
			dt = dynamic.Number
		case "DATE":
			dt = dynamic.Date
		case "TIMESTAMP":
			dt = dynamic.Timestamp
		case "CLOB":
			dt = dynamic.Clob
		default:
			return fmt.Errorf("unsupported type %q for column %s", dtype, colName)
		}
		cols = append(cols, dynamic.ColumnDef{
			Name:     colName,
			Type:     dt,
			Nullable: true,
		})
	}

	// Create or replace table via dynamic package
	if err := dynamic.CreateOrReplaceTable(ctx, db, tableName, cols); err != nil {
		return err
	}

	// If no data rows, we're done
	if len(rows) <= 2 {
		return nil
	}

	dataRows := rows[2:]

	// Prepare INSERT statement with Oracle-style placeholders :1, :2, ...
	placeholders := make([]string, len(cols))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(oracleCols, ", "), strings.Join(placeholders, ", "))

	stmt, err := db.PrepareContext(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	for rIdx, rec := range dataRows {
		vals := make([]any, len(cols))
		for cIdx := range cols {
			cell := ""
			if cIdx < len(rec) {
				cell = strings.TrimSpace(rec[cIdx])
			}
			if cell == "" {
				vals[cIdx] = sql.NullString{Valid: false}
				continue
			}
			switch cols[cIdx].Type {
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
			return fmt.Errorf("insert row %d: %w", rIdx+3, err)
		}
	}

	return nil
}

var identRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)

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
	if !identRe.MatchString(upper) {
		return ""
	}
	return upper
}
