package dynamic

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	sq "github.com/Masterminds/squirrel"
)

// DataType represents a basic Oracle data type supported by this helper.
// Only common basic types are supported to keep things simple.
// For VARCHAR2 and NUMBER you can provide length/precision/scale via ColumnDef.
type DataType string

const (
	Varchar2  DataType = "VARCHAR2"
	Number    DataType = "NUMBER"
	Date      DataType = "DATE"
	Timestamp DataType = "TIMESTAMP"
	Clob      DataType = "CLOB"
)

// ColumnDef describes one column to create on the table.
//
// Notes:
//   - For VARCHAR2: set Length (>0). If Length==0, default to 255.
//   - For NUMBER: set Precision (>0) and optional Scale (>=0). If Precision==0, NUMBER without precision/scale is used.
//   - Nullable defaults to true; set to false for NOT NULL.
//   - PrimaryKey marks the column to be included in the PRIMARY KEY constraint.
//   - Name and TableName must be simple Oracle identifiers (letters, digits, underscore), starting with a letter.
//     They are used unquoted and automatically uppercased.
//   - Oracle object name length is limited to 30 bytes; we enforce this for identifiers we generate.
type ColumnDef struct {
	Name       string
	Type       DataType
	Length     int // for VARCHAR2
	Precision  int // for NUMBER
	Scale      int // for NUMBER
	Nullable   bool
	PrimaryKey bool
}

var identRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)

// CreateOrReplaceTable drops an existing table (if found) and creates a new one
// with the provided name and columns in the current Oracle schema.
//
// It uses the Squirrel SQL builder for the existence check query, and executes
// the DDL statements via db.Exec. It assumes the *sql.DB is connected to Oracle
// via a compatible driver (e.g., godror or go-ora).
func CreateOrReplaceTable(ctx context.Context, db *sql.DB, tableName string, cols []ColumnDef) error {
	if db == nil {
		return errors.New("db is nil")
	}
	name, err := normalizeIdentifier(tableName)
	if err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}
	if len(cols) == 0 {
		return errors.New("at least one column is required")
	}
	for i := range cols {
		if _, err := normalizeIdentifier(cols[i].Name); err != nil {
			return fmt.Errorf("invalid column name '%s': %w", cols[i].Name, err)
		}
	}

	// 1) Drop if exists
	exists, err := tableExists(ctx, db, name)
	if err != nil {
		return fmt.Errorf("check table exists failed: %w", err)
	}
	if exists {
		// Drop with CASCADE CONSTRAINTS to handle PK/FK and purge to avoid recycle bin issues.
		// Some Oracle versions may not support PURGE; if it fails, try without it.
		dropDDL := fmt.Sprintf("DROP TABLE %s CASCADE CONSTRAINTS PURGE", name)
		if _, err := db.ExecContext(ctx, dropDDL); err != nil {
			// fallback without PURGE
			dropDDL = fmt.Sprintf("DROP TABLE %s CASCADE CONSTRAINTS", name)
			if _, err2 := db.ExecContext(ctx, dropDDL); err2 != nil {
				return fmt.Errorf("drop table failed: %v; fallback failed: %w", err, err2)
			}
		}
	}

	// 2) Build CREATE TABLE DDL
	ddl, err := buildCreateTableDDL(name, cols)
	if err != nil {
		return err
	}

	// 3) Execute CREATE TABLE
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}
	return nil
}

// tableExists uses Squirrel to check USER_TABLES for the given table name.
func tableExists(ctx context.Context, db *sql.DB, tableName string) (bool, error) {
	builder := sq.StatementBuilder.PlaceholderFormat(sq.Colon) // Oracle-friendly :1, :2 ...
	sqlStr, args, err := builder.
		Select("COUNT(1)").
		From("USER_TABLES").
		Where(sq.Eq{"TABLE_NAME": strings.ToUpper(tableName)}).
		ToSql()
	if err != nil {
		return false, err
	}
	var cnt int64
	if err := db.QueryRowContext(ctx, sqlStr, args...).Scan(&cnt); err != nil {
		return false, err
	}
	return cnt > 0, nil
}

func buildCreateTableDDL(tableName string, cols []ColumnDef) (string, error) {
	if len(cols) == 0 {
		return "", errors.New("no columns provided")
	}
	defs := make([]string, 0, len(cols))
	pkCols := make([]string, 0, len(cols))
	for _, c := range cols {
		colName, _ := normalizeIdentifier(c.Name)
		typeStr, err := oracleTypeString(c)
		if err != nil {
			return "", fmt.Errorf("column %s: %w", c.Name, err)
		}
		nullable := ""
		if !c.Nullable {
			nullable = " NOT NULL"
		}
		defs = append(defs, fmt.Sprintf("%s %s%s", colName, typeStr, nullable))
		if c.PrimaryKey {
			pkCols = append(pkCols, colName)
		}
	}

	// Add PRIMARY KEY constraint if provided
	if len(pkCols) > 0 {
		// Deterministic order for PK columns
		sort.Strings(pkCols)
		constraintName := truncateIdentifier(fmt.Sprintf("%s_PK", tableName))
		defs = append(defs, fmt.Sprintf("CONSTRAINT %s PRIMARY KEY (%s)", constraintName, strings.Join(pkCols, ", ")))
	}

	return fmt.Sprintf("CREATE TABLE %s (\n  %s\n)", tableName, strings.Join(defs, ",\n  ")), nil
}

func oracleTypeString(c ColumnDef) (string, error) {
	switch strings.ToUpper(string(c.Type)) {
	case string(Varchar2):
		length := c.Length
		if length <= 0 {
			length = 255
		}
		return fmt.Sprintf("VARCHAR2(%d)", length), nil
	case string(Number):
		if c.Precision > 0 {
			if c.Scale > 0 {
				return fmt.Sprintf("NUMBER(%d,%d)", c.Precision, c.Scale), nil
			}
			return fmt.Sprintf("NUMBER(%d)", c.Precision), nil
		}
		return "NUMBER", nil
	case string(Date):
		return "DATE", nil
	case string(Timestamp):
		return "TIMESTAMP", nil
	case string(Clob):
		return "CLOB", nil
	default:
		return "", fmt.Errorf("unsupported data type: %s", c.Type)
	}
}

func normalizeIdentifier(name string) (string, error) {
	if !identRe.MatchString(name) {
		return "", fmt.Errorf("identifier must match %s", identRe.String())
	}
	upper := strings.ToUpper(name)
	if len(upper) > 30 {
		return "", errors.New("identifier exceeds Oracle 30-byte limit")
	}
	return upper, nil
}

func truncateIdentifier(name string) string {
	upper := strings.ToUpper(name)
	if len(upper) <= 30 {
		return upper
	}
	return upper[:30]
}
