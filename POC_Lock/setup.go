package main

import (
	"context"
	"database/sql"
	"fmt"
)

// CleanupTables drops A, B, C, EVENT_LOG if they exist
func CleanupTables(ctx context.Context, db *sql.DB) error {
	tables := []string{"C", "B", "A", "EVENT_LOG"} // drop in reverse dependency order
	for _, tbl := range tables {
		_, err := db.ExecContext(ctx, "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "+tbl+" PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;")
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateTables creates A, B, C, EVENT_LOG and inserts sample data
func CreateTables(ctx context.Context, db *sql.DB) error {
	ddl := []string{
		`CREATE TABLE A (
			id   NUMBER PRIMARY KEY,
			data VARCHAR2(50)
		)`,
		`CREATE TABLE B (
			id   NUMBER PRIMARY KEY,
			a_id NUMBER REFERENCES A(id),
			data VARCHAR2(50)
		)`,
		`CREATE TABLE C (
			id         NUMBER PRIMARY KEY,
			b_id       NUMBER REFERENCES B(id),
			data       VARCHAR2(50),
			chain_data VARCHAR2(50),
			early_data VARCHAR2(50)
		)`,
		`CREATE TABLE EVENT_LOG (
			ts  TIMESTAMP(3) DEFAULT SYSTIMESTAMP,
			who VARCHAR2(50),
			msg VARCHAR2(4000)
		)`,
	}

	for _, stmt := range ddl {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}

	// Insert sample data
	inserts := []string{
		`INSERT INTO A (id, data) VALUES (1, 'A1')`,
		`INSERT INTO B (id, a_id, data) VALUES (1, 1, 'B1')`,
		`INSERT INTO C (id, b_id, data) VALUES (1, 1, 'C1')`,
	}
	for _, stmt := range inserts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}

	// Commit initial data
	_, err := db.ExecContext(ctx, "COMMIT")
	return err
}

// DisplayTableC prints final contents of table C
func DisplayTableC(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "SELECT id, b_id, data, chain_data, early_data FROM C ORDER BY id")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id, bID int
		var data, chainData, earlyData sql.NullString
		if err := rows.Scan(&id, &bID, &data, &chainData, &earlyData); err != nil {
			return err
		}
		fmt.Printf("  ID: %d  B_ID: %d  DATA: %s  CHAIN_DATA: %s  EARLY_DATA: %s\n",
			id, bID, nullStringValue(data), nullStringValue(chainData), nullStringValue(earlyData))
	}
	return rows.Err()
}

func nullStringValue(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return "<NULL>"
}
