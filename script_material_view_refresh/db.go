package main

import (
	"context"
	"database/sql"
	"fmt"
)

// OpenOracle opens a go-ora connection and verifies connectivity with Ping.
func OpenOracle(ctx context.Context, connString string) (*sql.DB, error) {
	db, err := sql.Open("oracle", connString)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return db, nil
}

// FetchMaxCreated returns MAX(CREATED_AT) formatted as 'YYYY-MM-DD HH24:MI:SS'.
func FetchMaxCreated(ctx context.Context, db *sql.DB, table string) (string, error) {
	qry := fmt.Sprintf("SELECT TO_CHAR(MAX(CREATED_AT), 'YYYY-MM-DD HH24:MI:SS') FROM %s", table)
	var s sql.NullString
	err := db.QueryRowContext(ctx, qry).Scan(&s)
	if err != nil {
		return "", err
	}
	if !s.Valid {
		return "", nil
	}
	return s.String, nil
}
