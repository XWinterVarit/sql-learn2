package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

// OpenOracle opens a go-ora connection using sqlx and verifies connectivity with Ping.
// It configures the connection pool based on concurrency level to handle:
// - Concurrent pollers
// - Bulk insert operations
// - DBMS_MVIEW.REFRESH operations
func OpenOracle(ctx context.Context, connString string, concurrency int) (*sqlx.DB, error) {
	db, err := sqlx.Connect("oracle", connString)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	// Configure connection pool for optimal concurrency handling
	// MaxOpenConns: Allow enough connections for all pollers + bulk operations + refresh
	// Formula: concurrency (pollers) * 3 (safety margin) + 5 (buffer for bulk/refresh ops)
	maxOpen := concurrency*3 + 5
	if maxOpen < 10 {
		maxOpen = 10 // Minimum pool size
	}

	// MaxIdleConns: Keep enough idle connections to avoid setup overhead
	// Formula: concurrency (pollers) + 3 (buffer for operations)
	maxIdle := concurrency + 3
	if maxIdle < 5 {
		maxIdle = 5 // Minimum idle connections
	}

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(5 * time.Minute) // Recycle connections every 5 minutes
	db.SetConnMaxIdleTime(2 * time.Minute) // Close idle connections after 2 minutes

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return db, nil
}

// FetchMaxCreated returns MAX(CREATED_AT) formatted as 'YYYY-MM-DD HH24:MI:SS'.
func FetchMaxCreated(ctx context.Context, db *sqlx.DB, table string) (string, error) {
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
