package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// PollSample represents a single polling observation from a worker.
type PollSample struct {
	When     time.Time
	WorkerID int
	Value    string
	Err      error
	Changed  bool
}

// StartPollers launches N goroutines that poll MAX(CREATED_AT) from the given table
// at the specified interval. It returns a samples channel and a WaitGroup pointer
// that will be done when all pollers exit (on ctx cancellation).
func StartPollers(ctx context.Context, db *sql.DB, table, baseline string, concurrency int, interval time.Duration) (chan PollSample, *sync.WaitGroup) {
	samples := make(chan PollSample, concurrency*4)
	var wg sync.WaitGroup
	qry := fmt.Sprintf("SELECT TO_CHAR(MAX(CREATED_AT), 'YYYY-MM-DD HH24:MI:SS') FROM %s", table)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					var s sql.NullString
					when := time.Now()
					err := db.QueryRowContext(ctx, qry).Scan(&s)
					val := ""
					if err == nil && s.Valid {
						val = s.String
					}
					changed := baseline != "" && val != "" && val != baseline
					samples <- PollSample{When: when, WorkerID: id, Value: val, Err: err, Changed: changed}
				}
			}
		}(i)
	}
	return samples, &wg
}
