package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
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

// StartPollers launches N goroutines that poll CREATED_AT from a randomly chosen row
// whose id is not greater than the current MAX(id) in the given table, at the specified
// interval. It returns a samples channel and a WaitGroup pointer that will be done when
// all pollers exit (on ctx cancellation).
func StartPollers(ctx context.Context, db *sql.DB, table, baseline string, concurrency int, interval time.Duration) (chan PollSample, *sync.WaitGroup) {
	samples := make(chan PollSample, concurrency*4)
	var wg sync.WaitGroup

	// Queries: fetch current MAX(id), and fetch CREATED_AT for a specific id
	maxIDQry := fmt.Sprintf("SELECT MAX(ID) FROM %s", table)
	createdAtByIDQry := fmt.Sprintf("SELECT TO_CHAR(CREATED_AT, 'YYYY-MM-DD HH24:MI:SS') FROM %s WHERE ID = :1", table)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			t := time.NewTicker(interval)
			defer t.Stop()

			// Each worker has its own RNG to avoid lock contention and ensure better distribution
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					when := time.Now()

					// 1) Get current MAX(id)
					var maxID sql.NullInt64
					err := db.QueryRowContext(ctx, maxIDQry).Scan(&maxID)
					if err != nil {
						samples <- PollSample{When: when, WorkerID: id, Value: "", Err: err, Changed: false}
						continue
					}
					if !maxID.Valid || maxID.Int64 <= 0 {
						// Table empty or invalid MAX(id)
						samples <- PollSample{When: when, WorkerID: id, Value: "", Err: nil, Changed: false}
						continue
					}

					// 2) Try a few random picks up to maxID to handle potential gaps
					var val string
					var s sql.NullString
					var pickErr error
					const maxAttempts = 3
					for attempt := 0; attempt < maxAttempts; attempt++ {
						r := 1 + rng.Int63n(maxID.Int64) // in [1, maxID]
						pickErr = db.QueryRowContext(ctx, createdAtByIDQry, r).Scan(&s)
						if pickErr == nil && s.Valid {
							val = s.String
							break
						}
					}
					// If after attempts no valid value, keep val as empty and report last error if any
					changed := baseline != "" && val != "" && val != baseline
					samples <- PollSample{When: when, WorkerID: id, Value: val, Err: pickErr, Changed: changed}
				}
			}
		}(i)
	}
	return samples, &wg
}
