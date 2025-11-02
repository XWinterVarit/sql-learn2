package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
)

// PollSample represents a single polling observation from a worker.
type PollSample struct {
	When       time.Time
	WorkerID   int
	Value      string
	Err        error
	Changed    bool
	Duration   time.Duration // Total query duration for this poll
	Congestion int           // Number of concurrent in-flight queries at sample time
}

// StartPollers launches N goroutines that poll CREATED_AT from a randomly chosen row
// using TPS-based rate limiting with congestion tracking.
// If TPS > 0, it controls the global query rate (queries per second).
// If TPS <= 0, falls back to interval-based polling per worker.
// MaxCongestion sets a hard limit on concurrent in-flight queries.
// QueryTimeout sets the timeout for individual queries.
// Returns: samples channel, wait group, and pointer to the congestion counter for real-time monitoring.
func StartPollers(ctx context.Context, db *sqlx.DB, table, baseline string, concurrency int, interval time.Duration, tps int, maxCongestion int, queryTimeout time.Duration) (chan PollSample, *sync.WaitGroup, *int64) {
	samples := make(chan PollSample, concurrency*4)
	var wg sync.WaitGroup
	congestionCounter := new(int64) // Atomic counter for in-flight queries (heap-allocated for external access)

	// Queries: fetch current MAX(id), and fetch CREATED_AT for a specific id
	maxIDQry := fmt.Sprintf("SELECT MAX(ID) FROM %s", table)
	createdAtByIDQry := fmt.Sprintf("SELECT TO_CHAR(CREATED_AT, 'YYYY-MM-DD HH24:MI:SS') FROM %s WHERE ID = :1", table)

	// TPS-based rate limiter: centralized ticker that controls query rate
	var rateLimiter <-chan time.Time
	if tps > 0 {
		// Create a ticker that fires 'tps' times per second
		ticker := time.NewTicker(time.Second / time.Duration(tps))
		rateLimiter = ticker.C
		// Note: ticker is not stopped here; it will be garbage collected when workers exit
		// and no longer reference rateLimiter channel
	}

	// Worker function that executes a single poll
	executePoll := func(workerID int, rng *rand.Rand) {
		when := time.Now()
		pollStart := time.Now()

		// Check congestion limit before attempting query
		currentCongestion := atomic.LoadInt64(congestionCounter)
		if maxCongestion > 0 && currentCongestion >= int64(maxCongestion) {
			// Instantly fail due to congestion limit
			samples <- PollSample{
				When:       when,
				WorkerID:   workerID,
				Value:      "",
				Err:        fmt.Errorf("congestion limit exceeded: %d >= %d", currentCongestion, maxCongestion),
				Changed:    false,
				Duration:   time.Since(pollStart),
				Congestion: int(currentCongestion),
			}
			return
		}

		// Increment congestion counter
		atomic.AddInt64(congestionCounter, 1)
		congestion := int(atomic.LoadInt64(congestionCounter))
		defer atomic.AddInt64(congestionCounter, -1)

		// Create timeout context for this query
		queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		defer cancel()

		// 1) Get current MAX(id)
		var maxID sql.NullInt64
		err := db.QueryRowContext(queryCtx, maxIDQry).Scan(&maxID)
		if err != nil {
			samples <- PollSample{When: when, WorkerID: workerID, Value: "", Err: err, Changed: false, Duration: time.Since(pollStart), Congestion: congestion}
			return
		}
		if !maxID.Valid || maxID.Int64 <= 0 {
			// Table empty or invalid MAX(id)
			samples <- PollSample{When: when, WorkerID: workerID, Value: "", Err: nil, Changed: false, Duration: time.Since(pollStart), Congestion: congestion}
			return
		}

		// 2) Try a few random picks up to maxID to handle potential gaps
		var val string
		var s sql.NullString
		var pickErr error
		const maxAttempts = 3
		for attempt := 0; attempt < maxAttempts; attempt++ {
			r := 1 + rng.Int63n(maxID.Int64) // in [1, maxID]
			pickErr = db.QueryRowContext(queryCtx, createdAtByIDQry, r).Scan(&s)
			if pickErr == nil && s.Valid {
				val = s.String
				break
			}
		}
		// If after attempts no valid value, keep val as empty and report last error if any
		changed := baseline != "" && val != "" && val != baseline
		samples <- PollSample{When: when, WorkerID: workerID, Value: val, Err: pickErr, Changed: changed, Duration: time.Since(pollStart), Congestion: congestion}
	}

	// Launch workers
	if tps > 0 {
		// TPS-based mode: workers pull from centralized rate limiter
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
				for {
					select {
					case <-ctx.Done():
						return
					case <-rateLimiter:
						executePoll(id, rng)
					}
				}
			}(i)
		}
	} else {
		// Interval-based mode (backward compatibility): each worker has its own ticker
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				t := time.NewTicker(interval)
				defer t.Stop()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
				for {
					select {
					case <-ctx.Done():
						return
					case <-t.C:
						executePoll(id, rng)
					}
				}
			}(i)
		}
	}
	return samples, &wg, congestionCounter
}
