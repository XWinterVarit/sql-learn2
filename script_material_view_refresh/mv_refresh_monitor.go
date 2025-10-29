package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/sijms/go-ora/v2"
)

// This tool concurrently polls a table (default: MV_BULK_DATA) for MAX(CREATED_AT)
// Then triggers simulate_bulk_load_and_refresh.sql via sql/sqlplus and keeps polling
// until it observes the CREATED_AT change. It records timings and writes a CSV log.

func main() {
	start := time.Now()
	log.Printf("App start: MV Refresh Monitor at %s", start.Format(time.RFC3339Nano))
	cfg := ParseConfig()
	if err := runMonitor(cfg); err != nil {
		log.Printf("App end (error) after %s: %v", time.Since(start), err)
		log.Fatalf("mv monitor: %v", err)
	}
	log.Printf("App end (ok) after %s", time.Since(start))
}

// runMonitor orchestrates the end-to-end workflow using smaller helpers.
func runMonitor(cfg Config) error {
	// Connect
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connString, err := ResolveDSN(cfg)
	if err != nil {
		return err
	}
	db, err := OpenOracle(ctx, connString, cfg.Concurrency)
	if err != nil {
		return err
	}
	defer db.Close()
	log.Printf("Connected: oracle://%s:***@%s:%s/%s (driver go-ora)", cfg.User, cfg.Host, cfg.Port, cfg.Service)
	log.Printf("Connection pool configured: concurrency=%d, maxOpen=%d, maxIdle=%d",
		cfg.Concurrency, cfg.Concurrency*3+5, cfg.Concurrency+3)

	// CSV output
	csvFile, w, csvPath, err := PrepareCSV(cfg.OutCSV)
	if err != nil {
		return fmt.Errorf("create csv: %w", err)
	}
	defer func() { _ = csvFile.Close() }()
	defer w.Flush()

	// Baseline
	baseline := determineBaseline(ctx, db, cfg.Table)
	log.Printf("Baseline %s MAX(CREATED_AT)=%q", cfg.Table, baseline)

	// Pollers
	samples, wg := StartPollers(ctx, db, cfg.Table, baseline, cfg.Concurrency, cfg.Interval)

	// Trigger
	triggerAt, resultCh := startTrigger(ctx, db, cfg)

	// Aggregate
	observeEnd := computeObserveEnd(cfg, triggerAt)
	firstChangeAt, firstChangeVal, finalBaseline, totalPolls, totalSuccess, totalErrors, p90 := aggregate(samples, w, baseline, !cfg.Quiet, observeEnd)

	// Cleanup pollers
	cancel()
	wg.Wait()
	w.Flush()

	// Collect trigger outcome (non-blocking if already done)
	var scriptStart, scriptEnd time.Time
	var scriptErr error
	select {
	case res := <-resultCh:
		scriptStart, scriptEnd, scriptErr = res.start, res.end, res.err
	default:
	}
	if scriptErr != nil {
		log.Printf("ERROR running simulate script: %v", scriptErr)
	} else if !scriptStart.IsZero() && !scriptEnd.IsZero() {
		log.Printf("Simulate script finished in %s", scriptEnd.Sub(scriptStart))
	}

	printSummary(cfg.Table, csvPath, finalBaseline, triggerAt, observeEnd, scriptStart, scriptEnd, firstChangeAt, firstChangeVal, totalPolls, totalSuccess, totalErrors, p90)
	return nil
}

func determineBaseline(ctx context.Context, db *sqlx.DB, table string) string {
	b, err := FetchMaxCreated(ctx, db, table)
	if err != nil {
		log.Printf("WARN: initial fetch failed: %v", err)
	}
	return b
}

func computeObserveEnd(cfg Config, triggerAt time.Time) time.Time {
	observeEnd := triggerAt.Add(cfg.Observe)
	if observeEnd.Before(time.Now()) {
		observeEnd = time.Now().Add(cfg.Observe)
	}
	return observeEnd
}

// startTrigger schedules the bulk load simulation to run after the preload duration.
// It returns the planned trigger time and a channel delivering the script's start/end times and error.
type scriptResult struct {
	start time.Time
	end   time.Time
	err   error
}

func startTrigger(ctx context.Context, db *sqlx.DB, cfg Config) (time.Time, <-chan scriptResult) {
	triggerAt := time.Now().Add(cfg.Preload)
	log.Printf("Warm-up for %s, will trigger bulk load simulation at ~%s", cfg.Preload.String(), triggerAt.Format(time.RFC3339))

	done := make(chan scriptResult, 1)
	go func() {
		time.Sleep(time.Until(triggerAt))
		st := time.Now()
		log.Printf("Triggering bulk load simulation (Go implementation, %d rows)", cfg.BulkCount)
		err := RunBulkLoadSimulation(ctx, db, cfg.BulkCount)
		ed := time.Now()
		done <- scriptResult{start: st, end: ed, err: err}
	}()
	return triggerAt, done
}

// aggregate consumes poll samples until observeEnd and writes CSV rows.
func aggregate(samples <-chan PollSample, w *csv.Writer, baseline string, verbose bool, observeEnd time.Time) (time.Time, string, string, int, int, int, time.Duration) {
	var firstChangeAt time.Time
	var firstChangeVal string
	var windowCount, windowErr, windowChanged int
	var totalPolls, totalSuccess, totalErrors int
	currentBaseline := baseline
	lastSeen := ""
	var durations []time.Duration       // Collect all query durations for overall p90 calculation
	var windowDurations []time.Duration // Collect query durations for current window p90

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Use a single deadline timer to avoid creating a new timer on every loop iteration
	deadline := time.NewTimer(time.Until(observeEnd))
	defer func() {
		if !deadline.Stop() {
			// drain if already fired to avoid leaking the timer goroutine
			select {
			case <-deadline.C:
			default:
			}
		}
	}()

	for {
		select {
		case s := <-samples:
			totalPolls++
			durations = append(durations, s.Duration)             // Collect duration for overall p90 calculation
			windowDurations = append(windowDurations, s.Duration) // Collect duration for window p90 calculation
			if s.Err != nil || s.Value == "" {
				windowErr++
				totalErrors++
				break
			}
			windowCount++
			totalSuccess++
			if s.Value != "" {
				lastSeen = s.Value
			}
			if currentBaseline == "" && s.Value != "" {
				currentBaseline = s.Value
				log.Printf("Baseline established: %q", currentBaseline)
			}
			if firstChangeAt.IsZero() && currentBaseline != "" && s.Value != "" && s.Value != currentBaseline {
				firstChangeAt = s.When
				firstChangeVal = s.Value
			}
			if s.Changed {
				windowChanged++
			}
			_ = w.Write([]string{s.When.Format(time.RFC3339Nano), fmt.Sprintf("%d", s.WorkerID), safeCSV(s.Value), fmt.Sprintf("%t", s.Changed)})
		case <-ticker.C:
			if verbose {
				windowP90 := calculateP90(windowDurations)
				log.Printf("stats: polls=%d errs=%d changed=%d latest=%q baseline=%q firstChange=%v p90=%v", windowCount, windowErr, windowChanged, lastSeen, currentBaseline, !firstChangeAt.IsZero(), windowP90)
			}
			windowCount, windowErr, windowChanged = 0, 0, 0
			windowDurations = nil // Reset window durations for next interval
		case <-deadline.C:
			p90 := calculateP90(durations)
			return firstChangeAt, firstChangeVal, currentBaseline, totalPolls, totalSuccess, totalErrors, p90
		}
	}
}

// calculateP90 calculates the 90th percentile of query durations.
func calculateP90(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	// Sort durations in ascending order
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	// Calculate 90th percentile index
	index := int(float64(len(sorted)) * 0.90)
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func printSummary(table, csvPath, baseline string, triggerAt, observeEnd, scriptStart, scriptEnd, firstChangeAt time.Time, firstChangeVal string, totalPolls, totalSuccess, totalErrors int, p90 time.Duration) {
	fmt.Println("==== Summary ====")
	fmt.Printf("Table: %s\n", table)
	fmt.Printf("Baseline MAX(CREATED_AT): %q\n", baseline)
	if !scriptStart.IsZero() {
		fmt.Printf("Script started: %s\n", scriptStart.Format(time.RFC3339Nano))
	}
	if !scriptEnd.IsZero() {
		fmt.Printf("Script ended:   %s (dur %s)\n", scriptEnd.Format(time.RFC3339Nano), scriptEnd.Sub(scriptStart))
	}
	if firstChangeAt.IsZero() {
		fmt.Println("First change not observed within observation window.")
	} else {
		fmt.Printf("First observed change: %s -> value=%q\n", firstChangeAt.Format(time.RFC3339Nano), firstChangeVal)
		if !scriptStart.IsZero() {
			fmt.Printf("Lag from script start to first observed change: %s\n", firstChangeAt.Sub(scriptStart))
		}
	}
	fmt.Printf("CSV log: %s\n", csvPath)
	fmt.Printf("Overall query count: %d\n", totalPolls)
	fmt.Printf("Query success count: %d\n", totalSuccess)
	fmt.Printf("Error count: %d\n", totalErrors)
	fmt.Printf("P90 query usage time: %v\n", p90)
	if !firstChangeAt.IsZero() {
		plotTimeline(triggerAt, observeEnd, firstChangeAt)
	}
}
