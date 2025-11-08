//go:build heavy

package bulkinsert

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"
)

// TestGenerate10M32Cols_MemoryTiming
//
// This opt-in heavy test generates 10,000,000 rows with 32 columns using
// BulkDataBuilder and reports elapsed time and memory usage in gigabytes.
//
// IMPORTANT: This requires a machine with many gigabytes of free RAM
// (interfaces alone can exceed ~5 GB for 10M x 32). Use with caution.
//
// How to run (opt-in):
//
//	go test -tags heavy -run TestGenerate10M32Cols_MemoryTiming -timeout 0 -v ./bulkinsert
//
// Optional: Set PROGRESS=1 to print periodic progress updates.
func TestGenerate5M32Cols_MemoryTiming(t *testing.T) {

	const rowsN = 500_000
	const colsN = 32

	// Precompute column names (captured from the first row only)
	colNames := make([]string, colsN)
	for c := 0; c < colsN; c++ {
		colNames[c] = fmt.Sprintf("COL_%02d", c)
	}

	// Force a GC cycle to establish a clean-ish baseline
	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)

	start := time.Now()

	builder := NewBulkDataBuilder(rowsN)

	// First row includes names to set the schema
	first := make(Row, colsN)
	for c := 0; c < colsN; c++ {
		first[c] = Column{Name: colNames[c], Value: c}
	}
	if err := builder.AddRow(first); err != nil {
		t.Fatalf("AddRow(first) error: %v", err)
	}

	// Prepare a reusable row with only values (names ignored after first capture)
	row := make(Row, colsN)

	progress := os.Getenv("PROGRESS") == "1"
	lastReport := time.Now()

	for r := 1; r < rowsN; r++ {
		base := r * 1000
		for c := 0; c < colsN; c++ {
			row[c] = Column{Value: base + c}
		}
		if err := builder.AddRow(row); err != nil {
			t.Fatalf("AddRow error at row %d: %v", r, err)
		}

		if progress && r%1_000_000 == 0 {
			// Lightweight periodic status (every 1M rows)
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			elapsed := time.Since(start)
			allocGB := float64(ms.Alloc-m0.Alloc) / (1024 * 1024 * 1024)
			t.Logf("progress: rows=%d (%.1f%%), elapsed=%v, alloc≈%.2f GB, since last=%v",
				r, float64(r)/float64(rowsN)*100.0, elapsed, allocGB, time.Since(lastReport))
			lastReport = time.Now()
		}
	}

	// Use the builder result to keep it alive
	if builder.GetNumRows() != rowsN {
		t.Fatalf("rows = %d, want %d", builder.GetNumRows(), rowsN)
	}
	_ = builder.GetColumnData()

	// Measure memory after build (without forcing a GC), this reflects in-use heap
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	elapsed := time.Since(start)

	allocDelta := int64(m1.Alloc) - int64(m0.Alloc)
	totalAllocDelta := int64(m1.TotalAlloc) - int64(m0.TotalAlloc)

	const GiB = 1024 * 1024 * 1024
	allocGB := float64(allocDelta) / float64(GiB)
	totalAllocGB := float64(totalAllocDelta) / float64(GiB)

	t.Logf("Generated %d rows x %d cols", rowsN, colsN)
	t.Logf("Elapsed time: %v", elapsed)
	t.Logf("Heap Alloc delta: %.2f GiB (Alloc: %d -> %d)", allocGB, m0.Alloc, m1.Alloc)
	t.Logf("TotalAlloc delta: %.2f GiB (TotalAlloc: %d -> %d)", totalAllocGB, m0.TotalAlloc, m1.TotalAlloc)

	// Optionally, force a GC and report post-GC retained memory
	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)
	retainedGB := float64(int64(m2.Alloc)-int64(m0.Alloc)) / float64(GiB)
	t.Logf("Post-GC retained heap (delta): %.2f GiB (Alloc: %d)", retainedGB, m2.Alloc)
}
