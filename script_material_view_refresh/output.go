package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// safeCSV ensures a printable value for CSV; csv.Writer handles quoting.
func safeCSV(s string) string {
	if s == "" {
		return ""
	}
	return s
}

// PrepareCSV creates the CSV writer and ensures the output directory exists.
// It also writes the header row.
func PrepareCSV(outPath string) (*os.File, *csv.Writer, string, error) {
	csvPath := strings.TrimSpace(outPath)
	if csvPath == "" {
		_ = os.MkdirAll("logs", 0o755)
		csvPath = filepath.Join("logs", time.Now().Format("mv_monitor_20060102_150405.csv"))
	} else {
		dir := filepath.Dir(csvPath)
		if dir != "." && dir != "" {
			_ = os.MkdirAll(dir, 0o755)
		}
	}
	f, err := os.Create(csvPath)
	if err != nil {
		return nil, nil, "", err
	}
	w := csv.NewWriter(f)
	_ = w.Write([]string{"ts", "worker", "value", "changed"})
	w.Flush()
	return f, w, csvPath, nil
}

// plotTimeline prints a simple ASCII timeline marking trigger and first change.
func plotTimeline(start, end, change time.Time) {
	total := end.Sub(start)
	if total <= 0 {
		return
	}
	const cols = 60
	buf := make([]rune, cols)
	for i := range buf {
		buf[i] = '-'
	}
	cpos := int(float64(change.Sub(start)) / float64(total) * float64(cols-1))
	if cpos < 0 {
		cpos = 0
	}
	if cpos >= cols {
		cpos = cols - 1
	}
	buf[0] = 'T'    // Trigger
	buf[cpos] = 'C' // Change observed
	fmt.Printf("Timeline: [%s]\n", string(buf))
	fmt.Println("Legend: T=trigger time, C=first observed change")
}
