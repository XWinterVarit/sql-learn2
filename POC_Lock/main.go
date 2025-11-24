package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/sijms/go-ora/v2"
)

func main() {
	// Connection flags
	user := flag.String("user", getEnv("ORA_USER", "LEARN1"), "Oracle username")
	pass := flag.String("pass", getEnv("ORA_PASS", "Welcome"), "Oracle password")
	host := flag.String("host", getEnv("ORA_HOST", "localhost"), "Oracle host")
	port := flag.String("port", getEnv("ORA_PORT", "1521"), "Oracle port")
	service := flag.String("service", getEnv("ORA_SERVICE", "XE"), "Oracle service name")
	flag.Parse()

	// Build DSN
	dsn := fmt.Sprintf("oracle://%s:%s@%s:%s/%s", *user, *pass, *host, *port, *service)

	// Connect
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("✓ Connected to Oracle")

	ctx := context.Background()

	// Step 1: Cleanup and setup tables
	log.Println("Step 1: Cleaning up and creating tables A, B, C, EVENT_LOG...")
	if err := CleanupTables(ctx, db); err != nil {
		log.Fatalf("Cleanup failed: %v", err)
	}
	if err := CreateTables(ctx, db); err != nil {
		log.Fatalf("Table creation failed: %v", err)
	}
	log.Println("✓ Tables created and sample data inserted")

	// Step 2: Initialize logger and timeline tracker
	logger := NewEventLogger(db)
	startTime := time.Now()
	timeline := NewTimelineTracker(startTime)

	// Step 3: Launch two concurrent goroutines
	log.Println("Step 2: Launching CHAIN and EARLY goroutines...")
	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: CHAIN flow
	go func() {
		defer wg.Done()
		if err := RunChainFlow(ctx, db, logger, timeline, startTime); err != nil {
			log.Printf("CHAIN flow error: %v", err)
		}
	}()

	// Goroutine 2: EARLY flow
	go func() {
		defer wg.Done()
		if err := RunEarlyFlow(ctx, db, logger, timeline, startTime); err != nil {
			log.Printf("EARLY flow error: %v", err)
		}
	}()

	// Wait for both to complete
	wg.Wait()
	log.Println("✓ Both flows completed")

	// Step 4: Display event log
	log.Println("\n=== Event Log (ordered by time) ===")
	if err := DisplayEventLog(ctx, db); err != nil {
		log.Printf("Failed to display event log: %v", err)
	}

	// Step 5: Display timeline graph
	timeline.RenderTimeline()

	// Step 6: Display final state of table C
	log.Println("\n=== Final rows in table C ===")
	if err := DisplayTableC(ctx, db); err != nil {
		log.Printf("Failed to display table C: %v", err)
	}

	log.Println("\n✓ Demo completed successfully")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
