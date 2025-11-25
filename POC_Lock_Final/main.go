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
	hideExpected := flag.Bool("hide-expected", true, "Hide expected timeline flows")
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

	// Step 3: Define Flows
	log.Println("Step 3: Defining CHAIN and EARLY flows...")

	// CHAIN Flow
	// 1. Lock A -> Wait 3s
	// 2. Update B -> Wait 2s
	// 3. Update C -> Wait 2s -> Commit
	chain := NewTxFlow("CHAIN", db, logger, timeline)
	chain.AddQuery("A", "Locked A.id=1", "SELECT id FROM A WHERE id = 1 FOR UPDATE")
	chain.AddWait(4 * time.Second)
	chain.AddUpdate("B", "Updating B.id=1", "UPDATE B SET data = 'B1_UPDATED_BY_CHAIN' WHERE id = 1")
	chain.AddWait(2 * time.Second)
	chain.AddUpdate("C", "Updating C.id=1", "UPDATE C SET chain_data = 'UPDATED_BY_CHAIN' WHERE id = 1")
	chain.AddWait(2 * time.Second)

	// EARLY Flow
	// 1. Wait 2s
	// 2. Update B
	// 3. Update C
	// 4. Wait 15s (holding lock) -> Commit
	early := NewTxFlow("EARLY", db, logger, timeline)
	early.AddWait(2 * time.Second)
	early.AddUpdate("B", "Updating B.id=1 (data column)", "UPDATE B SET data = 'UPDATED_EARLY' WHERE id = 1")
	early.AddUpdate("C", "Updating C.id=1 (early_data column)", "UPDATE C SET early_data = 'UPDATED_EARLY' WHERE id = 1")
	early.AddWait(15 * time.Second)
	early.AddUpdate("C", "Updating C.id=1 (early_data column)", "UPDATE C SET early_data = 'UPDATED_EARLY 2' WHERE id = 1")
	early.AddWait(5 * time.Second)

	// NONTX Flow (Reader)
	// Select B at 3, 6, 9, 12 seconds
	nontx := NewTxFlow("TX", db, logger, timeline)

	nontx.AddWait(5 * time.Second)
	//nontx.AddUpdate("B", "Update B (Sleep 10s)", "BEGIN UPDATE B SET data = 'NONTX_SLEEP' WHERE id = 1; DBMS_SESSION.SLEEP(1); END;")
	/*
		nontx.AddUpdate(
			"B",
			"Update B (No Sleep)",
			"UPDATE B SET data = 'NONTX_SLEEP' WHERE id = 2",
		)
	*/

	nontx.AddWait(3 * time.Second)
	nontx.AddQuery("B", "Read B (3s)", "SELECT id, data FROM B WHERE id = 1")
	nontx.AddWait(3 * time.Second)
	nontx.AddQuery("B", "Read B (6s)", "SELECT id, data FROM B WHERE id = 1")
	nontx.AddWait(3 * time.Second)
	nontx.AddQuery("B", "Read B (9s)", "SELECT id, data FROM B WHERE id = 1")
	nontx.AddWait(3 * time.Second)
	nontx.AddQuery("B", "Read B (12s)", "SELECT id, data FROM B WHERE id = 1")

	// Step 4: Launch two concurrent goroutines
	log.Println("Step 4: Launching CHAIN, EARLY, and NONTX goroutines...")
	var wg sync.WaitGroup
	wg.Add(3)

	// Goroutine 1: CHAIN flow
	go func() {
		defer wg.Done()
		if err := chain.Execute(ctx); err != nil {
			log.Printf("CHAIN flow error: %v", err)
		}
	}()

	// Goroutine 2: EARLY flow
	go func() {
		defer wg.Done()
		if err := early.Execute(ctx); err != nil {
			log.Printf("EARLY flow error: %v", err)
		}
	}()

	// Goroutine 3: NONTX flow
	go func() {
		defer wg.Done()
		if err := nontx.Execute(ctx); err != nil {
			log.Printf("NONTX flow error: %v", err)
		}
	}()

	// Wait for both to complete
	wg.Wait()
	log.Println("✓ Both flows completed")

	// Close logger to ensure all events are flushed to DB
	logger.Close()

	// Step 5: Display event log
	log.Println("\n=== Event Log (ordered by time) ===")
	if err := DisplayEventLog(ctx, db); err != nil {
		log.Printf("Failed to display event log: %v", err)
	}

	// Step 6: Display timeline graph
	timeline.RenderTimeline(!*hideExpected)

	// Step 7: Display final state of table C
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
