package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
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

	// Step 2: Initialize Runner
	runner := NewRunner(db)
	defer runner.Close()

	// Step 3: Define Flows
	log.Println("Step 3: Defining Flows...")

	// CHAIN Flow
	// 1. Lock A
	// 2. Update B (Wait 4s inside)
	// 3. Update C (Wait 2s inside)
	// 4. Wait 2s inside -> Commit
	chain := runner.AddTxFlow("CHAIN").SetTxTimeout(5 * time.Second)
	chain.AddQuery("A", "Locked A.id=1", "SELECT id FROM A WHERE id = 1 FOR UPDATE")
	chain.AddUpdate("B", "Updating B.id=1", "BEGIN DBMS_SESSION.SLEEP(4); UPDATE B SET data = 'B1_UPDATED_BY_CHAIN' WHERE id = 1; END;")
	chain.AddUpdate("C", "Updating C.id=1", "BEGIN DBMS_SESSION.SLEEP(2); UPDATE C SET chain_data = 'UPDATED_BY_CHAIN' WHERE id = 1; END;")
	chain.AddUpdate("SLEEP", "Wait 2s", "BEGIN DBMS_SESSION.SLEEP(2); END;")

	// EARLY Flow
	// 1. Update B (Wait 2s inside)
	// 2. Update C
	// 3. Update C (Wait 15s inside)
	// 4. Wait 5s inside -> Commit
	early := runner.AddTxFlow("EARLY").SetTxTimeout(7 * time.Second)
	early.AddUpdate("B", "Updating B.id=1 (data column)", "BEGIN DBMS_SESSION.SLEEP(2); UPDATE B SET data = 'UPDATED_EARLY' WHERE id = 1; END;")
	early.AddUpdate("C", "Updating C.id=1 (early_data column)", "UPDATE C SET early_data = 'UPDATED_EARLY' WHERE id = 1")
	early.AddUpdate("C", "Updating C.id=1 (early_data column)", "BEGIN DBMS_SESSION.SLEEP(15); UPDATE C SET early_data = 'UPDATED_EARLY 2' WHERE id = 1; END;")
	early.AddUpdate("SLEEP", "Wait 5s", "BEGIN DBMS_SESSION.SLEEP(5); END;")

	// NONTX Flow (Reader)
	// Select B at 3, 6, 9, 12 seconds
	/*
		nontx := runner.AddNonTxFlow("TX")

		nontx.AddWait(5 * time.Second)
		//nontx.AddUpdate("B", "Update B (Sleep 10s)", "BEGIN UPDATE B SET data = 'NONTX_SLEEP' WHERE id = 1; DBMS_SESSION.SLEEP(1); END;")

			nontx.AddUpdate(
				"B",
				"Update B (No Sleep)",
				"UPDATE B SET data = 'NONTX_SLEEP' WHERE id = 2",
			)


		nontx.AddWait(3 * time.Second)
		nontx.AddQuery("B", "Read B (3s)", "SELECT id, data FROM B WHERE id = 1")
		nontx.AddWait(3 * time.Second)
		nontx.AddQuery("B", "Read B (6s)", "SELECT id, data FROM B WHERE id = 1")
		nontx.AddWait(3 * time.Second)
		nontx.AddQuery("B", "Read B (9s)", "SELECT id, data FROM B WHERE id = 1")
		nontx.AddWait(3 * time.Second)
		nontx.AddQuery("B", "Read B (12s)", "SELECT id, data FROM B WHERE id = 1")
	*/

	// Step 4: Run All Flows
	runner.RunAll(ctx)

	// Step 5: Report Results
	runner.Report(ctx, !*hideExpected)

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
