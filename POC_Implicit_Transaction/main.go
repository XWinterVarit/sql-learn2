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
	// Flags
	setup := flag.Bool("setup", false, "Create table and insert 1,000,000 rows")
	test := flag.Bool("test", false, "Run update with timeout to demonstrate implicit transaction")
	timeout := flag.Duration("timeout", 100000*time.Millisecond, "Timeout for update operation in test mode")

	// Connection flags
	user := flag.String("user", getEnv("ORA_USER", "LEARN1"), "Oracle username")
	pass := flag.String("pass", getEnv("ORA_PASS", "Welcome"), "Oracle password")
	host := flag.String("host", getEnv("ORA_HOST", "localhost"), "Oracle host")
	port := flag.String("port", getEnv("ORA_PORT", "1521"), "Oracle port")
	service := flag.String("service", getEnv("ORA_SERVICE", "XE"), "Oracle service name")

	flag.Parse()

	dsn := fmt.Sprintf("oracle://%s:%s@%s:%s/%s", *user, *pass, *host, *port, *service)
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("Connected to Oracle.")

	if *setup {
		runSetup(db)
	} else if *test {
		runTest(db, *timeout)
	} else {
		fmt.Println("Please specify -setup or -test")
		flag.Usage()
	}
}

func runSetup(db *sql.DB) {
	log.Println("Running setup...")

	// Create table if not exists
	_, err := db.Exec(`
		DECLARE
			e exception;
			pragma exception_init(e, -955); -- ORA-00955: name is already used by an existing object
		BEGIN
			EXECUTE IMMEDIATE 'CREATE TABLE Implicit (id NUMBER PRIMARY KEY, updated_at DATE)';
		EXCEPTION
			WHEN e THEN NULL;
		END;
	`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	log.Println("Table Implicit checked/created.")

	// Check count
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM Implicit").Scan(&count)
	if err != nil {
		log.Fatalf("Failed to count rows: %v", err)
	}

	if count == 1000000 {
		log.Println("Table already has 1,000,000 rows. Resetting dates...")
		_, err = db.Exec("UPDATE Implicit SET updated_at = TO_DATE('2000-01-01', 'YYYY-MM-DD')")
		if err != nil {
			log.Fatalf("Failed to reset dates: %v", err)
		}
		log.Println("Dates reset to 2000-01-01.")
		return
	}

	log.Printf("Table has %d rows. Truncating and inserting 1,000,000 rows... (this may take 10-30 seconds)", count)
	// Truncate
	_, err = db.Exec("TRUNCATE TABLE Implicit")
	if err != nil {
		log.Fatalf("Failed to truncate: %v", err)
	}

	// Bulk insert using PL/SQL
	_, err = db.Exec(`
		BEGIN
			FOR i IN 1..1000000 LOOP
				INSERT INTO Implicit (id, updated_at) VALUES (i, TO_DATE('2000-01-01', 'YYYY-MM-DD'));
			END LOOP;
			COMMIT;
		END;
	`)
	if err != nil {
		log.Fatalf("Failed to insert rows: %v", err)
	}
	log.Println("Inserted 1,000,000 rows with date 2000-01-01.")
}

func runTest(db *sql.DB, timeout time.Duration) {

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	log.Println("Starting UPDATE Implicit SET updated_at = SYSDATE ...")
	start := time.Now()

	// Run update without explicit transaction
	_, err := db.ExecContext(ctx, "UPDATE Implicit SET updated_at = SYSDATE")
	duration := time.Since(start)

	if err == nil {
		log.Printf("Update finished successfully in %v. (Timeout was too long?)", duration)
	} else {
		log.Printf("Update failed as expected (or error occurred): %v", err)
		log.Printf("Operation duration: %v", duration)
	}

	// Verify results
	var updatedCount int
	err = db.QueryRow("SELECT COUNT(*) FROM Implicit WHERE updated_at > TO_DATE('2000-01-01', 'YYYY-MM-DD')").Scan(&updatedCount)
	if err != nil {
		log.Fatalf("Failed to query rows: %v", err)
	}

	log.Printf("Rows updated: %d / 1,000,000", updatedCount)

	if updatedCount == 0 {
		log.Println("RESULT: SUCCESS. No rows were updated.")
		log.Println("Conclusion: The implicit transaction was successfully rolled back upon timeout/cancellation.")
	} else if updatedCount == 1000000 {
		log.Println("RESULT: FAILURE (for test purpose). All rows were updated.")
		log.Println("Conclusion: The operation finished before the timeout.")
	} else {
		log.Printf("RESULT: UNEXPECTED. %d rows updated.", updatedCount)
		log.Println("Conclusion: Partial update occurred? (Should not happen in atomic transaction).")
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
