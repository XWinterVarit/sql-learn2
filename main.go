package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/sijms/go-ora/v2"

	"sql-learn2/csvdb"
)

func main() {
	// Flags and environment
	csvPath := flag.String("csv", defaultString(os.Getenv("CSV_PATH"), "example.csv"), "Path to CSV file to load")
	user := flag.String("user", defaultString(os.Getenv("ORA_USER"), "LEARN1"), "Oracle username")
	pass := flag.String("pass", defaultString(os.Getenv("ORA_PASS"), "Welcome"), "Oracle password")
	host := flag.String("host", defaultString(os.Getenv("ORA_HOST"), "localhost"), "Oracle host")
	port := flag.String("port", defaultString(os.Getenv("ORA_PORT"), "1521"), "Oracle port")
	service := flag.String("service", defaultString(os.Getenv("ORA_SERVICE"), "XE"), "Oracle service name (e.g., XE or XEPDB1)")
	dsn := flag.String("dsn", os.Getenv("ORA_DSN"), "Oracle DSN (oracle://user:pass@host:port/service). If set, overrides other connection flags.")
	timeout := flag.Duration("timeout", parseDurationEnv("ORA_TIMEOUT", 60*time.Second), "Context timeout for operations")
	flag.Parse()

	// Resolve DSN
	connString := *dsn
	if connString == "" {
		if *user == "" || *pass == "" {
			log.Fatalf("username/password must be provided via flags or env (ORA_USER/ORA_PASS)")
		}
		connString = fmt.Sprintf("oracle://%s:%s@%s:%s/%s", urlEncode(*user), urlEncode(*pass), *host, *port, *service)
	}

	// Open DB
	db, err := sql.Open("oracle", connString)
	if err != nil {
		log.Fatalf("open oracle: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("ping oracle: %v", err)
	}
	log.Printf("Connected to Oracle: %s", redacted(connString))

	// Load CSV
	absCSV := *csvPath
	if !filepath.IsAbs(absCSV) {
		if wd, _ := os.Getwd(); wd != "" {
			absCSV = filepath.Join(wd, absCSV)
		}
	}
	if _, err := os.Stat(absCSV); err != nil {
		log.Fatalf("csv not accessible: %v", err)
	}

	if err := csvdb.LoadCSVToDB(ctx, db, absCSV); err != nil {
		log.Fatalf("load csv: %v", err)
	}

	// Verify by counting rows
	tableName := normalizeIdentifierForOracle(strings.TrimSuffix(filepath.Base(absCSV), filepath.Ext(absCSV)))
	var cnt int64
	qry := fmt.Sprintf("SELECT COUNT(1) FROM %s", tableName)
	if err := db.QueryRowContext(ctx, qry).Scan(&cnt); err != nil {
		log.Printf("verify count failed: %v", err)
	} else {
		log.Printf("Loaded %d rows into table %s", cnt, tableName)
	}
}

func defaultString(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

func parseDurationEnv(env string, def time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(env)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func urlEncode(s string) string {
	// Minimal encoding for special characters in user/pass; avoid pulling net/url just for this.
	replacer := strings.NewReplacer("@", "%40", ":", "%3A", "/", "%2F", "?", "%3F", "#", "%23", " ", "%20")
	return replacer.Replace(s)
}

func redacted(dsn string) string {
	// Hide password in logs
	if i := strings.Index(dsn, "://"); i >= 0 {
		rest := dsn[i+3:]
		if j := strings.Index(rest, "@"); j >= 0 {
			// user:pass@...
			cred := rest[:j]
			if k := strings.Index(cred, ":"); k >= 0 {
				cred = cred[:k] + ":***"
			}
			return dsn[:i+3] + cred + rest[j:]
		}
	}
	return dsn
}

// normalizeIdentifierForOracle mirrors the logic in csvdb for deriving table names/columns.
func normalizeIdentifierForOracle(s string) string {
	if s == "" {
		return ""
	}
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, " ", "_")
	b := make([]rune, 0, len(s))
	for _, r := range s {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b = append(b, r)
		} else {
			b = append(b, '_')
		}
	}
	upper := strings.ToUpper(string(b))
	if len(upper) == 0 {
		return ""
	}
	if !(upper[0] >= 'A' && upper[0] <= 'Z') {
		upper = "X" + upper
	}
	if len(upper) > 30 {
		upper = upper[:30]
	}
	return upper
}
