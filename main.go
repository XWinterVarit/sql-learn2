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
	csvdbappend "sql-learn2/csvdb-append"
	"sql-learn2/partexchange"
	"sql-learn2/swapper"
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
	upsert := flag.Bool("upsert", false, "Use upsert mode: merge CSV rows into existing table")
	keys := flag.String("keys", strings.TrimSpace(os.Getenv("CSV_KEYS")), "Comma-separated key columns for upsert (e.g., ID,FIRST_NAME)")
	table := flag.String("table", strings.TrimSpace(os.Getenv("CSV_TABLE")), "Target table name. Defaults to CSV filename as table name.")
	sample := flag.String("sample", strings.TrimSpace(os.Getenv("CSV_SAMPLE")), "Quick preset for CSV: 'example' or 'append'. If set, overrides -csv.")

	// Synonym swap flags
	swapMode := flag.Bool("swap", false, "Run synonym-swap workflow: load CSV into inactive table, swap synonym, optionally truncate old active")
	baseName := flag.String("base", strings.TrimSpace(os.Getenv("SWAP_BASE")), "Base logical name (e.g., EXAMPLE). Physical tables are <BASE>_A and <BASE>_B; synonym defaults to <BASE>.")
	synonymName := flag.String("synonym", strings.TrimSpace(os.Getenv("SWAP_SYNONYM")), "Synonym name to repoint (defaults to base).")
	schema := flag.String("schema", strings.TrimSpace(os.Getenv("SWAP_SCHEMA")), "Owner/schema to qualify tables and synonym. Default: current schema.")
	cleanup := flag.Bool("cleanup", true, "After swap, TRUNCATE the old active table")
	validate := flag.Bool("validate", false, "Before swap, log row counts of active/inactive tables")

	// Partition exchange flags
	pexchange := flag.Bool("pexchange", false, "Run partition-exchange workflow: load CSV into staging, exchange partition into master, then cleanup")
	masterTable := flag.String("master", strings.TrimSpace(os.Getenv("PEX_MASTER")), "Partitioned master table name")
	stagingTable := flag.String("staging", strings.TrimSpace(os.Getenv("PEX_STAGING")), "Staging table name used for exchange")
	partitionName := flag.String("partition", strings.TrimSpace(os.Getenv("PEX_PARTITION")), "Partition name in the master to exchange")
	noValidate := flag.Bool("no-validate", true, "Use WITHOUT VALIDATION during exchange (assumes compatibility)")
	includeIdx := flag.Bool("include-indexes", false, "Use INCLUDING INDEXES during exchange")
	cleanupStaging := flag.Bool("cleanup-staging", true, "After exchange, TRUNCATE staging to remove old data")

	flag.Parse()

	// Apply sample preset for quick switching between CSVs
	switch strings.ToLower(strings.TrimSpace(*sample)) {
	case "example":
		*csvPath = "example.csv"
		log.Printf("Preset: sample=example -> CSV %s", *csvPath)
	case "append":
		*csvPath = "example_append.csv"
		log.Printf("Preset: sample=append -> CSV %s", *csvPath)
		// For convenience in append tests: if user chose upsert but didn't provide table/keys, set sensible defaults
		if *upsert && strings.TrimSpace(*table) == "" {
			*table = normalizeIdentifierForOracle("example") // upsert into EXAMPLE
			log.Printf("Preset default: -table set to %s (override with -table)", *table)
		}
		if *upsert && strings.TrimSpace(*keys) == "" {
			*keys = "ID,FIRST_NAME"
			log.Printf("Preset default: -keys set to %s (override with -keys)", *keys)
		}
	case "":
		// no preset used
	default:
		log.Fatalf("invalid -sample value: %s (use 'example' or 'append')", *sample)
	}

	totalSteps := 6
	step(1, totalSteps, "Resolve connection DSN")
	// Resolve DSN
	connString := *dsn
	if connString == "" {
		if *user == "" || *pass == "" {
			log.Fatalf("username/password must be provided via flags or env (ORA_USER/ORA_PASS)")
		}
		connString = fmt.Sprintf("oracle://%s:%s@%s:%s/%s", urlEncode(*user), urlEncode(*pass), *host, *port, *service)
	}

	step(2, totalSteps, "Connect to Oracle")
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
	log.Printf("Connected: %s", redacted(connString))

	step(3, totalSteps, "Prepare CSV path")
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

	// If running partition-exchange workflow, do it now and exit
	if *pexchange {
		step(4, totalSteps, "Run partition-exchange workflow")
		if strings.TrimSpace(*masterTable) == "" || strings.TrimSpace(*stagingTable) == "" || strings.TrimSpace(*partitionName) == "" {
			log.Fatalf("pexchange requires -master, -staging, -partition")
		}
		opt := partexchange.Options{
			MasterTable:       strings.TrimSpace(*masterTable),
			StagingTable:      strings.TrimSpace(*stagingTable),
			PartitionName:     strings.TrimSpace(*partitionName),
			CSVPath:           absCSV,
			Schema:            strings.TrimSpace(*schema),
			DropOldData:       *cleanupStaging,
			WithoutValidation: *noValidate,
			IncludingIndexes:  *includeIdx,
		}
		if err := partexchange.Run(ctx, db, opt); err != nil {
			log.Fatalf("partition-exchange failed: %v", err)
		}
		log.Printf("Partition exchange completed for master %s, partition %s using staging %s", strings.TrimSpace(*masterTable), strings.TrimSpace(*partitionName), strings.TrimSpace(*stagingTable))
		return
	}

	// If running synonym swap workflow, do it now and exit
	if *swapMode {
		step(4, totalSteps, "Run synonym-swap workflow")
		base := strings.TrimSpace(*baseName)
		if base == "" {
			base = normalizeIdentifierForOracle(strings.TrimSuffix(filepath.Base(absCSV), filepath.Ext(absCSV)))
		}
		opt := swapper.Options{
			BaseName:      base,
			SynonymName:   strings.TrimSpace(*synonymName),
			CSVPath:       absCSV,
			ValidateCount: *validate,
			DropOldData:   *cleanup,
			Schema:        strings.TrimSpace(*schema),
		}
		if err := swapper.Run(ctx, db, opt); err != nil {
			log.Fatalf("swap failed: %v", err)
		}
		log.Printf("Swap complete for base %s using CSV %s", base, absCSV)
		return
	}

	step(4, totalSteps, "Determine target table name")
	// Determine target table name
	tableName := normalizeIdentifierForOracle(strings.TrimSuffix(filepath.Base(absCSV), filepath.Ext(absCSV)))
	if strings.TrimSpace(*table) != "" {
		tableName = normalizeIdentifierForOracle(*table)
	}

	step(5, totalSteps, "Run operation")
	if *upsert {
		// Parse key columns
		kstr := strings.TrimSpace(*keys)
		if kstr == "" {
			log.Fatalf("upsert mode requires -keys (comma-separated key columns)")
		}
		parts := strings.Split(kstr, ",")
		keyCols := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				keyCols = append(keyCols, p)
			}
		}
		if len(keyCols) == 0 {
			log.Fatalf("no valid key columns parsed from -keys")
		}
		log.Printf("Summary: UPSERT into %s using keys [%s] from %s", tableName, strings.Join(keyCols, ", "), absCSV)
		if err := csvdbappend.UpsertCSVToDB(ctx, db, absCSV, tableName, keyCols); err != nil {
			log.Fatalf("upsert csv: %v", err)
		}
	} else {
		log.Printf("Summary: LOAD into %s from %s", tableName, absCSV)
		if err := csvdb.LoadCSVToDBAs(ctx, db, absCSV, tableName); err != nil {
			log.Fatalf("load csv: %v", err)
		}
	}

	step(6, totalSteps, "Verify row count")
	// Verify by counting rows
	var cnt int64
	qry := fmt.Sprintf("SELECT COUNT(1) FROM %s", tableName)
	if err := db.QueryRowContext(ctx, qry).Scan(&cnt); err != nil {
		log.Printf("verify count failed: %v", err)
	} else {
		mode := "Loaded"
		if *upsert {
			mode = "Upserted/Inserted"
		}
		log.Printf("%s rows into table %s (total now: %d)", mode, tableName, cnt)
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

func step(n, total int, title string) {
	log.Printf("[%d/%d] %s", n, total, title)
}
