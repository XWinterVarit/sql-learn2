package main

import (
	"flag"
	"os"
	"runtime"
	"strings"
	"time"
)

// Config holds all CLI/runtime options for the MV refresh monitor.
type Config struct {
	User        string
	Pass        string
	Host        string
	Port        string
	Service     string
	DSN         string
	Table       string
	Concurrency int
	Interval    time.Duration
	Preload     time.Duration
	Observe     time.Duration
	OutCSV      string
	SQLPath     string
	Client      string
	Quiet       bool
	BulkCount   int
}

// ParseConfig parses flags/env and returns a Config with defaults applied.
// It preserves the existing CLI (flags and defaults) for backward compatibility.
func ParseConfig() Config {
	user := flag.String("user", getenvDefault("ORA_USER", "LEARN1"), "Oracle username")
	pass := flag.String("pass", getenvDefault("ORA_PASS", "Welcome"), "Oracle password")
	host := flag.String("host", getenvDefault("ORA_HOST", "localhost"), "Oracle host")
	port := flag.String("port", getenvDefault("ORA_PORT", "1521"), "Oracle port")
	service := flag.String("service", getenvDefault("ORA_SERVICE", "XE"), "Oracle service (e.g., XE or XEPDB1)")
	dsn := flag.String("dsn", os.Getenv("ORA_DSN"), "Full DSN oracle://user:pass@host:port/service (optional)")
	table := flag.String("table", getenvDefault("MV_TABLE", "MV_BULK_DATA"), "Table or view to poll (expects CREATED_AT column)")
	concurrency := flag.Int("concurrency", intEnv("MV_CONCURRENCY", minInt(8, runtime.NumCPU()*2)), "Number of concurrent pollers")
	interval := flag.Duration("interval", durationEnv("MV_INTERVAL", 2*time.Millisecond), "Polling interval per goroutine")
	preload := flag.Duration("preload", durationEnv("MV_PRELOAD", 10*time.Second), "Warm-up duration before triggering bulk refresh script")
	observe := flag.Duration("observe", durationEnv("MV_OBSERVE", 200*time.Second), "Observation window after triggering the script")
	outCSV := flag.String("out", "", "Path to CSV output (default: logs/mv_monitor_YYYYmmdd_HHMMSS.csv)")
	sqlPath := flag.String("sql", getenvDefault("MV_SQL", "script_material_view_refresh/simulate_bulk_load_and_refresh.sql"), "Path to simulate_bulk_load_and_refresh.sql")
	client := flag.String("client", getenvDefault("ORACLE_CLI", "auto"), "Oracle CLI to use: auto|sql|sqlplus")
	quiet := flag.Bool("quiet", false, "Reduce per-interval logs; still prints summary")
	bulkCount := flag.Int("bulkcount", intEnv("MV_BULK_COUNT", 10000), "Number of rows to insert during bulk load simulation")
	flag.Parse()

	return Config{
		User:        *user,
		Pass:        *pass,
		Host:        *host,
		Port:        *port,
		Service:     *service,
		DSN:         *dsn,
		Table:       *table,
		Concurrency: *concurrency,
		Interval:    *interval,
		Preload:     *preload,
		Observe:     *observe,
		OutCSV:      *outCSV,
		SQLPath:     *sqlPath,
		Client:      *client,
		Quiet:       *quiet,
		BulkCount:   *bulkCount,
	}
}

// ResolveDSN returns the connection string to use with go-ora, honoring cfg.DSN if set.
func ResolveDSN(cfg Config) (string, error) {
	connString := strings.TrimSpace(cfg.DSN)
	if connString != "" {
		return connString, nil
	}
	if strings.TrimSpace(cfg.User) == "" || strings.TrimSpace(cfg.Pass) == "" {
		return "", ErrMissingCredentials
	}
	return "oracle://" + urlEncode(cfg.User) + ":" + urlEncode(cfg.Pass) + "@" + cfg.Host + ":" + cfg.Port + "/" + cfg.Service, nil
}

// ErrMissingCredentials is returned when user/pass are not provided and DSN is empty.
var ErrMissingCredentials = &configError{"username/password not provided; use flags or ORA_* envs"}

type configError struct{ msg string }

func (e *configError) Error() string { return e.msg }
