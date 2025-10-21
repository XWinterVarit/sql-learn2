package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// getenvDefault returns the environment variable value or a default if empty/whitespace.
func getenvDefault(k, def string) string {
	v := os.Getenv(k)
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

// durationEnv parses a duration from environment (e.g., "250ms", "3s"). Falls back to def on error/empty.
func durationEnv(k string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

// intEnv parses an integer from environment. Falls back to def on error/empty.
func intEnv(k string, def int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	var n int
	_, err := fmt.Sscanf(v, "%d", &n)
	if err != nil {
		return def
	}
	return n
}

// minInt returns the smaller of two ints.
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// urlEncode performs minimal URL-encoding for DSN components.
func urlEncode(s string) string {
	return strings.NewReplacer("@", "%40", ":", "%3A", "/", "%2F").Replace(s)
}
