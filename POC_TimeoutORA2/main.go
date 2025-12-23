package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
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
	flag.Parse()

	// Build DSN
	// We add ENABLE_OOB=true to attempt Out-Of-Band interrupts (if supported).
	dsn := fmt.Sprintf("oracle://%s:%s@%s:%s/%s?ENABLE_OOB=true&TIMEOUT=3", *user, *pass, *host, *port, *service)

	conn, err := sql.Open("oracle", dsn)
	if err != nil {
		fmt.Println("can't open connection: ", err)
		return
	}
	defer func() {
		err = conn.Close()
		if err != nil {
			fmt.Println("can't close connection: ", err)
			return
		}
	}()
	t := time.Now()
	defer func() {
		fmt.Println("finish: ", time.Now().Sub(t))
	}()
	execCtx, execCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer execCancel()

	stmt, err := conn.PrepareContext(execCtx, "begin DBMS_SESSION.SLEEP(7); end;")
	if err != nil {
		fmt.Println("prepare error: ", err)
		return
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(execCtx)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
