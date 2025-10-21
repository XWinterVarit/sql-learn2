package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// RunSimulateScript invokes the simulate_bulk_load_and_refresh.sql using SQLcl or SQL*Plus.
// client: "auto" (try sql then sqlplus), "sql" (SQLcl), or "sqlplus".
func RunSimulateScript(client, user, pass, host, port, service, sqlPath string) error {
	if _, err := os.Stat(sqlPath); err != nil {
		return fmt.Errorf("sql file not found: %s (%v)", sqlPath, err)
	}
	conn := fmt.Sprintf("%s/%s@%s:%s/%s", user, pass, host, port, service)
	var cmd *exec.Cmd
	switch strings.ToLower(strings.TrimSpace(client)) {
	case "sql":
		cmd = exec.Command("sql", "-S", conn, "@"+sqlPath)
	case "sqlplus":
		cmd = exec.Command("sqlplus", "-s", conn, "@"+sqlPath)
	default:
		// auto: try sql then sqlplus
		if hasCmd("sql") {
			cmd = exec.Command("sql", "-S", conn, "@"+sqlPath)
		} else if hasCmd("sqlplus") {
			cmd = exec.Command("sqlplus", "-s", conn, "@"+sqlPath)
		} else {
			return errors.New("neither SQLcl (sql) nor SQL*Plus (sqlplus) found in PATH")
		}
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// hasCmd returns true if the given executable is found in PATH.
func hasCmd(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}
