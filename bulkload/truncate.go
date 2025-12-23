package bulkload

import (
	"context"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
)

// truncateTable truncates the BULK_DATA table.
func truncateTable(ctx context.Context, db *sqlx.DB) error {
	log.Println("Truncating BULK_DATA ...")
	_, err := db.ExecContext(ctx, "TRUNCATE TABLE BULK_DATA")
	if err != nil {
		return fmt.Errorf("truncate failed: %w", err)
	}
	return nil
}
