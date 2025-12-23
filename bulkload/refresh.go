package bulkload

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
)

// refreshMaterializedView refreshes the MV_BULK_DATA materialized view.
func refreshMaterializedView(ctx context.Context, db *sqlx.DB) (time.Duration, error) {
	log.Println("Insert committed. Refreshing MV_BULK_DATA (COMPLETE, ATOMIC) ...")
	refreshStart := time.Now()

	refreshSQL := `
BEGIN
  DBMS_MVIEW.REFRESH(
    list           => 'MV_BULK_DATA',
    method         => 'C',
    atomic_refresh => TRUE
  );
END;`

	result, err := db.ExecContext(ctx, refreshSQL)
	if err != nil {
		return 0, fmt.Errorf("refresh materialized view failed: %w", err)
	}
	// Check if any rows were affected
	if result != nil {
		rowsAffected, _ := result.RowsAffected()
		log.Printf("Refresh result - rows affected: %d", rowsAffected)
	}

	log.Println("Refresh complete.")
	return time.Since(refreshStart), nil
}
