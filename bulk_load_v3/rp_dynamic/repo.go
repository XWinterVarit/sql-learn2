package rp_dynamic

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
)

// Repository defines the interface for database operations used by the bulk loader.
type Repository interface {
	// Truncate executes a TRUNCATE TABLE command.
	Truncate(ctx context.Context, tableName string) error

	// BulkInsert executes the bulk insert using the provided builder.
	BulkInsert(ctx context.Context, builder *BulkInsertBuilder) error

	// RefreshMaterializedView refreshes the MV_BULK_DATA materialized view.
	RefreshMaterializedView(ctx context.Context) (time.Duration, error)
}

// Repo implements the Repository interface.
type Repo struct {
	db *sqlx.DB
}

// NewRepo creates a new Repo instance.
func NewRepo(db *sqlx.DB) *Repo {
	return &Repo{db: db}
}

// Truncate executes a TRUNCATE TABLE command.
func (r *Repo) Truncate(ctx context.Context, tableName string) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", tableName)
	_, err := r.db.ExecContext(ctx, query)
	return err
}

// BulkInsert executes the bulk insert using the provided builder.
func (r *Repo) BulkInsert(ctx context.Context, builder *BulkInsertBuilder) error {
	query := builder.GetSQL()
	args := builder.GetArgs()
	_, err := r.db.ExecContext(ctx, query, args...)
	return err
}

// RefreshMaterializedView refreshes the MV_BULK_DATA materialized view.
func (r *Repo) RefreshMaterializedView(ctx context.Context) (time.Duration, error) {
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

	result, err := r.db.ExecContext(ctx, refreshSQL)
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
