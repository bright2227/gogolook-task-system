package configs

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/migrations"
	_ "github.com/lib/pq"
)

type PostgresResourcesManager struct {
	DB          *sql.DB
	cfg         Config
	mu          sync.Mutex
	initialized bool
}

func NewPostgresResourcesManager(cfg Config) *PostgresResourcesManager {
	return &PostgresResourcesManager{cfg: cfg}
}

func (r *PostgresResourcesManager) Initialization(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.initialized {
		return nil
	}
	db, err := OpenPostgres(ctx, r.cfg.DatabaseURL, r.cfg.DBMaxOpenConns, r.cfg.DBMaxIdleConns)
	if err != nil {
		return fmt.Errorf("initialize PostgreSQL: %w", err)
	}
	if err := migrations.Apply(ctx, db); err != nil {
		_ = db.Close()
		return fmt.Errorf("initialize schema: %w", err)
	}
	r.DB = db
	r.initialized = true
	return nil
}

func (r *PostgresResourcesManager) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.DB == nil {
		return nil
	}
	err := r.DB.Close()
	r.initialized = false
	return err
}

func OpenPostgres(ctx context.Context, url string, maxOpen, maxIdle int) (*sql.DB, error) {
	if maxOpen < 1 || maxIdle < 0 || maxIdle > maxOpen {
		return nil, fmt.Errorf("invalid PostgreSQL pool limits")
	}
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}
