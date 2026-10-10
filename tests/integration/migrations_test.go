//go:build integration

package integration

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/bright2227/gogolook-task-system/internal/migrations"
)

func TestMigrationBootstrapIsIdempotentConcurrentAndTransactional(t *testing.T) {
	s := newPostgresStack(t, false)
	ctx, db := s.ctx, s.db

	// Two application instances boot a new schema at the same time. The
	// migration advisory lock must serialize the ledger and schema updates.
	start := make(chan struct{})
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errCh <- migrations.Apply(ctx, db)
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent migration bootstrap: %v", err)
		}
	}

	ledger := func() []string {
		t.Helper()
		rows, err := db.QueryContext(ctx, `SELECT filename || ':' || checksum FROM schema_migrations ORDER BY filename`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var entries []string
		for rows.Next() {
			var entry string
			if err := rows.Scan(&entry); err != nil {
				t.Fatal(err)
			}
			entries = append(entries, entry)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return entries
	}
	first := ledger()
	if len(first) != 1 {
		t.Fatalf("migration ledger entries=%d, want 1: %v", len(first), first)
	}
	for _, entry := range first {
		if len(entry) < 65 || entry[len(entry)-65] != ':' {
			t.Fatalf("ledger entry has missing checksum: %q", entry)
		}
	}
	if err := migrations.Apply(ctx, db); err != nil {
		t.Fatalf("idempotent migration apply: %v", err)
	}
	if second := ledger(); fmt.Sprint(second) != fmt.Sprint(first) {
		t.Fatalf("idempotent apply changed ledger: before=%v after=%v", first, second)
	}

	// A pre-existing tasks table makes the migration fail. The transaction must
	// roll back the ledger table created earlier in the same transaction.
	if _, err := db.ExecContext(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public; CREATE TABLE tasks (id uuid PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(ctx, db); err == nil {
		t.Fatal("incompatible schema unexpectedly migrated")
	}
	var schemaMigrations sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('public.schema_migrations')`).Scan(&schemaMigrations); err != nil {
		t.Fatal(err)
	}
	if schemaMigrations.Valid {
		t.Fatalf("failed migration left its ledger: %v", schemaMigrations)
	}
}
