package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed *.sql
var files embed.FS

const advisoryLockKey int64 = 0x676f676f6c6f6f6b

type migration struct {
	name     string
	body     []byte
	checksum string
}

// Apply runs embedded SQL migrations in filename order. One transaction lock
// serializes startup across replicas, and each script plus its ledger entry
// commits atomically with the complete pending set.
func Apply(ctx context.Context, db *sql.DB) error {
	all, err := loadMigrations()
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, advisoryLockKey); err != nil {
		return fmt.Errorf("lock schema migrations: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		filename text PRIMARY KEY,
		checksum text NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("create migration ledger: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `SELECT filename,checksum FROM schema_migrations ORDER BY filename`)
	if err != nil {
		return fmt.Errorf("read migration ledger: %w", err)
	}
	applied := make(map[string]string)
	for rows.Next() {
		var name, checksum string
		if err = rows.Scan(&name, &checksum); err != nil {
			rows.Close()
			return fmt.Errorf("read migration ledger: %w", err)
		}
		applied[name] = checksum
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("read migration ledger: %w", err)
	}
	if err = rows.Close(); err != nil {
		return fmt.Errorf("close migration ledger: %w", err)
	}

	known := make(map[string]bool, len(all))
	for _, m := range all {
		known[m.name] = true
	}
	for name := range applied {
		if !known[name] {
			return fmt.Errorf("unknown migration in ledger: %s", name)
		}
	}
	seenPending := false
	for _, m := range all {
		checksum, ok := applied[m.name]
		if !ok {
			seenPending = true
			continue
		}
		if seenPending {
			return fmt.Errorf("migration ledger has a gap before %s", m.name)
		}
		if checksum != m.checksum {
			return fmt.Errorf("migration checksum mismatch for %s", m.name)
		}
	}
	for _, m := range all {
		if _, ok := applied[m.name]; ok {
			continue
		}
		body, stripErr := stripTransaction(m.body)
		if stripErr != nil {
			return fmt.Errorf("prepare migration %s: %w", m.name, stripErr)
		}
		if _, err = tx.ExecContext(ctx, string(body)); err != nil {
			return fmt.Errorf("apply migration %s: %w", m.name, err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO schema_migrations(filename,checksum) VALUES($1,$2)`, m.name, m.checksum); err != nil {
			return fmt.Errorf("record migration %s: %w", m.name, err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit schema migrations: %w", err)
	}
	return nil
}

func loadMigrations() ([]migration, error) {
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	all := make([]migration, 0, len(names))
	for _, name := range names {
		body, readErr := files.ReadFile(name)
		if readErr != nil {
			return nil, readErr
		}
		sum := sha256.Sum256(body)
		all = append(all, migration{name: name, body: body, checksum: hex.EncodeToString(sum[:])})
	}
	return all, nil
}

func stripTransaction(script []byte) ([]byte, error) {
	s := strings.TrimSpace(string(script))
	upper := strings.ToUpper(s)
	starts := strings.HasPrefix(upper, "BEGIN;")
	ends := strings.HasSuffix(upper, "COMMIT;")
	if starts != ends {
		return nil, errors.New("migration must have matching BEGIN and COMMIT wrappers")
	}
	if !starts {
		return []byte(s), nil
	}
	s = strings.TrimSpace(s[len("BEGIN;"):])
	s = strings.TrimSpace(s[:len(s)-len("COMMIT;")])
	if s == "" {
		return nil, errors.New("migration body is empty")
	}
	return []byte(s), nil
}
