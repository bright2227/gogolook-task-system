package migrations

import (
	"bytes"
	"strings"
	"testing"
)

func TestEmbeddedMigrationsAreOrderedAndTransactionalWrappersCanBeStripped(t *testing.T) {
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) == 0 {
		t.Fatal("no migrations embedded")
	}
	for i, migration := range migrations {
		if i > 0 && migrations[i-1].name >= migration.name {
			t.Fatalf("migrations are not ordered: %q then %q", migrations[i-1].name, migration.name)
		}
		if len(migration.checksum) != 64 {
			t.Fatalf("bad checksum for %s: %s", migration.name, migration.checksum)
		}
		body, err := stripTransaction(migration.body)
		if err != nil {
			t.Fatalf("strip %s: %v", migration.name, err)
		}
		if len(body) == 0 || bytes.Contains(bytes.ToUpper(body), []byte("BEGIN;")) || bytes.Contains(bytes.ToUpper(body), []byte("COMMIT;")) {
			t.Fatalf("transaction wrapper remained in %s: %s", migration.name, body)
		}
	}
}

func TestStripTransactionRejectsUnmatchedWrapper(t *testing.T) {
	if _, err := stripTransaction([]byte("BEGIN; SELECT 1;")); err == nil {
		t.Fatal("expected unpaired transaction wrapper error")
	}
	if _, err := stripTransaction([]byte("BEGIN; COMMIT;")); err == nil || !strings.Contains(err.Error(), "body is empty") {
		t.Fatalf("empty transaction error=%v", err)
	}
}
