package mountjournal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var journalSchemaV1 = []string{
	`CREATE TABLE IF NOT EXISTS journal_meta (
 key TEXT PRIMARY KEY,
 value TEXT NOT NULL
);`,
	`CREATE TABLE IF NOT EXISTS operations (
 id TEXT PRIMARY KEY,
 kind TEXT NOT NULL,
 entry_id TEXT NOT NULL DEFAULT '',
 parent_id TEXT NOT NULL DEFAULT '',
 base_version_id TEXT NOT NULL DEFAULT '',
 new_size INTEGER NOT NULL,
 mtime INTEGER NOT NULL DEFAULT 0,
 executable INTEGER NOT NULL DEFAULT 0,
 expected_entry_revision INTEGER NOT NULL DEFAULT 0,
 expected_parent_revision INTEGER NOT NULL DEFAULT 0,
 state TEXT NOT NULL,
 last_error TEXT NOT NULL DEFAULT '',
 request_fingerprint TEXT NOT NULL,
 created_at INTEGER NOT NULL,
 updated_at INTEGER NOT NULL,
 CHECK (kind IN ('create','mkdir','write','rename','move','trash','restore','metadata')),
 CHECK (state IN ('pending','local_durable','uploading','replica_pending','remote_committed','conflicted','blocked_auth','recovery','abandoned')),
 CHECK (new_size >= 0),
 CHECK (executable IN (0,1))
);`,
	`CREATE TABLE IF NOT EXISTS operation_dependencies (
 operation_id TEXT NOT NULL REFERENCES operations(id) ON DELETE RESTRICT,
 dependency_id TEXT NOT NULL REFERENCES operations(id) ON DELETE RESTRICT,
 created_at INTEGER NOT NULL,
 PRIMARY KEY (operation_id, dependency_id),
 CHECK (operation_id <> dependency_id)
);`,
	`CREATE TABLE IF NOT EXISTS segments (
 id TEXT PRIMARY KEY,
 file_name TEXT NOT NULL UNIQUE,
 state TEXT NOT NULL,
 observed_size INTEGER NOT NULL DEFAULT 0,
 created_at INTEGER NOT NULL,
 updated_at INTEGER NOT NULL,
 CHECK (state IN ('active','sealed','corrupt','missing'))
);`,
	`CREATE TABLE IF NOT EXISTS extents (
 id TEXT PRIMARY KEY,
 operation_id TEXT NOT NULL REFERENCES operations(id) ON DELETE RESTRICT,
 sequence INTEGER NOT NULL,
 kind TEXT NOT NULL,
 logical_offset INTEGER NOT NULL,
 length INTEGER NOT NULL,
 segment_id TEXT REFERENCES segments(id) ON DELETE RESTRICT,
 record_offset INTEGER NOT NULL DEFAULT 0,
 record_length INTEGER NOT NULL DEFAULT 0,
 payload_offset INTEGER NOT NULL DEFAULT 0,
 crc32 INTEGER NOT NULL DEFAULT 0,
 sha256 TEXT NOT NULL DEFAULT '',
 created_at INTEGER NOT NULL,
 UNIQUE (operation_id, sequence),
 UNIQUE (segment_id, record_offset),
 CHECK (kind IN ('data','zero','hole')),
 CHECK (logical_offset >= 0),
 CHECK (length > 0)
);`,
	`CREATE INDEX IF NOT EXISTS idx_extents_operation_sequence ON extents(operation_id, sequence);`,
	`CREATE TABLE IF NOT EXISTS recovery_records (
 id TEXT PRIMARY KEY,
 extent_id TEXT NOT NULL DEFAULT '',
 operation_id TEXT NOT NULL DEFAULT '',
 segment_id TEXT NOT NULL DEFAULT '',
 kind TEXT NOT NULL,
 record_offset INTEGER NOT NULL DEFAULT 0,
 available_bytes INTEGER NOT NULL DEFAULT 0,
 payload_offset INTEGER NOT NULL DEFAULT 0,
 payload_length INTEGER NOT NULL DEFAULT 0,
 logical_offset INTEGER NOT NULL DEFAULT 0,
 crc32 INTEGER NOT NULL DEFAULT 0,
 sha256 TEXT NOT NULL DEFAULT '',
 detail TEXT NOT NULL DEFAULT '',
 created_at INTEGER NOT NULL,
 updated_at INTEGER NOT NULL,
 CHECK (kind IN ('orphan','incomplete','corrupt','missing','mismatch','restored'))
);`,
	`CREATE INDEX IF NOT EXISTS idx_recovery_operation ON recovery_records(operation_id, kind, created_at);`,
	`CREATE INDEX IF NOT EXISTS idx_recovery_extent ON recovery_records(extent_id, kind);`,
	`CREATE TABLE IF NOT EXISTS journal_events (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 operation_id TEXT NOT NULL DEFAULT '',
 action TEXT NOT NULL,
 result TEXT NOT NULL,
 detail TEXT NOT NULL DEFAULT '',
 created_at INTEGER NOT NULL
);`,
}

var journalMigrationV2 = []string{
	`ALTER TABLE operations ADD COLUMN metadata_revision INTEGER NOT NULL DEFAULT 1;`,
	`CREATE TABLE operation_revisions (
 operation_id TEXT NOT NULL REFERENCES operations(id) ON DELETE RESTRICT,
 revision INTEGER NOT NULL,
 update_id TEXT NOT NULL,
 new_size INTEGER NOT NULL,
 mtime INTEGER NOT NULL,
 executable INTEGER NOT NULL,
 created_at INTEGER NOT NULL,
 PRIMARY KEY (operation_id, revision),
 UNIQUE (operation_id, update_id),
 CHECK (revision > 0),
 CHECK (new_size >= 0),
 CHECK (executable IN (0,1))
);`,
	`INSERT INTO operation_revisions (operation_id, revision, update_id, new_size, mtime, executable, created_at)
 SELECT id, 1, 'initial.' || id, new_size, mtime, executable, created_at FROM operations;`,
}

func initializeSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return errors.New("journal database is unavailable")
	}
	for _, statement := range []string{
		`PRAGMA journal_mode=WAL;`,
		`PRAGMA synchronous=FULL;`,
		`PRAGMA foreign_keys=ON;`,
		`PRAGMA busy_timeout=5000;`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("configure journal SQLite: %w", err)
		}
	}
	for _, statement := range journalSchemaV1 {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("initialize journal schema: %w", err)
		}
	}
	var versionValue string
	err := db.QueryRowContext(ctx, `SELECT value FROM journal_meta WHERE key = 'schema_version';`).Scan(&versionValue)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		versionValue = "1"
		if _, err := db.ExecContext(ctx, `INSERT INTO journal_meta (key, value) VALUES ('schema_version', '1');`); err != nil {
			return fmt.Errorf("record journal schema version: %w", err)
		}
	case err != nil:
		return fmt.Errorf("read journal schema version: %w", err)
	default:
	}
	version, parseErr := strconv.Atoi(strings.TrimSpace(versionValue))
	if parseErr != nil || version < 1 || version > SchemaVersion {
		return ErrJournalUpgradeRequired
	}
	if version < 2 {
		if err := migrateJournalV2(ctx, db); err != nil {
			return err
		}
		version = 2
	}
	if version != SchemaVersion {
		return ErrJournalUpgradeRequired
	}
	return verifySQLiteDurability(ctx, db)
}

func migrateJournalV2(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, statement := range journalMigrationV2 {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate journal schema to v2: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE journal_meta SET value = '2' WHERE key = 'schema_version';`); err != nil {
		return fmt.Errorf("record journal schema v2: %w", err)
	}
	return tx.Commit()
}

func verifySQLiteDurability(ctx context.Context, db *sql.DB) error {
	var journalMode string
	if err := db.QueryRowContext(ctx, `PRAGMA journal_mode;`).Scan(&journalMode); err != nil || !strings.EqualFold(strings.TrimSpace(journalMode), "wal") {
		return fmt.Errorf("journal SQLite WAL mode unavailable: mode=%q err=%w", journalMode, err)
	}
	var synchronous int
	if err := db.QueryRowContext(ctx, `PRAGMA synchronous;`).Scan(&synchronous); err != nil || synchronous != 2 {
		return fmt.Errorf("journal SQLite FULL synchronous mode unavailable: value=%d err=%w", synchronous, err)
	}
	var foreignKeys int
	if err := db.QueryRowContext(ctx, `PRAGMA foreign_keys;`).Scan(&foreignKeys); err != nil || foreignKeys != 1 {
		return fmt.Errorf("journal SQLite foreign keys unavailable: value=%d err=%w", foreignKeys, err)
	}
	var quickCheck string
	if err := db.QueryRowContext(ctx, `PRAGMA quick_check;`).Scan(&quickCheck); err != nil || strings.TrimSpace(quickCheck) != "ok" {
		return fmt.Errorf("journal SQLite quick check failed: result=%q err=%w", quickCheck, err)
	}
	return nil
}
