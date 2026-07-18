package mountjournal

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestJournalDurabilityOwnershipOverlayAndRestart(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	journal, err := Open(ctx, directory, Options{SegmentMaxBytes: minimumSegmentMaxBytes})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer journal.Close()
	if _, err := Open(ctx, directory, Options{}); !errors.Is(err, ErrJournalOwned) {
		t.Fatalf("second writer err=%v", err)
	}
	assertJournalPragmas(t, journal.db)

	metadata, replayed, err := journal.CreateOperation(ctx, CreateOperationRequest{
		ID: "operation-metadata", Kind: OperationMetadata, EntryID: "entry-a", NewSize: 0,
	})
	if err != nil || replayed {
		t.Fatalf("CreateOperation(metadata)=%+v replayed=%v err=%v", metadata, replayed, err)
	}
	if _, err := journal.TransitionOperation(ctx, metadata.ID, []string{StatePending}, StateLocalDurable, ""); err != nil {
		t.Fatalf("mark metadata durable: %v", err)
	}
	operationRequest := CreateOperationRequest{
		ID: "operation-write", Kind: OperationWrite, EntryID: "entry-a",
		BaseVersionID: "version-base", NewSize: 12, Mtime: 1_720_000_000,
		ExpectedEntryRevision: 3, ExpectedParentRevision: 7,
		Dependencies: []string{metadata.ID, metadata.ID},
	}
	operation, replayed, err := journal.CreateOperation(ctx, operationRequest)
	if err != nil || replayed || len(operation.Dependencies) != 1 || operation.Dependencies[0] != metadata.ID {
		t.Fatalf("CreateOperation=%+v replayed=%v err=%v", operation, replayed, err)
	}
	if replay, wasReplay, err := journal.CreateOperation(ctx, operationRequest); err != nil || !wasReplay || replay.ID != operation.ID {
		t.Fatalf("operation replay=%+v replayed=%v err=%v", replay, wasReplay, err)
	}
	reused := operationRequest
	reused.NewSize++
	if _, _, err := journal.CreateOperation(ctx, reused); !errors.Is(err, ErrIdempotencyReuse) {
		t.Fatalf("operation idempotency reuse err=%v", err)
	}

	first, err := journal.AppendData(ctx, AppendDataRequest{
		ExtentID: "extent-first", OperationID: operation.ID, LogicalOffset: 0, Payload: []byte("hello"),
	})
	if err != nil || first.Sequence != 1 || first.Length != 5 {
		t.Fatalf("AppendData(first)=%+v err=%v", first, err)
	}
	if replay, err := journal.AppendData(ctx, AppendDataRequest{
		ExtentID: "extent-first", OperationID: operation.ID, LogicalOffset: 0, Payload: []byte("hello"),
	}); err != nil || replay.ID != first.ID || replay.RecordOffset != first.RecordOffset {
		t.Fatalf("extent replay=%+v err=%v", replay, err)
	}
	if _, err := journal.AppendData(ctx, AppendDataRequest{
		ExtentID: "extent-first", OperationID: operation.ID, LogicalOffset: 0, Payload: []byte("HELLO"),
	}); !errors.Is(err, ErrIdempotencyReuse) {
		t.Fatalf("extent idempotency reuse err=%v", err)
	}
	if _, err := journal.AppendSparse(ctx, AppendSparseRequest{
		ExtentID: "extent-hole", OperationID: operation.ID, Kind: ExtentHole, LogicalOffset: 5, Length: 2,
	}); err != nil {
		t.Fatalf("AppendSparse: %v", err)
	}
	if _, err := journal.AppendData(ctx, AppendDataRequest{
		ExtentID: "extent-last", OperationID: operation.ID, LogicalOffset: 7, Payload: []byte("world"),
	}); err != nil {
		t.Fatalf("AppendData(last): %v", err)
	}
	current, err := journal.GetOperation(ctx, operation.ID)
	if err != nil || current.State != StateLocalDurable {
		t.Fatalf("locally durable operation=%+v err=%v", current, err)
	}

	buffer := make([]byte, 12)
	read, err := journal.ReadOverlay(ctx, operation.ID, nil, buffer, 0)
	if err != nil || read != len(buffer) || !bytes.Equal(buffer, []byte("hello\x00\x00world")) {
		t.Fatalf("overlay=%q n=%d err=%v", buffer, read, err)
	}
	longBuffer := make([]byte, 20)
	read, err = journal.ReadOverlay(ctx, operation.ID, nil, longBuffer, 0)
	if !errors.Is(err, io.EOF) || read != 12 || !bytes.Equal(longBuffer[:read], buffer) {
		t.Fatalf("short overlay=%q n=%d err=%v", longBuffer[:read], read, err)
	}
	withBase := make([]byte, 5)
	read, err = journal.ReadOverlay(ctx, operation.ID, bytes.NewReader([]byte("abcdefghijkl")), withBase, 4)
	if err != nil || read != 5 || !bytes.Equal(withBase, []byte("o\x00\x00wo")) {
		t.Fatalf("base overlay=%q n=%d err=%v", withBase, read, err)
	}
	truncated, replayed, err := journal.UpdateOperationMetadata(ctx, UpdateOperationMetadataRequest{
		UpdateID: "metadata-truncate", OperationID: operation.ID, ExpectedRevision: 1,
		NewSize: 10, Mtime: 1_720_000_100, Executable: true,
	})
	if err != nil || replayed || truncated.MetadataRevision != 2 || truncated.NewSize != 10 || !truncated.Executable {
		t.Fatalf("truncate metadata=%+v replayed=%v err=%v", truncated, replayed, err)
	}
	if replay, wasReplay, err := journal.UpdateOperationMetadata(ctx, UpdateOperationMetadataRequest{
		UpdateID: "metadata-truncate", OperationID: operation.ID, ExpectedRevision: 1,
		NewSize: 10, Mtime: 1_720_000_100, Executable: true,
	}); err != nil || !wasReplay || replay.MetadataRevision != 2 {
		t.Fatalf("truncate replay=%+v replayed=%v err=%v", replay, wasReplay, err)
	}
	if _, _, err := journal.UpdateOperationMetadata(ctx, UpdateOperationMetadataRequest{
		UpdateID: "metadata-stale", OperationID: operation.ID, ExpectedRevision: 1,
		NewSize: 11, Mtime: 1_720_000_101,
	}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("stale metadata update err=%v", err)
	}
	extended, replayed, err := journal.UpdateOperationMetadata(ctx, UpdateOperationMetadataRequest{
		UpdateID: "metadata-extend", OperationID: operation.ID, ExpectedRevision: 2,
		NewSize: 14, Mtime: 1_720_000_200,
	})
	if err != nil || replayed || extended.MetadataRevision != 3 || extended.NewSize != 14 || extended.Executable {
		t.Fatalf("extend metadata=%+v replayed=%v err=%v", extended, replayed, err)
	}
	if _, err := journal.AppendSparse(ctx, AppendSparseRequest{
		ExtentID: "extent-extend-hole", OperationID: operation.ID, Kind: ExtentHole,
		LogicalOffset: 10, Length: 4,
	}); err != nil {
		t.Fatalf("append extension hole: %v", err)
	}
	extendedBytes := make([]byte, 14)
	if n, err := journal.ReadOverlay(ctx, operation.ID, nil, extendedBytes, 0); err != nil || n != 14 ||
		!bytes.Equal(extendedBytes, []byte("hello\x00\x00wor\x00\x00\x00\x00")) {
		t.Fatalf("truncate/extend overlay=%q n=%d err=%v", extendedBytes, n, err)
	}

	partial, _, err := journal.CreateOperation(ctx, CreateOperationRequest{
		ID: "operation-partial", Kind: OperationWrite, EntryID: "entry-b", BaseVersionID: "base-b", NewSize: 8,
	})
	if err != nil {
		t.Fatalf("create partial operation: %v", err)
	}
	if _, err := journal.AppendData(ctx, AppendDataRequest{
		ExtentID: "extent-partial", OperationID: partial.ID, LogicalOffset: 4, Payload: []byte("data"),
	}); err != nil {
		t.Fatalf("append partial extent: %v", err)
	}
	if n, err := journal.ReadOverlay(ctx, partial.ID, nil, make([]byte, 8), 0); n != 0 || !errors.Is(err, ErrBaseUnavailable) {
		t.Fatalf("offline uncached read n=%d err=%v", n, err)
	}

	if err := journal.LocalBarrier(ctx); err != nil {
		t.Fatalf("LocalBarrier: %v", err)
	}
	stats, err := journal.Stats(ctx)
	if err != nil || stats.Operations != 3 || stats.RecoveryRecords != 0 || stats.DirtyBytes != 14 || stats.SegmentBytes <= segmentHeaderSize {
		t.Fatalf("Stats=%+v err=%v", stats, err)
	}
	inspector, err := OpenInspector(ctx, directory)
	if err != nil {
		t.Fatalf("OpenInspector while writer active: %v", err)
	}
	if operations, err := inspector.ListOperations(ctx); err != nil || len(operations) != 3 {
		t.Fatalf("inspector operations=%d err=%v", len(operations), err)
	}
	_ = inspector.Close()

	if _, err := journal.TransitionOperation(ctx, operation.ID, []string{StateLocalDurable}, StateRemoteCommitted, ""); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("skipped upload transition err=%v", err)
	}
	if _, err := journal.TransitionOperation(ctx, operation.ID, []string{StateLocalDurable}, StateUploading, ""); err != nil {
		t.Fatalf("transition uploading: %v", err)
	}
	if _, err := journal.TransitionOperation(ctx, operation.ID, []string{StateUploading}, StateReplicaPending, ""); err != nil {
		t.Fatalf("transition replica pending: %v", err)
	}
	if _, err := journal.TransitionOperation(ctx, operation.ID, []string{StateReplicaPending}, StateRemoteCommitted, ""); err != nil {
		t.Fatalf("transition remote committed: %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(ctx, directory, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	recovery, err := reopened.ListRecovery(ctx)
	if err != nil || len(recovery) != 0 {
		t.Fatalf("recovery after clean restart=%+v err=%v", recovery, err)
	}
	reopenedOperation, err := reopened.GetOperation(ctx, operation.ID)
	if err != nil || reopenedOperation.State != StateRemoteCommitted || len(reopenedOperation.Dependencies) != 1 || reopenedOperation.MetadataRevision != 3 {
		t.Fatalf("reopened operation=%+v err=%v", reopenedOperation, err)
	}
	info, err := os.Stat(filepath.Join(directory, "journal.sqlite"))
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("journal SQLite mode=%v err=%v", info.Mode(), err)
	}
}

func TestJournalFaultBoundariesRetainOrRestorePayload(t *testing.T) {
	points := []FaultPoint{
		FaultAfterSegmentWrite,
		FaultBeforeSegmentSync,
		FaultAfterSegmentSync,
		FaultAfterDirectorySync,
		FaultBeforeMetadataCommit,
		FaultAfterMetadataCommit,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			ctx := context.Background()
			directory := t.TempDir()
			injected := errors.New("injected journal crash boundary")
			journal, err := Open(ctx, directory, Options{
				SegmentMaxBytes: minimumSegmentMaxBytes,
				Fault: func(candidate FaultPoint) error {
					if candidate == point {
						return injected
					}
					return nil
				},
			})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			operation, _, err := journal.CreateOperation(ctx, CreateOperationRequest{
				ID: "operation-fault", Kind: OperationWrite, EntryID: "entry", NewSize: 14,
			})
			if err != nil {
				t.Fatalf("CreateOperation: %v", err)
			}
			payload := []byte("durable bytes!")
			_, err = journal.AppendData(ctx, AppendDataRequest{
				ExtentID: "extent-fault", OperationID: operation.ID, Payload: payload,
			})
			if !errors.Is(err, injected) {
				t.Fatalf("AppendData err=%v", err)
			}
			if point == FaultAfterMetadataCommit {
				replay, replayErr := journal.AppendData(ctx, AppendDataRequest{
					ExtentID: "extent-fault", OperationID: operation.ID, Payload: payload,
				})
				if replayErr != nil || replay.ID != "extent-fault" {
					t.Fatalf("post-commit replay=%+v err=%v", replay, replayErr)
				}
			} else if _, retryErr := journal.AppendData(ctx, AppendDataRequest{
				ExtentID: "extent-fault", OperationID: operation.ID, Payload: payload,
			}); !errors.Is(retryErr, ErrRecoveryRequired) {
				t.Fatalf("same-process ambiguous retry err=%v", retryErr)
			}
			if err := journal.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			reopened, err := Open(ctx, directory, Options{})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer reopened.Close()
			recovery, err := reopened.ListRecovery(ctx)
			if err != nil {
				t.Fatalf("ListRecovery: %v", err)
			}
			if point == FaultAfterMetadataCommit {
				if len(recovery) != 0 {
					t.Fatalf("committed extent incorrectly entered recovery: %+v", recovery)
				}
				return
			}
			if len(recovery) != 1 || recovery[0].Kind != RecoveryOrphan || recovery[0].ExtentID != "extent-fault" {
				t.Fatalf("recovery=%+v", recovery)
			}
			exportPath := filepath.Join(t.TempDir(), "recovered.bin")
			if _, err := reopened.ExportRecoveryPayload(ctx, recovery[0].ID, exportPath); err != nil {
				t.Fatalf("ExportRecoveryPayload: %v", err)
			}
			exported, err := os.ReadFile(exportPath)
			if err != nil || !bytes.Equal(exported, payload) {
				t.Fatalf("exported=%q err=%v", exported, err)
			}
			if _, err := reopened.ExportRecoveryPayload(ctx, recovery[0].ID, exportPath); !errors.Is(err, os.ErrExist) {
				t.Fatalf("recovery export replaced existing file err=%v", err)
			}
			restored, err := reopened.RestoreRecovery(ctx, recovery[0].ID)
			if err != nil || restored.ID != "extent-fault" {
				t.Fatalf("RestoreRecovery=%+v err=%v", restored, err)
			}
			buffer := make([]byte, len(payload))
			if n, err := reopened.ReadOverlay(ctx, operation.ID, nil, buffer, 0); err != nil || n != len(payload) || !bytes.Equal(buffer, payload) {
				t.Fatalf("restored overlay=%q n=%d err=%v", buffer, n, err)
			}
			recovery, err = reopened.ListRecovery(ctx)
			if err != nil || len(recovery) != 1 || recovery[0].Kind != RecoveryRestored {
				t.Fatalf("restored recovery=%+v err=%v", recovery, err)
			}
		})
	}
}

func TestJournalCorruptionAndTrailingBytesAreRetained(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	journal, err := Open(ctx, directory, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	operation, _, err := journal.CreateOperation(ctx, CreateOperationRequest{
		ID: "operation-corrupt", Kind: OperationWrite, EntryID: "entry", NewSize: 7,
	})
	if err != nil {
		t.Fatalf("CreateOperation: %v", err)
	}
	extent, err := journal.AppendData(ctx, AppendDataRequest{
		ExtentID: "extent-corrupt", OperationID: operation.ID, Payload: []byte("payload"),
	})
	if err != nil {
		t.Fatalf("AppendData: %v", err)
	}
	segmentPath, err := segmentPathForID(directory, journal.db, extent.SegmentID)
	if err != nil {
		t.Fatalf("segmentPathForID: %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	segmentInfo, _ := os.Stat(segmentPath)
	segment, err := os.OpenFile(segmentPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if _, err := segment.WriteAt([]byte{'X'}, extent.PayloadOffset+2); err != nil {
		t.Fatalf("corrupt payload: %v", err)
	}
	if err := segment.Sync(); err != nil {
		t.Fatalf("sync corruption: %v", err)
	}
	_ = segment.Close()

	reopened, err := Open(ctx, directory, Options{})
	if err != nil {
		t.Fatalf("reopen corrupt journal: %v", err)
	}
	recovery, err := reopened.ListRecovery(ctx)
	if err != nil || len(recovery) != 1 || recovery[0].Kind != RecoveryCorrupt {
		t.Fatalf("corrupt recovery=%+v err=%v", recovery, err)
	}
	if current, err := reopened.GetOperation(ctx, operation.ID); err != nil || current.State != StateRecovery {
		t.Fatalf("corrupt operation=%+v err=%v", current, err)
	}
	if _, err := reopened.RestoreRecovery(ctx, recovery[0].ID); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("corrupt restore err=%v", err)
	}
	if _, err := reopened.ReadOverlay(ctx, operation.ID, nil, make([]byte, 7), 0); !errors.Is(err, ErrExtentCorrupt) {
		t.Fatalf("corrupt overlay err=%v", err)
	}
	var eventCount int
	if err := reopened.db.QueryRow(`SELECT COUNT(*) FROM journal_events WHERE action = 'recovery_detected';`).Scan(&eventCount); err != nil || eventCount != 1 {
		t.Fatalf("recovery events=%d err=%v", eventCount, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close corrupt journal: %v", err)
	}
	afterInfo, _ := os.Stat(segmentPath)
	if afterInfo.Size() != segmentInfo.Size() {
		t.Fatalf("corrupt segment was truncated: before=%d after=%d", segmentInfo.Size(), afterInfo.Size())
	}
	again, err := Open(ctx, directory, Options{})
	if err != nil {
		t.Fatalf("second reopen: %v", err)
	}
	defer again.Close()
	if err := again.db.QueryRow(`SELECT COUNT(*) FROM journal_events WHERE action = 'recovery_detected';`).Scan(&eventCount); err != nil || eventCount != 1 {
		t.Fatalf("repeated recovery events=%d err=%v", eventCount, err)
	}

	trailingDirectory := t.TempDir()
	clean, err := Open(ctx, trailingDirectory, Options{})
	if err != nil {
		t.Fatalf("open trailing journal: %v", err)
	}
	cleanOperation, _, _ := clean.CreateOperation(ctx, CreateOperationRequest{
		ID: "operation-clean", Kind: OperationWrite, EntryID: "entry", NewSize: 4,
	})
	cleanExtent, err := clean.AppendData(ctx, AppendDataRequest{
		ExtentID: "extent-clean", OperationID: cleanOperation.ID, Payload: []byte("data"),
	})
	if err != nil {
		t.Fatalf("append clean data: %v", err)
	}
	cleanPath, _ := segmentPathForID(trailingDirectory, clean.db, cleanExtent.SegmentID)
	_ = clean.Close()
	file, err := os.OpenFile(cleanPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open trailing segment: %v", err)
	}
	if _, err := file.Write([]byte("partial-record")); err != nil {
		t.Fatalf("write trailing bytes: %v", err)
	}
	_ = file.Sync()
	_ = file.Close()
	withTrailing, err := Open(ctx, trailingDirectory, Options{})
	if err != nil {
		t.Fatalf("reopen trailing journal: %v", err)
	}
	defer withTrailing.Close()
	trailingRecovery, err := withTrailing.ListRecovery(ctx)
	if err != nil || len(trailingRecovery) != 1 || trailingRecovery[0].Kind != RecoveryIncomplete {
		t.Fatalf("trailing recovery=%+v err=%v", trailingRecovery, err)
	}
	secondOperation, _, _ := withTrailing.CreateOperation(ctx, CreateOperationRequest{
		ID: "operation-second", Kind: OperationWrite, EntryID: "entry-2", NewSize: 4,
	})
	secondExtent, err := withTrailing.AppendData(ctx, AppendDataRequest{
		ExtentID: "extent-second", OperationID: secondOperation.ID, Payload: []byte("more"),
	})
	if err != nil || secondExtent.SegmentID == cleanExtent.SegmentID {
		t.Fatalf("new data reused corrupt segment extent=%+v err=%v", secondExtent, err)
	}
}

func TestJournalMigratesV1AndRejectsUnsupportedSchema(t *testing.T) {
	ctx := context.Background()
	v1Directory := t.TempDir()
	v1DB, err := sql.Open("sqlite", filepath.Join(v1Directory, "journal.sqlite"))
	if err != nil {
		t.Fatalf("open v1 SQLite: %v", err)
	}
	for _, statement := range append([]string{
		`PRAGMA journal_mode=WAL;`, `PRAGMA synchronous=FULL;`, `PRAGMA foreign_keys=ON;`,
	}, journalSchemaV1...) {
		if _, err := v1DB.Exec(statement); err != nil {
			t.Fatalf("initialize v1 schema: %v", err)
		}
	}
	if _, err := v1DB.Exec(`INSERT INTO journal_meta (key, value) VALUES ('schema_version', '1');`); err != nil {
		t.Fatalf("insert v1 version: %v", err)
	}
	if _, err := v1DB.Exec(`INSERT INTO operations (
 id, kind, entry_id, parent_id, base_version_id, new_size, mtime, executable,
 expected_entry_revision, expected_parent_revision, state, last_error,
 request_fingerprint, created_at, updated_at
) VALUES ('v1-operation', 'write', 'entry', '', 'base', 9, 123, 1, 2, 3, 'local_durable', '', 'fingerprint', 10, 11);`); err != nil {
		t.Fatalf("insert v1 operation: %v", err)
	}
	_ = v1DB.Close()
	migrated, err := Open(ctx, v1Directory, Options{})
	if err != nil {
		t.Fatalf("migrate v1 journal: %v", err)
	}
	operation, err := migrated.GetOperation(ctx, "v1-operation")
	if err != nil || operation.MetadataRevision != 1 || operation.NewSize != 9 || !operation.Executable {
		t.Fatalf("migrated operation=%+v err=%v", operation, err)
	}
	var version string
	var revisions int
	if err := migrated.db.QueryRow(`SELECT value FROM journal_meta WHERE key = 'schema_version';`).Scan(&version); err != nil || version != "2" {
		t.Fatalf("migrated version=%q err=%v", version, err)
	}
	if err := migrated.db.QueryRow(`SELECT COUNT(*) FROM operation_revisions WHERE operation_id = 'v1-operation';`).Scan(&revisions); err != nil || revisions != 1 {
		t.Fatalf("migrated revisions=%d err=%v", revisions, err)
	}
	_ = migrated.Close()

	directory := t.TempDir()
	journal, err := Open(ctx, directory, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(directory, "journal.sqlite"))
	if err != nil {
		t.Fatalf("open raw SQLite: %v", err)
	}
	if _, err := db.Exec(`UPDATE journal_meta SET value = '3' WHERE key = 'schema_version';`); err != nil {
		t.Fatalf("raise schema version: %v", err)
	}
	_ = db.Close()
	if _, err := Open(ctx, directory, Options{}); !errors.Is(err, ErrJournalUpgradeRequired) {
		t.Fatalf("newer schema open err=%v", err)
	}
}

func assertJournalPragmas(t *testing.T, db *sql.DB) {
	t.Helper()
	var mode string
	var synchronous int
	var foreignKeys int
	if err := db.QueryRow(`PRAGMA journal_mode;`).Scan(&mode); err != nil || !strings.EqualFold(mode, "wal") {
		t.Fatalf("journal_mode=%q err=%v", mode, err)
	}
	if err := db.QueryRow(`PRAGMA synchronous;`).Scan(&synchronous); err != nil || synchronous != 2 {
		t.Fatalf("synchronous=%d err=%v", synchronous, err)
	}
	if err := db.QueryRow(`PRAGMA foreign_keys;`).Scan(&foreignKeys); err != nil || foreignKeys != 1 {
		t.Fatalf("foreign_keys=%d err=%v", foreignKeys, err)
	}
}
