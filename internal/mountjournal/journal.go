package mountjournal

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	defaultSegmentMaxBytes = int64(64 * 1024 * 1024)
	minimumSegmentMaxBytes = int64(1 * 1024 * 1024)
	maximumSegmentMaxBytes = int64(1 * 1024 * 1024 * 1024)
)

type Journal struct {
	dir               string
	db                *sql.DB
	lock              *journalFileLock
	options           Options
	mu                sync.Mutex
	active            *os.File
	activeID          string
	activeSize        int64
	reconcileRequired bool
	closed            bool
}

type Inspector struct {
	dir string
	db  *sql.DB
}

func Open(ctx context.Context, dir string, options Options) (*Journal, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, errors.New("journal directory is required")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := ensureJournalDirectory(abs); err != nil {
		return nil, err
	}
	lock, err := acquireJournalFileLock(filepath.Join(abs, "writer.lock"))
	if err != nil {
		return nil, err
	}
	db, err := openJournalDatabase(filepath.Join(abs, "journal.sqlite"), false)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	if err := initializeSchema(ctx, db); err != nil {
		_ = db.Close()
		_ = lock.Close()
		return nil, err
	}
	options = normalizeOptions(options)
	journal := &Journal{dir: abs, db: db, lock: lock, options: options}
	if err := syncJournalDirectory(abs); err != nil {
		_ = journal.Close()
		return nil, fmt.Errorf("sync journal directory: %w", err)
	}
	if err := journal.reconcile(ctx); err != nil {
		_ = journal.Close()
		return nil, err
	}
	return journal, nil
}

func OpenInspector(ctx context.Context, dir string) (*Inspector, error) {
	abs, err := filepath.Abs(strings.TrimSpace(dir))
	if err != nil {
		return nil, err
	}
	db, err := openJournalDatabase(filepath.Join(abs, "journal.sqlite"), true)
	if err != nil {
		return nil, err
	}
	var versionValue string
	if err := db.QueryRowContext(ctx, `SELECT value FROM journal_meta WHERE key = 'schema_version';`).Scan(&versionValue); err != nil || strings.TrimSpace(versionValue) != fmt.Sprint(SchemaVersion) {
		_ = db.Close()
		return nil, ErrJournalUpgradeRequired
	}
	return &Inspector{dir: abs, db: db}, nil
}

func normalizeOptions(options Options) Options {
	if options.SegmentMaxBytes < minimumSegmentMaxBytes || options.SegmentMaxBytes > maximumSegmentMaxBytes {
		options.SegmentMaxBytes = defaultSegmentMaxBytes
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return options
}

func ensureJournalDirectory(dir string) error {
	info, err := os.Lstat(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create journal directory: %w", err)
		}
	case err != nil:
		return err
	case info.Mode()&os.ModeSymlink != 0 || !info.IsDir():
		return errors.New("journal path must be a real directory")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("protect journal directory: %w", err)
	}
	return nil
}

func openJournalDatabase(path string, readOnly bool) (*sql.DB, error) {
	dsn := path
	if readOnly {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		slash := filepath.ToSlash(absolute)
		if len(slash) >= 2 && slash[1] == ':' {
			slash = "/" + slash
		}
		u := url.URL{Scheme: "file", Path: slash}
		query := u.Query()
		query.Set("mode", "ro")
		u.RawQuery = query.Encode()
		dsn = u.String()
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if !readOnly {
		if file, err := os.OpenFile(path, os.O_RDWR, 0o600); err == nil {
			_ = file.Chmod(0o600)
			_ = file.Close()
		}
	}
	return db, nil
}

func (j *Journal) Directory() string {
	if j == nil {
		return ""
	}
	return j.dir
}

func (j *Journal) Close() error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	var result error
	if j.active != nil {
		if err := j.active.Sync(); err != nil {
			result = errors.Join(result, err)
		}
		if err := j.active.Close(); err != nil {
			result = errors.Join(result, err)
		}
		j.active = nil
	}
	if j.db != nil {
		if err := j.db.Close(); err != nil {
			result = errors.Join(result, err)
		}
		j.db = nil
	}
	if j.lock != nil {
		if err := j.lock.Close(); err != nil {
			result = errors.Join(result, err)
		}
		j.lock = nil
	}
	return result
}

func (i *Inspector) Close() error {
	if i == nil || i.db == nil {
		return nil
	}
	err := i.db.Close()
	i.db = nil
	return err
}

func (j *Journal) CreateOperation(ctx context.Context, request CreateOperationRequest) (Operation, bool, error) {
	if j == nil || j.db == nil {
		return Operation{}, false, errors.New("journal is closed")
	}
	request = normalizeCreateOperationRequest(request)
	if err := validateCreateOperationRequest(request); err != nil {
		return Operation{}, false, err
	}
	fingerprint, err := operationFingerprint(request)
	if err != nil {
		return Operation{}, false, err
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return Operation{}, false, err
	}
	defer tx.Rollback()
	if existing, err := getOperation(ctx, tx, request.ID); err == nil {
		if existing.RequestFingerprint != fingerprint {
			return Operation{}, false, ErrIdempotencyReuse
		}
		if err := loadOperationDependencies(ctx, tx, &existing); err != nil {
			return Operation{}, false, err
		}
		return existing, true, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Operation{}, false, err
	}
	now := j.options.Now().UnixNano()
	for _, dependency := range request.Dependencies {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM operations WHERE id = ?;`, dependency).Scan(&exists); err != nil {
			return Operation{}, false, fmt.Errorf("journal dependency %q is unavailable: %w", dependency, err)
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO operations (
 id, kind, entry_id, parent_id, base_version_id, new_size, mtime, executable,
 expected_entry_revision, expected_parent_revision, state, last_error,
 request_fingerprint, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?);`,
		request.ID, request.Kind, request.EntryID, request.ParentID, request.BaseVersionID,
		request.NewSize, request.Mtime, boolInt(request.Executable), request.ExpectedEntryRevision,
		request.ExpectedParentRevision, StatePending, fingerprint, now, now)
	if err != nil {
		return Operation{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO operation_revisions (
 operation_id, revision, update_id, new_size, mtime, executable, created_at
) VALUES (?, 1, ?, ?, ?, ?, ?);`,
		request.ID, "initial."+request.ID, request.NewSize, request.Mtime,
		boolInt(request.Executable), now); err != nil {
		return Operation{}, false, err
	}
	for _, dependency := range request.Dependencies {
		if _, err := tx.ExecContext(ctx, `INSERT INTO operation_dependencies (operation_id, dependency_id, created_at) VALUES (?, ?, ?);`, request.ID, dependency, now); err != nil {
			return Operation{}, false, err
		}
	}
	if err := appendEvent(ctx, tx, request.ID, "operation_create", StatePending, "", now); err != nil {
		return Operation{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Operation{}, false, err
	}
	return Operation{
		ID: request.ID, Kind: request.Kind, EntryID: request.EntryID, ParentID: request.ParentID,
		BaseVersionID: request.BaseVersionID, NewSize: request.NewSize, Mtime: request.Mtime,
		Executable: request.Executable, ExpectedEntryRevision: request.ExpectedEntryRevision,
		ExpectedParentRevision: request.ExpectedParentRevision, State: StatePending,
		MetadataRevision: 1,
		Dependencies:     append([]string(nil), request.Dependencies...), CreatedAt: now, UpdatedAt: now,
		RequestFingerprint: fingerprint,
	}, false, nil
}

func normalizeCreateOperationRequest(request CreateOperationRequest) CreateOperationRequest {
	request.ID = strings.TrimSpace(request.ID)
	request.Kind = strings.TrimSpace(request.Kind)
	request.EntryID = strings.TrimSpace(request.EntryID)
	request.ParentID = strings.TrimSpace(request.ParentID)
	request.BaseVersionID = strings.TrimSpace(request.BaseVersionID)
	dependencies := make([]string, 0, len(request.Dependencies))
	seen := make(map[string]struct{}, len(request.Dependencies))
	for _, value := range request.Dependencies {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		dependencies = append(dependencies, value)
	}
	sort.Strings(dependencies)
	request.Dependencies = dependencies
	return request
}

func validateCreateOperationRequest(request CreateOperationRequest) error {
	if !validIdentifier(request.ID) || request.NewSize < 0 || request.ExpectedEntryRevision < 0 || request.ExpectedParentRevision < 0 {
		return errors.New("invalid journal operation")
	}
	switch request.Kind {
	case OperationCreate, OperationMkdir, OperationWrite, OperationRename, OperationMove, OperationTrash, OperationRestore, OperationMetadata:
	default:
		return errors.New("unsupported journal operation kind")
	}
	for _, dependency := range request.Dependencies {
		if !validIdentifier(dependency) || dependency == request.ID {
			return errors.New("invalid journal operation dependency")
		}
	}
	return nil
}

func operationFingerprint(request CreateOperationRequest) (string, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			continue
		}
		return false
	}
	return true
}

type sqlQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func getOperation(ctx context.Context, query sqlQuerier, id string) (Operation, error) {
	var operation Operation
	var executable int
	err := query.QueryRowContext(ctx, `SELECT id, kind, entry_id, parent_id, base_version_id,
 new_size, mtime, executable, expected_entry_revision, expected_parent_revision,
 metadata_revision, state, last_error, request_fingerprint, created_at, updated_at
 FROM operations WHERE id = ?;`, strings.TrimSpace(id)).Scan(
		&operation.ID, &operation.Kind, &operation.EntryID, &operation.ParentID,
		&operation.BaseVersionID, &operation.NewSize, &operation.Mtime, &executable,
		&operation.ExpectedEntryRevision, &operation.ExpectedParentRevision,
		&operation.MetadataRevision, &operation.State, &operation.LastError, &operation.RequestFingerprint,
		&operation.CreatedAt, &operation.UpdatedAt,
	)
	operation.Executable = executable != 0
	return operation, err
}

func loadOperationDependencies(ctx context.Context, query sqlQuerier, operation *Operation) error {
	rows, err := query.QueryContext(ctx, `SELECT dependency_id FROM operation_dependencies WHERE operation_id = ? ORDER BY dependency_id ASC;`, operation.ID)
	if err != nil {
		return err
	}
	defer rows.Close()
	operation.Dependencies = nil
	for rows.Next() {
		var dependency string
		if err := rows.Scan(&dependency); err != nil {
			return err
		}
		operation.Dependencies = append(operation.Dependencies, dependency)
	}
	return rows.Err()
}

func (j *Journal) GetOperation(ctx context.Context, id string) (Operation, error) {
	operation, err := getOperation(ctx, j.db, id)
	if err != nil {
		return Operation{}, err
	}
	if err := loadOperationDependencies(ctx, j.db, &operation); err != nil {
		return Operation{}, err
	}
	return operation, nil
}

func (j *Journal) UpdateOperationMetadata(ctx context.Context, request UpdateOperationMetadataRequest) (Operation, bool, error) {
	request.UpdateID = strings.TrimSpace(request.UpdateID)
	request.OperationID = strings.TrimSpace(request.OperationID)
	if !validIdentifier(request.UpdateID) || !validIdentifier(request.OperationID) || request.ExpectedRevision < 1 || request.NewSize < 0 {
		return Operation{}, false, errors.New("invalid journal metadata update")
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return Operation{}, false, err
	}
	defer tx.Rollback()
	var replayRevision int64
	var replaySize int64
	var replayMtime int64
	var replayExecutable int
	err = tx.QueryRowContext(ctx, `SELECT revision, new_size, mtime, executable
 FROM operation_revisions WHERE operation_id = ? AND update_id = ?;`, request.OperationID, request.UpdateID).Scan(
		&replayRevision, &replaySize, &replayMtime, &replayExecutable,
	)
	if err == nil {
		if replaySize != request.NewSize || replayMtime != request.Mtime || (replayExecutable != 0) != request.Executable {
			return Operation{}, false, ErrIdempotencyReuse
		}
		operation, err := getOperation(ctx, tx, request.OperationID)
		if err != nil {
			return Operation{}, false, err
		}
		if operation.MetadataRevision < replayRevision {
			return Operation{}, false, ErrExtentCorrupt
		}
		if err := loadOperationDependencies(ctx, tx, &operation); err != nil {
			return Operation{}, false, err
		}
		return operation, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Operation{}, false, err
	}
	operation, err := getOperation(ctx, tx, request.OperationID)
	if err != nil {
		return Operation{}, false, err
	}
	if operation.MetadataRevision != request.ExpectedRevision ||
		containsString([]string{StateRemoteCommitted, StateConflicted, StateAbandoned}, operation.State) {
		return Operation{}, false, ErrInvalidTransition
	}
	nextRevision := operation.MetadataRevision + 1
	now := j.options.Now().UnixNano()
	if _, err := tx.ExecContext(ctx, `INSERT INTO operation_revisions (
 operation_id, revision, update_id, new_size, mtime, executable, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?);`, request.OperationID, nextRevision, request.UpdateID,
		request.NewSize, request.Mtime, boolInt(request.Executable), now); err != nil {
		return Operation{}, false, err
	}
	nextState := operation.State
	if containsString([]string{StatePending, StateUploading, StateReplicaPending}, operation.State) {
		nextState = StateLocalDurable
	}
	result, err := tx.ExecContext(ctx, `UPDATE operations SET
 new_size = ?, mtime = ?, executable = ?, metadata_revision = ?, state = ?,
 last_error = CASE WHEN ? = 'local_durable' THEN '' ELSE last_error END,
 updated_at = ?
 WHERE id = ? AND metadata_revision = ?;`,
		request.NewSize, request.Mtime, boolInt(request.Executable), nextRevision,
		nextState, nextState, now, request.OperationID, operation.MetadataRevision)
	if err != nil {
		return Operation{}, false, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return Operation{}, false, ErrInvalidTransition
	}
	if err := appendEvent(ctx, tx, operation.ID, "operation_metadata", nextState, request.UpdateID, now); err != nil {
		return Operation{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Operation{}, false, err
	}
	operation.NewSize = request.NewSize
	operation.Mtime = request.Mtime
	operation.Executable = request.Executable
	operation.MetadataRevision = nextRevision
	operation.State = nextState
	operation.UpdatedAt = now
	if nextState == StateLocalDurable {
		operation.LastError = ""
	}
	if err := loadOperationDependencies(ctx, j.db, &operation); err != nil {
		return Operation{}, false, err
	}
	return operation, false, nil
}

func (j *Journal) ListOperations(ctx context.Context) ([]Operation, error) {
	return listOperations(ctx, j.db)
}

func (i *Inspector) ListOperations(ctx context.Context) ([]Operation, error) {
	return listOperations(ctx, i.db)
}

func listOperations(ctx context.Context, db *sql.DB) ([]Operation, error) {
	rows, err := db.QueryContext(ctx, `SELECT id FROM operations ORDER BY created_at ASC, id ASC;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	operations := make([]Operation, 0, len(ids))
	for _, id := range ids {
		operation, err := getOperation(ctx, db, id)
		if err != nil {
			return nil, err
		}
		if err := loadOperationDependencies(ctx, db, &operation); err != nil {
			return nil, err
		}
		operations = append(operations, operation)
	}
	return operations, nil
}

func (j *Journal) TransitionOperation(ctx context.Context, id string, expected []string, target string, detail string) (Operation, error) {
	id = strings.TrimSpace(id)
	target = strings.TrimSpace(target)
	if !validOperationState(target) || len(detail) > 512 {
		return Operation{}, ErrInvalidTransition
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return Operation{}, err
	}
	defer tx.Rollback()
	current, err := getOperation(ctx, tx, id)
	if err != nil {
		return Operation{}, err
	}
	if current.State == target {
		if err := loadOperationDependencies(ctx, tx, &current); err != nil {
			return Operation{}, err
		}
		return current, nil
	}
	if !containsString(expected, current.State) || !allowedOperationTransition(current.State, target) {
		return Operation{}, ErrInvalidTransition
	}
	now := j.options.Now().UnixNano()
	result, err := tx.ExecContext(ctx, `UPDATE operations SET state = ?, last_error = ?, updated_at = ? WHERE id = ? AND state = ?;`, target, detail, now, id, current.State)
	if err != nil {
		return Operation{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return Operation{}, ErrInvalidTransition
	}
	if err := appendEvent(ctx, tx, id, "operation_state", target, detail, now); err != nil {
		return Operation{}, err
	}
	if err := tx.Commit(); err != nil {
		return Operation{}, err
	}
	current.State, current.LastError, current.UpdatedAt = target, detail, now
	if err := loadOperationDependencies(ctx, j.db, &current); err != nil {
		return Operation{}, err
	}
	return current, nil
}

func allowedOperationTransition(current string, target string) bool {
	switch current {
	case StatePending:
		return containsString([]string{StateLocalDurable, StateBlockedAuth, StateRecovery, StateAbandoned}, target)
	case StateLocalDurable:
		return containsString([]string{StateUploading, StateBlockedAuth, StateRecovery, StateAbandoned}, target)
	case StateUploading:
		return containsString([]string{StateLocalDurable, StateReplicaPending, StateRemoteCommitted, StateConflicted, StateBlockedAuth, StateRecovery, StateAbandoned}, target)
	case StateReplicaPending:
		return containsString([]string{StateUploading, StateRemoteCommitted, StateConflicted, StateBlockedAuth, StateRecovery, StateAbandoned}, target)
	case StateBlockedAuth:
		return containsString([]string{StateLocalDurable, StateUploading, StateRecovery, StateAbandoned}, target)
	case StateRecovery:
		return containsString([]string{StateLocalDurable, StateBlockedAuth, StateAbandoned}, target)
	case StateConflicted:
		return target == StateRemoteCommitted || target == StateRecovery
	default:
		return false
	}
}

func validOperationState(state string) bool {
	return containsString([]string{StatePending, StateLocalDurable, StateUploading, StateReplicaPending, StateRemoteCommitted, StateConflicted, StateBlockedAuth, StateRecovery, StateAbandoned}, state)
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func appendEvent(ctx context.Context, tx *sql.Tx, operationID string, action string, result string, detail string, now int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO journal_events (operation_id, action, result, detail, created_at) VALUES (?, ?, ?, ?, ?);`, operationID, action, result, detail, now)
	return err
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (j *Journal) LocalBarrier(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return errors.New("journal is closed")
	}
	if j.active != nil {
		if err := j.active.Sync(); err != nil {
			return err
		}
	}
	if _, err := j.db.ExecContext(ctx, `PRAGMA wal_checkpoint(FULL);`); err != nil {
		return err
	}
	return syncJournalDirectory(j.dir)
}

func (j *Journal) callFault(point FaultPoint) error {
	if j.options.Fault == nil {
		return nil
	}
	return j.options.Fault(point)
}

func (j *Journal) ReadOverlay(ctx context.Context, operationID string, base BaseReaderAt, target []byte, offset int64) (int, error) {
	if len(target) == 0 {
		return 0, nil
	}
	requestedLength := len(target)
	if offset < 0 {
		return 0, errors.New("negative journal read offset")
	}
	operation, err := j.GetOperation(ctx, operationID)
	if err != nil {
		return 0, err
	}
	if offset >= operation.NewSize {
		return 0, io.EOF
	}
	shortRead := int64(len(target)) > operation.NewSize-offset
	if shortRead {
		target = target[:operation.NewSize-offset]
	}
	covered := make([]bool, len(target))
	if base != nil {
		n, readErr := base.ReadAt(target, offset)
		for index := 0; index < n; index++ {
			covered[index] = true
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return n, readErr
		}
	}
	extents, err := j.ListExtents(ctx, operationID)
	if err != nil {
		return 0, err
	}
	for _, extent := range extents {
		start := maxInt64(offset, extent.LogicalOffset)
		end := minInt64(offset+int64(len(target)), extent.LogicalOffset+extent.Length)
		if start >= end {
			continue
		}
		destination := target[start-offset : end-offset]
		switch extent.Kind {
		case ExtentZero, ExtentHole:
			clear(destination)
		case ExtentData:
			if err := j.readExtentRange(extent, destination, start-extent.LogicalOffset); err != nil {
				return 0, err
			}
		default:
			return 0, ErrExtentCorrupt
		}
		for index := start - offset; index < end-offset; index++ {
			covered[index] = true
		}
	}
	for index, ok := range covered {
		if !ok {
			if index == 0 {
				return 0, ErrBaseUnavailable
			}
			return index, ErrBaseUnavailable
		}
	}
	if shortRead && len(target) < requestedLength {
		return len(target), io.EOF
	}
	return len(target), nil
}

func maxInt64(left int64, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func minInt64(left int64, right int64) int64 {
	if left < right {
		return left
	}
	return right
}
