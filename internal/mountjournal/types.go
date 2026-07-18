package mountjournal

import (
	"errors"
	"io"
	"time"
)

const SchemaVersion = 2

const (
	OperationCreate   = "create"
	OperationMkdir    = "mkdir"
	OperationWrite    = "write"
	OperationRename   = "rename"
	OperationMove     = "move"
	OperationTrash    = "trash"
	OperationRestore  = "restore"
	OperationMetadata = "metadata"
)

const (
	StatePending         = "pending"
	StateLocalDurable    = "local_durable"
	StateUploading       = "uploading"
	StateReplicaPending  = "replica_pending"
	StateRemoteCommitted = "remote_committed"
	StateConflicted      = "conflicted"
	StateBlockedAuth     = "blocked_auth"
	StateRecovery        = "recovery"
	StateAbandoned       = "abandoned"
)

const (
	ExtentData = "data"
	ExtentZero = "zero"
	ExtentHole = "hole"
)

const (
	RecoveryOrphan     = "orphan"
	RecoveryIncomplete = "incomplete"
	RecoveryCorrupt    = "corrupt"
	RecoveryMissing    = "missing"
	RecoveryMismatch   = "mismatch"
	RecoveryRestored   = "restored"
)

type FaultPoint string

const (
	FaultAfterSegmentWrite    FaultPoint = "after_segment_write"
	FaultBeforeSegmentSync    FaultPoint = "before_segment_sync"
	FaultAfterSegmentSync     FaultPoint = "after_segment_sync"
	FaultAfterDirectorySync   FaultPoint = "after_directory_sync"
	FaultBeforeMetadataCommit FaultPoint = "before_metadata_commit"
	FaultAfterMetadataCommit  FaultPoint = "after_metadata_commit"
)

var (
	ErrJournalOwned           = errors.New("writable journal is already owned by another process")
	ErrJournalUpgradeRequired = errors.New("journal schema requires a newer idoud client")
	ErrIdempotencyReuse       = errors.New("journal idempotency identifier was reused with different data")
	ErrRecoveryRequired       = errors.New("journal record requires explicit recovery")
	ErrBaseUnavailable        = errors.New("uncached immutable base bytes are unavailable")
	ErrExtentCorrupt          = errors.New("journal extent failed integrity verification")
	ErrInvalidTransition      = errors.New("invalid journal operation state transition")
)

type Options struct {
	SegmentMaxBytes int64
	Now             func() time.Time
	Fault           func(FaultPoint) error
}

type CreateOperationRequest struct {
	ID                     string
	Kind                   string
	EntryID                string
	ParentID               string
	BaseVersionID          string
	NewSize                int64
	Mtime                  int64
	Executable             bool
	ExpectedEntryRevision  int64
	ExpectedParentRevision int64
	Dependencies           []string
}

type Operation struct {
	ID                     string   `json:"id"`
	Kind                   string   `json:"kind"`
	EntryID                string   `json:"entryId"`
	ParentID               string   `json:"parentId"`
	BaseVersionID          string   `json:"baseVersionId"`
	NewSize                int64    `json:"newSize"`
	Mtime                  int64    `json:"mtime"`
	Executable             bool     `json:"executable"`
	ExpectedEntryRevision  int64    `json:"expectedEntryRevision"`
	ExpectedParentRevision int64    `json:"expectedParentRevision"`
	MetadataRevision       int64    `json:"metadataRevision"`
	State                  string   `json:"state"`
	LastError              string   `json:"lastError,omitempty"`
	Dependencies           []string `json:"dependencies,omitempty"`
	CreatedAt              int64    `json:"createdAt"`
	UpdatedAt              int64    `json:"updatedAt"`
	RequestFingerprint     string   `json:"-"`
}

type UpdateOperationMetadataRequest struct {
	UpdateID         string
	OperationID      string
	ExpectedRevision int64
	NewSize          int64
	Mtime            int64
	Executable       bool
}

type AppendDataRequest struct {
	ExtentID      string
	OperationID   string
	LogicalOffset int64
	Payload       []byte
}

type AppendSparseRequest struct {
	ExtentID      string
	OperationID   string
	Kind          string
	LogicalOffset int64
	Length        int64
}

type Extent struct {
	ID            string `json:"id"`
	OperationID   string `json:"operationId"`
	Sequence      int64  `json:"sequence"`
	Kind          string `json:"kind"`
	LogicalOffset int64  `json:"logicalOffset"`
	Length        int64  `json:"length"`
	SegmentID     string `json:"segmentId,omitempty"`
	RecordOffset  int64  `json:"recordOffset,omitempty"`
	RecordLength  int64  `json:"recordLength,omitempty"`
	PayloadOffset int64  `json:"payloadOffset,omitempty"`
	CRC32         uint32 `json:"crc32,omitempty"`
	SHA256        string `json:"sha256,omitempty"`
	CreatedAt     int64  `json:"createdAt"`
}

type RecoveryRecord struct {
	ID             string `json:"id"`
	ExtentID       string `json:"extentId,omitempty"`
	OperationID    string `json:"operationId,omitempty"`
	SegmentID      string `json:"segmentId,omitempty"`
	Kind           string `json:"kind"`
	RecordOffset   int64  `json:"recordOffset"`
	AvailableBytes int64  `json:"availableBytes"`
	PayloadOffset  int64  `json:"payloadOffset,omitempty"`
	PayloadLength  int64  `json:"payloadLength,omitempty"`
	LogicalOffset  int64  `json:"logicalOffset,omitempty"`
	CRC32          uint32 `json:"crc32,omitempty"`
	SHA256         string `json:"sha256,omitempty"`
	Detail         string `json:"detail"`
	CreatedAt      int64  `json:"createdAt"`
	UpdatedAt      int64  `json:"updatedAt"`
}

type Stats struct {
	Operations        int64 `json:"operations"`
	PendingOperations int64 `json:"pendingOperations"`
	RecoveryRecords   int64 `json:"recoveryRecords"`
	DirtyBytes        int64 `json:"dirtyBytes"`
	SegmentBytes      int64 `json:"segmentBytes"`
}

type BaseReaderAt interface {
	ReadAt([]byte, int64) (int, error)
}

type PayloadReader interface {
	io.Reader
	io.ReaderAt
	io.Closer
}
