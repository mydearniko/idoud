package mountsupervisor

import (
	"errors"
	"time"

	"github.com/mydearniko/idoud/internal/mountadapter"
)

var (
	ErrUnsupported = errors.New("local mount supervisor is unavailable on this platform")
	ErrNotFound    = errors.New("active mount supervisor was not found")
	ErrReadOnly    = errors.New("remote flush is not applicable to a read-only mount")
)

type Record struct {
	SchemaVersion int    `json:"schemaVersion"`
	MountID       string `json:"mountId"`
	PID           int    `json:"pid"`
	Mountpoint    string `json:"mountpoint"`
	Platform      string `json:"platform"`
	ReadOnly      bool   `json:"readOnly"`
	SelectedNode  string `json:"selectedNode"`
	ControlPath   string `json:"controlPath"`
	StartedAt     int64  `json:"startedAt"`
}

type Snapshot struct {
	Record Record              `json:"mount"`
	Status mountadapter.Status `json:"status"`
}

type Control interface {
	Record() Record
	Close() error
}

type response struct {
	SchemaVersion int       `json:"schemaVersion"`
	OK            bool      `json:"ok"`
	ErrorCode     string    `json:"errorCode,omitempty"`
	Snapshot      *Snapshot `json:"snapshot,omitempty"`
	Timestamp     int64     `json:"timestamp"`
}

func nowUnix() int64 { return time.Now().Unix() }
