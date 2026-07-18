package mountadapter

import (
	"context"
	"errors"
	"time"

	"github.com/mydearniko/idoud/internal/mountcore"
	"github.com/mydearniko/idoud/internal/mountremote"
)

var (
	ErrUnsupported         = errors.New("native mount adapter is unavailable on this platform build")
	ErrBridgeMissing       = errors.New("native mount bridge is unavailable")
	ErrMountpointInvalid   = errors.New("native mountpoint is invalid")
	ErrMMapUnsupported     = errors.New("native mount kernel cannot preserve immutable mmap semantics")
	ErrInvalidationMissing = errors.New("native mount kernel does not support required invalidation notifications")
)

type Options struct {
	Mountpoint string
	Debug      bool
}

type Status struct {
	Platform        string    `json:"platform"`
	Mountpoint      string    `json:"mountpoint"`
	ReadOnly        bool      `json:"readOnly"`
	Sequence        int64     `json:"sequence"`
	State           string    `json:"state"`
	SelectedNode    string    `json:"selectedNode"`
	MMapSupported   bool      `json:"mmapSupported"`
	LastChangeAt    time.Time `json:"lastChangeAt,omitempty"`
	LastChangeError string    `json:"lastChangeError,omitempty"`
}

type Session interface {
	Wait()
	Unmount() error
	Status() Status
}

func Mount(ctx context.Context, core *mountcore.Core, remote *mountremote.Backend, options Options) (Session, error) {
	return mountPlatform(ctx, core, remote, options)
}
