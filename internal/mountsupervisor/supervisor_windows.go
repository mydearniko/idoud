//go:build windows

package mountsupervisor

import "github.com/mydearniko/idoud/internal/mountadapter"

type unsupportedControl struct{}

func Start(mountadapter.Session) (Control, error) { return nil, ErrUnsupported }

func (unsupportedControl) Record() Record { return Record{} }
func (unsupportedControl) Close() error   { return nil }

func List() ([]Snapshot, error)          { return nil, ErrUnsupported }
func Status(string) ([]Snapshot, error)  { return nil, ErrUnsupported }
func Unmount(string) ([]Snapshot, error) { return nil, ErrUnsupported }
func Flush(string) ([]Snapshot, error)   { return nil, ErrUnsupported }
