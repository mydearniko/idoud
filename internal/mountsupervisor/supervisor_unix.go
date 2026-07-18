//go:build !windows

package mountsupervisor

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mydearniko/idoud/internal/mountadapter"
)

const (
	supervisorSchemaVersion = 1
	maximumRegistryRecords  = 64
	maximumControlBytes     = 16 << 10
	maximumUnixSocketPath   = 100
)

type unixControl struct {
	record    Record
	session   mountadapter.Session
	listener  *net.UnixListener
	state     string
	directory string
	requests  chan struct{}

	closeOnce sync.Once
	closeErr  error
}

type controlRequest struct {
	Action string `json:"action"`
}

func Start(session mountadapter.Session) (Control, error) {
	if session == nil {
		return nil, errors.New("mount session is required")
	}
	directory, err := registryDirectory()
	if err != nil {
		return nil, err
	}
	registryLock, err := acquireRegistryLock(directory)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = syscall.Flock(int(registryLock.Fd()), syscall.LOCK_UN)
		_ = registryLock.Close()
	}()
	if err := ensureRegistryCapacity(directory); err != nil {
		return nil, err
	}
	mountID, err := randomMountID()
	if err != nil {
		return nil, err
	}
	status := session.Status()
	socketPath := filepath.Join(directory, mountID+".sock")
	statePath := filepath.Join(directory, mountID+".json")
	address := &net.UnixAddr{Name: socketPath, Net: "unix"}
	listener, err := net.ListenUnix("unix", address)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(socketPath)
		return nil, err
	}
	record := Record{
		SchemaVersion: supervisorSchemaVersion,
		MountID:       mountID, PID: os.Getpid(), Mountpoint: status.Mountpoint,
		Platform: status.Platform, ReadOnly: status.ReadOnly, SelectedNode: status.SelectedNode,
		ControlPath: socketPath, StartedAt: nowUnix(),
	}
	if err := createRecord(statePath, record); err != nil {
		_ = listener.Close()
		_ = os.Remove(socketPath)
		return nil, err
	}
	control := &unixControl{
		record: record, session: session, listener: listener,
		state: statePath, directory: directory, requests: make(chan struct{}, 8),
	}
	go control.serve()
	return control, nil
}

func (control *unixControl) Record() Record {
	if control == nil {
		return Record{}
	}
	return control.record
}

func (control *unixControl) Close() error {
	if control == nil {
		return nil
	}
	control.closeOnce.Do(func() {
		control.closeErr = control.listener.Close()
		if errors.Is(control.closeErr, net.ErrClosed) {
			control.closeErr = nil
		}
		control.closeErr = errors.Join(control.closeErr, removeOperational(control.state), removeOperational(control.record.ControlPath))
		control.closeErr = errors.Join(control.closeErr, syncOperationalDirectory(control.directory))
	})
	return control.closeErr
}

func (control *unixControl) serve() {
	for {
		connection, err := control.listener.AcceptUnix()
		if err != nil {
			return
		}
		select {
		case control.requests <- struct{}{}:
			go func() {
				defer func() { <-control.requests }()
				control.handle(connection)
			}()
		default:
			_ = connection.SetWriteDeadline(time.Now().Add(time.Second))
			_ = json.NewEncoder(connection).Encode(response{
				SchemaVersion: supervisorSchemaVersion, OK: false, ErrorCode: "busy", Timestamp: nowUnix(),
			})
			_ = connection.Close()
		}
	}
}

func (control *unixControl) handle(connection *net.UnixConn) {
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	var request controlRequest
	if decodeControlLine(connection, &request) != nil {
		_ = json.NewEncoder(connection).Encode(response{
			SchemaVersion: supervisorSchemaVersion, OK: false, ErrorCode: "invalid_request", Timestamp: nowUnix(),
		})
		return
	}
	snapshot := &Snapshot{Record: control.record, Status: control.session.Status()}
	result := response{SchemaVersion: supervisorSchemaVersion, OK: true, Snapshot: snapshot, Timestamp: nowUnix()}
	switch strings.TrimSpace(request.Action) {
	case "status":
	case "unmount":
		if err := control.session.Unmount(); err != nil {
			result.OK = false
			result.ErrorCode = "unmount_failed"
		}
	case "flush":
		result.OK = false
		result.ErrorCode = "read_only"
	default:
		result.OK = false
		result.ErrorCode = "invalid_request"
	}
	_ = json.NewEncoder(connection).Encode(result)
}

func List() ([]Snapshot, error) {
	directory, err := registryDirectory()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	if len(entries) > maximumRegistryRecords*2+4 {
		return nil, errors.New("mount supervisor registry exceeds its safety bound")
	}
	type candidate struct {
		statePath string
		record    Record
	}
	candidates := make([]candidate, 0, maximumRegistryRecords)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || len(candidates) >= maximumRegistryRecords {
			continue
		}
		statePath := filepath.Join(directory, entry.Name())
		record, readErr := readRecord(directory, statePath, entry.Name())
		if readErr != nil {
			continue
		}
		candidates = append(candidates, candidate{statePath: statePath, record: record})
	}
	resultChannel := make(chan Snapshot, len(candidates))
	var workers sync.WaitGroup
	slots := make(chan struct{}, 16)
	for _, item := range candidates {
		workers.Add(1)
		go func(item candidate) {
			defer workers.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			snapshot, callErr := call(item.record, "status")
			if callErr != nil {
				if !processAlive(item.record.PID) || pathMissing(item.record.ControlPath) {
					_ = removeOperational(item.statePath)
					_ = removeOperational(item.record.ControlPath)
				}
				return
			}
			resultChannel <- snapshot
		}(item)
	}
	workers.Wait()
	close(resultChannel)
	result := make([]Snapshot, 0, len(candidates))
	for snapshot := range resultChannel {
		result = append(result, snapshot)
	}
	sort.Slice(result, func(left int, right int) bool {
		if result[left].Record.Mountpoint == result[right].Record.Mountpoint {
			return result[left].Record.MountID < result[right].Record.MountID
		}
		return result[left].Record.Mountpoint < result[right].Record.Mountpoint
	})
	return result, nil
}

func Status(target string) ([]Snapshot, error) {
	return resolve(target)
}

func Unmount(target string) ([]Snapshot, error) {
	return controlTargets(target, "unmount")
}

func Flush(target string) ([]Snapshot, error) {
	return controlTargets(target, "flush")
}

func controlTargets(target string, action string) ([]Snapshot, error) {
	targets, err := resolve(target)
	if err != nil {
		return nil, err
	}
	result := make([]Snapshot, 0, len(targets))
	for _, target := range targets {
		snapshot, callErr := call(target.Record, action)
		if callErr != nil {
			return nil, callErr
		}
		result = append(result, snapshot)
	}
	return result, nil
}

func resolve(target string) ([]Snapshot, error) {
	snapshots, err := List()
	if err != nil {
		return nil, err
	}
	target = strings.TrimSpace(target)
	if target == "" {
		if len(snapshots) == 0 {
			return nil, ErrNotFound
		}
		return snapshots, nil
	}
	cleanTarget := filepath.Clean(target)
	matched := make([]Snapshot, 0, 1)
	for _, snapshot := range snapshots {
		if snapshot.Record.MountID == target || filepath.Clean(snapshot.Record.Mountpoint) == cleanTarget {
			matched = append(matched, snapshot)
		}
	}
	if len(matched) == 0 {
		return nil, ErrNotFound
	}
	return matched, nil
}

func call(record Record, action string) (Snapshot, error) {
	connection, err := net.DialTimeout("unix", record.ControlPath, 500*time.Millisecond)
	if err != nil {
		return Snapshot{}, err
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	if err := json.NewEncoder(connection).Encode(controlRequest{Action: action}); err != nil {
		return Snapshot{}, err
	}
	var result response
	if err := decodeControlLine(connection, &result); err != nil ||
		result.SchemaVersion != supervisorSchemaVersion || result.Snapshot == nil {
		return Snapshot{}, errors.New("mount supervisor returned an invalid response")
	}
	if !result.OK {
		if result.ErrorCode == "read_only" {
			return *result.Snapshot, ErrReadOnly
		}
		return *result.Snapshot, errors.New(result.ErrorCode)
	}
	return *result.Snapshot, nil
}

func registryDirectory() (string, error) {
	base, err := filepath.Abs(os.TempDir())
	if err != nil {
		return "", err
	}
	directoryName := "idoud-mounts-" + strconvUID(os.Getuid())
	path := filepath.Join(base, directoryName)
	if len(filepath.Join(path, strings.Repeat("0", 32)+".sock")) > maximumUnixSocketPath {
		path = filepath.Join("/tmp", directoryName)
	}
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return "", errors.New("mount supervisor runtime directory is unsafe")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Getuid()) {
		return "", errors.New("mount supervisor runtime directory has the wrong owner")
	}
	return path, nil
}

func ensureRegistryCapacity(directory string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	records := 0
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			statePath := filepath.Join(directory, entry.Name())
			record, readErr := readRecord(directory, statePath, entry.Name())
			if readErr == nil && (!processAlive(record.PID) || pathMissing(record.ControlPath)) {
				_ = removeOperational(statePath)
				_ = removeOperational(record.ControlPath)
				continue
			}
			records++
		}
	}
	if records >= maximumRegistryRecords {
		return errors.New("active mount supervisor limit is reached")
	}
	return nil
}

func acquireRegistryLock(directory string) (*os.File, error) {
	path := filepath.Join(directory, ".lock")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		_ = file.Close()
		return nil, errors.New("mount supervisor registry lock is unsafe")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Getuid()) {
		_ = file.Close()
		return nil, errors.New("mount supervisor registry lock has the wrong owner")
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func randomMountID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

func createRecord(path string, record Record) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(append(payload, '\n')); err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err == nil {
		err = syncOperationalDirectory(filepath.Dir(path))
	}
	return err
}

func readRecord(directory string, path string, name string) (Record, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > maximumControlBytes {
		return Record{}, errors.New("unsafe mount supervisor record")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Getuid()) {
		return Record{}, errors.New("mount supervisor record has the wrong owner")
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return Record{}, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	var record Record
	if decoder.Decode(&record) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		record.SchemaVersion != supervisorSchemaVersion ||
		record.MountID+".json" != name || record.PID < 1 || !filepath.IsAbs(record.Mountpoint) ||
		filepath.Dir(record.ControlPath) != directory || filepath.Base(record.ControlPath) != record.MountID+".sock" {
		return Record{}, errors.New("invalid mount supervisor record")
	}
	return record, nil
}

func processAlive(pid int) bool {
	if pid < 1 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func pathMissing(path string) bool {
	_, err := os.Lstat(path)
	return errors.Is(err, os.ErrNotExist)
}

func syncOperationalDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	if errors.Is(syncErr, syscall.EINVAL) || errors.Is(syncErr, syscall.ENOTSUP) {
		syncErr = nil
	}
	return errors.Join(syncErr, directory.Close())
}

func decodeControlLine(input io.Reader, target any) error {
	reader := bufio.NewReader(io.LimitReader(input, maximumControlBytes+1))
	payload, err := reader.ReadBytes('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if len(payload) == 0 || len(payload) > maximumControlBytes {
		return errors.New("mount supervisor control payload exceeds its safety bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("mount supervisor control payload contains trailing data")
	}
	return nil
}

func removeOperational(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func strconvUID(uid int) string {
	const digits = "0123456789"
	if uid == 0 {
		return "0"
	}
	value := make([]byte, 0, 10)
	for uid > 0 {
		value = append(value, digits[uid%10])
		uid /= 10
	}
	for left, right := 0, len(value)-1; left < right; left, right = left+1, right-1 {
		value[left], value[right] = value[right], value[left]
	}
	return string(value)
}
