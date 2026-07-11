package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const uploadResumeStateVersion = 1
const uploadResumeStateLimit = 128

type uploadResumeRecord struct {
	UploadKey string `json:"uploadKey"`
	UpdatedAt int64  `json:"updatedAt"`
}

type uploadResumeState struct {
	Version int                           `json:"version"`
	Records map[string]uploadResumeRecord `json:"records"`
}

func uploadResumeStatePath() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil || strings.TrimSpace(base) == "" {
		return "", fmt.Errorf("resolve resume state directory: %w", err)
	}
	return filepath.Join(base, "idoud", "upload-resume.json"), nil
}

func uploadResumeIdentity(opts options, src *sourceFile) (string, error) {
	if src == nil || src.fromStdin || !src.knownSize || strings.TrimSpace(src.displayName) == "" {
		return "", nil
	}
	absPath, err := filepath.Abs(src.displayName)
	if err != nil {
		return "", err
	}
	passwordSum := sha256.Sum256([]byte(opts.password))
	parts := []string{
		strings.TrimSpace(opts.serverURL),
		filepath.Clean(absPath),
		src.uploadName,
		strconv.FormatInt(src.size, 10),
		strconv.FormatInt(src.modTimeUnixNano, 10),
		strconv.FormatInt(opts.downloadLimit, 10),
		hex.EncodeToString(passwordSum[:]),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:]), nil
}

func loadUploadResumeState(path string) uploadResumeState {
	state := uploadResumeState{Version: uploadResumeStateVersion, Records: make(map[string]uploadResumeRecord)}
	data, err := os.ReadFile(path)
	if err != nil {
		return state
	}
	if json.Unmarshal(data, &state) != nil || state.Version != uploadResumeStateVersion || state.Records == nil {
		return uploadResumeState{Version: uploadResumeStateVersion, Records: make(map[string]uploadResumeRecord)}
	}
	return state
}

func saveUploadResumeState(path string, state uploadResumeState) error {
	if state.Records == nil {
		state.Records = make(map[string]uploadResumeRecord)
	}
	if len(state.Records) > uploadResumeStateLimit {
		type pair struct {
			id string
			at int64
		}
		ordered := make([]pair, 0, len(state.Records))
		for id, rec := range state.Records {
			ordered = append(ordered, pair{id: id, at: rec.UpdatedAt})
		}
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].at < ordered[j].at })
		for len(state.Records) > uploadResumeStateLimit {
			delete(state.Records, ordered[0].id)
			ordered = ordered[1:]
		}
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return replaceFilePath(tmp, path)
}

func replaceFilePath(source string, target string) error {
	if err := os.Rename(source, target); err == nil {
		return nil
	}
	if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(source, target)
}

func configureUploadResume(opts *options, src *sourceFile) (string, error) {
	if opts == nil || src == nil || opts.speedtest || opts.uploadKeyExplicit {
		return "", nil
	}
	id, err := uploadResumeIdentity(*opts, src)
	if err != nil || id == "" {
		return "", err
	}
	path, err := uploadResumeStatePath()
	if err != nil {
		return "", err
	}
	state := loadUploadResumeState(path)
	rec := state.Records[id]
	if strings.TrimSpace(rec.UploadKey) == "" {
		rec.UploadKey = randomUploadKey()
	}
	rec.UpdatedAt = time.Now().Unix()
	state.Records[id] = rec
	if err := saveUploadResumeState(path, state); err != nil {
		return "", fmt.Errorf("save upload resume state: %w", err)
	}
	opts.uploadKey = rec.UploadKey
	return id, nil
}

func completeUploadResume(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	path, err := uploadResumeStatePath()
	if err != nil {
		return err
	}
	state := loadUploadResumeState(path)
	delete(state.Records, id)
	return saveUploadResumeState(path, state)
}
