package mountremote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/mydearniko/idoud/internal/mountcore"
)

type remoteVersion struct {
	backend    *Backend
	entryID    string
	versionID  string
	size       int64
	mtime      int64
	executable bool
	etag       string

	mu          sync.Mutex
	handleToken string
	expiresAt   int64
	closed      bool
}

type dataGrantResponse struct {
	SchemaVersion int    `json:"schemaVersion"`
	VersionID     string `json:"versionId"`
	Start         int64  `json:"start"`
	End           int64  `json:"end"`
	SelectedNode  struct {
		Name string `json:"name"`
		URL  string `json:"url"`
	} `json:"selectedNode"`
	Parts []struct {
		LogicalOffset int64  `json:"logicalOffset"`
		Length        int64  `json:"length"`
		Zero          bool   `json:"zero"`
		GrantToken    string `json:"grantToken"`
		DataURL       string `json:"dataUrl"`
		ExpiresAt     int64  `json:"expiresAt"`
	} `json:"parts"`
}

func (source *remoteVersion) Size() int64 {
	return source.size
}

func (source *remoteVersion) VersionMetadata() mountcore.VersionMetadata {
	return mountcore.VersionMetadata{
		VersionID: source.versionID, Size: source.size, Mtime: source.mtime,
		Executable: source.executable,
	}
}

func (source *remoteVersion) ReadAt(ctx context.Context, target []byte, offset int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, errors.New("negative remote mount read offset")
	}
	if len(target) == 0 {
		return 0, nil
	}
	if offset >= source.size {
		return 0, io.EOF
	}
	requested := target
	partialEOF := false
	if remaining := source.size - offset; int64(len(requested)) > remaining {
		requested = requested[:int(remaining)]
		partialEOF = true
	}
	written := 0
	for written < len(requested) {
		chunkLength := min(int64(len(requested)-written), source.backend.readChunkSize)
		release, err := source.backend.reads.acquire(ctx, chunkLength)
		if err != nil {
			return written, err
		}
		handle, err := source.usableHandle(ctx)
		if err == nil {
			var count int
			count, err = source.readExact(
				ctx,
				handle,
				requested[written:written+int(chunkLength)],
				offset+int64(written),
			)
			written += count
		}
		release()
		if err != nil {
			return written, err
		}
	}
	if partialEOF {
		return written, io.EOF
	}
	return written, nil
}

func (source *remoteVersion) usableHandle(ctx context.Context) (string, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed || source.handleToken == "" {
		return "", mountcore.ErrHandleClosed
	}
	if err := source.refreshIfNeededLocked(ctx); err != nil {
		return "", err
	}
	return source.handleToken, nil
}

func (source *remoteVersion) refreshIfNeededLocked(ctx context.Context) error {
	now := source.backend.clock()
	lead := source.backend.negotiation.HandleTTL / 3
	if lead < 30*time.Second {
		lead = 30 * time.Second
	}
	if now.Add(lead).Unix() < source.expiresAt {
		return nil
	}
	var response struct {
		SchemaVersion int `json:"schemaVersion"`
		Handle        struct {
			EntryID   string `json:"entryId"`
			VersionID string `json:"versionId"`
			ExpiresAt int64  `json:"expiresAt"`
			State     string `json:"state"`
		} `json:"handle"`
	}
	path := "/v1/folders/" + url.PathEscape(source.backend.shareID) + "/entries/" +
		url.PathEscape(source.entryID) + "/open/refresh"
	if err := source.backend.sessionJSON(ctx, source.handleToken, http.MethodPost, path, []byte(`{}`), &response); err != nil {
		return err
	}
	if response.SchemaVersion != protocolVersion || response.Handle.EntryID != source.entryID ||
		response.Handle.VersionID != source.versionID || response.Handle.State != "open" ||
		response.Handle.ExpiresAt <= now.Unix() || response.Handle.ExpiresAt > source.backend.negotiation.SessionExpiry {
		return ErrInvalidProtocol
	}
	source.expiresAt = response.Handle.ExpiresAt
	return nil
}

func (source *remoteVersion) readExact(ctx context.Context, handle string, target []byte, offset int64) (int, error) {
	end := offset + int64(len(target))
	body, _ := json.Marshal(map[string]int64{"start": offset, "end": end})
	var grant dataGrantResponse
	path := "/v1/folders/" + url.PathEscape(source.backend.shareID) + "/entries/" +
		url.PathEscape(source.entryID) + "/data-grants"
	if err := source.backend.sessionJSON(ctx, handle, http.MethodPost, path, body, &grant); err != nil {
		return 0, err
	}
	if grant.SchemaVersion != protocolVersion || grant.VersionID != source.versionID ||
		grant.Start != offset || grant.End != end || len(grant.Parts) == 0 ||
		strings.TrimSpace(grant.SelectedNode.Name) != source.backend.negotiation.SelectedNode {
		return 0, ErrInvalidProtocol
	}
	cursor := offset
	written := 0
	for _, part := range grant.Parts {
		if part.LogicalOffset != cursor || part.Length < 1 || part.Length > int64(len(target)-written) {
			return written, ErrInvalidProtocol
		}
		segment := target[written : written+int(part.Length)]
		if part.Zero {
			if part.GrantToken != "" || part.DataURL != "" {
				return written, ErrInvalidProtocol
			}
			clear(segment)
		} else {
			if !validCapabilityText(part.GrantToken, 32) || strings.TrimSpace(part.DataURL) == "" ||
				part.ExpiresAt <= source.backend.clock().Unix() ||
				part.ExpiresAt > source.backend.negotiation.SessionExpiry {
				return written, ErrInvalidProtocol
			}
			if err := source.backend.fetchGrant(ctx, part.DataURL, part.GrantToken, segment); err != nil {
				return written, err
			}
		}
		written += len(segment)
		cursor += part.Length
	}
	if written != len(target) || cursor != end {
		return written, ErrInvalidProtocol
	}
	return written, nil
}

func (b *Backend) fetchGrant(ctx context.Context, rawURL string, grantToken string, target []byte) error {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		parsed.Path != "/internal/v1/folder-data" {
		return ErrInvalidProtocol
	}
	origin := parsed.Scheme + "://" + parsed.Host
	if origin != b.selectedNodeOrigin || (parsed.Scheme != "https" && !(b.allowHTTP && parsed.Scheme == "http")) {
		return ErrInvalidProtocol
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+grantToken)
	request.Header.Set("Accept", "application/octet-stream")
	request.Header.Set("Referrer-Policy", "no-referrer")
	if traceID := newTraceID(); traceID != "" {
		request.Header.Set("X-Idoud-Trace-ID", traceID)
	}
	response, err := b.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return &APIError{Status: response.StatusCode, Code: "blocked_auth", Message: "scoped data grant was rejected"}
	}
	if length, present := parseContentLength(response.Header); !present || length != int64(len(target)) {
		return fmt.Errorf("%w: scoped data length", ErrInvalidProtocol)
	}
	read, err := io.ReadFull(response.Body, target)
	if err != nil {
		return err
	}
	var extra [1]byte
	if count, extraErr := response.Body.Read(extra[:]); count != 0 || (extraErr != nil && !errors.Is(extraErr, io.EOF)) {
		return ErrInvalidProtocol
	}
	if read != len(target) {
		return ErrInvalidProtocol
	}
	return nil
}

func (source *remoteVersion) Close() error {
	if source == nil {
		return nil
	}
	source.backend.mu.RLock()
	token := source.backend.sessionToken
	closed := source.backend.closed
	source.backend.mu.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if closed {
		token = ""
	}
	return source.closeWithSession(ctx, token)
}

func (source *remoteVersion) closeWithSession(ctx context.Context, sessionToken string) error {
	source.mu.Lock()
	if source.closed {
		source.mu.Unlock()
		return nil
	}
	source.closed = true
	handle := source.handleToken
	source.handleToken = ""
	source.mu.Unlock()
	source.backend.unregister(source)
	if strings.TrimSpace(sessionToken) == "" || strings.TrimSpace(handle) == "" {
		return nil
	}
	path := "/v1/folders/" + url.PathEscape(source.backend.shareID) + "/entries/" +
		url.PathEscape(source.entryID) + "/open/close"
	var response struct {
		SchemaVersion int  `json:"schemaVersion"`
		Closed        bool `json:"closed"`
	}
	err := requestJSON(ctx, source.backend.client, source.backend.baseURL, sessionToken, handle,
		http.MethodPost, path, []byte(`{}`), &response, sessionToken, handle)
	if err != nil {
		return err
	}
	if response.SchemaVersion != protocolVersion || !response.Closed {
		return ErrInvalidProtocol
	}
	return nil
}
