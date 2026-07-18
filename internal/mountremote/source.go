package mountremote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mydearniko/idoud/internal/mountcore"
)

const (
	maximumGrantFetchAttempts = 3
	maximumGrantRetryWait     = 30 * time.Second
	minimumSpeculativeRead    = 128 << 10
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

	scheduleMu      sync.Mutex
	lastReadEnd     int64
	sequentialReads int
	prefetching     bool
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
	speculate := source.shouldSpeculate(offset, int64(len(requested)))
	written := 0
	for written < len(requested) {
		logicalOffset := offset + int64(written)
		blockOffset := logicalOffset / source.backend.readChunkSize * source.backend.readChunkSize
		blockLength := min(source.backend.readChunkSize, source.size-blockOffset)
		blockKey := cleanBlockKey{versionID: source.versionID, offset: blockOffset}
		if speculate {
			source.backend.blocks.whenReady(ctx, blockKey, func() {
				source.startPrefetch(blockOffset + blockLength)
			})
		}
		block, err := source.loadBlock(ctx, blockOffset, speculate)
		if err != nil {
			return written, err
		}
		withinBlock := logicalOffset - blockOffset
		available := int64(len(block)) - withinBlock
		if available < 1 {
			return written, ErrInvalidProtocol
		}
		amount := min(int64(len(requested)-written), available)
		copy(requested[written:written+int(amount)], block[withinBlock:withinBlock+amount])
		written += int(amount)
		if speculate {
			source.startPrefetch(blockOffset + int64(len(block)))
		}
	}
	if partialEOF {
		return written, io.EOF
	}
	return written, nil
}

func (source *remoteVersion) shouldSpeculate(offset int64, length int64) bool {
	source.scheduleMu.Lock()
	defer source.scheduleMu.Unlock()
	if offset == source.lastReadEnd && source.lastReadEnd > 0 {
		source.sequentialReads++
	} else {
		source.sequentialReads = 0
	}
	source.lastReadEnd = offset + length
	return length >= minimumSpeculativeRead || source.sequentialReads >= 2
}

func (source *remoteVersion) loadBlock(ctx context.Context, blockOffset int64, speculate bool) ([]byte, error) {
	blockLength := min(source.backend.readChunkSize, source.size-blockOffset)
	if blockOffset < 0 || blockLength < 1 {
		return nil, ErrInvalidProtocol
	}
	return source.backend.blocks.load(ctx, cleanBlockKey{
		versionID: source.versionID,
		offset:    blockOffset,
	}, func() ([]byte, error) {
		blockKey := cleanBlockKey{versionID: source.versionID, offset: blockOffset}
		release, err := source.backend.reads.acquire(ctx, blockLength)
		if err != nil {
			return nil, err
		}
		defer release()
		handle, err := source.usableHandle(ctx)
		if err != nil {
			return nil, err
		}
		data := make([]byte, blockLength)
		onExactReady := func() {
			source.backend.blocks.markReady(blockKey)
			if speculate {
				source.startPrefetch(blockOffset + blockLength)
			}
		}
		count, err := source.readExact(ctx, handle, data, blockOffset, onExactReady)
		if err != nil {
			return nil, err
		}
		if count != len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		return data, nil
	})
}

func (source *remoteVersion) startPrefetch(blockOffset int64) {
	if blockOffset < 0 || blockOffset >= source.size ||
		source.backend.negotiation.Scheduler.MaxSpeculativeLead < source.backend.readChunkSize {
		return
	}
	source.scheduleMu.Lock()
	if source.prefetching {
		source.scheduleMu.Unlock()
		return
	}
	source.prefetching = true
	source.scheduleMu.Unlock()
	go func() {
		_, _ = source.loadBlock(source.backend.ctx, blockOffset, false)
		source.scheduleMu.Lock()
		source.prefetching = false
		source.scheduleMu.Unlock()
	}()
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

func (source *remoteVersion) readExact(
	ctx context.Context,
	handle string,
	target []byte,
	offset int64,
	onExactReady func(),
) (int, error) {
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
	type fetchTask struct {
		target []byte
		url    string
		token  string
	}
	tasks := make([]fetchTask, 0, len(grant.Parts))
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
			tasks = append(tasks, fetchTask{target: segment, url: part.DataURL, token: part.GrantToken})
		}
		written += len(segment)
		cursor += part.Length
	}
	if written != len(target) || cursor != end {
		return written, ErrInvalidProtocol
	}
	if len(tasks) == 0 {
		if onExactReady != nil {
			onExactReady()
		}
		return written, nil
	}

	// Reserve capacity for the exact read before admitting speculative work.
	// The remaining grant parts still run concurrently within the negotiated
	// request and byte ceilings.
	fetchContext, cancel := context.WithCancel(ctx)
	defer cancel()
	firstRelease, err := source.backend.fetches.acquire(fetchContext, int64(len(tasks[0].target)))
	if err != nil {
		return 0, err
	}
	if onExactReady != nil {
		onExactReady()
	}
	var firstError error
	var firstErrorOnce sync.Once
	var wait sync.WaitGroup
	for index, task := range tasks {
		index, task := index, task
		wait.Add(1)
		go func() {
			defer wait.Done()
			release := firstRelease
			var fetchErr error
			if index != 0 {
				release, fetchErr = source.backend.fetches.acquire(fetchContext, int64(len(task.target)))
			}
			if fetchErr == nil {
				fetchErr = source.backend.fetchGrant(fetchContext, task.url, task.token, task.target)
				release()
			}
			if fetchErr != nil {
				firstErrorOnce.Do(func() {
					firstError = fetchErr
					cancel()
				})
			}
		}()
	}
	wait.Wait()
	if firstError != nil {
		return 0, firstError
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
	for attempt := 0; attempt < maximumGrantFetchAttempts; attempt++ {
		retry, delay, err := b.fetchGrantAttempt(ctx, parsed.String(), grantToken, target, attempt)
		if err == nil || !retry || attempt == maximumGrantFetchAttempts-1 {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ErrInvalidProtocol
}

func (b *Backend) fetchGrantAttempt(ctx context.Context, rawURL string, grantToken string, target []byte, attempt int) (bool, time.Duration, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return false, 0, err
	}
	request.Header.Set("Authorization", "Bearer "+grantToken)
	request.Header.Set("Accept", "application/octet-stream")
	request.Header.Set("Referrer-Policy", "no-referrer")
	if traceID := newTraceID(); traceID != "" {
		request.Header.Set("X-Idoud-Trace-ID", traceID)
	}
	response, err := b.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return false, 0, ctx.Err()
		}
		delay := grantRetryDelay("", b.clock(), attempt)
		return delay >= 0, delay, err
	}
	if response.StatusCode != http.StatusOK {
		_ = response.Body.Close()
		apiError := &APIError{Status: response.StatusCode, RetryAfter: response.Header.Get("Retry-After")}
		switch response.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
			apiError.Code = "blocked_auth"
			apiError.Message = "scoped data grant was rejected"
			return false, 0, apiError
		}
		apiError.Code = "data_unavailable"
		apiError.Message = "selected data node is temporarily unavailable"
		if grantStatusRetryable(response.StatusCode) {
			delay := grantRetryDelay(apiError.RetryAfter, b.clock(), attempt)
			return delay >= 0, delay, apiError
		}
		return false, 0, apiError
	}
	if length, present := parseContentLength(response.Header); !present || length != int64(len(target)) {
		_ = response.Body.Close()
		return false, 0, fmt.Errorf("%w: scoped data length", ErrInvalidProtocol)
	}
	read, err := io.ReadFull(response.Body, target)
	if err != nil {
		_ = response.Body.Close()
		if ctx.Err() != nil {
			return false, 0, ctx.Err()
		}
		delay := grantRetryDelay("", b.clock(), attempt)
		return delay >= 0, delay, err
	}
	var extra [1]byte
	count, extraErr := response.Body.Read(extra[:])
	_ = response.Body.Close()
	if count != 0 {
		return false, 0, ErrInvalidProtocol
	}
	if extraErr != nil && !errors.Is(extraErr, io.EOF) {
		if ctx.Err() != nil {
			return false, 0, ctx.Err()
		}
		delay := grantRetryDelay("", b.clock(), attempt)
		return delay >= 0, delay, extraErr
	}
	if read != len(target) {
		return false, 0, ErrInvalidProtocol
	}
	return false, 0, nil
}

func grantStatusRetryable(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooEarly ||
		status == http.StatusTooManyRequests ||
		(status >= http.StatusInternalServerError && status <= 599)
}

func grantRetryDelay(raw string, now time.Time, attempt int) time.Duration {
	delay := time.Duration(1<<min(attempt, 5)) * 100 * time.Millisecond
	raw = strings.TrimSpace(raw)
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil && seconds >= 0 {
		if seconds > int64(maximumGrantRetryWait/time.Second) {
			return -1
		}
		delay = time.Duration(seconds) * time.Second
	} else if deadline, err := http.ParseTime(raw); err == nil {
		delay = deadline.Sub(now)
		if delay < 0 {
			delay = 0
		}
	}
	if delay > maximumGrantRetryWait {
		return -1
	}
	return delay
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
