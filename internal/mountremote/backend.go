package mountremote

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
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
	"unicode"
	"unicode/utf8"

	"github.com/mydearniko/idoud/internal/mountcore"
)

const (
	protocolVersion       = 1
	maximumJSONBytes      = 8 << 20
	maximumListingPages   = 10_000
	defaultMaximumEntries = 1_000_000
	mountHandleHeader     = "X-Idoud-Mount-Handle"
)

var (
	ErrClosed                  = errors.New("remote mount backend is closed")
	ErrInvalidProtocol         = errors.New("remote mount protocol response is invalid")
	ErrProtocolUpgradeRequired = errors.New("remote mount protocol upgrade is required")
	ErrResetRequired           = errors.New("remote mount namespace reset is required")
	ErrBlockedAuth             = errors.New("remote mount authorization is blocked")
	ErrQuarantined             = errors.New("remote mount content is quarantined")
	ErrWriteUnsupported        = errors.New("writable remote mounting is not enabled in this build")
)

type Config struct {
	BaseURL      string
	ShareID      string
	SessionToken string
	DeviceLabel  string
	Write        bool
	AllowHTTP    bool
	Client       *http.Client
	Clock        func() time.Time
}

type SchedulerPlan struct {
	MaxInflightRequests  int
	MaxInflightBytes     int64
	RecommendedBlockSize int64
	MaxSpeculativeLead   int64
	ReplicationFactor    int
}

type Negotiation struct {
	ShareID       string
	FolderName    string
	RootEntryID   string
	Sequence      int64
	ReadPolicy    string
	SelectedNode  string
	SelectedURL   string
	SessionKind   string
	SessionExpiry int64
	HandleTTL     time.Duration
	Scheduler     SchedulerPlan
}

type Backend struct {
	baseURL            string
	shareID            string
	client             *http.Client
	clock              func() time.Time
	allowHTTP          bool
	selectedNodeOrigin string
	negotiation        Negotiation
	root               mountcore.Entry
	maximumEntries     int
	readChunkSize      int64
	reads              *readLimiter
	blocks             *cleanBlockCache

	mu              sync.RWMutex
	sessionToken    string
	currentSequence int64
	closed          bool
	sources         map[*remoteVersion]struct{}
}

type APIError struct {
	Status     int
	Code       string
	Message    string
	RetryAfter string
}

func (e *APIError) Error() string {
	if e == nil {
		return "remote mount request failed"
	}
	if e.Code != "" && e.Message != "" {
		return e.Code + ": " + e.Message
	}
	if e.Code != "" {
		return e.Code
	}
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("remote mount server returned HTTP %d", e.Status)
}

func (e *APIError) Is(target error) bool {
	if e == nil {
		return false
	}
	switch target {
	case ErrProtocolUpgradeRequired:
		return e.Code == "protocol_upgrade_required" || e.Code == "bridge_missing"
	case ErrResetRequired:
		return e.Code == "reset_required" || e.Code == "revision_conflict"
	case ErrBlockedAuth:
		return e.Code == "blocked_auth" || e.Code == "auth_required" ||
			e.Code == "write_capability_required" || e.Code == "read_password_invalid"
	case ErrQuarantined:
		return e.Code == "quarantined"
	}
	return false
}

type descriptorResponse struct {
	SchemaVersion int `json:"schemaVersion"`
	Folder        struct {
		ShareID     string `json:"shareId"`
		Name        string `json:"name"`
		RootEntryID string `json:"rootEntryId"`
		Sequence    int64  `json:"sequence"`
		ReadPolicy  string `json:"readPolicy"`
		State       string `json:"state"`
		Permitted   struct {
			Browse    bool `json:"browse"`
			Download  bool `json:"download"`
			MountRead bool `json:"mountRead"`
		} `json:"permittedActions"`
		Limits struct {
			MaxActiveEntries int64 `json:"maxActiveEntries"`
		} `json:"limits"`
	} `json:"folder"`
}

type mountSessionResponse struct {
	SchemaVersion int    `json:"schemaVersion"`
	SessionToken  string `json:"sessionToken"`
	Session       struct {
		Kind      string `json:"kind"`
		ExpiresAt int64  `json:"expiresAt"`
		Write     bool   `json:"write"`
	} `json:"session"`
	SelectedNode struct {
		Name string `json:"name"`
		URL  string `json:"url"`
	} `json:"selectedNode"`
	SchedulerPlan struct {
		MaxInflightRequests  int   `json:"maxInflightRequests"`
		MaxInflightBytes     int64 `json:"maxInflightBytes"`
		RecommendedBlockSize int64 `json:"recommendedBlockSize"`
		MaxSpeculativeLead   int64 `json:"maxSpeculativeLead"`
		ReplicationFactor    int   `json:"replicationFactor"`
	} `json:"schedulerPlan"`
	Capabilities struct {
		ImmutableOpenHandles bool   `json:"immutableOpenHandles"`
		OpenHandleHeader     string `json:"openHandleHeader"`
		OpenHandleTTLSeconds int64  `json:"openHandleTTLSeconds"`
		ScopedDataGrants     bool   `json:"scopedDataGrants"`
	} `json:"capabilities"`
}

type entryPayload struct {
	ID               string `json:"id"`
	ParentID         string `json:"parentId"`
	Name             string `json:"name"`
	Kind             string `json:"kind"`
	VersionID        string `json:"versionId"`
	LogicalSize      *int64 `json:"logicalSize"`
	EntryRevision    int64  `json:"entryRevision"`
	ChildSetRevision int64  `json:"childSetRevision"`
	State            string `json:"state"`
	Visibility       string `json:"visibility"`
	Mtime            int64  `json:"mtime"`
	Executable       bool   `json:"executable"`
}

type listingResponse struct {
	SchemaVersion int            `json:"schemaVersion"`
	Sequence      int64          `json:"sequence"`
	Parent        entryPayload   `json:"parent"`
	Entries       []entryPayload `json:"entries"`
	NextCursor    string         `json:"nextCursor"`
}

type openHandleResponse struct {
	SchemaVersion int    `json:"schemaVersion"`
	HandleToken   string `json:"handleToken"`
	Handle        struct {
		EntryID     string `json:"entryId"`
		VersionID   string `json:"versionId"`
		LogicalSize int64  `json:"logicalSize"`
		Mtime       int64  `json:"mtime"`
		Executable  bool   `json:"executable"`
		ETag        string `json:"etag"`
		ContentHash string `json:"contentHash"`
		State       string `json:"state"`
		ExpiresAt   int64  `json:"expiresAt"`
	} `json:"handle"`
}

func New(ctx context.Context, config Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config.Write {
		return nil, ErrWriteUnsupported
	}
	baseURL, _, err := normalizeOrigin(config.BaseURL, config.AllowHTTP)
	if err != nil {
		return nil, err
	}
	shareID := strings.TrimSpace(config.ShareID)
	if !validCapabilityText(shareID, 32) {
		return nil, errors.New("remote mount share id is invalid")
	}
	clock := config.Clock
	if clock == nil {
		clock = time.Now
	}
	client := secureHTTPClient(config.Client)
	ordinarySession := strings.TrimSpace(config.SessionToken)
	if ordinarySession != "" && !validCapabilityText(ordinarySession, 32) {
		return nil, errors.New("remote mount session token is invalid")
	}
	var descriptor descriptorResponse
	if err := requestJSON(ctx, client, baseURL, ordinarySession, "", http.MethodGet,
		"/v1/folders/"+url.PathEscape(shareID), nil, &descriptor, ordinarySession); err != nil {
		return nil, err
	}
	if descriptor.SchemaVersion != protocolVersion || descriptor.Folder.ShareID != shareID ||
		strings.TrimSpace(descriptor.Folder.RootEntryID) == "" || descriptor.Folder.Sequence < 1 ||
		descriptor.Folder.State != "active" || !descriptor.Folder.Permitted.Browse ||
		!descriptor.Folder.Permitted.Download || !descriptor.Folder.Permitted.MountRead ||
		(descriptor.Folder.ReadPolicy != "public" && descriptor.Folder.ReadPolicy != "password") ||
		descriptor.Folder.Limits.MaxActiveEntries < 1 ||
		descriptor.Folder.Limits.MaxActiveEntries > defaultMaximumEntries {
		return nil, ErrInvalidProtocol
	}
	deviceLabel := strings.TrimSpace(config.DeviceLabel)
	if deviceLabel == "" {
		deviceLabel = "idoud native mount"
	}
	if !validDisplayText(deviceLabel, 128) {
		return nil, errors.New("remote mount device label is invalid")
	}
	body, _ := json.Marshal(map[string]any{"write": false, "deviceLabel": deviceLabel})
	var mounted mountSessionResponse
	if err := requestJSON(ctx, client, baseURL, ordinarySession, "", http.MethodPost,
		"/v1/folders/"+url.PathEscape(shareID)+"/mount-sessions", body, &mounted, ordinarySession); err != nil {
		return nil, err
	}
	nodeOrigin, _, err := normalizeOrigin(mounted.SelectedNode.URL, config.AllowHTTP)
	if err != nil {
		return nil, fmt.Errorf("%w: selected node URL", ErrInvalidProtocol)
	}
	if mounted.SchemaVersion != protocolVersion || !validCapabilityText(mounted.SessionToken, 32) ||
		mounted.Session.Kind != "mount_read" || mounted.Session.Write ||
		mounted.Session.ExpiresAt <= clock().Unix() || !validDisplayText(mounted.SelectedNode.Name, 128) ||
		!mounted.Capabilities.ImmutableOpenHandles || !mounted.Capabilities.ScopedDataGrants ||
		http.CanonicalHeaderKey(mounted.Capabilities.OpenHandleHeader) != mountHandleHeader ||
		mounted.Capabilities.OpenHandleTTLSeconds < 60 ||
		!validSchedulerPlan(mounted) {
		return nil, ErrInvalidProtocol
	}
	maximumEntries := int(descriptor.Folder.Limits.MaxActiveEntries)
	negotiation := Negotiation{
		ShareID: shareID, FolderName: descriptor.Folder.Name,
		RootEntryID: descriptor.Folder.RootEntryID, Sequence: descriptor.Folder.Sequence,
		ReadPolicy: descriptor.Folder.ReadPolicy, SelectedNode: mounted.SelectedNode.Name,
		SelectedURL: nodeOrigin, SessionKind: mounted.Session.Kind,
		SessionExpiry: mounted.Session.ExpiresAt,
		HandleTTL:     time.Duration(mounted.Capabilities.OpenHandleTTLSeconds) * time.Second,
		Scheduler: SchedulerPlan{
			MaxInflightRequests:  mounted.SchedulerPlan.MaxInflightRequests,
			MaxInflightBytes:     mounted.SchedulerPlan.MaxInflightBytes,
			RecommendedBlockSize: mounted.SchedulerPlan.RecommendedBlockSize,
			MaxSpeculativeLead:   mounted.SchedulerPlan.MaxSpeculativeLead,
			ReplicationFactor:    mounted.SchedulerPlan.ReplicationFactor,
		},
	}
	backend := &Backend{
		baseURL: baseURL, shareID: shareID, client: client, clock: clock,
		allowHTTP: config.AllowHTTP, selectedNodeOrigin: nodeOrigin,
		negotiation: negotiation, maximumEntries: maximumEntries,
		readChunkSize: min(mounted.SchedulerPlan.RecommendedBlockSize, mounted.SchedulerPlan.MaxInflightBytes),
		reads: newReadLimiter(
			mounted.SchedulerPlan.MaxInflightRequests,
			mounted.SchedulerPlan.MaxInflightBytes,
		),
		blocks:       newCleanBlockCache(mounted.SchedulerPlan.MaxInflightBytes),
		sessionToken: mounted.SessionToken, currentSequence: descriptor.Folder.Sequence,
		sources: make(map[*remoteVersion]struct{}),
	}
	backend.root = mountcore.Entry{
		ID: descriptor.Folder.RootEntryID, Kind: mountcore.KindRoot, Size: 0,
		EntryRevision: 0, ChildSetRevision: 0,
	}
	return backend, nil
}

func validSchedulerPlan(response mountSessionResponse) bool {
	plan := response.SchedulerPlan
	return plan.MaxInflightRequests > 0 && plan.MaxInflightRequests <= 256 &&
		plan.MaxInflightBytes > 0 && plan.MaxInflightBytes <= 1<<30 &&
		plan.RecommendedBlockSize > 0 && plan.RecommendedBlockSize <= 64<<20 &&
		plan.RecommendedBlockSize <= plan.MaxInflightBytes &&
		plan.MaxSpeculativeLead >= 0 && plan.MaxSpeculativeLead <= 1<<30 &&
		(plan.ReplicationFactor == 1 || plan.ReplicationFactor == 2)
}

func (b *Backend) Negotiation() Negotiation {
	if b == nil {
		return Negotiation{}
	}
	return b.negotiation
}

func (b *Backend) Root(ctx context.Context) (mountcore.Entry, int64, error) {
	if err := ctx.Err(); err != nil {
		return mountcore.Entry{}, 0, err
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return mountcore.Entry{}, 0, ErrClosed
	}
	return b.root, b.currentSequence, nil
}

func (b *Backend) List(ctx context.Context, parentID string) (mountcore.Listing, error) {
	if err := ctx.Err(); err != nil {
		return mountcore.Listing{}, err
	}
	parentID = strings.TrimSpace(parentID)
	if parentID == "" {
		return mountcore.Listing{}, mountcore.ErrNotFound
	}
	cursor := ""
	seenCursors := make(map[string]struct{})
	entries := make([]mountcore.Entry, 0, 128)
	var parent mountcore.Entry
	var sequence int64
	for pageNumber := 0; pageNumber < maximumListingPages; pageNumber++ {
		query := url.Values{}
		query.Set("parent", parentID)
		query.Set("limit", "1000")
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		var page listingResponse
		if err := b.sessionJSON(ctx, "", http.MethodGet,
			"/v1/folders/"+url.PathEscape(b.shareID)+"/entries?"+query.Encode(), nil, &page); err != nil {
			return mountcore.Listing{}, err
		}
		if page.SchemaVersion != protocolVersion || page.Sequence < 1 || page.Parent.ID != parentID {
			return mountcore.Listing{}, ErrInvalidProtocol
		}
		mappedParent, err := mapEntry(page.Parent, b.root.ID)
		if err != nil || (mappedParent.Kind != mountcore.KindRoot && mappedParent.Kind != mountcore.KindDirectory) {
			return mountcore.Listing{}, ErrInvalidProtocol
		}
		if pageNumber == 0 {
			sequence = page.Sequence
			parent = mappedParent
		} else if page.Sequence != sequence || mappedParent.ID != parent.ID || mappedParent.Kind != parent.Kind {
			return mountcore.Listing{}, ErrInvalidProtocol
		}
		for _, item := range page.Entries {
			entry, err := mapEntry(item, b.root.ID)
			if err != nil || entry.ParentID != parentID {
				return mountcore.Listing{}, ErrInvalidProtocol
			}
			entries = append(entries, entry)
			if len(entries) > b.maximumEntries {
				return mountcore.Listing{}, fmt.Errorf("%w: listing exceeds negotiated entry limit", ErrInvalidProtocol)
			}
		}
		next := strings.TrimSpace(page.NextCursor)
		if next == "" {
			b.recordSequence(sequence)
			return mountcore.Listing{Parent: parent, Entries: entries, Sequence: sequence}, nil
		}
		if _, repeated := seenCursors[next]; repeated {
			return mountcore.Listing{}, ErrInvalidProtocol
		}
		seenCursors[next] = struct{}{}
		cursor = next
	}
	return mountcore.Listing{}, fmt.Errorf("%w: listing page bound exceeded", ErrInvalidProtocol)
}

func mapEntry(item entryPayload, rootID string) (mountcore.Entry, error) {
	entry := mountcore.Entry{
		ID: strings.TrimSpace(item.ID), ParentID: strings.TrimSpace(item.ParentID), Name: item.Name,
		Kind: strings.TrimSpace(item.Kind), VersionID: strings.TrimSpace(item.VersionID),
		Mtime: item.Mtime, Executable: item.Executable,
		EntryRevision: item.EntryRevision, ChildSetRevision: item.ChildSetRevision,
	}
	if entry.ID == "" || item.State != "active" || (item.Visibility != "" && item.Visibility != "public") {
		return mountcore.Entry{}, ErrInvalidProtocol
	}
	switch entry.Kind {
	case mountcore.KindRoot:
		if entry.ID != rootID {
			return mountcore.Entry{}, ErrInvalidProtocol
		}
		entry.ParentID, entry.Name, entry.Size = "", "", 0
	case mountcore.KindDirectory:
		entry.Size = 0
	case mountcore.KindFile:
		if item.LogicalSize == nil || *item.LogicalSize < 0 || (*item.LogicalSize > 0 && entry.VersionID == "") {
			return mountcore.Entry{}, ErrInvalidProtocol
		}
		entry.Size = *item.LogicalSize
	default:
		return mountcore.Entry{}, ErrInvalidProtocol
	}
	return entry, nil
}

func (b *Backend) OpenVersion(ctx context.Context, entry mountcore.Entry) (mountcore.VersionSource, error) {
	if entry.Kind != mountcore.KindFile || strings.TrimSpace(entry.ID) == "" {
		return nil, mountcore.ErrNotFile
	}
	var response openHandleResponse
	path := "/v1/folders/" + url.PathEscape(b.shareID) + "/entries/" + url.PathEscape(entry.ID) + "/open"
	if err := b.sessionJSON(ctx, "", http.MethodPost, path, []byte(`{}`), &response); err != nil {
		return nil, err
	}
	now := b.clock().Unix()
	if response.SchemaVersion != protocolVersion || !validCapabilityText(response.HandleToken, 32) ||
		response.Handle.EntryID != entry.ID || response.Handle.LogicalSize < 0 ||
		(response.Handle.LogicalSize > 0 && strings.TrimSpace(response.Handle.VersionID) == "") ||
		strings.TrimSpace(response.Handle.ETag) == "" || response.Handle.State != "open" ||
		response.Handle.ExpiresAt <= now || response.Handle.ExpiresAt > b.negotiation.SessionExpiry {
		return nil, ErrInvalidProtocol
	}
	source := &remoteVersion{
		backend: b, entryID: entry.ID, versionID: response.Handle.VersionID,
		size: response.Handle.LogicalSize, mtime: response.Handle.Mtime,
		executable: response.Handle.Executable, etag: response.Handle.ETag,
		handleToken: response.HandleToken, expiresAt: response.Handle.ExpiresAt,
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = source.closeWithSession(context.Background(), "")
		return nil, ErrClosed
	}
	b.sources[source] = struct{}{}
	b.mu.Unlock()
	return source, nil
}

func (b *Backend) Close() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	token := b.sessionToken
	b.sessionToken = ""
	b.reads.close()
	b.blocks.close()
	sources := make([]*remoteVersion, 0, len(b.sources))
	for source := range b.sources {
		sources = append(sources, source)
	}
	b.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var result error
	for _, source := range sources {
		result = errors.Join(result, source.closeWithSession(ctx, token))
	}
	return result
}

func (b *Backend) unregister(source *remoteVersion) {
	b.mu.Lock()
	delete(b.sources, source)
	b.mu.Unlock()
}

func (b *Backend) recordSequence(sequence int64) {
	b.mu.Lock()
	if sequence > b.currentSequence {
		b.currentSequence = sequence
	}
	b.mu.Unlock()
}

func (b *Backend) sessionJSON(ctx context.Context, handle string, method string, path string, body []byte, out any) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrClosed
	}
	token := b.sessionToken
	b.mu.RUnlock()
	if token == "" {
		return ErrClosed
	}
	return requestJSON(ctx, b.client, b.baseURL, token, handle, method, path, body, out, token, handle)
}

func requestJSON(ctx context.Context, client *http.Client, baseURL string, token string, handle string, method string, path string, body []byte, out any, secrets ...string) error {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(baseURL, "/")+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if handle != "" {
		req.Header.Set(mountHandleHeader, handle)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Referrer-Policy", "no-referrer")
	if traceID := newTraceID(); traceID != "" {
		req.Header.Set("X-Idoud-Trace-ID", traceID)
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, maximumJSONBytes+1))
	if err != nil {
		return err
	}
	if len(payload) > maximumJSONBytes {
		return ErrInvalidProtocol
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var envelope struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		apiError := &APIError{Status: response.StatusCode, RetryAfter: response.Header.Get("Retry-After")}
		if json.Unmarshal(payload, &envelope) == nil {
			apiError.Code = strings.TrimSpace(envelope.Error.Code)
			apiError.Message = redact(envelope.Error.Message, secrets...)
		}
		return apiError
	}
	if out != nil {
		if err := json.Unmarshal(payload, out); err != nil {
			return fmt.Errorf("%w: decode JSON", ErrInvalidProtocol)
		}
	}
	return nil
}

func secureHTTPClient(input *http.Client) *http.Client {
	if input == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxIdleConns = 32
		transport.MaxIdleConnsPerHost = 16
		input = &http.Client{Transport: transport}
	}
	copy := *input
	copy.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &copy
}

func normalizeOrigin(raw string, allowHTTP bool) (string, *url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Path != "" && parsed.Path != "/") {
		return "", nil, errors.New("remote mount server must be an origin URL")
	}
	if parsed.Scheme != "https" && !(allowHTTP && parsed.Scheme == "http") {
		return "", nil, errors.New("remote mount server must use HTTPS")
	}
	parsed.Path, parsed.RawPath = "", ""
	return parsed.Scheme + "://" + parsed.Host, parsed, nil
}

func redact(message string, secrets ...string) string {
	result := strings.TrimSpace(message)
	for _, secret := range secrets {
		if secret = strings.TrimSpace(secret); len(secret) >= 8 {
			result = strings.ReplaceAll(result, secret, "[REDACTED]")
		}
	}
	if len(result) > 1024 {
		result = result[:1024]
	}
	return result
}

func newTraceID() string {
	value := make([]byte, 8)
	if _, err := rand.Read(value); err != nil {
		return ""
	}
	return hex.EncodeToString(value)
}

func validCapabilityText(value string, minimumLength int) bool {
	if len(value) < minimumLength || len(value) > 512 || value != strings.TrimSpace(value) {
		return false
	}
	for _, char := range value {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_') {
			return false
		}
	}
	return true
}

func validDisplayText(value string, maximumBytes int) bool {
	if value == "" || len(value) > maximumBytes || !utf8.ValidString(value) || value != strings.TrimSpace(value) {
		return false
	}
	for _, char := range value {
		if unicode.IsControl(char) {
			return false
		}
	}
	return true
}

func parseContentLength(header http.Header) (int64, bool) {
	value := strings.TrimSpace(header.Get("Content-Length"))
	if value == "" {
		return 0, false
	}
	length, err := strconv.ParseInt(value, 10, 64)
	return length, err == nil && length >= 0
}
