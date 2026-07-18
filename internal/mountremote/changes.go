package mountremote

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	maximumChangeLimit = 1_000
	maximumChangeWait  = 30 * time.Second
)

type Change struct {
	Sequence           int64
	TransactionID      string
	MutationType       string
	AffectedEntries    []string
	AffectedParents    []string
	ResultingRevisions []int64
	Visibility         string
	CreatedAt          int64
}

type ChangeBatch struct {
	After           int64
	CurrentSequence int64
	Changes         []Change
}

type changeResponse struct {
	SchemaVersion   int   `json:"schemaVersion"`
	After           int64 `json:"after"`
	CurrentSequence int64 `json:"currentSequence"`
	Changes         []struct {
		Sequence           int64    `json:"sequence"`
		TransactionID      string   `json:"transactionId"`
		MutationType       string   `json:"mutationType"`
		AffectedEntries    []string `json:"affectedEntries"`
		AffectedParents    []string `json:"affectedParents"`
		ResultingRevisions []int64  `json:"resultingRevisions"`
		Visibility         string   `json:"visibility"`
		CreatedAt          int64    `json:"createdAt"`
	} `json:"changes"`
}

// PollChanges performs one bounded long poll. Callers retain control over the
// watch loop so adapter shutdown, backoff, and namespace refresh are explicit.
func (b *Backend) PollChanges(ctx context.Context, after int64, wait time.Duration, limit int) (ChangeBatch, error) {
	if err := ctx.Err(); err != nil {
		return ChangeBatch{}, err
	}
	if after < 0 {
		return ChangeBatch{}, errors.New("remote mount change cursor must be non-negative")
	}
	if wait < 0 || wait > maximumChangeWait {
		return ChangeBatch{}, errors.New("remote mount change wait is outside the supported bound")
	}
	if limit < 1 || limit > maximumChangeLimit {
		return ChangeBatch{}, errors.New("remote mount change limit is outside the supported bound")
	}
	query := url.Values{}
	query.Set("after", strconv.FormatInt(after, 10))
	query.Set("limit", strconv.Itoa(limit))
	if wait > 0 {
		seconds := int((wait + time.Second - 1) / time.Second)
		query.Set("wait", strconv.Itoa(seconds))
	}
	var response changeResponse
	path := "/v1/folders/" + url.PathEscape(b.shareID) + "/changes?" + query.Encode()
	if err := b.sessionJSON(ctx, "", http.MethodGet, path, nil, &response); err != nil {
		return ChangeBatch{}, err
	}
	if response.SchemaVersion != protocolVersion || response.After != after ||
		response.CurrentSequence < after || len(response.Changes) > limit {
		return ChangeBatch{}, ErrInvalidProtocol
	}
	changes := make([]Change, 0, len(response.Changes))
	previous := after
	for _, incoming := range response.Changes {
		if incoming.Sequence <= previous || incoming.Sequence > response.CurrentSequence ||
			incoming.TransactionID == "" || incoming.TransactionID != strings.TrimSpace(incoming.TransactionID) ||
			incoming.MutationType == "" || incoming.MutationType != strings.TrimSpace(incoming.MutationType) ||
			incoming.Visibility != "public" || incoming.CreatedAt < 1 ||
			len(incoming.AffectedEntries) != len(incoming.ResultingRevisions) ||
			!validChangeIDs(incoming.AffectedEntries) || !validChangeIDs(incoming.AffectedParents) {
			return ChangeBatch{}, ErrInvalidProtocol
		}
		changes = append(changes, Change{
			Sequence: incoming.Sequence, TransactionID: incoming.TransactionID,
			MutationType:       incoming.MutationType,
			AffectedEntries:    append([]string(nil), incoming.AffectedEntries...),
			AffectedParents:    append([]string(nil), incoming.AffectedParents...),
			ResultingRevisions: append([]int64(nil), incoming.ResultingRevisions...),
			Visibility:         incoming.Visibility, CreatedAt: incoming.CreatedAt,
		})
		previous = incoming.Sequence
	}
	b.recordSequence(response.CurrentSequence)
	return ChangeBatch{After: after, CurrentSequence: response.CurrentSequence, Changes: changes}, nil
}

func validChangeIDs(values []string) bool {
	for _, value := range values {
		if value == "" || value != strings.TrimSpace(value) {
			return false
		}
	}
	return true
}
