package cli

import "net/http"

func (u *uploader) clientForChunk(chunkIndex int64) *http.Client {
	if u == nil {
		return nil
	}
	if chunkIndex >= 0 && len(u.chunkClients) > 0 {
		idx := int(chunkIndex % int64(len(u.chunkClients)))
		return u.chunkClients[idx]
	}
	return u.client
}

func (u *uploader) clientForUpload(chunkIndex int64, lease *uploadBodyLease) *http.Client {
	if u != nil && lease != nil && lease.connectionLane >= 0 && lease.connectionLane < len(u.chunkClients) {
		return u.chunkClients[lease.connectionLane]
	}
	return u.clientForChunk(chunkIndex)
}
