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
