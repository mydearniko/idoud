package main

import (
	"net/http"
	"testing"
)

func TestUploaderClientForChunkFallback(t *testing.T) {
	base := &http.Client{}
	u := &uploader{client: base}
	if got := u.clientForChunk(5); got != base {
		t.Fatal("clientForChunk should return base client when chunkClients is empty")
	}
	if got := u.clientForChunk(-1); got != base {
		t.Fatal("clientForChunk should return base client for negative chunk index")
	}
}

func TestUploaderClientForChunkRoundRobin(t *testing.T) {
	base := &http.Client{}
	c1 := &http.Client{}
	c2 := &http.Client{}
	c3 := &http.Client{}
	u := &uploader{
		client:       base,
		chunkClients: []*http.Client{c1, c2, c3},
	}

	cases := []struct {
		idx  int64
		want *http.Client
	}{
		{idx: 0, want: c1},
		{idx: 1, want: c2},
		{idx: 2, want: c3},
		{idx: 3, want: c1},
		{idx: 4, want: c2},
		{idx: -1, want: base},
	}

	for _, tc := range cases {
		got := u.clientForChunk(tc.idx)
		if got != tc.want {
			t.Fatalf("clientForChunk(%d) returned unexpected client", tc.idx)
		}
	}
}
