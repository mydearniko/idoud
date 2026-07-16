package cli

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
)

func TestDirectRouteProbesUseRouteLanesWithoutRedundantWarmup(t *testing.T) {
	routeA := mustParseURL(t, "https://route-a.example/file")
	routeB := mustParseURL(t, "https://route-b.example/file")
	type requestKey struct {
		lane int
		host string
	}
	var mu sync.Mutex
	requests := make(map[requestKey]int)
	clients := make([]*http.Client, 2)
	for lane := range clients {
		lane := lane
		clients[lane] = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			requests[requestKey{lane: lane, host: req.URL.Host}]++
			mu.Unlock()
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})}
	}
	u := &uploader{
		opts:         options{parallel: 2, chunkSize: 10},
		client:       clients[0],
		chunkClients: clients,
		chunkBodyLanes: func() chan int {
			lanes := make(chan int, 2)
			lanes <- 0
			lanes <- 1
			return lanes
		}(),
	}
	src := &sourceFile{
		readerAt:             bytes.NewReader(make([]byte, 40)),
		size:                 40,
		knownSize:            true,
		committedChunks:      map[int64]struct{}{0: {}, 1: {}, 2: {}},
		uploadRouteTargets:   []uploadRouteTarget{{rawURL: routeA.String(), parsedURL: routeA, nodeID: "node-a"}, {rawURL: routeB.String(), parsedURL: routeB, nodeID: "node-b"}},
		uploadTargetSchedule: []int{0, 1},
	}
	indexes := uploadWarmChunkIndexes(u, src, 2)
	if len(indexes) != 1 || indexes[0] != 3 {
		t.Fatalf("warm chunk indexes=%v, want missing final chunk 3", indexes)
	}

	u.warmConnections(context.Background(), src, 1)
	lease, err := u.acquireUploadBody(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if lease.connectionLane != 0 || u.clientForUpload(3, lease) != clients[0] {
		lease.releaseRequest()
		t.Fatalf("payload lane/client=(%d,%p), want warmed lane/client=(0,%p)", lease.connectionLane, u.clientForUpload(3, lease), clients[0])
	}
	lease.releaseRequest()
	mu.Lock()
	defer mu.Unlock()
	if got := requests[requestKey{lane: 0, host: "route-b.example"}]; got != 0 {
		t.Fatalf("redundant missing-chunk warm requests=%d, want none (all=%v)", got, requests)
	}
	if got := requests[requestKey{lane: 0, host: "route-a.example"}]; got != 1 {
		t.Fatalf("route A lane 0 requests=%d, want probe only (all=%v)", got, requests)
	}
	if got := requests[requestKey{lane: 1, host: "route-b.example"}]; got != 1 {
		t.Fatalf("route B lane 1 requests=%d, want route probe only (all=%v)", got, requests)
	}
}

func TestKnownStreamProgressAndWarmWindowIncludesConcurrentFinalPart(t *testing.T) {
	u := &uploader{opts: options{parallel: 8, chunkSize: 2, streamMemory: 1 << 20}}
	src := &sourceFile{knownSize: true, size: 3, stream: strings.NewReader("abc")}
	if got := uploadProgressParallel(u, src, 2); got != 2 {
		t.Fatalf("known stream parallel=%d, want non-final + concurrent final", got)
	}
	indexes := uploadWarmChunkIndexes(u, src, 8)
	if len(indexes) != 2 || indexes[0] != 0 || indexes[1] != 1 {
		t.Fatalf("known stream warm indexes=%v, want both concurrent parts", indexes)
	}
}
