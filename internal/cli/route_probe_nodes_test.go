package cli

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"
)

func TestRouteProbeRequiresHealthyAliasPerScheduledPhysicalNode(t *testing.T) {
	target := func(raw, nodeID string) uploadRouteTarget {
		parsed := mustParseURL(t, raw)
		return uploadRouteTarget{rawURL: parsed.String(), parsedURL: parsed, nodeID: nodeID}
	}
	targets := []uploadRouteTarget{
		target("https://node-a-slow.example/file", "node-a"),
		target("https://node-a-fast.example/file", "node-a"),
		target("https://node-b-slow.example/file", "node-b"),
		target("https://node-b-fast.example/file", "node-b"),
	}
	nodeBStarted := make(chan struct{})
	releaseNodeB := make(chan struct{})
	u := &uploader{client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Host {
		case "node-a-fast.example":
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		case "node-b-fast.example":
			select {
			case <-nodeBStarted:
			default:
				close(nodeBStarted)
			}
			select {
			case <-releaseNodeB:
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		default:
			<-req.Context().Done()
			return nil, req.Context().Err()
		}
	})}}
	src := &sourceFile{
		uploadRouteTargets:   targets,
		uploadTargetSchedule: []int{0, 2},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		u.probeUploadRoutes(ctx, src)
		close(done)
	}()

	select {
	case <-nodeBStarted:
	case <-time.After(time.Second):
		t.Fatal("second scheduled physical node was not probed")
	}
	select {
	case <-done:
		t.Fatal("probe returned after only one scheduled physical node was healthy")
	case <-time.After(40 * time.Millisecond):
	}
	close(releaseNodeB)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probe did not become ready after both scheduled physical nodes were healthy")
	}
	cancel()
}

func TestRouteProbeBrieflyJoinsAliasesOnlyWhenReadyCapacityIsTooSmall(t *testing.T) {
	target := func(raw string) uploadRouteTarget {
		parsed := mustParseURL(t, raw)
		return uploadRouteTarget{rawURL: parsed.String(), parsedURL: parsed, nodeID: "same-node", maxParallel: 1}
	}
	targets := []uploadRouteTarget{
		target("https://fast.example/file"),
		target("https://joining.example/file"),
	}

	t.Run("large enough transfer waits for immediately joining capacity", func(t *testing.T) {
		releaseJoining := make(chan struct{})
		joiningDone := make(chan struct{})
		u := &uploader{
			opts: options{parallel: 2, chunkSize: 10},
			client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Host == "joining.example" {
					<-releaseJoining
					close(joiningDone)
				}
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
			})},
		}
		src := &sourceFile{
			knownSize:          true,
			size:               20,
			readerAt:           bytes.NewReader(make([]byte, 20)),
			uploadRouteTargets: targets,
		}
		go func() {
			time.Sleep(20 * time.Millisecond)
			close(releaseJoining)
		}()

		started := time.Now()
		u.probeUploadRoutes(context.Background(), src)
		if elapsed := time.Since(started); elapsed >= routeProbeJoinGrace+100*time.Millisecond {
			t.Fatalf("capacity join took %s, want a tightly bounded wait", elapsed)
		}
		select {
		case <-joiningDone:
		default:
			t.Fatal("probe returned before the capacity needed by pending chunks joined")
		}
	})

	t.Run("small transfer starts on first sufficient route", func(t *testing.T) {
		releaseJoining := make(chan struct{})
		joiningDone := make(chan struct{})
		u := &uploader{
			opts: options{parallel: 2, chunkSize: 10},
			client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Host == "joining.example" {
					<-releaseJoining
					close(joiningDone)
				}
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
			})},
		}
		src := &sourceFile{
			knownSize:          true,
			size:               10,
			readerAt:           bytes.NewReader(make([]byte, 10)),
			uploadRouteTargets: targets,
		}

		started := time.Now()
		u.probeUploadRoutes(context.Background(), src)
		if elapsed := time.Since(started); elapsed >= routeProbeJoinGrace {
			close(releaseJoining)
			t.Fatalf("small transfer waited %s for unnecessary alias capacity", elapsed)
		}
		select {
		case <-joiningDone:
			close(releaseJoining)
			t.Fatal("slow alias completed before it was released")
		default:
		}
		close(releaseJoining)
		select {
		case <-joiningDone:
		case <-time.After(time.Second):
			t.Fatal("background alias probe did not finish")
		}
	})

	t.Run("missing extra capacity never extends beyond join grace", func(t *testing.T) {
		releaseJoining := make(chan struct{})
		joiningDone := make(chan struct{})
		u := &uploader{
			opts: options{parallel: 2, chunkSize: 10},
			client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Host == "joining.example" {
					<-releaseJoining
					close(joiningDone)
				}
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
			})},
		}
		src := &sourceFile{
			knownSize:          true,
			size:               20,
			readerAt:           bytes.NewReader(make([]byte, 20)),
			uploadRouteTargets: targets,
		}

		started := time.Now()
		u.probeUploadRoutes(context.Background(), src)
		if elapsed := time.Since(started); elapsed >= routeProbeJoinGrace+100*time.Millisecond {
			close(releaseJoining)
			t.Fatalf("missing alias delayed startup for %s, want only the bounded join grace", elapsed)
		}
		close(releaseJoining)
		select {
		case <-joiningDone:
		case <-time.After(time.Second):
			t.Fatal("timed-out background alias probe did not finish")
		}
	})
}
