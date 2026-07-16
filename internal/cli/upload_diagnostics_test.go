package cli

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"testing"
	"time"
)

func tracedUploadRoundTripper(status int, transportErr error, reused bool) roundTripFunc {
	return func(req *http.Request) (*http.Response, error) {
		trace := httptrace.ContextClientTrace(req.Context())
		if trace != nil {
			trace.GetConn(req.URL.Host)
			if !reused {
				trace.DNSStart(httptrace.DNSStartInfo{Host: req.URL.Hostname()})
				time.Sleep(time.Millisecond)
				trace.DNSDone(httptrace.DNSDoneInfo{})
				trace.ConnectStart("tcp", req.URL.Host)
				time.Sleep(time.Millisecond)
				trace.ConnectDone("tcp", req.URL.Host, nil)
				trace.TLSHandshakeStart()
				time.Sleep(time.Millisecond)
				trace.TLSHandshakeDone(tls.ConnectionState{}, nil)
			}
			time.Sleep(time.Millisecond)
			trace.GotConn(httptrace.GotConnInfo{Reused: reused})
			trace.WroteHeaders()
		}
		_, _ = io.Copy(io.Discard, req.Body)
		time.Sleep(time.Millisecond)
		if trace != nil {
			trace.WroteRequest(httptrace.WroteRequestInfo{})
		}
		time.Sleep(time.Millisecond)
		if transportErr != nil {
			return nil, transportErr
		}
		header := make(http.Header)
		var body io.ReadCloser = http.NoBody
		if status == http.StatusTooManyRequests {
			header.Set("Retry-After", "0.01")
			body = io.NopCloser(bytes.NewBufferString("slow down"))
		}
		return &http.Response{StatusCode: status, Header: header, Body: body}, nil
	}
}

func TestUploadDiagnosticsSeparateTransportStagesAndSuccessfulProviderWait(t *testing.T) {
	target := mustParseURL(t, "https://node.example/file")
	tests := []struct {
		name             string
		status           int
		transportErr     error
		reused           bool
		wantProviderWait int64
		wantAcquire      int64
		wantPool         int64
		wantDialStages   int64
	}{
		{name: "success fresh", status: http.StatusOK, wantProviderWait: 1, wantAcquire: 1, wantDialStages: 1},
		{name: "success reused", status: http.StatusOK, reused: true, wantProviderWait: 1, wantPool: 1},
		{name: "429", status: http.StatusTooManyRequests, wantAcquire: 1, wantDialStages: 1},
		{name: "503", status: http.StatusServiceUnavailable, wantAcquire: 1, wantDialStages: 1},
		{name: "transport error", transportErr: errors.New("transport down"), wantAcquire: 1, wantDialStages: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dbg := newUploadDebugStats("file", "file.bin")
			u := &uploader{
				opts:   options{uploadKey: "test-key"},
				client: &http.Client{Transport: tracedUploadRoundTripper(tc.status, tc.transportErr, tc.reused)},
				dbg:    dbg,
			}
			src := &sourceFile{uploadURL: target.String(), uploadURLParsed: target}
			_, _, _ = u.uploadPUT(context.Background(), src, bytes.NewReader([]byte("data")), 4, "bytes 0-3/*", 0, false, true, 0)

			if dbg.providerWaitCount != tc.wantProviderWait {
				t.Fatalf("provider wait count=%d, want %d", dbg.providerWaitCount, tc.wantProviderWait)
			}
			if dbg.bodySendCount != 1 {
				t.Fatalf("body send count=%d, want 1", dbg.bodySendCount)
			}
			if dbg.connAcquireCount != tc.wantAcquire || dbg.connPoolCount != tc.wantPool || dbg.dnsCount != tc.wantDialStages || dbg.connectCount != tc.wantDialStages || dbg.tlsCount != tc.wantDialStages {
				t.Fatalf("connection timing counts acquire=%d pool=%d dns=%d connect=%d tls=%d", dbg.connAcquireCount, dbg.connPoolCount, dbg.dnsCount, dbg.connectCount, dbg.tlsCount)
			}
			if dbg.httpCount != 1 {
				t.Fatalf("request total count=%d, want 1", dbg.httpCount)
			}
		})
	}
}
