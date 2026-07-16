package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func newProgressFormatter(width int) *transferUI {
	return newTransferUI(transferUIConfig{
		enabled: true,
		color:   false,
		unicode: true,
		writer:  &bytes.Buffer{},
		width:   func() int { return width },
		kind:    "upload",
		source:  "file",
		name:    "fixture.bin",
		total:   1024,
	})
}

func TestKnownProgressUsesConfirmedBytesAndETA(t *testing.T) {
	ui := newProgressFormatter(120)
	snapshot := transferProgressSnapshot{
		kind:            "upload",
		source:          "file",
		phase:           transferPhaseTransferring,
		total:           1024 * 1024 * 1024,
		transferred:     512 * 1024 * 1024,
		totalChunks:     100,
		completedChunks: 50,
		inFlight:        12,
		rate:            100 * 1024 * 1024,
		phaseElapsed:    4 * time.Second,
	}
	line := ui.formatProgress(snapshot)
	for _, want := range []string{"50.0%", "512.00MiB/1.00GiB", "100.00MiB/s", "eta ~5s", "50/100 parts", "12 active"} {
		if !strings.Contains(line, want) {
			t.Fatalf("progress line %q does not contain %q", line, want)
		}
	}
	if strings.Contains(line, "100.0%") {
		t.Fatalf("progress line falsely reached 100%%: %q", line)
	}
}

func TestKnownProgressShowsSmoothSentAndStoredTruth(t *testing.T) {
	ui := newProgressFormatter(140)
	snapshot := transferProgressSnapshot{
		kind:            "upload",
		source:          "file",
		phase:           transferPhaseTransferring,
		total:           100 * 1024 * 1024,
		transferred:     10 * 1024 * 1024,
		bodySentBytes:   50 * 1024 * 1024,
		totalChunks:     10,
		completedChunks: 1,
		inFlight:        9,
		sendRate:        20 * 1024 * 1024,
		phaseElapsed:    4 * time.Second,
	}
	line := ui.formatProgress(snapshot)
	for _, want := range []string{
		"sent 50.0%",
		"50.00MiB/100.00MiB",
		"sending 20.00MiB/s",
		"stored 10.00MiB",
		"eta ~3s",
		"1/10 parts",
		"9 active",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("smooth progress line %q does not contain %q", line, want)
		}
	}
	if strings.Contains(line, "sent 10.0%") {
		t.Fatalf("smooth progress used stored bytes for the sent bar: %q", line)
	}
}

func TestUnknownStreamProgressNeverInventsPercentOrETA(t *testing.T) {
	ui := newProgressFormatter(100)
	snapshot := transferProgressSnapshot{
		kind:        "upload",
		source:      "stdin",
		phase:       transferPhaseTransferring,
		total:       -1,
		transferred: 80 * 1024 * 1024,
		readBytes:   120 * 1024 * 1024,
		rate:        40 * 1024 * 1024,
		tick:        3,
	}
	line := ui.formatProgress(snapshot)
	for _, want := range []string{"streaming", "stored 80.00MiB", "read 120.00MiB", "40.00MiB/s"} {
		if !strings.Contains(line, want) {
			t.Fatalf("stream line %q does not contain %q", line, want)
		}
	}
	if strings.Contains(line, "%") || strings.Contains(strings.ToLower(line), "eta") {
		t.Fatalf("unknown stream line invented percent or ETA: %q", line)
	}
}

func TestClosedStreamShowsProviderConfirmationInsteadOfDecayingInputRate(t *testing.T) {
	ui := newProgressFormatter(100)
	line := ui.formatProgress(transferProgressSnapshot{
		kind:         "upload",
		source:       "stdin",
		phase:        transferPhaseTransferring,
		total:        -1,
		readBytes:    4 * 1024,
		readRate:     100,
		phaseElapsed: 4 * time.Second,
		inFlight:     1,
		inputClosed:  true,
	})
	if !strings.Contains(line, "awaiting confirmation") || strings.Contains(line, "input 100B/s") {
		t.Fatalf("closed stream progress=%q", line)
	}
}

func TestClosedStreamShowsStorageWaitAfterEveryBodyByteWasSent(t *testing.T) {
	ui := newProgressFormatter(100)
	line := ui.formatProgress(transferProgressSnapshot{
		kind:          "upload",
		source:        "archive",
		phase:         transferPhaseTransferring,
		total:         -1,
		transferred:   0,
		readBytes:     4 * 1024,
		bodyReadBytes: 4 * 1024,
		bodySentBytes: 4 * 1024,
		bodyWritten:   4 * 1024,
		sendRate:      18.35 * 1024,
		inFlight:      1,
		inputClosed:   true,
	})
	if !strings.Contains(line, "awaiting storage") || strings.Contains(line, "sending 18.35KiB/s") {
		t.Fatalf("closed stream progress=%q, want durable storage wait", line)
	}
}

func TestClosedStreamStillShowsRequestBodyWritingBeforeConfirmation(t *testing.T) {
	ui := newProgressFormatter(100)
	line := ui.formatProgress(transferProgressSnapshot{
		kind:          "upload",
		source:        "stdin",
		phase:         transferPhaseTransferring,
		total:         -1,
		readBytes:     1024,
		bodyReadBytes: 512,
		inFlight:      1,
		inputClosed:   true,
	})
	if !strings.Contains(line, "sending request bodies") || strings.Contains(line, "awaiting confirmation") {
		t.Fatalf("closed stream progress=%q, want active body-write state", line)
	}
	if got := plainProgressState(transferProgressSnapshot{
		phase:         transferPhaseTransferring,
		bodyReadBytes: 512,
		inFlight:      1,
		inputClosed:   true,
	}); got != "writing_request_bodies" {
		t.Fatalf("plain state=%q, want writing_request_bodies", got)
	}
}

func TestArchiveStreamLabelsCompressedOutput(t *testing.T) {
	ui := newProgressFormatter(100)
	snapshot := transferProgressSnapshot{
		kind:        "upload",
		source:      "archive",
		phase:       transferPhaseTransferring,
		total:       -1,
		transferred: 8 * 1024 * 1024,
		readBytes:   12 * 1024 * 1024,
	}
	line := ui.formatProgress(snapshot)
	if !strings.Contains(line, "packed 12.00MiB") {
		t.Fatalf("archive progress=%q, want packed byte label", line)
	}
}

func TestActivityBarStaysWithinTrackAtEveryFrame(t *testing.T) {
	ui := newProgressFormatter(100)
	for tick := 0; tick < 100; tick++ {
		bar := ui.activityBar(tick, 16)
		if got := visibleTerminalWidth(bar); got != 18 {
			t.Fatalf("tick=%d bar=%q width=%d, want 18", tick, bar, got)
		}
	}
}

func TestFinalizationIsAnExplicitPhase(t *testing.T) {
	ui := newProgressFormatter(90)
	line := ui.formatProgress(transferProgressSnapshot{
		kind:         "upload",
		phase:        transferPhaseFinalizing,
		transferred:  6 * 1024 * 1024 * 1024,
		phaseElapsed: 2300 * time.Millisecond,
	})
	if !strings.Contains(line, "committing provider data") || !strings.Contains(line, "2.3s") {
		t.Fatalf("finalization progress=%q", line)
	}
}

func TestPlanningProgressSurfacesRetries(t *testing.T) {
	ui := newProgressFormatter(90)
	line := ui.formatProgress(transferProgressSnapshot{
		kind:         "upload",
		phase:        transferPhasePlanning,
		retries:      2,
		phaseElapsed: 5 * time.Second,
	})
	if !strings.Contains(line, "retry 2") {
		t.Fatalf("planning progress=%q, want retry count", line)
	}
}

func TestProgressETAIncludesSlowKnownInput(t *testing.T) {
	snapshot := transferProgressSnapshot{
		source:       "stdin",
		total:        1000,
		transferred:  500,
		readBytes:    600,
		rate:         500,
		readRate:     100,
		phaseElapsed: 4 * time.Second,
	}
	if got := progressETA(snapshot); got != 4*time.Second {
		t.Fatalf("ETA=%s, want 4s input-bound ETA", got)
	}
}

func TestProgressETAUsesBodySendRateBeforeConfirmations(t *testing.T) {
	snapshot := transferProgressSnapshot{
		kind:          "upload",
		source:        "file",
		total:         100 * 1024 * 1024,
		bodySentBytes: 40 * 1024 * 1024,
		sendRate:      20 * 1024 * 1024,
		totalChunks:   10,
		phaseElapsed:  4 * time.Second,
	}
	if got := progressETA(snapshot); got != 3*time.Second {
		t.Fatalf("ETA=%s, want 3s body-send ETA", got)
	}
}

func TestProgressETAKeepsProviderConfirmationLatencyFloor(t *testing.T) {
	snapshot := transferProgressSnapshot{
		source:              "file",
		total:               32 * 1024 * 1024,
		transferred:         30 * 1024 * 1024,
		totalChunks:         4,
		completedChunks:     3,
		inFlight:            1,
		rate:                20 * 1024 * 1024,
		confirmationLatency: 6 * time.Second,
		phaseElapsed:        4 * time.Second,
	}
	if got := progressETA(snapshot); got != 3*time.Second {
		t.Fatalf("ETA=%s, want 3s confirmation-latency floor", got)
	}
}

func TestProgressRateEstimatorSmoothsAndDetectsStall(t *testing.T) {
	start := time.Unix(100, 0)
	estimator := progressRateEstimator{}
	estimator.reset(start, 0)
	rate, stalled := estimator.observe(start.Add(time.Second), 100*1024*1024)
	if stalled || rate < 99*1024*1024 || rate > 101*1024*1024 {
		t.Fatalf("rate=%f stalled=%t, want about 100 MiB/s", rate, stalled)
	}
	rate, stalled = estimator.observe(start.Add(2*time.Second), 200*1024*1024)
	if stalled || rate < 99*1024*1024 || rate > 101*1024*1024 {
		t.Fatalf("second-window rate=%f stalled=%t, want about 100 MiB/s", rate, stalled)
	}
	_, stalled = estimator.observe(start.Add(5*time.Second), 200*1024*1024)
	if !stalled {
		t.Fatal("estimator did not report a confirmation stall")
	}
}

func TestTransferUIWritesStyledInformationAndCompletion(t *testing.T) {
	var output bytes.Buffer
	ui := newTransferUI(transferUIConfig{
		enabled:     true,
		color:       false,
		unicode:     false,
		writer:      &output,
		width:       func() int { return 100 },
		kind:        "upload",
		source:      "stdin",
		name:        "stream.bin",
		total:       1024,
		totalChunks: 2,
	})
	ui.start()
	ui.setPlan("2 routes · 2 workers · 512B parts")
	ui.setDestination("/tmp/stream.bin")
	ui.setPhase(transferPhaseTransferring)
	ui.addRead(1024)
	ui.chunkStarted()
	ui.addTransferred(1024)
	ui.chunkFinished(true)
	ui.setPhase(transferPhaseFinalizing)
	ui.stop(true)

	got := output.String()
	for _, want := range []string{"idoud · upload · stdin stream.bin · 1.00KiB · 2 routes", "save  /tmp/stream.bin", "complete", "1.00KiB stored"} {
		if !strings.Contains(got, want) {
			t.Fatalf("UI output %q does not contain %q", got, want)
		}
	}
	if strings.Contains(got, "\nstdin  ") || strings.Contains(got, "\nplan  ") {
		t.Fatalf("UI emitted separate source or plan headings: %q", got)
	}
	if strings.Contains(got, "\x1b[") {
		t.Fatalf("color-disabled UI emitted ANSI styling: %q", got)
	}
}

func TestTransferSummaryGainsPlanOnTheSameLine(t *testing.T) {
	var output bytes.Buffer
	ui := newTransferUI(transferUIConfig{
		enabled: true,
		writer:  &output,
		width:   func() int { return 140 },
		kind:    "upload",
		source:  "file",
		name:    "fixture.bin",
		total:   100 * 1024 * 1024,
	})
	before := ui.formatTransferSummary(140)
	ui.setPlan("1 route · up to 10 parallel · 10 × 10.00MiB")
	after := ui.formatTransferSummary(140)

	if before != "idoud · upload · fixture.bin · 100.00MiB" {
		t.Fatalf("initial summary=%q", before)
	}
	if !strings.HasPrefix(after, before+" · ") || !strings.Contains(after, "1 route · up to 10 parallel") {
		t.Fatalf("expanded summary=%q does not extend %q", after, before)
	}
	if strings.Contains(after, "\n") {
		t.Fatalf("expanded summary contains a newline: %q", after)
	}
}

func TestLineProgressKeepsInteractiveLayoutWithTimestampedNewlines(t *testing.T) {
	var output bytes.Buffer
	ui := newTransferUI(transferUIConfig{
		enabled:     true,
		lines:       true,
		color:       false,
		unicode:     true,
		writer:      &output,
		width:       func() int { return 140 },
		kind:        "upload",
		source:      "file",
		name:        "fixture.bin",
		total:       10 * 1024 * 1024,
		totalChunks: 1,
	})
	ui.start()
	ui.setPlan("1 route · up to 1 parallel · 1 × 10.00MiB")
	ui.setPhase(transferPhaseTransferring)
	ui.chunkStarted()
	ui.addBodyRead(10 * 1024 * 1024)
	ui.bodyRequestWritten(10 * 1024 * 1024)
	ui.addTransferred(10 * 1024 * 1024)
	ui.chunkFinished(true)
	ui.stop(true)

	got := output.String()
	for _, want := range []string{
		"idoud · upload · fixture.bin · 10.00MiB · 1 route · up to 1 parallel",
		"◆ [",
		"100.0%",
		"10.00MiB/10.00MiB",
		"1/1 parts",
		"✓ complete",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("line progress output %q does not contain %q", got, want)
		}
	}
	if strings.Contains(got, "\r") || strings.Contains(got, "event=progress") || strings.Contains(got, "\x1b[") {
		t.Fatalf("line progress used dynamic, diagnostic, or ANSI output: %q", got)
	}
	lines := strings.Split(strings.TrimSpace(got), "\n")
	if len(lines) < 3 {
		t.Fatalf("line progress emitted only %d lines: %q", len(lines), got)
	}
	for _, line := range lines {
		firstField := strings.Fields(line)[0]
		if _, err := time.Parse(time.RFC3339Nano, firstField); err != nil {
			t.Fatalf("line progress line has no RFC3339 timestamp: %q", line)
		}
	}
}

func TestLineProgressSuppressesUnchangedSnapshotsUntilHeartbeat(t *testing.T) {
	var output bytes.Buffer
	ui := newTransferUI(transferUIConfig{
		enabled: true,
		lines:   true,
		writer:  &output,
	})
	start := time.Date(2026, 7, 12, 3, 0, 0, 0, time.FixedZone("MSK", 3*60*60))
	ui.renderDynamic(start, "  ◆ [──────────] 0.0%")
	ui.renderDynamic(start.Add(200*time.Millisecond), "  ◆ [──────────] 0.0%")
	ui.renderDynamic(start.Add(time.Second), "  ◆ [──────────] 0.0%")

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("unchanged snapshots emitted %d lines, want initial + 1s heartbeat: %q", len(lines), output.String())
	}
}

func TestBodyProgressCountsEachPartOnlyOnceAcrossRetries(t *testing.T) {
	ui := newTransferUI(transferUIConfig{})
	ui.setBaseline(10, 1)
	ui.recordBodyRead(2, 4, 10, 4)
	ui.recordBodyRead(2, 3, 10, 3)
	ui.recordBodyRead(2, 8, 10, 5)
	ui.recordBodyRead(3, 2, 10, 2)

	if got := ui.bodyReadBytes.Load(); got != 14 {
		t.Fatalf("raw body bytes=%d, want 14 including retry traffic", got)
	}
	if got := ui.bodySentBytes.Load(); got != 20 {
		t.Fatalf("unique sent bytes=%d, want 10 baseline + 8 + 2", got)
	}
}

func TestPlainProgressIsANSIFreeLineOrientedAndDiagnostic(t *testing.T) {
	var output bytes.Buffer
	ui := newTransferUI(transferUIConfig{
		enabled:     true,
		plain:       true,
		writer:      &output,
		kind:        "upload",
		source:      "file",
		name:        "fixture with spaces.bin",
		total:       10 * 1024 * 1024,
		totalChunks: 1,
	})
	ui.start()
	ui.setPlan("1 route · up to 1 parallel · 1 × 10.00MiB")
	ui.setPhase(transferPhaseTransferring)
	ui.chunkStarted()
	ui.addBodyRead(10 * 1024 * 1024)
	ui.bodyRequestWritten(10 * 1024 * 1024)
	ui.recordRequestDuration(2 * time.Second)
	ui.addTransferred(10 * 1024 * 1024)
	ui.chunkFinished(true)
	ui.setPhase(transferPhaseFinalizing)
	ui.stop(true)

	got := output.String()
	for _, want := range []string{
		"idoud transfer=upload event=start",
		"name=\"fixture with spaces.bin\"",
		"event=info label=\"plan\"",
		"event=progress phase=finalizing",
		"completed_bytes=10485760",
		"body_read_bytes=10485760",
		"body_sent_bytes=10485760",
		"body_written_bytes=10485760",
		"confirmation_average_ms=2000",
		"event=complete result=success",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("plain progress output %q does not contain %q", got, want)
		}
	}
	if strings.Contains(got, "\r") || strings.Contains(got, "\x1b[") {
		t.Fatalf("plain progress emitted terminal control sequences: %q", got)
	}
	for _, line := range strings.Split(strings.TrimSpace(got), "\n") {
		firstField := strings.Fields(line)[0]
		if _, err := time.Parse(time.RFC3339Nano, firstField); err != nil {
			t.Fatalf("plain progress line has no RFC3339 timestamp: %q", line)
		}
	}
}

func TestPlainProgressReportsArchivePackingAndResumeBaseline(t *testing.T) {
	var output bytes.Buffer
	ui := newTransferUI(transferUIConfig{
		enabled: true,
		plain:   true,
		writer:  &output,
		kind:    "upload",
		source:  "archive",
		name:    "root.tar.lz4",
		total:   -1,
	})
	ui.start()
	ui.setBaseline(10*1024*1024, 1)
	ui.setPhase(transferPhaseTransferring)
	ui.addRead(24 * 1024 * 1024)
	ui.stop(false)

	got := output.String()
	for _, want := range []string{
		"event=info label=\"resume\"",
		"baseline_bytes=10485760",
		"packed_bytes=25165824",
		"event=complete result=failure",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("plain archive progress %q does not contain %q", got, want)
		}
	}
}

func TestPlainDownloadStateUsesDiskSemantics(t *testing.T) {
	state := plainProgressState(transferProgressSnapshot{
		kind:     "download",
		phase:    transferPhaseTransferring,
		inFlight: 4,
		rate:     10 * 1024 * 1024,
	})
	if state != "downloading" {
		t.Fatalf("download state=%q, want downloading", state)
	}
}

func TestCompletionUsesEndToEndAverageNotLastConfirmationBurst(t *testing.T) {
	var output bytes.Buffer
	ui := newTransferUI(transferUIConfig{
		enabled: true,
		writer:  &output,
		width:   func() int { return 100 },
		kind:    "upload",
		source:  "file",
		name:    "fixture.bin",
		total:   100 * 1024 * 1024,
	})
	now := time.Unix(200, 0)
	ui.transferStart.Store(now.Add(-10 * time.Second).UnixNano())
	ui.transferred.Store(100 * 1024 * 1024)
	ui.finishLine(true, 1024, now)

	got := output.String()
	if !strings.Contains(got, "10.00MiB/s avg") {
		t.Fatalf("completion=%q, want end-to-end 10 MiB/s average", got)
	}
	if strings.Contains(got, "1.00KiB/s") {
		t.Fatalf("completion used final burst rate: %q", got)
	}
}

func TestVisibleTerminalWidthIgnoresANSI(t *testing.T) {
	value := "\x1b[38;2;120;182;173midoud\x1b[0m · upload"
	want := utf8.RuneCountInString("idoud · upload")
	if got := visibleTerminalWidth(value); got != want {
		t.Fatalf("visible width=%d, want %d", got, want)
	}
}

func TestNoProgressOptionDisablesRendererBeforeTTYProbe(t *testing.T) {
	if transferProgressEnabled(options{noProgress: true}) {
		t.Fatal("--no-progress did not disable progress")
	}
	if transferProgressEnabled(options{debug: true}) {
		t.Fatal("debug mode did not disable interactive progress")
	}
	if transferProgressEnabled(options{verbose: true}) {
		t.Fatal("verbose mode did not disable interactive progress")
	}
	if !transferProgressEnabled(options{debug: true, progressMode: progressModePlain}) {
		t.Fatal("plain progress should remain enabled with debug logs")
	}
	if !transferProgressEnabled(options{debug: true, progressMode: progressModeLines}) {
		t.Fatal("line progress should remain enabled with debug logs")
	}
}

func TestUploadProgressParallelReportsUsefulWindow(t *testing.T) {
	uploader := &uploader{opts: options{parallel: 384, chunkSize: browserChunkSize, streamMemory: 256 * 1024 * 1024}}
	regular := &sourceFile{knownSize: true, size: browserChunkSize}
	if got := uploadProgressParallel(uploader, regular, 1); got != 1 {
		t.Fatalf("single-part parallel=%d, want 1", got)
	}
	stream := &sourceFile{knownSize: false, stream: strings.NewReader("stream")}
	got := uploadProgressParallel(uploader, stream, -1)
	if got != 24 {
		t.Fatalf("unknown-stream parallel=%d, want bounded useful window", got)
	}
}

func TestCommittedUploadProgressSeedsResumeTruthfully(t *testing.T) {
	src := &sourceFile{
		knownSize:       true,
		size:            25,
		committedChunks: map[int64]struct{}{0: {}, 2: {}, 99: {}},
	}
	bytes, chunks := src.committedUploadProgress(10)
	if bytes != 15 || chunks != 2 {
		t.Fatalf("resume baseline bytes=%d chunks=%d, want 15 and 2", bytes, chunks)
	}
}
