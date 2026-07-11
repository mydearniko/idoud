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
	for _, want := range []string{"idoud · upload", "stdin  stream.bin · 1.00KiB", "plan  2 routes", "save  /tmp/stream.bin", "complete", "1.00KiB stored"} {
		if !strings.Contains(got, want) {
			t.Fatalf("UI output %q does not contain %q", got, want)
		}
	}
	if strings.Contains(got, "\x1b[") {
		t.Fatalf("color-disabled UI emitted ANSI styling: %q", got)
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
}

func TestUploadProgressParallelReportsUsefulWindow(t *testing.T) {
	uploader := &uploader{opts: options{parallel: 384, chunkSize: browserChunkSize}}
	regular := &sourceFile{knownSize: true, size: browserChunkSize}
	if got := uploadProgressParallel(uploader, regular, 1); got != 1 {
		t.Fatalf("single-part parallel=%d, want 1", got)
	}
	stream := &sourceFile{knownSize: false, stream: strings.NewReader("stream")}
	got := uploadProgressParallel(uploader, stream, -1)
	if got >= uploader.opts.parallel || got > 24 {
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
