package cli

import (
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"golang.org/x/term"
)

type transferPhase uint32

const (
	transferPhasePlanning transferPhase = iota
	transferPhaseConnecting
	transferPhaseTransferring
	transferPhaseFinalizing
	transferPhaseSaving
)

type transferUIConfig struct {
	enabled     bool
	plain       bool
	lines       bool
	color       bool
	unicode     bool
	writer      io.Writer
	width       func() int
	kind        string
	source      string
	name        string
	total       int64
	totalChunks int64
}

// transferUI keeps upload transport and durability progress separate. Smooth
// request-body progress is retry-safe and explicitly labeled as sent, while
// stored bytes advance only after confirmation. Download progress counts only
// bytes actually written to the destination file.
type transferUI struct {
	enabled bool
	plain   bool
	lines   bool
	color   bool
	unicode bool
	writer  io.Writer
	width   func() int
	kind    string
	source  string
	started time.Time

	detailsMu        sync.RWMutex
	name             string
	planDetail       string
	phase            transferPhase
	phaseSince       time.Time
	summaryFinalized bool

	total           atomic.Int64
	totalChunks     atomic.Int64
	transferred     atomic.Int64
	baselineBytes   atomic.Int64
	readBytes       atomic.Int64
	bodyReadBytes   atomic.Int64
	bodySentBytes   atomic.Int64
	bodyWritten     atomic.Int64
	bodyRequests    atomic.Int64
	completedChunks atomic.Int64
	inFlight        atomic.Int64
	retries         atomic.Int64
	rateGeneration  atomic.Int64
	transferStart   atomic.Int64
	inputClosed     atomic.Bool
	requestNanos    atomic.Int64
	requestCount    atomic.Int64
	bodyProgress    sync.Map

	outputMu         sync.Mutex
	lastLineWidth    int
	lastRenderedLine string
	lastRenderedAt   time.Time
	planOnce         sync.Once
	resumeOnce       sync.Once
	destinationOnce  sync.Once

	stopOnce sync.Once
	stopCh   chan bool
	doneCh   chan struct{}
}

type transferProgressSnapshot struct {
	kind                string
	source              string
	phase               transferPhase
	total               int64
	transferred         int64
	readBytes           int64
	totalChunks         int64
	completedChunks     int64
	inFlight            int64
	retries             int64
	rate                float64
	readRate            float64
	stalled             bool
	inputClosed         bool
	confirmationLatency time.Duration
	phaseElapsed        time.Duration
	elapsed             time.Duration
	baselineBytes       int64
	bodyReadBytes       int64
	bodySentBytes       int64
	bodyWritten         int64
	bodyRequests        int64
	sendRate            float64
	tick                int
}

type progressRateSample struct {
	at    time.Time
	bytes int64
}

type progressRateEstimator struct {
	base        progressRateSample
	advances    []progressRateSample
	smoothed    float64
	lastBytes   int64
	lastAdvance time.Time
}

func terminalTransferUIConfig(opts options, kind, source, name string, total, totalChunks int64) transferUIConfig {
	mode := resolvedTransferProgressMode(opts)
	enabled := mode != progressModeNone
	plain := mode == progressModePlain
	lines := mode == progressModeLines
	return transferUIConfig{
		enabled:     enabled,
		plain:       plain,
		lines:       lines,
		color:       enabled && !plain && colorOutputEnabled(),
		unicode:     strings.TrimSpace(os.Getenv("IDOUD_ASCII")) == "",
		writer:      os.Stderr,
		width:       stderrTerminalWidth,
		kind:        kind,
		source:      source,
		name:        name,
		total:       total,
		totalChunks: totalChunks,
	}
}

func newTransferUI(config transferUIConfig) *transferUI {
	now := time.Now()
	if config.writer == nil {
		config.writer = io.Discard
	}
	if config.width == nil {
		config.width = func() int { return 96 }
	}
	ui := &transferUI{
		enabled:    config.enabled,
		plain:      config.plain,
		lines:      config.lines,
		color:      config.color,
		unicode:    config.unicode,
		writer:     config.writer,
		width:      config.width,
		kind:       strings.TrimSpace(config.kind),
		source:     strings.TrimSpace(config.source),
		name:       strings.TrimSpace(config.name),
		started:    now,
		phase:      transferPhasePlanning,
		phaseSince: now,
		stopCh:     make(chan bool, 1),
		doneCh:     make(chan struct{}),
	}
	ui.total.Store(config.total)
	ui.totalChunks.Store(config.totalChunks)
	return ui
}

func transferProgressEnabled(opts options) bool {
	return resolvedTransferProgressMode(opts) != progressModeNone
}

func resolvedTransferProgressMode(opts options) progressMode {
	if opts.noProgress {
		return progressModeNone
	}
	if strings.TrimSpace(os.Getenv("IDOUD_NO_PROGRESS")) != "" {
		return progressModeNone
	}
	mode := opts.progressMode
	if mode == "" {
		mode = progressModeAuto
	}
	if mode == progressModeLines || mode == progressModePlain || mode == progressModeNone {
		return mode
	}
	if opts.debug || opts.verbose {
		return progressModeNone
	}
	termName := strings.ToLower(strings.TrimSpace(os.Getenv("TERM")))
	if termName == "dumb" {
		return progressModeNone
	}
	if !term.IsTerminal(int(os.Stderr.Fd())) {
		return progressModeNone
	}
	return progressModeAuto
}

func stderrTerminalWidth() int {
	width, _, err := term.GetSize(int(os.Stderr.Fd()))
	if err != nil || width <= 0 {
		if envWidth, parseErr := strconv.Atoi(strings.TrimSpace(os.Getenv("COLUMNS"))); parseErr == nil && envWidth > 0 {
			width = envWidth
		} else {
			width = 96
		}
	}
	if width < 40 {
		return 40
	}
	if width > 180 {
		return 180
	}
	return width
}

func (ui *transferUI) start() {
	if ui == nil || !ui.enabled {
		return
	}
	now := time.Now()
	if ui.plain {
		ui.writePlainStart(now)
		go ui.loop()
		return
	}
	go ui.loop()
}

func (ui *transferUI) loop() {
	defer close(ui.doneCh)
	if ui.plain {
		ui.loopPlain()
		return
	}
	ui.loopInteractive()
}

func (ui *transferUI) loopInteractive() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	rate := progressRateEstimator{}
	readRate := progressRateEstimator{}
	sendRate := progressRateEstimator{}
	generation := ui.rateGeneration.Load()
	tick := 0

	render := func(now time.Time) (float64, float64, bool) {
		if currentGeneration := ui.rateGeneration.Load(); currentGeneration != generation {
			generation = currentGeneration
			rate.reset(now, ui.transferred.Load())
			sendRate.reset(now, ui.bodySentBytes.Load())
		}
		confirmedRate, stalled := rate.observe(now, ui.transferred.Load())
		inputRate, _ := readRate.observe(now, ui.readBytes.Load())
		bodyRate, _ := sendRate.observe(now, ui.bodySentBytes.Load())
		snapshot := ui.snapshot(now, confirmedRate, inputRate, bodyRate, stalled, tick)
		ui.renderDynamic(now, ui.formatProgress(snapshot))
		return confirmedRate, inputRate, stalled
	}

	now := time.Now()
	rate.reset(now, ui.transferred.Load())
	readRate.reset(now, ui.readBytes.Load())
	sendRate.reset(now, ui.bodySentBytes.Load())
	lastRate, _, _ := render(now)
	for {
		select {
		case success := <-ui.stopCh:
			now = time.Now()
			lastRate, _, _ = render(now)
			ui.finishLine(success, lastRate, now)
			return
		case now = <-ticker.C:
			tick++
			lastRate, _, _ = render(now)
		}
	}
}

func (ui *transferUI) loopPlain() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	rate := progressRateEstimator{}
	readRate := progressRateEstimator{}
	sendRate := progressRateEstimator{}
	generation := ui.rateGeneration.Load()
	tick := 0
	lastPhase := transferPhase(^uint32(0))
	lastTransferred := int64(-1)
	lastCompleted := int64(-1)
	lastRetries := int64(-1)
	lastBodyRead := int64(-1)
	lastBodySent := int64(-1)
	lastBodyWritten := int64(-1)
	lastInputClosed := false
	lastEmit := time.Time{}

	render := func(now time.Time, force bool) float64 {
		if currentGeneration := ui.rateGeneration.Load(); currentGeneration != generation {
			generation = currentGeneration
			rate.reset(now, ui.transferred.Load())
			sendRate.reset(now, ui.bodySentBytes.Load())
		}
		confirmedRate, stalled := rate.observe(now, ui.transferred.Load())
		inputRate, _ := readRate.observe(now, ui.readBytes.Load())
		bodyRate, _ := sendRate.observe(now, ui.bodySentBytes.Load())
		snapshot := ui.snapshot(now, confirmedRate, inputRate, bodyRate, stalled, tick)
		bodyReadAdvanced := lastBodyRead < 0 || snapshot.bodyReadBytes-lastBodyRead >= 8*1024*1024
		changed := snapshot.phase != lastPhase ||
			snapshot.transferred != lastTransferred ||
			snapshot.completedChunks != lastCompleted ||
			snapshot.retries != lastRetries ||
			snapshot.bodySentBytes != lastBodySent ||
			snapshot.bodyWritten != lastBodyWritten ||
			snapshot.inputClosed != lastInputClosed || bodyReadAdvanced
		heartbeat := lastEmit.IsZero() || now.Sub(lastEmit) >= 2*time.Second
		if force || changed || heartbeat {
			ui.writePlainSnapshot(now, snapshot)
			lastPhase = snapshot.phase
			lastTransferred = snapshot.transferred
			lastCompleted = snapshot.completedChunks
			lastRetries = snapshot.retries
			lastBodyRead = snapshot.bodyReadBytes
			lastBodySent = snapshot.bodySentBytes
			lastBodyWritten = snapshot.bodyWritten
			lastInputClosed = snapshot.inputClosed
			lastEmit = now
		}
		return confirmedRate
	}

	now := time.Now()
	rate.reset(now, ui.transferred.Load())
	readRate.reset(now, ui.readBytes.Load())
	sendRate.reset(now, ui.bodySentBytes.Load())
	lastRate := render(now, true)
	for {
		select {
		case success := <-ui.stopCh:
			now = time.Now()
			lastRate = render(now, true)
			ui.finishLine(success, lastRate, now)
			return
		case now = <-ticker.C:
			tick++
			lastRate = render(now, false)
		}
	}
}

func (ui *transferUI) stop(success bool) {
	if ui == nil || !ui.enabled {
		return
	}
	ui.stopOnce.Do(func() {
		ui.stopCh <- success
		<-ui.doneCh
	})
}

func (ui *transferUI) setPhase(phase transferPhase) {
	if ui == nil || !ui.enabled {
		return
	}
	now := time.Now()
	ui.detailsMu.Lock()
	if ui.phase != phase {
		ui.phase = phase
		ui.phaseSince = now
	}
	ui.detailsMu.Unlock()
	if phase == transferPhaseTransferring {
		ui.transferStart.CompareAndSwap(0, now.UnixNano())
		ui.finalizeSummary(now)
	}
}

func (ui *transferUI) configure(name string, total, totalChunks int64) {
	if ui == nil {
		return
	}
	ui.detailsMu.Lock()
	if strings.TrimSpace(name) != "" {
		ui.name = strings.TrimSpace(name)
	}
	ui.detailsMu.Unlock()
	ui.total.Store(total)
	ui.totalChunks.Store(totalChunks)
}

func (ui *transferUI) setPlan(detail string) {
	if ui == nil || !ui.enabled || strings.TrimSpace(detail) == "" {
		return
	}
	ui.planOnce.Do(func() {
		detail = strings.TrimSpace(detail)
		ui.detailsMu.Lock()
		ui.planDetail = detail
		ui.detailsMu.Unlock()
		if ui.plain {
			ui.emitInfo("plan", detail)
			return
		}
		ui.renderDynamic(time.Now(), ui.formatTransferSummary(ui.currentWidth()))
	})
}

func (ui *transferUI) setDestination(path string) {
	if ui == nil || !ui.enabled || strings.TrimSpace(path) == "" {
		return
	}
	ui.destinationOnce.Do(func() { ui.emitInfo("save", path) })
}

func (ui *transferUI) setBaseline(bytes, chunks int64) {
	if ui == nil {
		return
	}
	if bytes < 0 {
		bytes = 0
	}
	if chunks < 0 {
		chunks = 0
	}
	ui.baselineBytes.Store(bytes)
	ui.transferred.Store(bytes)
	ui.bodySentBytes.Store(bytes)
	ui.completedChunks.Store(chunks)
	ui.rateGeneration.Add(1)
	if ui.enabled && bytes > 0 {
		ui.resumeOnce.Do(func() {
			ui.emitInfo("resume", formatByteSize(bytes)+" already verified")
		})
	}
}

func (ui *transferUI) addBodyRead(bytes int64) {
	if ui != nil && bytes > 0 {
		ui.bodyReadBytes.Add(bytes)
		ui.bodySentBytes.Add(bytes)
	}
}

func (ui *transferUI) recordBodyRead(chunkIndex, attemptBytes, contentLength, bytes int64) {
	if ui == nil || bytes <= 0 {
		return
	}
	if chunkIndex < 0 || contentLength <= 0 {
		ui.bodyReadBytes.Add(bytes)
		ui.bodySentBytes.Add(bytes)
		return
	}
	ui.recordBodyReadProgress(ui.bodyProgressTracker(chunkIndex), attemptBytes, contentLength, bytes)
}

func (ui *transferUI) bodyProgressTracker(chunkIndex int64) *atomic.Int64 {
	if ui == nil || chunkIndex < 0 {
		return nil
	}
	candidate := &atomic.Int64{}
	value, _ := ui.bodyProgress.LoadOrStore(chunkIndex, candidate)
	tracker, _ := value.(*atomic.Int64)
	return tracker
}

func (ui *transferUI) recordBodyReadProgress(tracker *atomic.Int64, attemptBytes, contentLength, bytes int64) {
	if ui == nil || bytes <= 0 {
		return
	}
	ui.bodyReadBytes.Add(bytes)
	if tracker == nil || contentLength <= 0 {
		ui.bodySentBytes.Add(bytes)
		return
	}
	if attemptBytes < 0 {
		attemptBytes = 0
	}
	if attemptBytes > contentLength {
		attemptBytes = contentLength
	}
	for {
		previous := tracker.Load()
		if attemptBytes <= previous {
			return
		}
		if tracker.CompareAndSwap(previous, attemptBytes) {
			ui.bodySentBytes.Add(attemptBytes - previous)
			return
		}
	}
}

func (ui *transferUI) bodyRequestWritten(bytes int64) {
	if ui == nil {
		return
	}
	if bytes > 0 {
		ui.bodyWritten.Add(bytes)
	}
	ui.bodyRequests.Add(1)
}

func (ui *transferUI) addTransferred(bytes int64) {
	if ui == nil || bytes == 0 {
		return
	}
	for {
		current := ui.transferred.Load()
		next := current + bytes
		if next < 0 {
			next = 0
		}
		if ui.transferred.CompareAndSwap(current, next) {
			return
		}
	}
}

func (ui *transferUI) addRead(bytes int64) {
	if ui != nil && bytes > 0 {
		ui.readBytes.Add(bytes)
	}
}

func (ui *transferUI) markInputClosed() {
	if ui != nil {
		ui.inputClosed.Store(true)
	}
}

func (ui *transferUI) chunkStarted() {
	if ui != nil {
		ui.inFlight.Add(1)
	}
}

func (ui *transferUI) chunkFinished(success bool) {
	if ui == nil {
		return
	}
	ui.inFlight.Add(-1)
	if success {
		ui.completedChunks.Add(1)
	}
}

func (ui *transferUI) retried() {
	if ui != nil {
		ui.retries.Add(1)
	}
}

func (ui *transferUI) recordRequestDuration(duration time.Duration) {
	if ui == nil || duration <= 0 {
		return
	}
	ui.requestNanos.Add(duration.Nanoseconds())
	ui.requestCount.Add(1)
}

func (ui *transferUI) emitInfo(label, detail string) {
	now := time.Now()
	if ui.plain {
		ui.writePlainLine(now, "info",
			"label="+plainQuote(label),
			"detail="+plainQuote(detail),
		)
		return
	}
	ui.outputMu.Lock()
	defer ui.outputMu.Unlock()
	if !ui.lines {
		ui.clearDynamicLocked()
	} else {
		ui.lastRenderedLine = ""
	}
	ui.writeStyledLineLocked(now, ui.dim(label)+"  "+ui.trimVisible(detail, ui.currentWidth()-utf8.RuneCountInString(label)-3))
}

func (ui *transferUI) formatTransferSummary(width int) string {
	if ui == nil {
		return ""
	}
	ui.detailsMu.RLock()
	name := strings.TrimSpace(ui.name)
	plan := strings.TrimSpace(ui.planDetail)
	ui.detailsMu.RUnlock()
	if name == "" {
		name = "unnamed"
	}

	plain := "idoud"
	kind := strings.TrimSpace(ui.kind)
	if kind != "" {
		plain += " · " + kind
	}
	source := strings.TrimSpace(ui.source)
	if source == "stdin" || source == "archive" {
		name = source + " " + name
	}
	plain += " · " + name
	if total := ui.total.Load(); total >= 0 {
		plain += " · " + formatByteSize(total)
	}
	if plan != "" {
		plain += " · " + plan
	}
	plain = ui.trimVisible(plain, width)

	base := "idoud"
	if kind != "" {
		base += " · " + kind
	}
	if strings.HasPrefix(plain, base) {
		styled := ui.accent("idoud")
		if kind != "" {
			styled += ui.dim(" · " + kind)
		}
		return styled + strings.TrimPrefix(plain, base)
	}
	return plain
}

func (ui *transferUI) formatTransferSummaryStatus(status string) string {
	status = strings.TrimSpace(status)
	if status == "" {
		return ui.formatTransferSummary(ui.currentWidth())
	}
	available := ui.currentWidth() - visibleTerminalWidth(status) - 3
	if available < 20 {
		return "  " + status
	}
	return ui.formatTransferSummary(available) + ui.dim(" · ") + status
}

func (ui *transferUI) finalizeSummary(now time.Time) {
	if ui == nil || !ui.enabled || ui.plain {
		return
	}
	ui.detailsMu.Lock()
	if ui.summaryFinalized {
		ui.detailsMu.Unlock()
		return
	}
	ui.summaryFinalized = true
	ui.detailsMu.Unlock()

	line := ui.formatTransferSummary(ui.currentWidth())
	ui.outputMu.Lock()
	defer ui.outputMu.Unlock()
	if ui.lines {
		if line != ui.lastRenderedLine {
			ui.writeStyledLineLocked(now, line)
		}
		ui.lastRenderedLine = ""
		return
	}
	ui.clearDynamicLocked()
	ui.writeStyledLineLocked(now, line)
}

func (ui *transferUI) snapshot(now time.Time, rate, readRate, sendRate float64, stalled bool, tick int) transferProgressSnapshot {
	ui.detailsMu.RLock()
	phase := ui.phase
	phaseSince := ui.phaseSince
	ui.detailsMu.RUnlock()
	requestCount := ui.requestCount.Load()
	confirmationLatency := time.Duration(0)
	if requestCount > 0 {
		confirmationLatency = time.Duration(ui.requestNanos.Load() / requestCount)
	}
	elapsed := now.Sub(ui.started)
	if transferStarted := ui.transferStart.Load(); transferStarted > 0 {
		elapsed = now.Sub(time.Unix(0, transferStarted))
	}
	if elapsed < 0 {
		elapsed = 0
	}
	return transferProgressSnapshot{
		kind:                ui.kind,
		source:              ui.source,
		phase:               phase,
		total:               ui.total.Load(),
		transferred:         ui.transferred.Load(),
		baselineBytes:       ui.baselineBytes.Load(),
		readBytes:           ui.readBytes.Load(),
		bodyReadBytes:       ui.bodyReadBytes.Load(),
		bodySentBytes:       ui.bodySentBytes.Load(),
		bodyWritten:         ui.bodyWritten.Load(),
		bodyRequests:        ui.bodyRequests.Load(),
		sendRate:            sendRate,
		totalChunks:         ui.totalChunks.Load(),
		completedChunks:     ui.completedChunks.Load(),
		inFlight:            ui.inFlight.Load(),
		retries:             ui.retries.Load(),
		rate:                rate,
		readRate:            readRate,
		stalled:             stalled,
		inputClosed:         ui.inputClosed.Load(),
		confirmationLatency: confirmationLatency,
		phaseElapsed:        now.Sub(phaseSince),
		elapsed:             elapsed,
		tick:                tick,
	}
}

func plainQuote(value string) string {
	return strconv.Quote(strings.TrimSpace(value))
}

func (ui *transferUI) writePlainLine(now time.Time, event string, fields ...string) {
	if ui == nil || !ui.enabled || !ui.plain {
		return
	}
	var line strings.Builder
	fmt.Fprintf(&line, "%s idoud transfer=%s event=%s", now.Format(time.RFC3339Nano), ui.kind, event)
	for _, field := range fields {
		if strings.TrimSpace(field) != "" {
			line.WriteByte(' ')
			line.WriteString(field)
		}
	}
	ui.outputMu.Lock()
	defer ui.outputMu.Unlock()
	fmt.Fprintln(ui.writer, line.String())
}

func (ui *transferUI) writePlainStart(now time.Time) {
	source := strings.TrimSpace(ui.source)
	if source == "" {
		source = "file"
	}
	semantics := "provider_confirmed"
	if ui.kind == "download" {
		semantics = "disk_written"
	}
	ui.writePlainLine(now, "start",
		"source="+plainQuote(source),
		"name="+plainQuote(ui.currentName()),
		"progress_semantics="+semantics,
		fmt.Sprintf("total_bytes=%d", ui.total.Load()),
	)
}

func transferPhaseName(phase transferPhase) string {
	switch phase {
	case transferPhasePlanning:
		return "planning"
	case transferPhaseConnecting:
		return "connecting"
	case transferPhaseTransferring:
		return "transferring"
	case transferPhaseFinalizing:
		return "finalizing"
	case transferPhaseSaving:
		return "saving"
	default:
		return "unknown"
	}
}

func finiteRateInt(rate float64) int64 {
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) || rate >= float64(math.MaxInt64) {
		return 0
	}
	return int64(math.Round(rate))
}

func confirmedAverageRate(snapshot transferProgressSnapshot) float64 {
	moved := snapshot.transferred - snapshot.baselineBytes
	if moved <= 0 || snapshot.elapsed <= 0 {
		return 0
	}
	return float64(moved) / snapshot.elapsed.Seconds()
}

func bodyAverageRate(snapshot transferProgressSnapshot) float64 {
	if snapshot.bodyReadBytes <= 0 || snapshot.elapsed <= 0 {
		return 0
	}
	return float64(snapshot.bodyReadBytes) / snapshot.elapsed.Seconds()
}

func plainProgressState(snapshot transferProgressSnapshot) string {
	switch snapshot.phase {
	case transferPhasePlanning:
		return "preparing_plan"
	case transferPhaseConnecting:
		return "connecting_routes"
	case transferPhaseFinalizing:
		return "committing_provider_data"
	case transferPhaseSaving:
		return "saving_file"
	}
	if snapshot.kind == "download" {
		if snapshot.stalled && snapshot.inFlight > 0 {
			return "awaiting_download_data"
		}
		if snapshot.rate > 0 || snapshot.inFlight > 0 {
			return "downloading"
		}
		return "waiting"
	}
	if snapshot.bodyReadBytes > snapshot.bodyWritten && snapshot.inFlight > 0 {
		return "writing_request_bodies"
	}
	if snapshot.inputClosed && snapshot.inFlight > 0 {
		return "awaiting_confirmation"
	}
	if snapshot.bodyWritten > snapshot.transferred && snapshot.inFlight > 0 {
		return "awaiting_provider_storage"
	}
	if snapshot.stalled && snapshot.inFlight > 0 {
		return "awaiting_confirmation"
	}
	if snapshot.rate > 0 {
		return "confirming"
	}
	if snapshot.readRate > 0 {
		return "reading_input"
	}
	if snapshot.inFlight > 0 {
		return "warming_up"
	}
	return "waiting"
}

func (ui *transferUI) writePlainSnapshot(now time.Time, snapshot transferProgressSnapshot) {
	semantics := "provider_confirmed"
	if ui.kind == "download" {
		semantics = "disk_written"
	}
	fields := []string{
		"phase=" + transferPhaseName(snapshot.phase),
		"state=" + plainProgressState(snapshot),
		"progress_semantics=" + semantics,
		fmt.Sprintf("phase_elapsed_ms=%d", snapshot.phaseElapsed.Milliseconds()),
		fmt.Sprintf("elapsed_ms=%d", snapshot.elapsed.Milliseconds()),
		fmt.Sprintf("completed_bytes=%d", snapshot.transferred),
		fmt.Sprintf("baseline_bytes=%d", snapshot.baselineBytes),
		fmt.Sprintf("completed_rate_bps=%d", finiteRateInt(snapshot.rate)),
		fmt.Sprintf("completed_average_bps=%d", finiteRateInt(confirmedAverageRate(snapshot))),
		fmt.Sprintf("active_requests=%d", snapshot.inFlight),
		fmt.Sprintf("retries=%d", snapshot.retries),
	}
	if snapshot.total >= 0 {
		percent := float64(100)
		if snapshot.total > 0 {
			percent = float64(snapshot.transferred) / float64(snapshot.total) * 100
		}
		if percent < 0 {
			percent = 0
		}
		if percent > 100 {
			percent = 100
		}
		fields = append(fields,
			fmt.Sprintf("total_bytes=%d", snapshot.total),
			fmt.Sprintf("percent=%.2f", percent),
		)
	}
	if snapshot.totalChunks >= 0 {
		fields = append(fields,
			fmt.Sprintf("completed_parts=%d", snapshot.completedChunks),
			fmt.Sprintf("total_parts=%d", snapshot.totalChunks),
		)
	}
	if snapshot.readBytes > 0 || snapshot.source == "stdin" || snapshot.source == "archive" {
		field := "input_bytes"
		if snapshot.source == "archive" {
			field = "packed_bytes"
		}
		fields = append(fields, fmt.Sprintf("%s=%d", field, snapshot.readBytes))
	}
	if ui.kind == "upload" {
		fields = append(fields,
			fmt.Sprintf("body_read_bytes=%d", snapshot.bodyReadBytes),
			fmt.Sprintf("body_sent_bytes=%d", snapshot.bodySentBytes),
			fmt.Sprintf("body_send_rate_bps=%d", finiteRateInt(snapshot.sendRate)),
			fmt.Sprintf("body_written_bytes=%d", snapshot.bodyWritten),
			fmt.Sprintf("body_requests_written=%d", snapshot.bodyRequests),
			fmt.Sprintf("body_average_bps=%d", finiteRateInt(bodyAverageRate(snapshot))),
			fmt.Sprintf("confirmation_average_ms=%d", snapshot.confirmationLatency.Milliseconds()),
		)
	}
	eta := progressETA(snapshot)
	etaMillis := int64(-1)
	if eta > 0 {
		etaMillis = eta.Milliseconds()
	}
	fields = append(fields, fmt.Sprintf("eta_ms=%d", etaMillis))
	ui.writePlainLine(now, "progress", fields...)
}

func (ui *transferUI) formatProgress(snapshot transferProgressSnapshot) string {
	width := ui.currentWidth()
	switch snapshot.phase {
	case transferPhasePlanning:
		status := ui.spinner(snapshot.tick) + " " + ui.accentLight("preparing plan") + ui.dim(" · "+formatProgressElapsed(snapshot.phaseElapsed))
		if snapshot.retries > 0 {
			status += ui.link(fmt.Sprintf(" · retry %d", snapshot.retries))
		}
		return ui.formatTransferSummaryStatus(status)
	case transferPhaseConnecting:
		status := ui.spinner(snapshot.tick) + " " + ui.accentLight("connecting routes") + ui.dim(" · "+formatProgressElapsed(snapshot.phaseElapsed))
		return ui.formatTransferSummaryStatus(status)
	case transferPhaseFinalizing:
		label := "committing provider data"
		if snapshot.kind == "download" {
			label = "verifying download"
		}
		if width < 62 {
			label = "finalizing"
		}
		return "  " + ui.spinner(snapshot.tick) + " " + ui.accentLight(label) + ui.dim(" · "+formatByteSize(snapshot.transferred)+" · "+formatProgressElapsed(snapshot.phaseElapsed))
	case transferPhaseSaving:
		label := "syncing completed file"
		if width < 62 {
			label = "saving"
		}
		return "  " + ui.spinner(snapshot.tick) + " " + ui.accentLight(label) + ui.dim(" · "+formatByteSize(snapshot.transferred)+" · "+formatProgressElapsed(snapshot.phaseElapsed))
	}
	if snapshot.total < 0 {
		return ui.formatUnknownProgress(snapshot, width)
	}
	return ui.formatKnownProgress(snapshot, width)
}

func (ui *transferUI) formatKnownProgress(snapshot transferProgressSnapshot, width int) string {
	total := snapshot.total
	confirmed := snapshot.transferred
	if total <= 0 {
		confirmed = 0
	}
	if confirmed < 0 {
		confirmed = 0
	}
	if total > 0 && confirmed > total {
		confirmed = total
	}
	sent := snapshot.bodySentBytes
	if sent < confirmed {
		sent = confirmed
	}
	if sent < 0 {
		sent = 0
	}
	if total > 0 && sent > total {
		sent = total
	}
	showSent := snapshot.kind == "upload" && sent > confirmed
	done := confirmed
	if showSent {
		done = sent
	}
	ratio := 1.0
	if total > 0 {
		ratio = float64(done) / float64(total)
	}
	percent := fmt.Sprintf("%5.1f%%", ratio*100)
	rateText := "warming up"
	if showSent && sent < total && snapshot.sendRate > 0 {
		rateText = "sending " + formatRateFromPerSecond(snapshot.sendRate)
	} else if showSent && sent < total && snapshot.inFlight > 0 {
		rateText = "sending request bodies"
	} else if showSent && confirmed < total {
		rateText = "awaiting storage"
	} else if snapshot.bodyReadBytes > snapshot.bodyWritten && snapshot.inFlight > 0 {
		rateText = "sending request bodies"
	} else if snapshot.inputClosed && snapshot.inFlight > 0 {
		rateText = "awaiting confirmation"
	} else if snapshot.bodyWritten > snapshot.transferred && snapshot.inFlight > 0 {
		rateText = "awaiting provider storage"
	} else if snapshot.inputClosed && snapshot.source != "file" {
		rateText = "input complete"
	} else if snapshot.stalled {
		if snapshot.inFlight > 0 {
			rateText = "awaiting confirmation"
		} else if snapshot.source == "file" {
			rateText = "waiting"
		} else {
			rateText = "waiting for input"
		}
	} else if snapshot.rate > 0 {
		rateText = formatRateFromPerSecond(snapshot.rate)
	} else if snapshot.readRate > 0 && snapshot.source != "file" {
		rateText = "input " + formatRateFromPerSecond(snapshot.readRate)
	}
	eta := progressETA(snapshot)
	etaText := "eta —"
	if eta > 0 {
		etaText = "eta ~" + formatProgressETA(eta)
	}

	barWidth := 0
	switch {
	case width >= 112:
		barWidth = 24
	case width >= 88:
		barWidth = 16
	case width >= 72:
		barWidth = 10
	}
	line := "  " + ui.progressMark() + " "
	if barWidth > 0 {
		line += ui.accent(ui.progressBar(ratio, barWidth)) + " "
	}
	if showSent {
		line += ui.accentLight("sent "+strings.TrimSpace(percent)) + "  "
	} else {
		line += ui.accentLight(percent) + "  "
	}
	line += formatByteSize(done) + "/" + formatByteSize(total)
	if width >= 72 {
		line += ui.dim(" · ") + ui.accent(rateText)
		if showSent && width >= 96 {
			line += ui.dim(" · stored ") + formatByteSize(confirmed)
		}
		line += ui.dim(" · ") + ui.link(etaText)
	}
	if width >= 118 && snapshot.totalChunks > 0 {
		line += ui.dim(fmt.Sprintf(" · %d/%d parts", snapshot.completedChunks, snapshot.totalChunks))
		if snapshot.inFlight > 0 {
			line += ui.dim(fmt.Sprintf(" · %d active", snapshot.inFlight))
		}
	}
	if snapshot.retries > 0 && width >= 96 {
		line += ui.link(fmt.Sprintf(" · %d retries", snapshot.retries))
	}
	return line
}

func (ui *transferUI) formatUnknownProgress(snapshot transferProgressSnapshot, width int) string {
	barWidth := 16
	if width < 80 {
		barWidth = 10
	}
	if width < 62 {
		barWidth = 0
	}
	line := "  " + ui.progressMark() + " "
	if barWidth > 0 {
		line += ui.accent(ui.activityBar(snapshot.tick, barWidth)) + " "
	}
	sent := snapshot.bodySentBytes
	if sent < snapshot.transferred {
		sent = snapshot.transferred
	}
	showSent := snapshot.kind == "upload" && sent > snapshot.transferred
	line += ui.accentLight("streaming") + "  "
	if showSent {
		line += "sent " + formatByteSize(sent) + ui.dim(" · stored ") + formatByteSize(snapshot.transferred)
	} else {
		line += "stored " + formatByteSize(snapshot.transferred)
	}
	if width >= 58 && (snapshot.readBytes > snapshot.transferred || snapshot.source == "stdin" || snapshot.source == "archive") {
		readLabel := "read"
		if snapshot.source == "archive" {
			readLabel = "packed"
		}
		line += ui.dim(" · ") + readLabel + " " + formatByteSize(snapshot.readBytes)
	}
	if width >= 76 {
		rateText := "warming up"
		if showSent && snapshot.sendRate > 0 {
			rateText = "sending " + formatRateFromPerSecond(snapshot.sendRate)
		} else if showSent && snapshot.inFlight > 0 {
			rateText = "sending request bodies"
		} else if snapshot.bodyReadBytes > snapshot.bodyWritten && snapshot.inFlight > 0 {
			rateText = "sending request bodies"
		} else if snapshot.inputClosed && snapshot.inFlight > 0 {
			rateText = "awaiting confirmation"
		} else if snapshot.bodyWritten > snapshot.transferred && snapshot.inFlight > 0 {
			rateText = "awaiting provider storage"
		} else if snapshot.inputClosed {
			rateText = "input complete"
		} else if snapshot.stalled {
			if snapshot.inFlight > 0 {
				rateText = "awaiting confirmation"
			} else if snapshot.source == "file" {
				rateText = "waiting"
			} else {
				rateText = "waiting for input"
			}
		} else if snapshot.rate > 0 {
			rateText = formatRateFromPerSecond(snapshot.rate)
		} else if snapshot.readRate > 0 {
			rateText = "input " + formatRateFromPerSecond(snapshot.readRate)
		}
		line += ui.dim(" · ") + ui.accent(rateText)
	}
	if snapshot.retries > 0 && width >= 100 {
		line += ui.link(fmt.Sprintf(" · %d retries", snapshot.retries))
	}
	return line
}

func progressETA(snapshot transferProgressSnapshot) time.Duration {
	if snapshot.total <= 0 || snapshot.transferred >= snapshot.total {
		return 0
	}
	// TCP ramp-up and the first provider confirmation are not representative
	// enough for a useful ETA. Learn for three seconds, unless a very fast
	// transfer has already moved a substantial sample.
	if snapshot.phaseElapsed < 3*time.Second && snapshot.transferred < 64*1024*1024 {
		return 0
	}
	remaining := float64(snapshot.total - snapshot.transferred)
	eta := time.Duration(0)
	confirmedRateReady := snapshot.totalChunks <= 2 || snapshot.completedChunks >= 2
	if confirmedRateReady && snapshot.rate > 0 && !snapshot.stalled {
		eta = time.Duration(remaining / snapshot.rate * float64(time.Second))
	}
	if snapshot.kind == "upload" && snapshot.sendRate > 0 {
		sent := snapshot.bodySentBytes
		if sent < snapshot.transferred {
			sent = snapshot.transferred
		}
		if sent < snapshot.total {
			sendRemaining := float64(snapshot.total - sent)
			sendETA := time.Duration(sendRemaining / snapshot.sendRate * float64(time.Second))
			if sendETA > eta {
				eta = sendETA
			}
		}
	}
	if snapshot.source != "file" && snapshot.readRate > 0 && snapshot.readBytes < snapshot.total {
		inputRemaining := float64(snapshot.total - snapshot.readBytes)
		inputETA := time.Duration(inputRemaining / snapshot.readRate * float64(time.Second))
		if inputETA > eta {
			eta = inputETA
		}
	}
	if snapshot.confirmationLatency > 0 {
		latencyFloor := snapshot.confirmationLatency
		if snapshot.inFlight > 0 {
			latencyFloor /= 2
		}
		if latencyFloor > eta {
			eta = latencyFloor
		}
	}
	if eta < time.Second && eta > 0 {
		return time.Second
	}
	return eta
}

func (ui *transferUI) finishLine(success bool, lastRate float64, now time.Time) {
	ui.finalizeSummary(now)
	snapshot := ui.snapshot(now, lastRate, 0, 0, false, 0)
	averageRate := confirmedAverageRate(snapshot)
	if ui.plain {
		result := "failure"
		if success {
			result = "success"
		}
		semantics := "provider_confirmed"
		if ui.kind == "download" {
			semantics = "disk_written"
		}
		fields := []string{
			"result=" + result,
			"progress_semantics=" + semantics,
			fmt.Sprintf("completed_bytes=%d", snapshot.transferred),
			fmt.Sprintf("elapsed_ms=%d", snapshot.elapsed.Milliseconds()),
			fmt.Sprintf("average_bps=%d", finiteRateInt(averageRate)),
			fmt.Sprintf("retries=%d", snapshot.retries),
		}
		if ui.kind == "upload" {
			fields = append(fields,
				fmt.Sprintf("body_read_bytes=%d", snapshot.bodyReadBytes),
				fmt.Sprintf("body_sent_bytes=%d", snapshot.bodySentBytes),
				fmt.Sprintf("body_written_bytes=%d", snapshot.bodyWritten),
			)
		}
		ui.writePlainLine(now, "complete", fields...)
		return
	}
	ui.outputMu.Lock()
	defer ui.outputMu.Unlock()
	if !ui.lines {
		ui.clearDynamicLocked()
	}
	transferred := snapshot.transferred
	elapsed := snapshot.elapsed
	if success {
		verb := "stored"
		if ui.kind == "download" {
			verb = "saved"
		}
		line := ui.success(ui.successMark() + " complete")
		line += ui.dim(" · ") + formatByteSize(transferred) + " " + verb
		line += ui.dim(" · ") + formatProgressElapsed(elapsed)
		if averageRate > 0 {
			line += ui.dim(" · ") + ui.accent(formatRateFromPerSecond(averageRate)+" avg")
		}
		ui.writeStyledLineLocked(now, "  "+line)
		return
	}
	ui.writeStyledLineLocked(now, "  "+ui.failure(ui.failureMark()+" transfer stopped")+ui.dim(" · "+formatByteSize(transferred)+" confirmed"))
}

func (ui *transferUI) renderDynamic(now time.Time, line string) {
	ui.outputMu.Lock()
	defer ui.outputMu.Unlock()
	if ui.lines {
		if line == ui.lastRenderedLine && !ui.lastRenderedAt.IsZero() && now.Sub(ui.lastRenderedAt) < time.Second {
			return
		}
		ui.writeStyledLineLocked(now, line)
		ui.lastRenderedLine = line
		ui.lastRenderedAt = now
		return
	}
	visible := visibleTerminalWidth(line)
	padding := ui.lastLineWidth - visible
	if padding < 0 {
		padding = 0
	}
	fmt.Fprintf(ui.writer, "\r%s%s", line, strings.Repeat(" ", padding))
	ui.lastLineWidth = visible
}

func (ui *transferUI) writeStyledLineLocked(now time.Time, line string) {
	if ui.lines {
		fmt.Fprintf(ui.writer, "%s %s\n", now.Format("2006-01-02T15:04:05.000Z07:00"), line)
		return
	}
	fmt.Fprintln(ui.writer, line)
}

func (ui *transferUI) clearDynamicLocked() {
	if ui.lastLineWidth <= 0 {
		return
	}
	fmt.Fprintf(ui.writer, "\r%s\r", strings.Repeat(" ", ui.lastLineWidth))
	ui.lastLineWidth = 0
}

func (ui *transferUI) currentName() string {
	ui.detailsMu.RLock()
	defer ui.detailsMu.RUnlock()
	if ui.name == "" {
		return "unnamed"
	}
	return ui.name
}

func (ui *transferUI) currentWidth() int {
	width := ui.width()
	if width < 40 {
		return 40
	}
	if width > 180 {
		return 180
	}
	return width
}

func (ui *transferUI) trimVisible(value string, width int) string {
	if width <= 0 || utf8.RuneCountInString(value) <= width {
		return value
	}
	if width <= 3 {
		return strings.Repeat(".", width)
	}
	runes := []rune(value)
	return string(runes[:width-3]) + "..."
}

func (ui *transferUI) progressBar(ratio float64, width int) string {
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	filledFloat := ratio * float64(width)
	filled := int(math.Floor(filledFloat))
	partial := filled < width && filledFloat-float64(filled) >= 0.15
	fullChar, partialChar, emptyChar := "=", ">", "-"
	if ui.unicode {
		fullChar, partialChar, emptyChar = "━", "╺", "─"
	}
	var out strings.Builder
	out.WriteByte('[')
	out.WriteString(strings.Repeat(fullChar, filled))
	if partial {
		out.WriteString(partialChar)
		filled++
	}
	if filled < width {
		out.WriteString(strings.Repeat(emptyChar, width-filled))
	}
	out.WriteByte(']')
	return out.String()
}

func (ui *transferUI) activityBar(tick, width int) string {
	fullChar, headChar, emptyChar := "=", ">", "-"
	if ui.unicode {
		fullChar, headChar, emptyChar = "━", "╺", "─"
	}
	pulse := 4
	if pulse+1 > width {
		pulse = width - 1
	}
	position := 0
	travel := width - pulse - 1
	if travel > 0 {
		position = tick % (travel + 1)
	}
	return "[" + strings.Repeat(emptyChar, position) + strings.Repeat(fullChar, pulse) + headChar + strings.Repeat(emptyChar, width-position-pulse-1) + "]"
}

func (ui *transferUI) spinner(tick int) string {
	frames := []string{"o", "O", "o", "."}
	if ui.unicode {
		frames = []string{"◐", "◓", "◑", "◒"}
	}
	return ui.accent(frames[tick%len(frames)])
}

func (ui *transferUI) progressMark() string {
	if ui.unicode {
		return ui.accent("◆")
	}
	return ui.accent(">")
}

func (ui *transferUI) successMark() string {
	if ui.unicode {
		return "✓"
	}
	return "ok"
}

func (ui *transferUI) failureMark() string {
	if ui.unicode {
		return "×"
	}
	return "x"
}

func (ui *transferUI) accent(value string) string {
	if !ui.color {
		return value
	}
	return "\x1b[38;2;120;182;173m" + value + ansiReset
}

func (ui *transferUI) accentLight(value string) string {
	if !ui.color {
		return value
	}
	return "\x1b[38;2;135;201;229m" + value + ansiReset
}

func (ui *transferUI) link(value string) string {
	if !ui.color {
		return value
	}
	return "\x1b[38;2;226;174;162m" + value + ansiReset
}

func (ui *transferUI) dim(value string) string {
	if !ui.color {
		return value
	}
	return ansiDimWhite + value + ansiReset
}

func (ui *transferUI) success(value string) string {
	if !ui.color {
		return value
	}
	return ansiBoldGreen + value + ansiReset
}

func (ui *transferUI) failure(value string) string {
	if !ui.color {
		return value
	}
	return ansiBoldRed + value + ansiReset
}

func (r *progressRateEstimator) reset(now time.Time, bytes int64) {
	r.base = progressRateSample{at: now, bytes: bytes}
	r.advances = r.advances[:0]
	r.smoothed = 0
	r.lastBytes = bytes
	r.lastAdvance = now
}

func (r *progressRateEstimator) observe(now time.Time, bytes int64) (float64, bool) {
	if r.base.at.IsZero() {
		r.reset(now, bytes)
		return 0, false
	}
	if bytes < r.lastBytes {
		r.reset(now, bytes)
		return 0, false
	}
	if bytes > r.lastBytes {
		r.lastAdvance = now
		r.lastBytes = bytes
		r.advances = append(r.advances, progressRateSample{at: now, bytes: bytes})
		cutoff := now.Add(-8 * time.Second)
		for len(r.advances) > 2 && r.advances[1].at.Before(cutoff) {
			r.advances = r.advances[1:]
		}
		first := r.base
		if len(r.advances) >= 2 {
			first = r.advances[0]
		}
		elapsed := now.Sub(first.at)
		raw := float64(0)
		if elapsed >= 100*time.Millisecond && bytes >= first.bytes {
			raw = float64(bytes-first.bytes) / elapsed.Seconds()
		}
		if raw > 0 && !math.IsNaN(raw) && !math.IsInf(raw, 0) {
			if r.smoothed <= 0 || len(r.advances) <= 2 {
				r.smoothed = raw
			} else {
				r.smoothed = r.smoothed*0.55 + raw*0.45
			}
		}
	}
	stalled := bytes > 0 && now.Sub(r.lastAdvance) >= 3*time.Second
	if stalled {
		return r.smoothed, true
	}
	return r.smoothed, false
}

func formatProgressETA(eta time.Duration) string {
	if eta <= 0 {
		return "—"
	}
	eta = eta.Round(time.Second)
	if eta < time.Second {
		eta = time.Second
	}
	if eta < time.Minute {
		return eta.String()
	}
	if eta < time.Hour {
		minutes := int(eta / time.Minute)
		seconds := int((eta % time.Minute) / time.Second)
		return fmt.Sprintf("%dm%02ds", minutes, seconds)
	}
	if eta < 100*time.Hour {
		hours := int(eta / time.Hour)
		minutes := int((eta % time.Hour) / time.Minute)
		return fmt.Sprintf("%dh%02dm", hours, minutes)
	}
	return ">99h"
}

func formatProgressElapsed(elapsed time.Duration) string {
	if elapsed < 0 {
		elapsed = 0
	}
	if elapsed < time.Minute {
		return elapsed.Round(100 * time.Millisecond).String()
	}
	return formatProgressETA(elapsed)
}

func visibleTerminalWidth(value string) int {
	width := 0
	escape := false
	for _, r := range value {
		if escape {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				escape = false
			}
			continue
		}
		if r == '\x1b' {
			escape = true
			continue
		}
		width++
	}
	return width
}

func uploadProgressSource(src *sourceFile) string {
	switch {
	case src == nil:
		return "file"
	case src.archive:
		return "archive"
	case src.fromStdin:
		return "stdin"
	default:
		return "file"
	}
}

func (u *uploader) startUploadProgress(src *sourceFile) *transferUI {
	if u == nil || src == nil {
		return nil
	}
	total := int64(-1)
	totalChunks := int64(-1)
	if src.knownSize {
		total = src.size
		if u.opts.chunkSize > 0 && src.size > 0 {
			totalChunks = (src.size + u.opts.chunkSize - 1) / u.opts.chunkSize
		} else if src.size == 0 {
			totalChunks = 0
		}
	}
	ui := newTransferUI(terminalTransferUIConfig(
		u.opts,
		"upload",
		uploadProgressSource(src),
		src.uploadName,
		total,
		totalChunks,
	))
	if !ui.enabled {
		return nil
	}
	u.ui = ui
	ui.start()
	return ui
}

func (u *uploader) configureUploadProgress(src *sourceFile) {
	if u == nil || u.ui == nil || src == nil {
		return
	}
	total := int64(-1)
	totalChunks := int64(-1)
	if src.knownSize {
		total = src.size
		if src.size > 0 && u.opts.chunkSize > 0 {
			totalChunks = (src.size + u.opts.chunkSize - 1) / u.opts.chunkSize
		} else if src.size == 0 {
			totalChunks = 0
		}
	}
	u.ui.configure(src.uploadName, total, totalChunks)

	activeRoutes := len(src.uploadRouteTargets)
	if activeRoutes == 0 {
		activeRoutes = len(src.uploadURLs)
	}
	if activeRoutes < 1 {
		activeRoutes = 1
	}
	parallel := uploadProgressParallel(u, src, totalChunks)
	parts := formatByteSize(u.opts.chunkSize) + " parts"
	if totalChunks >= 0 {
		parts = fmt.Sprintf("%d × %s", totalChunks, formatByteSize(u.opts.chunkSize))
	}
	plan := fmt.Sprintf("%d %s · up to %d parallel · %s",
		activeRoutes,
		pluralizeProgress(activeRoutes, "route", "routes"),
		parallel,
		parts,
	)
	if standby := len(src.uploadFallbackTargets); standby > 0 {
		plan += fmt.Sprintf(" · %d standby %s ready", standby, pluralizeProgress(standby, "route", "routes"))
	}
	u.ui.setPlan(plan)

	if src.knownSize && src.readerAt != nil {
		bytes, chunks := src.committedUploadProgress(u.opts.chunkSize)
		u.ui.setBaseline(bytes, chunks)
	}
}

func uploadProgressParallel(u *uploader, src *sourceFile, totalChunks int64) int {
	parallel := u.effectiveUploadParallel()
	if parallel < 1 {
		parallel = 1
	}
	if !src.knownSize {
		bufferWindow := u.streamParallelLimit(parallel)
		if bufferWindow < 1 {
			bufferWindow = 1
		}
		if parallel > bufferWindow {
			parallel = bufferWindow
		}
		return parallel
	}
	concurrentChunks := totalChunks
	if src.stream != nil && src.readerAt == nil {
		concurrentChunks--
	}
	if concurrentChunks < 1 {
		concurrentChunks = 1
	}
	if int64(parallel) > concurrentChunks {
		parallel = int(concurrentChunks)
	}
	if src.stream != nil && src.readerAt == nil {
		bufferWindow := u.streamBufferCountLimit(parallel, 0)
		if parallel > bufferWindow {
			parallel = bufferWindow
		}
	}
	return parallel
}

func (src *sourceFile) committedUploadProgress(chunkSize int64) (int64, int64) {
	if src == nil || !src.knownSize || src.size <= 0 || chunkSize <= 0 {
		return 0, 0
	}
	src.committedMu.Lock()
	defer src.committedMu.Unlock()
	var bytes int64
	var chunks int64
	for index := range src.committedChunks {
		start := index * chunkSize
		if index < 0 || start < 0 || start >= src.size {
			continue
		}
		end := start + chunkSize
		if end > src.size {
			end = src.size
		}
		bytes += end - start
		chunks++
	}
	if bytes > src.size {
		bytes = src.size
	}
	return bytes, chunks
}

func pluralizeProgress(value int, singular, plural string) string {
	if value == 1 {
		return singular
	}
	return plural
}
