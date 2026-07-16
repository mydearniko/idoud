package cli

import (
	"bytes"
	"strings"
	"testing"
)

func tabProgressEnv(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestTerminalTabProgressCapabilityDetection(t *testing.T) {
	tests := []struct {
		name     string
		terminal bool
		env      map[string]string
		want     bool
	}{
		{name: "windows terminal", terminal: true, env: map[string]string{"WT_SESSION": "session"}, want: true},
		{name: "windows terminal profile", terminal: true, env: map[string]string{"WT_PROFILE_ID": "profile"}, want: true},
		{name: "conemu compatible", terminal: true, env: map[string]string{"ConEmuANSI": "ON"}, want: true},
		{name: "explicit enable", terminal: true, env: map[string]string{"IDOUD_TAB_PROGRESS": "yes"}, want: true},
		{name: "explicit disable wins", terminal: true, env: map[string]string{"IDOUD_TAB_PROGRESS": "off", "WT_SESSION": "session"}},
		{name: "redirected output", terminal: false, env: map[string]string{"IDOUD_TAB_PROGRESS": "on", "WT_SESSION": "session"}},
		{name: "unrelated terminal", terminal: true, env: map[string]string{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := terminalTabProgressEnabled(test.terminal, tabProgressEnv(test.env)); got != test.want {
				t.Fatalf("enabled=%t, want %t", got, test.want)
			}
		})
	}
}

func TestTerminalTabProgressSequenceIsOSC94(t *testing.T) {
	tests := []struct {
		state terminalTabProgressState
		value int
		want  string
	}{
		{state: terminalTabProgressHidden, value: 0, want: "\x1b]9;4;0;0\a"},
		{state: terminalTabProgressNormal, value: 50, want: "\x1b]9;4;1;50\a"},
		{state: terminalTabProgressError, value: 25, want: "\x1b]9;4;2;25\a"},
		{state: terminalTabProgressIndeterminate, value: 0, want: "\x1b]9;4;3;0\a"},
		{state: terminalTabProgressIndeterminate, value: 75, want: "\x1b]9;4;3;0\a"},
		{state: terminalTabProgressWarning, value: 75, want: "\x1b]9;4;4;75\a"},
		{state: terminalTabProgressNormal, value: 200, want: "\x1b]9;4;1;100\a"},
		{state: terminalTabProgressState(99), value: 50, want: "\x1b]9;4;0;0\a"},
	}

	for _, test := range tests {
		if got := terminalTabProgressSequence(test.state, test.value); got != test.want {
			t.Fatalf("sequence(%d, %d)=%q, want %q", test.state, test.value, got, test.want)
		}
	}
}

func TestTerminalTabProgressMapsFullTransferLifecycle(t *testing.T) {
	tests := []struct {
		name     string
		snapshot transferProgressSnapshot
		state    terminalTabProgressState
		value    int
	}{
		{name: "planning", snapshot: transferProgressSnapshot{phase: transferPhasePlanning, total: 100}, state: terminalTabProgressIndeterminate},
		{name: "connecting", snapshot: transferProgressSnapshot{phase: transferPhaseConnecting, total: 100}, state: terminalTabProgressIndeterminate},
		{name: "known upload sending", snapshot: transferProgressSnapshot{kind: "upload", phase: transferPhaseTransferring, total: 100, transferred: 20, bodySentBytes: 50}, state: terminalTabProgressNormal, value: 50},
		{name: "known upload storage wait", snapshot: transferProgressSnapshot{kind: "upload", phase: transferPhaseTransferring, total: 100, transferred: 20, bodySentBytes: 100}, state: terminalTabProgressNormal, value: 99},
		{name: "known upload durable", snapshot: transferProgressSnapshot{kind: "upload", phase: transferPhaseFinalizing, total: 100, transferred: 100, bodySentBytes: 100}, state: terminalTabProgressNormal, value: 100},
		{name: "known download", snapshot: transferProgressSnapshot{kind: "download", phase: transferPhaseTransferring, total: 100, transferred: 65}, state: terminalTabProgressNormal, value: 65},
		{name: "empty transfer", snapshot: transferProgressSnapshot{phase: transferPhaseSaving, total: 0}, state: terminalTabProgressNormal, value: 100},
		{name: "unknown stream", snapshot: transferProgressSnapshot{kind: "upload", phase: transferPhaseTransferring, total: -1, bodySentBytes: 100}, state: terminalTabProgressIndeterminate},
		{name: "retry warning", snapshot: transferProgressSnapshot{kind: "download", phase: transferPhaseTransferring, total: 100, transferred: 40, retries: 1}, state: terminalTabProgressWarning, value: 40},
		{name: "unknown stall warning", snapshot: transferProgressSnapshot{phase: transferPhaseTransferring, total: -1, stalled: true}, state: terminalTabProgressWarning},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state, value := terminalTabProgressForSnapshot(test.snapshot)
			if state != test.state || value != test.value {
				t.Fatalf("state/value=%d/%d, want %d/%d", state, value, test.state, test.value)
			}
		})
	}
}

func TestTransferUIEmitsAndClearsTabProgressOnFailure(t *testing.T) {
	var output bytes.Buffer
	ui := newTransferUI(transferUIConfig{
		enabled:     true,
		lines:       true,
		tabProgress: true,
		writer:      &output,
		width:       func() int { return 100 },
		kind:        "upload",
		source:      "file",
		name:        "fixture.bin",
		total:       100,
	})
	ui.start()
	ui.setPhase(transferPhaseTransferring)
	ui.chunkStarted()
	ui.addBodyRead(50)
	ui.bodyRequestWritten(50)
	ui.stop(false)

	got := output.String()
	normal := terminalTabProgressSequence(terminalTabProgressNormal, 50)
	failure := terminalTabProgressSequence(terminalTabProgressError, 50)
	hidden := terminalTabProgressSequence(terminalTabProgressHidden, 0)
	for _, want := range []string{normal, failure, hidden, "transfer stopped"} {
		if !strings.Contains(got, want) {
			t.Fatalf("output %q does not contain %q", got, want)
		}
	}
	if !(strings.Index(got, normal) < strings.Index(got, failure) && strings.Index(got, failure) < strings.LastIndex(got, hidden)) {
		t.Fatalf("tab progress lifecycle is out of order: %q", got)
	}
}

func TestTransferUICompletesUnknownTabProgressAndPlainModeStaysClean(t *testing.T) {
	var interactive bytes.Buffer
	ui := newTransferUI(transferUIConfig{
		enabled:     true,
		lines:       true,
		tabProgress: true,
		writer:      &interactive,
		kind:        "upload",
		source:      "stdin",
		name:        "stream.bin",
		total:       -1,
	})
	ui.start()
	ui.stop(true)

	got := interactive.String()
	for _, want := range []string{
		terminalTabProgressSequence(terminalTabProgressIndeterminate, 0),
		terminalTabProgressSequence(terminalTabProgressNormal, 100),
		terminalTabProgressSequence(terminalTabProgressHidden, 0),
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("unknown progress output %q does not contain %q", got, want)
		}
	}

	var plain bytes.Buffer
	plainUI := newTransferUI(transferUIConfig{
		enabled:     true,
		plain:       true,
		tabProgress: true,
		writer:      &plain,
		kind:        "download",
		total:       100,
	})
	plainUI.start()
	plainUI.stop(true)
	if strings.Contains(plain.String(), "\x1b]9;4;") {
		t.Fatalf("plain progress emitted tab control sequences: %q", plain.String())
	}
}
