package cli

import (
	"fmt"
	"os"
	"strings"
)

// Windows Terminal implements the ConEmu OSC 9;4 progress protocol. It shows
// the state in both the active tab header and the Windows taskbar. Detection is
// environment-based rather than GOOS-based so the native Windows binary and a
// Linux binary running under WSL behave identically.
type terminalTabProgressState int

const (
	terminalTabProgressHidden        terminalTabProgressState = 0
	terminalTabProgressNormal        terminalTabProgressState = 1
	terminalTabProgressError         terminalTabProgressState = 2
	terminalTabProgressIndeterminate terminalTabProgressState = 3
	terminalTabProgressWarning       terminalTabProgressState = 4
)

func terminalTabProgressEnabled(terminal bool, getenv func(string) string) bool {
	if !terminal {
		return false
	}
	if getenv == nil {
		getenv = os.Getenv
	}
	switch strings.ToLower(strings.TrimSpace(getenv("IDOUD_TAB_PROGRESS"))) {
	case "0", "false", "no", "off", "never", "disabled":
		return false
	case "1", "true", "yes", "on", "always", "enabled":
		return true
	}
	if strings.TrimSpace(getenv("WT_SESSION")) != "" || strings.TrimSpace(getenv("WT_PROFILE_ID")) != "" {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(getenv("ConEmuANSI")), "ON")
}

func terminalTabProgressPercent(snapshot transferProgressSnapshot) (int, bool) {
	if snapshot.total < 0 {
		return 0, false
	}
	if snapshot.total == 0 {
		return 100, true
	}
	completed := snapshot.transferred
	if snapshot.kind == "upload" && snapshot.bodySentBytes > completed {
		completed = snapshot.bodySentBytes
	}
	if completed < 0 {
		completed = 0
	}
	if completed > snapshot.total {
		completed = snapshot.total
	}
	value := int(float64(completed) / float64(snapshot.total) * 100)
	if value < 0 {
		value = 0
	}
	if value > 100 {
		value = 100
	}
	// The request body reaching the network is not upload completion. Reserve
	// 100% for provider-confirmed storage, matching the detailed CLI display.
	if snapshot.kind == "upload" && completed >= snapshot.total && snapshot.transferred < snapshot.total {
		value = 99
	}
	return value, true
}

func terminalTabProgressForSnapshot(snapshot transferProgressSnapshot) (terminalTabProgressState, int) {
	value, determinate := terminalTabProgressPercent(snapshot)
	if snapshot.retries > 0 || snapshot.stalled {
		return terminalTabProgressWarning, value
	}
	switch snapshot.phase {
	case transferPhasePlanning, transferPhaseConnecting:
		return terminalTabProgressIndeterminate, 0
	case transferPhaseTransferring, transferPhaseFinalizing, transferPhaseSaving:
		if determinate {
			return terminalTabProgressNormal, value
		}
		return terminalTabProgressIndeterminate, 0
	default:
		return terminalTabProgressIndeterminate, 0
	}
}

func terminalTabProgressSequence(state terminalTabProgressState, value int) string {
	if state < terminalTabProgressHidden || state > terminalTabProgressWarning {
		state = terminalTabProgressHidden
	}
	if state == terminalTabProgressHidden || state == terminalTabProgressIndeterminate {
		value = 0
	}
	if value < 0 {
		value = 0
	}
	if value > 100 {
		value = 100
	}
	return fmt.Sprintf("\x1b]9;4;%d;%d\a", state, value)
}

func (ui *transferUI) renderTabProgress(snapshot transferProgressSnapshot) {
	if ui == nil || !ui.tabProgress {
		return
	}
	state, value := terminalTabProgressForSnapshot(snapshot)
	ui.outputMu.Lock()
	ui.writeTabProgressLocked(state, value)
	ui.outputMu.Unlock()
}

func (ui *transferUI) writeTabProgressLocked(state terminalTabProgressState, value int) {
	if ui == nil || !ui.tabProgress || ui.writer == nil {
		return
	}
	if state == terminalTabProgressIndeterminate || state == terminalTabProgressHidden {
		value = 0
	}
	if ui.tabProgressSet && ui.tabProgressState == state && ui.tabProgressValue == value {
		return
	}
	_, _ = fmt.Fprint(ui.writer, terminalTabProgressSequence(state, value))
	ui.tabProgressSet = true
	ui.tabProgressState = state
	ui.tabProgressValue = value
}

func (ui *transferUI) finishTabProgressLocked(snapshot transferProgressSnapshot, success bool) {
	if ui == nil || !ui.tabProgress {
		return
	}
	value, determinate := terminalTabProgressPercent(snapshot)
	if !determinate {
		value = 0
	}
	if success {
		ui.writeTabProgressLocked(terminalTabProgressNormal, 100)
		return
	}
	ui.writeTabProgressLocked(terminalTabProgressError, value)
}

func (ui *transferUI) clearTabProgressLocked() {
	if ui == nil || !ui.tabProgress {
		return
	}
	ui.writeTabProgressLocked(terminalTabProgressHidden, 0)
}
