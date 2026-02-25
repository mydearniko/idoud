package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	ansiReset       = "\x1b[0m"
	ansiBoldRed     = "\x1b[1;31m"
	ansiBoldGreen   = "\x1b[1;32m"
	ansiBoldYellow  = "\x1b[1;33m"
	ansiBoldBlue    = "\x1b[1;34m"
	ansiBoldMagenta = "\x1b[1;35m"
	ansiBoldCyan    = "\x1b[1;36m"
	ansiBoldWhite   = "\x1b[1;37m"
	ansiDimWhite    = "\x1b[2;37m"
	ansiCyan        = "\x1b[36m"
	ansiMagenta     = "\x1b[35m"
	ansiGray        = "\x1b[90m"
	ansiYellow      = "\x1b[33m"
)

func stderrWritef(format string, args ...any) {
	raw := fmt.Sprintf(format, args...)
	writeColoredLines(raw)
}

func stderrLogf(format string, args ...any) {
	raw := fmt.Sprintf(format, args...)
	writeColoredLines(raw)
}

func writeColoredLines(raw string) {
	trimmed := strings.TrimSuffix(raw, "\n")
	lines := strings.Split(trimmed, "\n")
	if len(lines) == 0 {
		return
	}
	for _, line := range lines {
		if line == "" {
			fmt.Fprintln(os.Stderr)
			continue
		}
		fmt.Fprintln(os.Stderr, colorizeLogLine(line))
	}
}

func colorizeLogLine(line string) string {
	if !colorOutputEnabled() {
		return line
	}
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return line
	}
	for i, tok := range fields {
		if i == 0 {
			fields[i] = colorizePrefix(tok)
			continue
		}
		if tok == "|" {
			fields[i] = style(ansiGray, tok)
			continue
		}
		if eq := strings.IndexByte(tok, '='); eq > 0 {
			key := tok[:eq]
			val := tok[eq+1:]
			fields[i] = style(ansiBoldCyan, key) + "=" + colorizeValue(key, val)
			continue
		}
		if strings.HasSuffix(tok, ":") {
			switch strings.TrimSuffix(strings.ToLower(tok), ":") {
			case "error", "failed":
				fields[i] = style(ansiBoldRed, tok)
			case "summary":
				fields[i] = style(ansiMagenta, tok)
			}
		}
	}
	return strings.Join(fields, " ")
}

func colorizePrefix(tok string) string {
	switch strings.ToLower(tok) {
	case "debug":
		return style(ansiBoldCyan, tok)
	case "chunk_attempt":
		return style(ansiBoldYellow, tok)
	case "finalize_ready":
		return style(ansiBoldGreen, tok)
	case "finalize_poll":
		return style(ansiBoldMagenta, tok)
	case "finalize_progress":
		return style(ansiBoldBlue, tok)
	case "upload":
		return style(ansiBoldWhite, tok)
	case "error:":
		return style(ansiBoldRed, tok)
	default:
		return tok
	}
}

func colorizeValue(key, val string) string {
	lk := strings.ToLower(strings.TrimSpace(key))
	switch lk {
	case "status":
		if code, err := strconv.Atoi(strings.Trim(val, "\"")); err == nil {
			switch {
			case code >= 500:
				return style(ansiBoldRed, val)
			case code >= 400:
				return style(ansiBoldYellow, val)
			case code >= 200:
				return style(ansiBoldGreen, val)
			}
		}
	case "failed", "final_failed", "timeouts", "status_429", "status_5xx":
		if !valueLooksZero(val) {
			return style(ansiBoldRed, val)
		}
		return style(ansiDimWhite, val)
	case "err":
		trim := strings.TrimSpace(strings.ToLower(val))
		if trim == "<nil>" || trim == "nil" {
			return style(ansiDimWhite, val)
		}
		return style(ansiBoldRed, val)
	case "read_rate", "upload_rate", "read_rate_avg7", "upload_rate_avg7", "attempt_rate", "done_rate":
		return style(ansiBoldGreen, val)
	case "delay", "dur", "elapsed", "t", "stdin_idle", "server_wait":
		return style(ansiCyan, val)
	case "stdin_state":
		return style(ansiMagenta, val)
	}
	return val
}

func valueLooksZero(v string) bool {
	v = strings.Trim(strings.TrimSpace(v), "\"")
	if v == "" {
		return true
	}
	switch strings.ToLower(v) {
	case "0", "0.0", "0s", "0b", "0b/s", "0/s", "-", "n/a":
		return true
	}
	n, ok := parseLeadingFloat(v)
	if !ok {
		return false
	}
	return n == 0
}

func parseLeadingFloat(v string) (float64, bool) {
	started := false
	end := 0
	for i, r := range v {
		if (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '+' {
			started = true
			end = i + 1
			continue
		}
		if started {
			break
		}
		return 0, false
	}
	if !started || end == 0 {
		return 0, false
	}
	n, err := strconv.ParseFloat(v[:end], 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func style(code, text string) string {
	return code + text + ansiReset
}

func colorOutputEnabled() bool {
	if strings.TrimSpace(os.Getenv("NO_COLOR")) != "" {
		return false
	}
	if v := strings.TrimSpace(os.Getenv("CLICOLOR")); v == "0" {
		return false
	}
	if v := strings.TrimSpace(os.Getenv("CLICOLOR_FORCE")); v != "" && v != "0" {
		return true
	}
	if v := strings.TrimSpace(os.Getenv("FORCE_COLOR")); v != "" && v != "0" {
		return true
	}
	term := strings.ToLower(strings.TrimSpace(os.Getenv("TERM")))
	if term == "" || term == "dumb" {
		return false
	}
	info, err := os.Stderr.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}
