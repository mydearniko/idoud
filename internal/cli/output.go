package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

const outputJSONSchemaVersion = 2
const unsetOutputModeValue = "\x00"

type primaryOutput struct {
	mode outputMode
}

type jsonEnvelope struct {
	SchemaVersion int         `json:"schema_version"`
	OK            bool        `json:"ok"`
	Type          string      `json:"type"`
	ExitCode      int         `json:"exit_code"`
	Result        *jsonResult `json:"result,omitempty"`
	Error         *jsonError  `json:"error,omitempty"`
	Help          *jsonHelp   `json:"help,omitempty"`
}

type jsonResult struct {
	URL       string `json:"url"`
	Path      string `json:"path,omitempty"`
	Name      string `json:"name,omitempty"`
	Source    string `json:"source"`
	KnownSize bool   `json:"known_size"`
	Size      *int64 `json:"size,omitempty"`
}

type jsonError struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
	Hint   string `json:"hint,omitempty"`
}

type jsonHelp struct {
	Text string `json:"text"`
}

func newPrimaryOutput(args []string) primaryOutput {
	return primaryOutput{mode: detectRequestedOutputMode(args)}
}

func parseOutputMode(raw string) (outputMode, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(outputModeURL):
		return outputModeURL, nil
	case string(outputModeJSON):
		return outputModeJSON, nil
	case string(outputModeNone):
		return outputModeNone, nil
	default:
		return "", fmt.Errorf("unsupported mode %q (want: url, json, none)", raw)
	}
}

func detectRequestedOutputMode(args []string) outputMode {
	opts := options{}
	chunkSizeRaw := strconv.FormatInt(defaultChunkSize, 10)
	stdinSizeRaw := ""
	ipsRaw := ""
	outputRaw := unsetOutputModeValue
	progressRaw := string(progressModeAuto)
	jsonOutput := false
	nonInteractive := false
	helpAll := false
	fs := flag.NewFlagSet(cliCommandName(), flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	registerFlags(fs, &opts, &chunkSizeRaw, &stdinSizeRaw, &ipsRaw, &outputRaw, &progressRaw, &jsonOutput, &nonInteractive, &helpAll)
	valueFlags := flagValueNames(fs)

	mode := outputModeURL
	stopParsingFlags := false
	forceJSON := false

	for idx := 0; idx < len(args); idx++ {
		token := strings.TrimSpace(args[idx])
		if stopParsingFlags {
			continue
		}
		switch {
		case token == "--":
			stopParsingFlags = true
		case token == "--json" || token == "-j":
			forceJSON = true
		case strings.HasPrefix(token, "-"):
			name, hasInlineValue := splitFlagToken(token)
			if (name == "json" || name == "j") && hasInlineValue {
				if enabled, err := strconv.ParseBool(token[strings.IndexByte(token, '=')+1:]); err == nil && enabled {
					forceJSON = true
				}
				continue
			}
			if name == "output" && hasInlineValue {
				if parsed, err := parseOutputMode(strings.TrimPrefix(token, "--output=")); err == nil {
					mode = parsed
				}
				continue
			}
			if _, needsValue := valueFlags[name]; needsValue && !hasInlineValue {
				if name == "output" && idx+1 < len(args) {
					if parsed, err := parseOutputMode(args[idx+1]); err == nil {
						mode = parsed
					}
				}
				idx++
			}
		}
	}

	if forceJSON {
		return outputModeJSON
	}
	return mode
}

func (o primaryOutput) printHelp(text string) {
	if o.mode == outputModeJSON {
		o.writeJSON(jsonEnvelope{
			SchemaVersion: outputJSONSchemaVersion,
			OK:            true,
			Type:          "help",
			ExitCode:      0,
			Help: &jsonHelp{
				Text: text,
			},
		})
		return
	}
	fmt.Fprintln(os.Stdout, text)
}

func (o primaryOutput) printSuccess(src *sourceFile, finalURL string) {
	switch o.mode {
	case outputModeNone:
		return
	case outputModeJSON:
		o.writeJSON(jsonEnvelope{
			SchemaVersion: outputJSONSchemaVersion,
			OK:            true,
			Type:          "result",
			ExitCode:      0,
			Result:        jsonResultFromSource(src, finalURL),
		})
	default:
		fmt.Fprintln(os.Stdout, finalURL)
	}
}

func (o primaryOutput) printDownloadSuccess(path string) {
	switch o.mode {
	case outputModeNone:
		return
	case outputModeJSON:
		o.writeJSON(jsonEnvelope{
			SchemaVersion: outputJSONSchemaVersion,
			OK:            true,
			Type:          "result",
			ExitCode:      0,
			Result: &jsonResult{
				Path:      path,
				Source:    "download",
				KnownSize: false,
			},
		})
	default:
		fmt.Fprintln(os.Stdout, path)
	}
}

func (o primaryOutput) printUsageError(err error) {
	hint := usageHint(err)
	if o.mode == outputModeJSON {
		o.writeJSON(jsonEnvelope{
			SchemaVersion: outputJSONSchemaVersion,
			OK:            false,
			Type:          "error",
			ExitCode:      2,
			Error: &jsonError{
				Code:   "usage_error",
				Detail: err.Error(),
				Hint:   hint,
			},
		})
		return
	}
	stderrWritef("error: %v", err)
	if hint != "" {
		stderrWritef("hint: %s", hint)
	}
}

func (o primaryOutput) printInputError(err error) {
	if o.mode == outputModeJSON {
		o.writeJSON(jsonEnvelope{
			SchemaVersion: outputJSONSchemaVersion,
			OK:            false,
			Type:          "error",
			ExitCode:      1,
			Error: &jsonError{
				Code:   "input_error",
				Detail: err.Error(),
			},
		})
		return
	}
	stderrLogf("error: %v", err)
}

func (o primaryOutput) printDownloadError(err error) {
	if o.mode == outputModeJSON {
		o.writeJSON(jsonEnvelope{
			SchemaVersion: outputJSONSchemaVersion,
			OK:            false,
			Type:          "error",
			ExitCode:      1,
			Error: &jsonError{
				Code:   "download_failed",
				Detail: err.Error(),
			},
		})
		return
	}
	stderrLogf("download failed: %v", err)
}

func (o primaryOutput) printUploadError(err error) {
	if o.mode == outputModeJSON {
		o.writeJSON(jsonEnvelope{
			SchemaVersion: outputJSONSchemaVersion,
			OK:            false,
			Type:          "error",
			ExitCode:      1,
			Error: &jsonError{
				Code:   "upload_failed",
				Detail: err.Error(),
			},
		})
		return
	}
	stderrLogf("upload failed: %v", err)
}

func (o primaryOutput) writeJSON(payload jsonEnvelope) {
	if err := encodeJSON(os.Stdout, payload); err != nil {
		stderrWritef("error: failed to encode JSON output: %v", err)
	}
}

func encodeJSON(w io.Writer, payload jsonEnvelope) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(payload)
}

func jsonResultFromSource(src *sourceFile, finalURL string) *jsonResult {
	result := &jsonResult{
		URL:       finalURL,
		Source:    "file",
		KnownSize: src != nil && src.knownSize,
	}
	if src == nil {
		return result
	}
	if src.fromStdin {
		result.Source = "stdin"
	}
	result.Name = src.uploadName
	if src.knownSize {
		size := src.size
		result.Size = &size
	}
	return result
}

func usageHint(err error) string {
	name := cliCommandName()
	switch {
	case errors.Is(err, errMissingInput):
		return fmt.Sprintf("pass a file path (%s <file>) or pipe data directly (cat <file> | %s)", name, name)
	default:
		return fmt.Sprintf("run `%s --help` for usage (`%s --help-all` shows advanced options)", name, name)
	}
}
