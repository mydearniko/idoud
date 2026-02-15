package main

import "testing"

func TestParseFlagsStdinPositionalName(t *testing.T) {
	opts, filePath, err := parseFlags([]string{"--stdin", "archive.zip"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if !opts.stdin {
		t.Fatal("stdin flag should be enabled")
	}
	if opts.nameOverride != "archive.zip" {
		t.Fatalf("nameOverride = %q, want %q", opts.nameOverride, "archive.zip")
	}
	if filePath != "" {
		t.Fatalf("filePath = %q, want empty for stdin mode", filePath)
	}
}

func TestParseFlagsStdinPositionalNameConflict(t *testing.T) {
	_, _, err := parseFlags([]string{"--stdin", "--name", "from-flag.zip", "from-arg.zip"})
	if err == nil {
		t.Fatal("expected conflict error when using --name and positional stdin filename together")
	}
}

func TestParseFlagsStdinTooManyPositionalArgs(t *testing.T) {
	_, _, err := parseFlags([]string{"--stdin", "a.zip", "b.zip"})
	if err == nil {
		t.Fatal("expected error for too many positional args in stdin mode")
	}
}
