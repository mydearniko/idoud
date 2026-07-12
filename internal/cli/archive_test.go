package cli

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pierrec/lz4/v4"
)

type archiveTestEntry struct {
	header *tar.Header
	body   []byte
}

func TestParseFlagsArchiveAliases(t *testing.T) {
	for _, flagName := range []string{"-z", "--archive"} {
		opts, filePath, err := parseFlags([]string{flagName, "."})
		if err != nil {
			t.Fatalf("parseFlags(%q) returned error: %v", flagName, err)
		}
		if !opts.archive {
			t.Fatalf("parseFlags(%q) archive=false, want true", flagName)
		}
		if filePath != "." {
			t.Fatalf("parseFlags(%q) path=%q, want .", flagName, filePath)
		}
	}
}

func TestParseFlagsArchiveAcceptsMultiplePaths(t *testing.T) {
	opts, filePath, err := parseFlags([]string{"-z", "alpha.txt", "beta", "--name", "selected"})
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if filePath != "alpha.txt" {
		t.Fatalf("filePath=%q, want first archive path", filePath)
	}
	if len(opts.archivePaths) != 2 || opts.archivePaths[0] != "alpha.txt" || opts.archivePaths[1] != "beta" {
		t.Fatalf("archivePaths=%q, want [alpha.txt beta]", opts.archivePaths)
	}
	if opts.nameOverride != "selected" {
		t.Fatalf("nameOverride=%q, want selected", opts.nameOverride)
	}
}

func TestExpandArchivePathPatternsForWindowsShells(t *testing.T) {
	parent := t.TempDir()
	for _, name := range []string{"beta.txt", "alpha.txt"} {
		if err := os.WriteFile(filepath.Join(parent, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	pattern := filepath.Join(parent, "*.txt")
	expanded, err := expandArchivePathPatterns([]string{pattern}, true)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Join(parent, "alpha.txt"), filepath.Join(parent, "beta.txt")}
	if len(expanded) != len(want) || expanded[0] != want[0] || expanded[1] != want[1] {
		t.Fatalf("expanded=%q, want %q", expanded, want)
	}

	unexpanded, err := expandArchivePathPatterns([]string{pattern}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(unexpanded) != 1 || unexpanded[0] != pattern {
		t.Fatalf("unexpanded=%q, want literal pattern %q", unexpanded, pattern)
	}
}

func TestParseFlagsArchiveRejectsIncompatibleModes(t *testing.T) {
	tests := [][]string{
		{"-z", "--stdin", "."},
		{"-z", "--download", "AbC123"},
	}
	for _, args := range tests {
		if _, _, err := parseFlags(args); err == nil {
			t.Fatalf("parseFlags(%q) succeeded, want conflict error", args)
		}
	}
}

func TestParseFlagsArchiveRequiresPath(t *testing.T) {
	_, _, err := parseFlags([]string{"-z"})
	if !errors.Is(err, errMissingInput) {
		t.Fatalf("parseFlags(-z) error=%v, want errMissingInput", err)
	}
}

func TestArchiveSourceStreamsCompatibleTarLZ4(t *testing.T) {
	parent := t.TempDir()
	sourcePath := filepath.Join(parent, "project")
	if err := os.MkdirAll(filepath.Join(sourcePath, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(sourcePath, "empty"), 0o750); err != nil {
		t.Fatal(err)
	}
	payload := []byte("idoud tar.lz4 payload\n")
	if err := os.WriteFile(filepath.Join(sourcePath, "nested", "data.txt"), payload, 0o740); err != nil {
		t.Fatal(err)
	}
	symlinkCreated := false
	if err := os.Symlink(filepath.Join("nested", "data.txt"), filepath.Join(sourcePath, "data-link")); err == nil {
		symlinkCreated = true
	} else if runtime.GOOS != "windows" {
		t.Fatalf("create symlink: %v", err)
	}

	opts, _, err := parseFlags([]string{"-z", sourcePath})
	if err != nil {
		t.Fatal(err)
	}
	src, cleanup, err := openSource(sourcePath, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if src.knownSize || src.size != -1 || src.readerAt != nil || src.stream == nil {
		t.Fatalf("archive source is not an unknown-size stream: %+v", src)
	}
	if src.uploadName != "project.tar.lz4" {
		t.Fatalf("uploadName=%q, want project.tar.lz4", src.uploadName)
	}

	compressed, err := io.ReadAll(src.stream)
	if err != nil {
		t.Fatalf("read archive stream: %v", err)
	}
	entries := readTarLZ4Entries(t, compressed)

	for name := range entries {
		if strings.HasPrefix(name, "/") || strings.Contains(name, "../") {
			t.Fatalf("unsafe archive entry name %q", name)
		}
	}
	for _, name := range []string{"project/", "project/nested/", "project/empty/", "project/nested/data.txt"} {
		if entries[name] == nil {
			t.Errorf("missing archive entry %q", name)
		}
	}
	gotPayload := entries["project/nested/data.txt"]
	if gotPayload == nil {
		t.Fatal("missing archived payload")
	}
	if !bytes.Equal(gotPayload.body, payload) {
		t.Fatalf("archived payload=%q, want %q", gotPayload.body, payload)
	}
	if symlinkCreated {
		got := entries["project/data-link"]
		if got == nil {
			t.Fatal("missing symlink entry")
		}
		if got.header.Typeflag != tar.TypeSymlink || got.header.Linkname != filepath.Join("nested", "data.txt") {
			t.Fatalf("symlink header type=%d link=%q", got.header.Typeflag, got.header.Linkname)
		}
	}
}

func TestArchiveSourceStreamsMultiplePathsInArgumentOrder(t *testing.T) {
	parent := t.TempDir()
	firstPath := filepath.Join(parent, "alpha.txt")
	secondPath := filepath.Join(parent, "beta")
	if err := os.WriteFile(firstPath, []byte("alpha payload\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(secondPath, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(secondPath, "child.bin"), []byte("beta payload\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	opts, filePath, err := parseFlags([]string{"-z", firstPath, secondPath})
	if err != nil {
		t.Fatal(err)
	}
	src, cleanup, err := openSource(filePath, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if src.uploadName != multiArchiveUploadName {
		t.Fatalf("uploadName=%q, want %q", src.uploadName, multiArchiveUploadName)
	}

	compressed, err := io.ReadAll(src.stream)
	if err != nil {
		t.Fatalf("read archive stream: %v", err)
	}
	entries := readTarLZ4EntryList(t, compressed)
	wantNames := []string{"alpha.txt", "beta/", "beta/child.bin"}
	if len(entries) != len(wantNames) {
		t.Fatalf("archive entries=%d, want %d: %v", len(entries), len(wantNames), archiveEntryNames(entries))
	}
	for idx, want := range wantNames {
		if entries[idx].header.Name != want {
			t.Fatalf("entry[%d]=%q, want %q (all=%v)", idx, entries[idx].header.Name, want, archiveEntryNames(entries))
		}
	}
	if got := string(entries[0].body); got != "alpha payload\n" {
		t.Fatalf("alpha body=%q", got)
	}
	if got := string(entries[2].body); got != "beta payload\n" {
		t.Fatalf("beta body=%q", got)
	}
}

func TestArchiveSourceMultiplePathsHonorsExplicitName(t *testing.T) {
	parent := t.TempDir()
	firstPath := filepath.Join(parent, "one")
	secondPath := filepath.Join(parent, "two")
	if err := os.WriteFile(firstPath, []byte("1"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondPath, []byte("2"), 0o600); err != nil {
		t.Fatal(err)
	}

	opts, filePath, err := parseFlags([]string{"-z", firstPath, secondPath, "--name", "bundle"})
	if err != nil {
		t.Fatal(err)
	}
	src, cleanup, err := openSource(filePath, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if src.uploadName != "bundle.tar.lz4" {
		t.Fatalf("uploadName=%q, want bundle.tar.lz4", src.uploadName)
	}
	if _, err := io.Copy(io.Discard, src.stream); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveSourceRejectsCollidingTopLevelNames(t *testing.T) {
	parent := t.TempDir()
	firstPath := filepath.Join(parent, "one", "same.txt")
	secondPath := filepath.Join(parent, "two", "same.txt")
	if err := os.MkdirAll(filepath.Dir(firstPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(secondPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(firstPath, []byte("1"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondPath, []byte("2"), 0o600); err != nil {
		t.Fatal(err)
	}

	opts, filePath, err := parseFlags([]string{"-z", firstPath, secondPath})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := openSource(filePath, opts); err == nil || !strings.Contains(err.Error(), "both map to top-level entry") {
		t.Fatalf("openSource error=%v, want top-level collision", err)
	}
}

func TestArchiveSourceValidatesEveryPathBeforeStreaming(t *testing.T) {
	parent := t.TempDir()
	firstPath := filepath.Join(parent, "present.txt")
	if err := os.WriteFile(firstPath, []byte("present"), 0o600); err != nil {
		t.Fatal(err)
	}
	missingPath := filepath.Join(parent, "missing.txt")

	opts, filePath, err := parseFlags([]string{"-z", firstPath, missingPath})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := openSource(filePath, opts); err == nil || !strings.Contains(err.Error(), missingPath) {
		t.Fatalf("openSource error=%v, want missing second path", err)
	}
}

func TestAutomaticArchivePrefetchPolicyIsResourceBounded(t *testing.T) {
	if got := automaticArchivePrefetchPolicy(63*1024*1024, 64); got.workers != 0 {
		t.Fatalf("very-low-memory policy=%+v, want sequential mode", got)
	}
	unknown := automaticArchivePrefetchPolicy(0, 64)
	if unknown.workers != 2 || int64(unknown.workers)*unknown.maxFileBytes > 16*1024*1024 {
		t.Fatalf("unknown-memory policy=%+v, want <=16MiB across two workers", unknown)
	}
	high := automaticArchivePrefetchPolicy(8*1024*1024*1024, 4)
	if high.workers != 8 || high.maxFileBytes != 32*1024*1024 {
		t.Fatalf("high-memory policy=%+v, want 8 x 32MiB", high)
	}
	if int64(high.workers)*high.maxFileBytes > archivePrefetchMaxBytes {
		t.Fatalf("high-memory policy exceeds cap: %+v", high)
	}
}

func TestPrefetchedTarMatchesSequentialTar(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fixture")
	if err := os.MkdirAll(filepath.Join(root, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{
		"small.txt":         []byte("small archive entry\n"),
		"nested/medium.bin": bytes.Repeat([]byte("medium-prefetch-data"), 8192),
		"nested/large.bin":  bytes.Repeat([]byte("direct-read-data"), 16384),
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(name)), data, 0o640); err != nil {
			t.Fatal(err)
		}
	}

	build := func(prefetched bool) []byte {
		t.Helper()
		var output bytes.Buffer
		writer := tar.NewWriter(&output)
		var err error
		if prefetched {
			err = writeTarPathPrefetched(t.Context(), writer, root, "fixture", archivePrefetchPolicy{workers: 3, maxFileBytes: 192 * 1024})
		} else {
			err = writeTarPathSequential(t.Context(), writer, root, "fixture")
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		return output.Bytes()
	}
	sequential := build(false)
	prefetched := build(true)
	if !bytes.Equal(prefetched, sequential) {
		t.Fatalf("prefetched tar differs from sequential tar: got %d bytes, want %d", len(prefetched), len(sequential))
	}
}

func TestPrefetchedMultiPathTarMatchesSequentialTar(t *testing.T) {
	parent := t.TempDir()
	firstPath := filepath.Join(parent, "first.bin")
	secondPath := filepath.Join(parent, "second.bin")
	if err := os.WriteFile(firstPath, bytes.Repeat([]byte("first-prefetch-data"), 8192), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondPath, bytes.Repeat([]byte("second-prefetch-data"), 8192), 0o600); err != nil {
		t.Fatal(err)
	}
	paths, err := prepareArchivePaths([]string{secondPath, firstPath})
	if err != nil {
		t.Fatal(err)
	}

	build := func(prefetched bool) []byte {
		t.Helper()
		var output bytes.Buffer
		writer := tar.NewWriter(&output)
		var err error
		if prefetched {
			err = writeTarPathsPrefetched(t.Context(), writer, paths, archivePrefetchPolicy{workers: 2, maxFileBytes: 256 * 1024})
		} else {
			err = writeTarPathsSequential(t.Context(), writer, paths)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		return output.Bytes()
	}
	sequential := build(false)
	prefetched := build(true)
	if !bytes.Equal(prefetched, sequential) {
		t.Fatalf("prefetched multi-path tar differs from sequential tar: got %d bytes, want %d", len(prefetched), len(sequential))
	}
}

func TestArchiveSourceDerivesCurrentDirectoryName(t *testing.T) {
	parent := t.TempDir()
	rootDir := filepath.Join(parent, "root")
	if err := os.Mkdir(rootDir, 0o755); err != nil {
		t.Fatal(err)
	}
	oldWorkingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(rootDir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldWorkingDirectory) })

	opts, filePath, err := parseFlags([]string{"-z", "."})
	if err != nil {
		t.Fatal(err)
	}
	src, cleanup, err := openSource(filePath, opts)
	if err != nil {
		t.Fatal(err)
	}
	if src.uploadName != "root.tar.lz4" {
		cleanup()
		t.Fatalf("uploadName=%q, want root.tar.lz4", src.uploadName)
	}
	if _, err := io.Copy(io.Discard, src.stream); err != nil {
		cleanup()
		t.Fatal(err)
	}
	cleanup()
}

func TestArchiveSourceHonorsExplicitName(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "payload.txt")
	if err := os.WriteFile(sourcePath, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	opts, _, err := parseFlags([]string{"-z", "--name", "custom.bundle", sourcePath})
	if err != nil {
		t.Fatal(err)
	}
	src, cleanup, err := openSource(sourcePath, opts)
	if err != nil {
		t.Fatal(err)
	}
	if src.uploadName != "custom.bundle" {
		cleanup()
		t.Fatalf("uploadName=%q, want custom.bundle", src.uploadName)
	}
	if _, err := io.Copy(io.Discard, src.stream); err != nil {
		cleanup()
		t.Fatal(err)
	}
	cleanup()
}

func TestArchiveSourceCompletesExtensionlessExplicitName(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "payload.txt")
	if err := os.WriteFile(sourcePath, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	opts, _, err := parseFlags([]string{"-z", "--name", "hello", sourcePath})
	if err != nil {
		t.Fatal(err)
	}
	src, cleanup, err := openSource(sourcePath, opts)
	if err != nil {
		t.Fatal(err)
	}
	if src.uploadName != "hello.tar.lz4" {
		cleanup()
		t.Fatalf("uploadName=%q, want hello.tar.lz4", src.uploadName)
	}
	if _, err := io.Copy(io.Discard, src.stream); err != nil {
		cleanup()
		t.Fatal(err)
	}
	cleanup()
}

func TestArchiveSourceCleanupCancelsBlockedStream(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "large.bin")
	file, err := os.Create(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(64 * 1024 * 1024); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	opts, _, err := parseFlags([]string{"-z", sourcePath})
	if err != nil {
		t.Fatal(err)
	}
	_, cleanup, err := openSource(sourcePath, opts)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		cleanup()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("archive cleanup did not stop the blocked compressor")
	}
}

func TestArchiveProducerCleanupWaitIsBounded(t *testing.T) {
	done := make(chan struct{})
	started := time.Now()
	waitForArchiveProducer(done, 20*time.Millisecond)
	elapsed := time.Since(started)
	if elapsed < 10*time.Millisecond {
		t.Fatalf("cleanup wait returned too early: %s", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("cleanup wait was not bounded: %s", elapsed)
	}
}

func TestArchiveContextReaderStopsBeforeAnotherRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader := &archiveContextReader{ctx: ctx, reader: strings.NewReader("data")}
	if _, err := reader.Read(make([]byte, 4)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read error=%v, want context.Canceled", err)
	}
}

func TestArchiveSourceRejectsMissingPath(t *testing.T) {
	opts, _, err := parseFlags([]string{"-z", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := openSource(filepath.Join(t.TempDir(), "missing"), opts); err == nil {
		t.Fatal("openSource succeeded for a missing archive path")
	}
}

func readTarLZ4Entries(t *testing.T, compressed []byte) map[string]*archiveTestEntry {
	t.Helper()
	list := readTarLZ4EntryList(t, compressed)
	entries := make(map[string]*archiveTestEntry, len(list))
	for _, entry := range list {
		entries[entry.header.Name] = entry
	}
	return entries
}

func readTarLZ4EntryList(t *testing.T, compressed []byte) []*archiveTestEntry {
	t.Helper()
	reader := tar.NewReader(lz4.NewReader(bytes.NewReader(compressed)))
	entries := make([]*archiveTestEntry, 0)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read tar.lz4 entry: %v", err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("read tar body %q: %v", header.Name, err)
		}
		clone := *header
		entries = append(entries, &archiveTestEntry{header: &clone, body: body})
	}
	return entries
}

func archiveEntryNames(entries []*archiveTestEntry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry != nil && entry.header != nil {
			names = append(names, entry.header.Name)
		}
	}
	return names
}
