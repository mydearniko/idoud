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
		failIf(t, err != nil, "parseFlags(%q) returned error: %v", flagName, err)
		failIf(t, !opts.archive, "parseFlags(%q) archive=false, want true", flagName)
		failIf(t, filePath != ".", "parseFlags(%q) path=%q, want .", flagName, filePath)
	}
}

func TestParseFlagsArchiveAcceptsMultiplePaths(t *testing.T) {
	opts, filePath, err := parseFlags([]string{"-z", "alpha.txt", "beta", "--name", "selected"})
	requireNoError(t, err, "parseFlags returned error: %v")

	failIf(t, filePath != "alpha.txt", "filePath=%q, want first archive path", filePath)
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
		requireNoError(t, os.WriteFile(filepath.Join(parent, name), []byte(name), 0o600), "")

	}
	pattern := filepath.Join(parent, "*.txt")
	expanded, err := expandArchivePathPatterns([]string{pattern}, true)
	requireNoError(t, err, "")

	want := []string{filepath.Join(parent, "alpha.txt"), filepath.Join(parent, "beta.txt")}
	failIf(t, len(expanded) != len(want) || expanded[0] != want[0] || expanded[1] != want[1], "expanded=%q, want %q", expanded, want)

	unexpanded, err := expandArchivePathPatterns([]string{pattern}, false)
	requireNoError(t, err, "")

	failIf(t, len(unexpanded) != 1 || unexpanded[0] != pattern, "unexpanded=%q, want literal pattern %q", unexpanded, pattern)
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
	failIf(t, !errors.Is(err, errMissingInput), "parseFlags(-z) error=%v, want errMissingInput", err)
}

func TestArchiveSourceStreamsCompatibleTarLZ4(t *testing.T) {
	parent := t.TempDir()
	sourcePath := filepath.Join(parent, "project")
	requireNoError(t, os.MkdirAll(filepath.Join(sourcePath, "nested"), 0o755), "")
	requireNoError(t, os.Mkdir(filepath.Join(sourcePath, "empty"), 0o750), "")

	payload := []byte("idoud tar.lz4 payload\n")
	requireNoError(t, os.WriteFile(filepath.Join(sourcePath, "nested", "data.txt"), payload, 0o740), "")

	symlinkCreated := false
	if err := os.Symlink(filepath.Join("nested", "data.txt"), filepath.Join(sourcePath, "data-link")); err == nil {
		symlinkCreated = true
	} else if runtime.GOOS != "windows" {
		t.Fatalf("create symlink: %v", err)
	}

	opts, _, err := parseFlags([]string{"-z", sourcePath})
	requireNoError(t, err, "")

	src, cleanup, err := openSource(sourcePath, opts)
	requireNoError(t, err, "")

	defer cleanup()
	failIf(t, src.knownSize || src.size != -1 || src.readerAt != nil || src.stream == nil, "archive source is not an unknown-size stream: %+v", src)
	if src.uploadName != "project.tar.lz4" {
		t.Fatalf("uploadName=%q, want project.tar.lz4", src.uploadName)
	}

	compressed, err := io.ReadAll(src.stream)
	requireNoError(t, err, "read archive stream: %v")

	entries := readTarLZ4Entries(t, compressed)

	for name := range entries {
		failIf(t, strings.HasPrefix(name, "/") || strings.Contains(name, "../"), "unsafe archive entry name %q", name)
	}
	for _, name := range []string{"project/", "project/nested/", "project/empty/", "project/nested/data.txt"} {
		if entries[name] == nil {
			t.Errorf("missing archive entry %q", name)
		}
	}
	gotPayload := entries["project/nested/data.txt"]
	fatalIf(t, gotPayload == nil, "missing archived payload")
	if !bytes.Equal(gotPayload.body, payload) {
		t.Fatalf("archived payload=%q, want %q", gotPayload.body, payload)
	}
	if symlinkCreated {
		got := entries["project/data-link"]
		fatalIf(t, got == nil, "missing symlink entry")
		if got.header.Typeflag != tar.TypeSymlink || got.header.Linkname != filepath.Join("nested", "data.txt") {
			t.Fatalf("symlink header type=%d link=%q", got.header.Typeflag, got.header.Linkname)
		}
	}
}

func TestArchiveSourceStreamsMultiplePathsInArgumentOrder(t *testing.T) {
	parent := t.TempDir()
	firstPath := filepath.Join(parent, "alpha.txt")
	secondPath := filepath.Join(parent, "beta")
	requireNoError(t, os.WriteFile(firstPath, []byte("alpha payload\n"), 0o640), "")
	requireNoError(t, os.Mkdir(secondPath, 0o750), "")
	requireNoError(t, os.WriteFile(filepath.Join(secondPath, "child.bin"), []byte("beta payload\n"), 0o600), "")

	opts, filePath, err := parseFlags([]string{"-z", firstPath, secondPath})
	requireNoError(t, err, "")

	src, cleanup, err := openSource(filePath, opts)
	requireNoError(t, err, "")

	defer cleanup()
	if src.uploadName != multiArchiveUploadName {
		t.Fatalf("uploadName=%q, want %q", src.uploadName, multiArchiveUploadName)
	}

	compressed, err := io.ReadAll(src.stream)
	requireNoError(t, err, "read archive stream: %v")

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
	requireNoError(t, os.WriteFile(firstPath, []byte("1"), 0o600), "")
	requireNoError(t, os.WriteFile(secondPath, []byte("2"), 0o600), "")

	opts, filePath, err := parseFlags([]string{"-z", firstPath, secondPath, "--name", "bundle"})
	requireNoError(t, err, "")

	src, cleanup, err := openSource(filePath, opts)
	requireNoError(t, err, "")

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
	requireNoError(t, os.MkdirAll(filepath.Dir(firstPath), 0o755), "")
	requireNoError(t, os.MkdirAll(filepath.Dir(secondPath), 0o755), "")
	requireNoError(t, os.WriteFile(firstPath, []byte("1"), 0o600), "")
	requireNoError(t, os.WriteFile(secondPath, []byte("2"), 0o600), "")

	opts, filePath, err := parseFlags([]string{"-z", firstPath, secondPath})
	requireNoError(t, err, "")

	if _, _, err := openSource(filePath, opts); err == nil || !strings.Contains(err.Error(), "both map to top-level entry") {
		t.Fatalf("openSource error=%v, want top-level collision", err)
	}
}

func TestArchiveSourceValidatesEveryPathBeforeStreaming(t *testing.T) {
	parent := t.TempDir()
	firstPath := filepath.Join(parent, "present.txt")
	requireNoError(t, os.WriteFile(firstPath, []byte("present"), 0o600), "")

	missingPath := filepath.Join(parent, "missing.txt")

	opts, filePath, err := parseFlags([]string{"-z", firstPath, missingPath})
	requireNoError(t, err, "")

	if _, _, err := openSource(filePath, opts); err == nil || !strings.Contains(err.Error(), missingPath) {
		t.Fatalf("openSource error=%v, want missing second path", err)
	}
}

func TestAutomaticArchivePrefetchPolicyIsResourceBounded(t *testing.T) {
	if got := automaticArchivePrefetchPolicy(63*1024*1024, 64); got.workers != 0 {
		t.Fatalf("very-low-memory policy=%+v, want sequential mode", got)
	}
	unknown := automaticArchivePrefetchPolicy(0, 64)
	failIf(t, unknown.workers != 2 || int64(unknown.workers)*unknown.maxFileBytes > 16*1024*1024, "unknown-memory policy=%+v, want <=16MiB across two workers", unknown)
	high := automaticArchivePrefetchPolicy(8*1024*1024*1024, 4)
	failIf(t, high.workers != 8 || high.maxFileBytes != 32*1024*1024, "high-memory policy=%+v, want 8 x 32MiB", high)
	failIf(t, int64(high.workers)*high.maxFileBytes > archivePrefetchMaxBytes, "high-memory policy exceeds cap: %+v", high)
}

func TestPrefetchedTarMatchesSequentialTar(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fixture")
	requireNoError(t, os.MkdirAll(filepath.Join(root, "nested"), 0o755), "")

	files := map[string][]byte{
		"small.txt":         []byte("small archive entry\n"),
		"nested/medium.bin": bytes.Repeat([]byte("medium-prefetch-data"), 8192),
		"nested/large.bin":  bytes.Repeat([]byte("direct-read-data"), 16384),
	}
	for name, data := range files {
		requireNoError(t, os.WriteFile(filepath.Join(root, filepath.FromSlash(name)), data, 0o640), "")

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
		requireNoError(t, err, "")

		requireNoError(t, writer.Close(), "")

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
	requireNoError(t, os.WriteFile(firstPath, bytes.Repeat([]byte("first-prefetch-data"), 8192), 0o640), "")
	requireNoError(t, os.WriteFile(secondPath, bytes.Repeat([]byte("second-prefetch-data"), 8192), 0o600), "")

	paths, err := prepareArchivePaths([]string{secondPath, firstPath})
	requireNoError(t, err, "")

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
		requireNoError(t, err, "")

		requireNoError(t, writer.Close(), "")

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
	requireNoError(t, os.Mkdir(rootDir, 0o755), "")

	oldWorkingDirectory, err := os.Getwd()
	requireNoError(t, err, "")

	requireNoError(t, os.Chdir(rootDir), "")

	t.Cleanup(func() { _ = os.Chdir(oldWorkingDirectory) })

	opts, filePath, err := parseFlags([]string{"-z", "."})
	requireNoError(t, err, "")

	src, cleanup, err := openSource(filePath, opts)
	requireNoError(t, err, "")

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

func testArchiveSourceExplicitName(t *testing.T, explicitName, wantName string) {
	t.Helper()
	sourcePath := filepath.Join(t.TempDir(), "payload.txt")
	requireNoError(t, os.WriteFile(sourcePath, []byte("payload"), 0o600), "")

	opts, _, err := parseFlags([]string{"-z", "--name", explicitName, sourcePath})
	requireNoError(t, err, "")

	src, cleanup, err := openSource(sourcePath, opts)
	requireNoError(t, err, "")

	defer cleanup()
	if src.uploadName != wantName {
		t.Fatalf("uploadName=%q, want %s", src.uploadName, wantName)
	}
	if _, err := io.Copy(io.Discard, src.stream); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveSourceHonorsExplicitName(t *testing.T) {
	testArchiveSourceExplicitName(t, "custom.bundle", "custom.bundle")
}

func TestArchiveSourceCompletesExtensionlessExplicitName(t *testing.T) {
	testArchiveSourceExplicitName(t, "hello", "hello.tar.lz4")
}

func TestArchiveSourceCleanupCancelsBlockedStream(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "large.bin")
	file, err := os.Create(sourcePath)
	requireNoError(t, err, "")

	if err := file.Truncate(64 * 1024 * 1024); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	requireNoError(t, file.Close(), "")

	opts, _, err := parseFlags([]string{"-z", sourcePath})
	requireNoError(t, err, "")

	_, cleanup, err := openSource(sourcePath, opts)
	requireNoError(t, err, "")

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
	failIf(t, elapsed < 10*time.Millisecond, "cleanup wait returned too early: %s", elapsed)
	failIf(t, elapsed > 500*time.Millisecond, "cleanup wait was not bounded: %s", elapsed)
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
	requireNoError(t, err, "")

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
		requireNoError(t, err, "read tar.lz4 entry: %v")

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
