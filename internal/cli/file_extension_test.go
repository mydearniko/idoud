package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pierrec/lz4/v4"
)

func TestSniffFileExtensionCommonFormats(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{name: "empty", data: nil, want: ".bin"},
		{name: "plain text", data: []byte("hello from idoud\n"), want: ".txt"},
		{name: "json", data: []byte("{\"idoud\":true}\n"), want: ".json"},
		{name: "png", data: append([]byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}, make([]byte, 32)...), want: ".png"},
		{name: "jpeg", data: []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10}, want: ".jpg"},
		{name: "pdf", data: []byte("%PDF-1.7\n"), want: ".pdf"},
		{name: "zip", data: []byte("PK\x03\x04\x14\x00\x00\x00"), want: ".zip"},
		{name: "zstd", data: []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00}, want: ".zst"},
		{name: "xz", data: []byte{0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00}, want: ".xz"},
		{name: "mp4", data: []byte{0x00, 0x00, 0x00, 0x18, 'f', 't', 'y', 'p', 'i', 's', 'o', 'm'}, want: ".mp4"},
		{name: "java class", data: []byte{0xca, 0xfe, 0xba, 0xbe, 0x00, 0x00, 0x00, 0x34}, want: ".class"},
		{name: "fat mach-o", data: []byte{0xca, 0xfe, 0xba, 0xbe, 0x00, 0x00, 0x00, 0x02}, want: ".macho"},
		{name: "binary", data: []byte{0x00, 0xff, 0x10, 0x80}, want: ".bin"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prefix, extension, err := sniffFileExtension(bytes.NewReader(test.data))
			if err != nil {
				t.Fatal(err)
			}
			if extension != test.want {
				t.Fatalf("extension=%q, want %q", extension, test.want)
			}
			if !bytes.Equal(prefix, test.data) {
				t.Fatalf("short input prefix=%x, want exact input %x", prefix, test.data)
			}
		})
	}
}

func FuzzExtensionFromPrefix(f *testing.F) {
	for _, seed := range [][]byte{
		nil,
		{0x04, 0x22, 0x4d, 0x18},
		{0x1f, 0x8b, 0x08},
		[]byte("BZh9"),
		[]byte("PK\x03\x04"),
		[]byte("plain text"),
	} {
		f.Add(seed, false)
	}
	f.Fuzz(func(t *testing.T, data []byte, complete bool) {
		if len(data) > maximumFileSniffSize {
			data = data[:maximumFileSniffSize]
		}
		extension, _ := extensionFromPrefix(data, complete)
		if extension != "" && (!strings.HasPrefix(extension, ".") || strings.ContainsAny(extension, "/\\")) {
			t.Fatalf("unsafe extension %q", extension)
		}
	})
}

func TestSniffFileExtensionDetectsTarAndCompressedTar(t *testing.T) {
	tarData := makeTestTar(t, []byte("idoud archive payload\n"))

	var gzipData bytes.Buffer
	gzipWriter := gzip.NewWriter(&gzipData)
	if _, err := gzipWriter.Write(tarData); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}

	lz4Data := compressTestLZ4(t, tarData)
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{name: "tar", data: tarData, want: ".tar"},
		{name: "tar gzip", data: gzipData.Bytes(), want: ".tar.gz"},
		{name: "tar lz4", data: lz4Data, want: ".tar.lz4"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := bytes.NewReader(test.data)
			prefix, extension, err := sniffFileExtension(reader)
			if err != nil {
				t.Fatal(err)
			}
			if extension != test.want {
				t.Fatalf("extension=%q, want %q", extension, test.want)
			}
			replayed, err := io.ReadAll(io.MultiReader(bytes.NewReader(prefix), reader))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(replayed, test.data) {
				t.Fatalf("sniffing changed replayed stream: got %d bytes, want %d", len(replayed), len(test.data))
			}
		})
	}
}

func TestSniffTarLZ4NeedsOnlySmallPrefixOfLargeBlock(t *testing.T) {
	payload := make([]byte, 5*1024*1024)
	if _, err := rand.New(rand.NewSource(1)).Read(payload); err != nil {
		t.Fatal(err)
	}
	tarData := makeTestTar(t, payload)
	lz4Data := compressTestLZ4(t, tarData)
	if len(lz4Data) < 4*1024*1024 {
		t.Fatalf("test fixture compressed to %d bytes; want a large first block", len(lz4Data))
	}

	reader := bytes.NewReader(lz4Data)
	prefix, extension, err := sniffFileExtension(reader)
	if err != nil {
		t.Fatal(err)
	}
	if extension != ".tar.lz4" {
		t.Fatalf("extension=%q, want .tar.lz4", extension)
	}
	if len(prefix) > 4*1024 {
		t.Fatalf("sniff buffered %d bytes, want at most 4 KiB for the large LZ4 block", len(prefix))
	}
}

func TestDetectedNameAppendsOnlyWhenExtensionIsMissing(t *testing.T) {
	tests := []struct {
		name      string
		extension string
		want      string
	}{
		{name: "hello", extension: ".tar.lz4", want: "hello.tar.lz4"},
		{name: "hello.", extension: ".zip", want: "hello.zip"},
		{name: "hello.tar", extension: ".lz4", want: "hello.tar"},
		{name: ".hidden", extension: ".txt", want: ".hidden"},
		{name: "payload", extension: "png", want: "payload.png"},
	}
	for _, test := range tests {
		if got := appendDetectedExtension(test.name, test.extension); got != test.want {
			t.Errorf("appendDetectedExtension(%q, %q)=%q, want %q", test.name, test.extension, got, test.want)
		}
	}
}

func TestOpenStdinSourceDetectsNameAndReplaysPipe(t *testing.T) {
	tarData := makeTestTar(t, []byte("streamed idoud data"))
	payload := compressTestLZ4(t, tarData)
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := writer.Write(payload)
		writeDone <- errors.Join(writeErr, writer.Close())
	}()

	opts, _, err := parseFlags([]string{"--stdin", "--name", "hello"})
	if err != nil {
		t.Fatal(err)
	}
	src, cleanup, err := openStdinSource(reader, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if src.uploadName != "hello.tar.lz4" {
		t.Fatalf("uploadName=%q, want hello.tar.lz4", src.uploadName)
	}
	got, err := io.ReadAll(src.stream)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("uploaded stream changed: got %d bytes, want %d", len(got), len(payload))
	}
}

func TestKnownSizeStdinSniffDoesNotWaitForPipeClose(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	writeDone := make(chan error, 1)
	closeWriter := make(chan struct{})
	go func() {
		_, writeErr := writer.Write([]byte("hello"))
		writeDone <- writeErr
		<-closeWriter
		_ = writer.Close()
	}()

	opts, _, err := parseFlags([]string{"--stdin", "--stdin-size", "5", "--name", "greeting"})
	if err != nil {
		close(closeWriter)
		t.Fatal(err)
	}
	type openResult struct {
		src     *sourceFile
		cleanup func()
		err     error
	}
	opened := make(chan openResult, 1)
	go func() {
		src, cleanup, openErr := openStdinSource(reader, opts)
		opened <- openResult{src: src, cleanup: cleanup, err: openErr}
	}()

	var result openResult
	select {
	case result = <-opened:
	case <-time.After(2 * time.Second):
		close(closeWriter)
		t.Fatal("known-size stdin type detection waited for pipe close")
	}
	close(closeWriter)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	defer result.cleanup()
	if result.src.uploadName != "greeting.txt" || !result.src.knownSize || result.src.size != 5 {
		t.Fatalf("source name=%q known=%t size=%d", result.src.uploadName, result.src.knownSize, result.src.size)
	}
}

func TestOpenFileSourceDetectsExtensionlessNameOverride(t *testing.T) {
	payload := compressTestLZ4(t, makeTestTar(t, []byte("file payload")))
	filePath := filepath.Join(t.TempDir(), "opaque")
	if err := os.WriteFile(filePath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	opts, parsedPath, err := parseFlags([]string{"--name", "hello", filePath})
	if err != nil {
		t.Fatal(err)
	}
	src, cleanup, err := openSource(parsedPath, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if src.uploadName != "hello.tar.lz4" {
		t.Fatalf("uploadName=%q, want hello.tar.lz4", src.uploadName)
	}
}

func makeTestTar(t *testing.T, payload []byte) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	header := &tar.Header{Name: "payload.bin", Mode: 0o600, Size: int64(len(payload))}
	if err := writer.WriteHeader(header); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func compressTestLZ4(t *testing.T, input []byte) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := lz4.NewWriter(&output)
	if err := writer.Apply(
		lz4.BlockSizeOption(lz4.Block4Mb),
		lz4.BlockChecksumOption(false),
		lz4.ChecksumOption(true),
		lz4.CompressionLevelOption(lz4.Fast),
		lz4.ConcurrencyOption(1),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(input); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}
