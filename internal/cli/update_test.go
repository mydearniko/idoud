package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestIsUpdateCommand(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, args := range [][]string{{"update"}, {"UPDATE"}, {" update "}, {"--update"}, {shortUpdateFlag}} {
		if !isUpdateCommand(args) {
			t.Fatalf("isUpdateCommand(%q)=false, want true", args)
		}
	}
	for _, args := range [][]string{nil, {"./update"}, {"--", "update"}, {"update", "--force"}} {
		if isUpdateCommand(args) {
			t.Fatalf("isUpdateCommand(%q)=true, want false", args)
		}
	}

	if err := os.WriteFile("update", []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	if isUpdateCommand([]string{"update"}) {
		t.Fatal("positional update command stole an existing local file")
	}
	for _, args := range [][]string{{"--update"}, {shortUpdateFlag}} {
		if !isUpdateCommand(args) {
			t.Fatalf("explicit update command %q was shadowed by a local file", args)
		}
	}
	opts, filePath, err := parseFlags([]string{"update"})
	if err != nil {
		t.Fatalf("parse existing update file: %v", err)
	}
	if filePath != "update" || opts.stdin || opts.download {
		t.Fatalf("existing update parsed as filePath=%q opts=%+v", filePath, opts)
	}
	if err := os.Remove("update"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir("update", 0o700); err != nil {
		t.Fatal(err)
	}
	if !isUpdateCommand([]string{"update"}) {
		t.Fatal("a directory named update incorrectly shadowed the update command")
	}
}

func TestUpdateAssetName(t *testing.T) {
	tests := []struct {
		goos   string
		goarch string
		want   string
	}{
		{goos: "linux", goarch: "amd64", want: "idoud_linux_amd64"},
		{goos: "linux", goarch: "arm64", want: "idoud_linux_arm64"},
		{goos: "linux", goarch: "arm", want: "idoud_linux_arm"},
		{goos: "darwin", goarch: "amd64", want: "idoud_darwin_amd64"},
		{goos: "darwin", goarch: "arm64", want: "idoud_darwin_arm64"},
		{goos: "windows", goarch: "amd64", want: "idoud_windows_amd64.exe"},
		{goos: "windows", goarch: "arm64", want: "idoud_windows_arm64.exe"},
	}
	for _, test := range tests {
		got, err := updateAssetName(test.goos, test.goarch)
		if err != nil {
			t.Fatalf("updateAssetName(%s/%s): %v", test.goos, test.goarch, err)
		}
		if got != test.want {
			t.Fatalf("updateAssetName(%s/%s)=%q, want %q", test.goos, test.goarch, got, test.want)
		}
	}
	if _, err := updateAssetName("freebsd", "amd64"); err == nil {
		t.Fatal("updateAssetName(freebsd/amd64) succeeded, want unsupported error")
	}
}

func TestUpdateTagFromRedirect(t *testing.T) {
	parsed, err := url.Parse("https://github.com/mydearniko/idoud/releases/download/v1.3.0/checksums.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, err := updateTagFromRedirect(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if got != "v1.3.0" {
		t.Fatalf("tag=%q, want v1.3.0", got)
	}
	latest, _ := url.Parse("https://github.com/mydearniko/idoud/releases/latest/download/checksums.txt")
	if _, err := updateTagFromRedirect(latest); err == nil {
		t.Fatal("latest URL produced a release tag without a redirect")
	}
}

func TestChecksumForUpdateAsset(t *testing.T) {
	want := sha256.Sum256([]byte("binary"))
	payload := fmt.Sprintf("%x  idoud_linux_arm64\n%x  idoud_linux_amd64\n", sha256.Sum256([]byte("other")), want)
	got, err := checksumForUpdateAsset([]byte(payload), "idoud_linux_amd64")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want[:]) {
		t.Fatalf("checksum=%x, want %x", got, want)
	}
	if _, err := checksumForUpdateAsset([]byte(payload), "missing"); err == nil {
		t.Fatal("missing asset checksum succeeded")
	}
	duplicate := fmt.Sprintf("%x idoud_linux_amd64\n%x *idoud_linux_amd64\n", want, want)
	if _, err := checksumForUpdateAsset([]byte(duplicate), "idoud_linux_amd64"); err == nil {
		t.Fatal("duplicate asset checksums succeeded")
	}
}

func TestReleaseVersionAtLeast(t *testing.T) {
	tests := []struct {
		current string
		latest  string
		want    bool
	}{
		{current: "1.3.0", latest: "v1.3.0", want: true},
		{current: "1.4.0", latest: "1.3.9", want: true},
		{current: "2.0.0", latest: "1.99.99", want: true},
		{current: "1.2.9", latest: "1.3.0", want: false},
		{current: "1.3.0-beta.1", latest: "1.3.0", want: false},
		{current: "1.3.0", latest: "1.3.0-beta.1", want: true},
		{current: "1.3.0-beta.1", latest: "1.3.0-beta.2", want: false},
		{current: "dev", latest: "1.3.0", want: false},
		{current: "unknown", latest: "1.3.0", want: false},
	}
	for _, test := range tests {
		if got := releaseVersionAtLeast(test.current, test.latest); got != test.want {
			t.Fatalf("releaseVersionAtLeast(%q, %q)=%t, want %t", test.current, test.latest, got, test.want)
		}
	}
}

func TestReleaseUpdaterReplacesVerifiedExecutable(t *testing.T) {
	replacement := []byte("new idoud executable")
	fixture := newUpdateServer(t, "v9.8.7", replacement, nil)
	defer fixture.server.Close()

	target := filepath.Join(t.TempDir(), "idoud")
	if err := os.WriteFile(target, []byte("old idoud executable"), 0o755); err != nil {
		t.Fatal(err)
	}
	updater := releaseUpdater{
		baseURL:       fixture.server.URL,
		client:        fixture.server.Client(),
		goos:          "linux",
		goarch:        "amd64",
		currentTarget: target,
		validate: func(_ context.Context, path, version string) error {
			if version != "9.8.7" {
				return fmt.Errorf("version=%q", version)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if !bytes.Equal(got, replacement) {
				return errors.New("replacement bytes differ")
			}
			return nil
		},
	}
	result, err := updater.update(context.Background(), "1.2.0")
	if err != nil {
		t.Fatal(err)
	}
	if !result.updated || result.current != "1.2.0" || result.latest != "9.8.7" {
		t.Fatalf("result=%+v", result)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, replacement) {
		t.Fatalf("installed bytes=%q, want %q", got, replacement)
	}
	if fixture.assetRequests.Load() != 1 {
		t.Fatalf("asset requests=%d, want 1", fixture.assetRequests.Load())
	}
}

func TestReleaseUpdaterAlreadyCurrentDoesNotDownload(t *testing.T) {
	fixture := newUpdateServer(t, "v1.3.0", []byte("replacement"), nil)
	defer fixture.server.Close()
	target := filepath.Join(t.TempDir(), "idoud")
	original := []byte("current executable")
	if err := os.WriteFile(target, original, 0o755); err != nil {
		t.Fatal(err)
	}
	updater := releaseUpdater{
		baseURL:       fixture.server.URL,
		client:        fixture.server.Client(),
		goos:          "linux",
		goarch:        "amd64",
		currentTarget: target,
		validate: func(context.Context, string, string) error {
			t.Fatal("validator called for current release")
			return nil
		},
	}
	result, err := updater.update(context.Background(), "1.3.0")
	if err != nil {
		t.Fatal(err)
	}
	if result.updated {
		t.Fatalf("result=%+v, want no update", result)
	}
	if fixture.assetRequests.Load() != 0 {
		t.Fatalf("asset requests=%d, want 0", fixture.assetRequests.Load())
	}
	got, _ := os.ReadFile(target)
	if !bytes.Equal(got, original) {
		t.Fatalf("target changed while already current: %q", got)
	}
}

func TestReleaseUpdaterChecksumFailurePreservesExecutable(t *testing.T) {
	wrong := sha256.Sum256([]byte("not the replacement"))
	fixture := newUpdateServer(t, "v1.3.0", []byte("replacement"), wrong[:])
	defer fixture.server.Close()
	target := filepath.Join(t.TempDir(), "idoud")
	original := []byte("current executable")
	if err := os.WriteFile(target, original, 0o755); err != nil {
		t.Fatal(err)
	}
	updater := releaseUpdater{
		baseURL:       fixture.server.URL,
		client:        fixture.server.Client(),
		goos:          "linux",
		goarch:        "amd64",
		currentTarget: target,
		validate: func(context.Context, string, string) error {
			t.Fatal("validator called after checksum failure")
			return nil
		},
	}
	if _, err := updater.update(context.Background(), "1.2.0"); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("update error=%v, want checksum mismatch", err)
	}
	got, _ := os.ReadFile(target)
	if !bytes.Equal(got, original) {
		t.Fatalf("target changed after checksum failure: %q", got)
	}
	assertNoUpdateTemps(t, filepath.Dir(target))
}

func TestReleaseUpdaterRetriesCorruptedDownload(t *testing.T) {
	replacement := []byte("verified replacement")
	fixture := newUpdateServer(t, "v1.3.0", replacement, nil)
	fixture.firstAsset = []byte("corrupted first response")
	defer fixture.server.Close()
	target := filepath.Join(t.TempDir(), "idoud")
	if err := os.WriteFile(target, []byte("current executable"), 0o755); err != nil {
		t.Fatal(err)
	}
	updater := releaseUpdater{
		baseURL:       fixture.server.URL,
		client:        fixture.server.Client(),
		goos:          "linux",
		goarch:        "amd64",
		currentTarget: target,
		validate:      func(context.Context, string, string) error { return nil },
	}
	result, err := updater.update(context.Background(), "1.2.0")
	if err != nil {
		t.Fatal(err)
	}
	if !result.updated {
		t.Fatalf("result=%+v, want updated", result)
	}
	if fixture.assetRequests.Load() != 2 {
		t.Fatalf("asset requests=%d, want 2", fixture.assetRequests.Load())
	}
	got, _ := os.ReadFile(target)
	if !bytes.Equal(got, replacement) {
		t.Fatalf("installed bytes=%q, want %q", got, replacement)
	}
}

func TestReleaseUpdaterValidationFailurePreservesExecutable(t *testing.T) {
	fixture := newUpdateServer(t, "v1.3.0", []byte("replacement"), nil)
	defer fixture.server.Close()
	target := filepath.Join(t.TempDir(), "idoud")
	original := []byte("current executable")
	if err := os.WriteFile(target, original, 0o755); err != nil {
		t.Fatal(err)
	}
	updater := releaseUpdater{
		baseURL:       fixture.server.URL,
		client:        fixture.server.Client(),
		goos:          "linux",
		goarch:        "amd64",
		currentTarget: target,
		validate: func(context.Context, string, string) error {
			return errors.New("not a valid idoud binary")
		},
	}
	if _, err := updater.update(context.Background(), "1.2.0"); err == nil || !strings.Contains(err.Error(), "not a valid idoud binary") {
		t.Fatalf("update error=%v, want validation error", err)
	}
	got, _ := os.ReadFile(target)
	if !bytes.Equal(got, original) {
		t.Fatalf("target changed after validation failure: %q", got)
	}
	assertNoUpdateTemps(t, filepath.Dir(target))
}

type updateServerFixture struct {
	server        *httptest.Server
	assetRequests atomic.Int64
	firstAsset    []byte
}

func newUpdateServer(t *testing.T, tag string, replacement, checksumOverride []byte) *updateServerFixture {
	t.Helper()
	fixture := &updateServerFixture{}
	assetName := "idoud_linux_amd64"
	checksum := sha256.Sum256(replacement)
	checksumBytes := checksum[:]
	if checksumOverride != nil {
		checksumBytes = checksumOverride
	}
	checksumText := hex.EncodeToString(checksumBytes) + "  " + assetName + "\n"

	fixture.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/releases/latest/download/checksums.txt":
			http.Redirect(w, r, "/releases/download/"+tag+"/checksums.txt", http.StatusFound)
		case "/releases/download/" + tag + "/checksums.txt":
			http.Redirect(w, r, "/release-assets/"+tag+"/checksums.txt", http.StatusFound)
		case "/release-assets/" + tag + "/checksums.txt":
			_, _ = io.WriteString(w, checksumText)
		case "/releases/download/" + tag + "/" + assetName:
			requestNumber := fixture.assetRequests.Add(1)
			if requestNumber == 1 && fixture.firstAsset != nil {
				_, _ = w.Write(fixture.firstAsset)
			} else {
				_, _ = w.Write(replacement)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	return fixture
}

func assertNoUpdateTemps(t *testing.T, directory string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(directory, ".idoud-update-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary update files remain: %v", matches)
	}
}
