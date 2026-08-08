package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigureUploadResumeReusesKeyForSameFile(t *testing.T) {
	cacheDir := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", cacheDir)
	filePath := filepath.Join(t.TempDir(), "large.bin")
	requireNoError(t, os.WriteFile(filePath, []byte("stable upload body"), 0o600), "write fixture: %v")

	info, err := os.Stat(filePath)
	requireNoError(t, err, "stat fixture: %v")

	src := &sourceFile{
		knownSize:       true,
		size:            info.Size(),
		uploadName:      "large.bin",
		displayName:     filePath,
		modTimeUnixNano: info.ModTime().UnixNano(),
	}

	first := options{serverURL: "https://idoud.cc", uploadKey: randomUploadKey()}
	resumeID, err := configureUploadResume(&first, src)
	requireNoError(t, err, "first configure: %v")

	if resumeID == "" || first.uploadKey == "" {
		t.Fatalf("resumeID=%q uploadKey=%q", resumeID, first.uploadKey)
	}

	second := options{serverURL: "https://idoud.cc", uploadKey: randomUploadKey()}
	secondID, err := configureUploadResume(&second, src)
	requireNoError(t, err, "second configure: %v")

	if secondID != resumeID || second.uploadKey != first.uploadKey {
		t.Fatalf("resume identity/key changed: first=%q/%q second=%q/%q", resumeID, first.uploadKey, secondID, second.uploadKey)
	}
	requireNoError(t, completeUploadResume(resumeID), "complete resume: %v")

	third := options{serverURL: "https://idoud.cc", uploadKey: randomUploadKey()}
	if _, err := configureUploadResume(&third, src); err != nil {
		t.Fatalf("third configure: %v", err)
	}
	fatalIf(t, third.uploadKey == first.uploadKey, "completed upload key was reused")
}
