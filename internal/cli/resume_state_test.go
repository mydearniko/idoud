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
	if err := os.WriteFile(filePath, []byte("stable upload body"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("stat fixture: %v", err)
	}
	src := &sourceFile{
		knownSize:       true,
		size:            info.Size(),
		uploadName:      "large.bin",
		displayName:     filePath,
		modTimeUnixNano: info.ModTime().UnixNano(),
	}

	first := options{serverURL: "https://idoud.cc", uploadKey: randomUploadKey()}
	resumeID, err := configureUploadResume(&first, src)
	if err != nil {
		t.Fatalf("first configure: %v", err)
	}
	if resumeID == "" || first.uploadKey == "" {
		t.Fatalf("resumeID=%q uploadKey=%q", resumeID, first.uploadKey)
	}

	second := options{serverURL: "https://idoud.cc", uploadKey: randomUploadKey()}
	secondID, err := configureUploadResume(&second, src)
	if err != nil {
		t.Fatalf("second configure: %v", err)
	}
	if secondID != resumeID || second.uploadKey != first.uploadKey {
		t.Fatalf("resume identity/key changed: first=%q/%q second=%q/%q", resumeID, first.uploadKey, secondID, second.uploadKey)
	}

	if err := completeUploadResume(resumeID); err != nil {
		t.Fatalf("complete resume: %v", err)
	}
	third := options{serverURL: "https://idoud.cc", uploadKey: randomUploadKey()}
	if _, err := configureUploadResume(&third, src); err != nil {
		t.Fatalf("third configure: %v", err)
	}
	if third.uploadKey == first.uploadKey {
		t.Fatal("completed upload key was reused")
	}
}
