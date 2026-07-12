package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	defaultUpdateBaseURL       = "https://github.com/mydearniko/idoud"
	updateChecksumsName        = "checksums.txt"
	maxUpdateChecksumsBytes    = 1024 * 1024
	maxUpdateExecutableBytes   = 256 * 1024 * 1024
	defaultUpdateTimeout       = 5 * time.Minute
	updateRequestRetryAttempts = 3
	shortUpdateFlag            = "-a"
)

type updateRelease struct {
	tag       string
	version   string
	assetName string
	assetURL  string
	checksum  []byte
}

type updateResult struct {
	current string
	latest  string
	updated bool
}

type retryableUpdateDownloadError struct {
	err error
}

func (e *retryableUpdateDownloadError) Error() string { return e.err.Error() }
func (e *retryableUpdateDownloadError) Unwrap() error { return e.err }

type releaseUpdater struct {
	baseURL       string
	client        *http.Client
	goos          string
	goarch        string
	currentTarget string
	validate      func(context.Context, string, string) error
}

func runSelfUpdate(parent context.Context, currentVersion string) int {
	ctx, cancel := context.WithTimeout(parent, defaultUpdateTimeout)
	defer cancel()

	baseURL := strings.TrimSpace(os.Getenv("IDOUD_UPDATE_BASE_URL"))
	if baseURL == "" {
		baseURL = defaultUpdateBaseURL
	}
	fmt.Fprintf(os.Stdout, "Checking for idoud updates...\n")

	updater := releaseUpdater{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: 2 * time.Minute,
		},
		goos:     runtime.GOOS,
		goarch:   runtime.GOARCH,
		validate: validateUpdateExecutable,
	}
	result, err := updater.update(ctx, currentVersion)
	if err != nil {
		stderrLogf("update failed: %v", friendlyUpdateError(err))
		return 1
	}
	if !result.updated {
		fmt.Fprintf(os.Stdout, "Already up to date: idoud %s\n", result.current)
		return 0
	}
	fmt.Fprintf(os.Stdout, "Updated idoud %s -> %s\n", result.current, result.latest)
	return 0
}

func isUpdateCommand(args []string) bool {
	if len(args) != 1 {
		return false
	}
	token := strings.TrimSpace(args[0])
	if token == "--update" || token == shortUpdateFlag {
		return true
	}
	if !strings.EqualFold(token, "update") {
		return false
	}
	// Preserve the convenient positional command without stealing a real local
	// file. An explicit --update/-a always updates, even when ./update exists.
	if info, err := os.Stat(token); err == nil {
		return !info.Mode().IsRegular()
	} else if !errors.Is(err, os.ErrNotExist) {
		return false
	}
	return true
}

func (u releaseUpdater) update(ctx context.Context, currentVersion string) (updateResult, error) {
	current := normalizeReleaseVersion(currentVersion)
	if current == "" {
		current = "dev"
	}
	release, err := u.latestRelease(ctx)
	if err != nil {
		return updateResult{}, err
	}
	result := updateResult{current: current, latest: release.version}
	if releaseVersionAtLeast(current, release.version) {
		return result, nil
	}

	target := strings.TrimSpace(u.currentTarget)
	if target == "" {
		target, err = os.Executable()
		if err != nil {
			return updateResult{}, fmt.Errorf("locate current executable: %w", err)
		}
	}
	target, err = filepath.Abs(target)
	if err != nil {
		return updateResult{}, fmt.Errorf("resolve current executable: %w", err)
	}
	if resolved, resolveErr := filepath.EvalSymlinks(target); resolveErr == nil {
		target = resolved
	}
	targetInfo, err := os.Stat(target)
	if err != nil {
		return updateResult{}, fmt.Errorf("stat current executable: %w", err)
	}
	if !targetInfo.Mode().IsRegular() {
		return updateResult{}, fmt.Errorf("current executable is not a regular file: %s", target)
	}

	newPath, err := u.downloadRelease(ctx, release, filepath.Dir(target), targetInfo.Mode().Perm())
	if err != nil {
		return updateResult{}, err
	}
	removeNew := true
	defer func() {
		if removeNew {
			_ = os.Remove(newPath)
		}
	}()

	validate := u.validate
	if validate == nil {
		validate = validateUpdateExecutable
	}
	if err := validate(ctx, newPath, release.version); err != nil {
		return updateResult{}, fmt.Errorf("validate downloaded executable: %w", err)
	}
	if err := replaceExecutable(newPath, target); err != nil {
		return updateResult{}, fmt.Errorf("replace current executable: %w", err)
	}
	removeNew = false
	result.updated = true
	return result, nil
}

func (u releaseUpdater) latestRelease(ctx context.Context) (updateRelease, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(u.baseURL), "/")
	parsedBase, err := url.Parse(baseURL)
	if err != nil || parsedBase.Scheme == "" || parsedBase.Host == "" {
		return updateRelease{}, fmt.Errorf("invalid update repository URL %q", u.baseURL)
	}
	assetName, err := updateAssetName(u.goos, u.goarch)
	if err != nil {
		return updateRelease{}, err
	}

	checksumsURL := baseURL + "/releases/latest/download/" + updateChecksumsName
	tag, err := u.resolveLatestTag(ctx, checksumsURL)
	if err != nil {
		return updateRelease{}, err
	}
	version := normalizeReleaseVersion(tag)
	if _, valid := releaseVersionParts(version); !valid {
		return updateRelease{}, fmt.Errorf("latest release has unsupported version tag %q", tag)
	}
	pinnedChecksumsURL := baseURL + "/releases/download/" + url.PathEscape(tag) + "/" + updateChecksumsName
	resp, err := u.getWithRetry(ctx, pinnedChecksumsURL)
	if err != nil {
		return updateRelease{}, fmt.Errorf("fetch latest release metadata: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 32*1024))
		return updateRelease{}, fmt.Errorf("fetch latest release metadata: HTTP %d", resp.StatusCode)
	}
	checksums, err := io.ReadAll(io.LimitReader(resp.Body, maxUpdateChecksumsBytes+1))
	if err != nil {
		return updateRelease{}, fmt.Errorf("read latest release metadata: %w", err)
	}
	if len(checksums) > maxUpdateChecksumsBytes {
		return updateRelease{}, errors.New("latest release checksum file is unexpectedly large")
	}
	checksum, err := checksumForUpdateAsset(checksums, assetName)
	if err != nil {
		return updateRelease{}, err
	}
	assetURL := baseURL + "/releases/download/" + url.PathEscape(tag) + "/" + url.PathEscape(assetName)
	return updateRelease{
		tag:       tag,
		version:   version,
		assetName: assetName,
		assetURL:  assetURL,
		checksum:  checksum,
	}, nil
}

func (u releaseUpdater) resolveLatestTag(ctx context.Context, checksumsURL string) (string, error) {
	baseClient := u.client
	if baseClient == nil {
		baseClient = http.DefaultClient
	}
	client := *baseClient
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err := u.getWithClientRetry(ctx, checksumsURL, &client)
	if err != nil {
		return "", fmt.Errorf("resolve latest release: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		location, err := resp.Location()
		if err != nil {
			return "", fmt.Errorf("resolve latest release redirect: %w", err)
		}
		tag, err := updateTagFromRedirect(location)
		if err != nil {
			return "", err
		}
		return tag, nil
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return updateTagFromRedirect(resp.Request.URL)
	}
	return "", fmt.Errorf("resolve latest release: HTTP %d", resp.StatusCode)
}

func (u releaseUpdater) downloadRelease(ctx context.Context, release updateRelease, targetDir string, mode os.FileMode) (string, error) {
	var lastErr error
	for attempt := 0; attempt < updateRequestRetryAttempts; attempt++ {
		path, err := u.downloadReleaseOnce(ctx, release, targetDir, mode)
		if err == nil {
			return path, nil
		}
		var retryable *retryableUpdateDownloadError
		if !errors.As(err, &retryable) {
			return "", err
		}
		lastErr = retryable.err
		if attempt+1 >= updateRequestRetryAttempts {
			break
		}
		delay := time.Duration(attempt+1) * 300 * time.Millisecond
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(delay):
		}
	}
	return "", lastErr
}

func (u releaseUpdater) downloadReleaseOnce(ctx context.Context, release updateRelease, targetDir string, mode os.FileMode) (string, error) {
	pattern := ".idoud-update-*"
	if u.goos == "windows" {
		pattern += ".exe"
	}
	file, err := os.CreateTemp(targetDir, pattern)
	if err != nil {
		return "", fmt.Errorf("create update beside current executable: %w", err)
	}
	path := file.Name()
	keep := false
	defer func() {
		_ = file.Close()
		if !keep {
			_ = os.Remove(path)
		}
	}()

	resp, err := u.getWithRetry(ctx, release.assetURL)
	if err != nil {
		return "", &retryableUpdateDownloadError{err: fmt.Errorf("download %s: %w", release.assetName, err)}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 32*1024))
		return "", fmt.Errorf("download %s: HTTP %d", release.assetName, resp.StatusCode)
	}
	if resp.ContentLength > maxUpdateExecutableBytes {
		return "", fmt.Errorf("download %s: executable is unexpectedly large", release.assetName)
	}

	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(resp.Body, maxUpdateExecutableBytes+1))
	if err != nil {
		return "", &retryableUpdateDownloadError{err: fmt.Errorf("download %s: %w", release.assetName, err)}
	}
	if written > maxUpdateExecutableBytes {
		return "", fmt.Errorf("download %s: executable is unexpectedly large", release.assetName)
	}
	if err := file.Sync(); err != nil {
		return "", fmt.Errorf("flush downloaded executable: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close downloaded executable: %w", err)
	}
	actualChecksum := hash.Sum(nil)
	if len(release.checksum) != sha256.Size || subtle.ConstantTimeCompare(actualChecksum, release.checksum) != 1 {
		return "", &retryableUpdateDownloadError{err: fmt.Errorf("downloaded executable checksum mismatch: expected %x, got %x", release.checksum, actualChecksum)}
	}
	if mode == 0 {
		mode = 0o755
	}
	if err := os.Chmod(path, mode); err != nil {
		return "", fmt.Errorf("make downloaded executable runnable: %w", err)
	}
	keep = true
	return path, nil
}

func (u releaseUpdater) getWithRetry(ctx context.Context, rawURL string) (*http.Response, error) {
	client := u.client
	if client == nil {
		client = http.DefaultClient
	}
	return u.getWithClientRetry(ctx, rawURL, client)
}

func (u releaseUpdater) getWithClientRetry(ctx context.Context, rawURL string, client *http.Client) (*http.Response, error) {
	var lastErr error
	for attempt := 0; attempt < updateRequestRetryAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/octet-stream, text/plain;q=0.9")
		req.Header.Set("User-Agent", "idoud-self-update")
		resp, err := client.Do(req)
		if err == nil && !retryableUpdateStatus(resp.StatusCode) {
			return resp, nil
		}
		if err == nil {
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 32*1024))
			_ = resp.Body.Close()
		} else {
			lastErr = err
		}
		if attempt+1 >= updateRequestRetryAttempts {
			break
		}
		delay := time.Duration(attempt+1) * 300 * time.Millisecond
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, lastErr
}

func retryableUpdateStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

func updateAssetName(goos, goarch string) (string, error) {
	supported := false
	switch goos {
	case "linux":
		supported = goarch == "amd64" || goarch == "arm64" || goarch == "arm"
	case "darwin":
		supported = goarch == "amd64" || goarch == "arm64"
	case "windows":
		supported = goarch == "amd64" || goarch == "arm64"
	}
	if !supported {
		return "", fmt.Errorf("self-update is not available for %s/%s; install a matching release manually", goos, goarch)
	}
	name := "idoud_" + goos + "_" + goarch
	if goos == "windows" {
		name += ".exe"
	}
	return name, nil
}

func updateTagFromRedirect(finalURL *url.URL) (string, error) {
	if finalURL == nil {
		return "", errors.New("latest release response had no final URL")
	}
	parts := strings.Split(strings.Trim(finalURL.Path, "/"), "/")
	for i := 0; i+2 < len(parts); i++ {
		if parts[i] == "releases" && parts[i+1] == "download" {
			tag, err := url.PathUnescape(parts[i+2])
			if err != nil || strings.TrimSpace(tag) == "" || tag == "latest" {
				break
			}
			return tag, nil
		}
	}
	return "", fmt.Errorf("could not determine release version from %s", finalURL.Redacted())
}

func checksumForUpdateAsset(checksums []byte, assetName string) ([]byte, error) {
	var found []byte
	for _, line := range strings.Split(string(checksums), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || strings.TrimPrefix(fields[len(fields)-1], "*") != assetName {
			continue
		}
		decoded, err := hex.DecodeString(fields[0])
		if err != nil || len(decoded) != sha256.Size {
			return nil, fmt.Errorf("invalid SHA-256 entry for %s", assetName)
		}
		if found != nil {
			return nil, fmt.Errorf("duplicate SHA-256 entries for %s", assetName)
		}
		found = decoded
	}
	if found == nil {
		return nil, fmt.Errorf("latest release has no checksum for %s", assetName)
	}
	return found, nil
}

func normalizeReleaseVersion(version string) string {
	version = strings.TrimSpace(version)
	version = strings.TrimPrefix(version, "idoud ")
	version = strings.TrimPrefix(version, "v")
	return strings.TrimSpace(version)
}

func releaseVersionAtLeast(current, latest string) bool {
	current = normalizeReleaseVersion(current)
	latest = normalizeReleaseVersion(latest)
	if current == latest && current != "" {
		return true
	}
	if current == "" || current == "dev" {
		return false
	}
	currentParts, currentOK := releaseVersionParts(current)
	latestParts, latestOK := releaseVersionParts(latest)
	if !currentOK || !latestOK {
		return false
	}
	for i := range currentParts {
		if currentParts[i] != latestParts[i] {
			return currentParts[i] > latestParts[i]
		}
	}
	currentPrerelease := releaseVersionPrerelease(current)
	latestPrerelease := releaseVersionPrerelease(latest)
	if currentPrerelease == "" {
		return true
	}
	if latestPrerelease == "" {
		return false
	}
	// Different prereleases at the same numeric version are uncommon for the
	// stable "latest" channel. Prefer revalidating the published asset instead
	// of guessing at full semantic-version prerelease ordering here.
	return currentPrerelease == latestPrerelease
}

func releaseVersionParts(version string) ([3]int, bool) {
	var result [3]int
	version = strings.SplitN(version, "+", 2)[0]
	version = strings.SplitN(version, "-", 2)[0]
	parts := strings.Split(version, ".")
	if len(parts) != len(result) {
		return result, false
	}
	for index, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return result, false
		}
		result[index] = value
	}
	return result, true
}

func releaseVersionPrerelease(version string) string {
	version = strings.SplitN(version, "+", 2)[0]
	parts := strings.SplitN(version, "-", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return ""
}

func validateUpdateExecutable(parent context.Context, executablePath, expectedVersion string) error {
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executablePath, "--version")
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run --version: %w", err)
	}
	got := normalizeReleaseVersion(output.String())
	want := normalizeReleaseVersion(expectedVersion)
	if got != want {
		return fmt.Errorf("downloaded binary reported version %q, expected %q", got, want)
	}
	return nil
}

func friendlyUpdateError(err error) error {
	if errors.Is(err, os.ErrPermission) {
		return fmt.Errorf("%w; rerun from an elevated shell or install idoud in a user-writable directory", err)
	}
	return err
}
