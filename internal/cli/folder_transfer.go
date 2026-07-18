package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const (
	folderCLIProviderPayloadMaximum = int64(10_485_689)
	folderCLITransferParallelMax    = 8
	folderCLITransferResponseMax    = int64(1 << 20)
	folderCLITransferRetryAttempts  = 3
)

var folderCLIFoldCaser = cases.Fold()

type folderCLIPortableLimits struct {
	MaxComponentUnits    int
	MaxEncodedPath       int
	MaxDepth             int
	MaxActiveEntries     int64
	MaxDirectChildren    int64
	MaxMetadataBytes     int64
	ProviderPayloadBytes int64
	MaxUploadObjects     int
	MaxUploadBlocks      int
	ReplicationFactor    int
	RemainingEntries     *int64
}

type folderCLILocalEntry struct {
	Path        string
	Parts       []string
	ParentPath  string
	Name        string
	LocalPath   string
	Kind        string
	Size        int64
	Mtime       int64
	Executable  bool
	InitialInfo os.FileInfo
	RemoteID    string
	ETag        string
	VersionID   string
}

type folderCLILocalTree struct {
	Directories  []folderCLILocalEntry
	Files        []folderCLILocalEntry
	EntryCount   int64
	MetadataSize int64
	TotalBytes   int64
	TotalObjects int64
}

type folderCLIUploadObjectPlan struct {
	Index          int64  `json:"index"`
	Size           int64  `json:"size"`
	SHA256         string `json:"sha256"`
	Classification string `json:"classification"`
	Start          int64  `json:"-"`
}

type folderCLIUploadBlockPlan struct {
	Index        int64  `json:"index"`
	Kind         string `json:"kind"`
	Length       int64  `json:"length"`
	SHA256       string `json:"sha256"`
	CRC32        uint32 `json:"crc32"`
	ObjectIndex  int64  `json:"objectIndex"`
	ObjectOffset int64  `json:"objectOffset"`
}

type folderCLIFileUploadPlan struct {
	Entry   folderCLILocalEntry
	Objects []folderCLIUploadObjectPlan
	Blocks  []folderCLIUploadBlockPlan
}

type folderCLIRemoteTarget struct {
	Entry  folderCLIEntry
	Parent folderCLIEntry
}

type folderCLIPreparedUpload struct {
	SchemaVersion    int    `json:"schemaVersion"`
	OperationID      string `json:"operationId"`
	State            string `json:"state"`
	Replayed         bool   `json:"replayed"`
	VersionID        string `json:"versionId"`
	RequiredReplicas int    `json:"requiredReplicas"`
	Objects          []struct {
		Index            int64  `json:"index"`
		ID               string `json:"id"`
		Size             int64  `json:"size"`
		SHA256           string `json:"sha256"`
		State            string `json:"state"`
		VerifiedReplicas int    `json:"verifiedReplicas"`
		UploadURL        string `json:"uploadUrl"`
	} `json:"objects"`
	CommitURL string `json:"commitUrl"`
}

type folderCLIUploadCommit struct {
	SchemaVersion int            `json:"schemaVersion"`
	OperationID   string         `json:"operationId"`
	State         string         `json:"state"`
	Sequence      int64          `json:"sequence"`
	Replayed      bool           `json:"replayed"`
	Entry         folderCLIEntry `json:"entry"`
	Version       struct {
		ID          string `json:"id"`
		State       string `json:"state"`
		LogicalSize int64  `json:"logicalSize"`
		ContentHash string `json:"contentHash"`
		CRC32       uint32 `json:"crc32"`
	} `json:"version"`
}

type folderCLITransferResult struct {
	Path      string `json:"path"`
	State     string `json:"state"`
	Bytes     int64  `json:"bytes"`
	EntryID   string `json:"entry_id,omitempty"`
	VersionID string `json:"version_id,omitempty"`
	Error     string `json:"error,omitempty"`
}

func runFolderPush(args []string) int {
	fs := flag.NewFlagSet("idoud folder push", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	server := fs.String("server", defaultServerURL, "idoud server origin")
	jsonOutput := fs.Bool("json", false, "schema-versioned JSON")
	sessionFile := fs.String("session-file", "", "read a writer session from a 0600 file")
	remotePath := fs.String("path", "", "existing remote destination directory")
	parallel := fs.Int("parallel", 2, "bounded concurrent file uploads (1-8)")
	if err := fs.Parse(normalizeInterspersedArgs(fs, args)); err != nil {
		fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
		return 2
	}
	if fs.NArg() != 2 || strings.TrimSpace(*sessionFile) == "" {
		fmt.Fprintln(os.Stderr, "idoud: usage: idoud folder push LOCAL_DIR FOLDER --session-file FILE [--path REMOTE_DIR]")
		return 2
	}
	if *parallel < 1 || *parallel > folderCLITransferParallelMax {
		fmt.Fprintf(os.Stderr, "idoud: --parallel must be between 1 and %d\n", folderCLITransferParallelMax)
		return 2
	}
	localRoot, err := filepath.Abs(fs.Arg(0))
	if err != nil {
		printFolderCommandError(*jsonOutput, "invalid_local_path", err)
		return 2
	}
	rootInfo, err := os.Lstat(localRoot)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		if err == nil {
			err = errors.New("LOCAL_DIR must be a real directory, not a file or symbolic link")
		}
		printFolderCommandError(*jsonOutput, "invalid_local_path", err)
		return 2
	}

	client, shareID, err := newFolderCLIClient(*server, fs.Arg(1), *sessionFile)
	if err != nil {
		printFolderCommandError(*jsonOutput, "invalid_folder", err)
		return 2
	}
	ctx, stopSignals := newInterruptContext()
	defer stopSignals()
	descriptor, err := client.descriptor(ctx, shareID)
	if err != nil {
		return printFolderTransferFailure(*jsonOutput, "folder_push", "descriptor_failed", err)
	}
	if !descriptor.Folder.PermittedActions.Write {
		return printFolderTransferFailure(*jsonOutput, "folder_push", "write_capability_required", errors.New("the server did not authorize live-folder writes for this session"))
	}
	limits, err := folderCLILimitsFromDescriptor(descriptor, true)
	if err != nil {
		return printFolderTransferFailure(*jsonOutput, "folder_push", "protocol_upgrade_required", err)
	}
	baseParts, err := folderCLINormalizeRelativePath(strings.ReplaceAll(*remotePath, "\\", "/"), limits)
	if err != nil {
		return printFolderTransferFailure(*jsonOutput, "folder_push", "unsupported_name", err)
	}
	tree, err := folderCLIBuildLocalTree(localRoot, baseParts, limits)
	if err != nil {
		return printFolderTransferFailure(*jsonOutput, "folder_push", folderCLITransferErrorCode(err, "preflight_failed"), err)
	}
	filePlans, err := folderCLIHashLocalFiles(ctx, tree.Files, limits, !*jsonOutput)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return interruptExitCode
		}
		return printFolderTransferFailure(*jsonOutput, "folder_push", "local_read_failed", err)
	}
	destination, snapshotSequence, err := client.folderCLIDestination(ctx, shareID, baseParts)
	if err != nil {
		return printFolderTransferFailure(*jsonOutput, "folder_push", "destination_unavailable", err)
	}
	if destination.Kind != "root" && destination.Kind != "directory" {
		return printFolderTransferFailure(*jsonOutput, "folder_push", "invalid_entry", errors.New("remote destination is not a directory"))
	}
	if err := client.folderCLIPreflightRemotePush(ctx, shareID, destination, snapshotSequence, tree, limits); err != nil {
		return printFolderTransferFailure(*jsonOutput, "folder_push", folderCLITransferErrorCode(err, "preflight_failed"), err)
	}
	targets, err := client.folderCLIMaterializePush(ctx, shareID, destination, tree)
	if err != nil {
		return printFolderTransferFailure(*jsonOutput, "folder_push", folderCLITransferErrorCode(err, "namespace_failed"), err)
	}
	results := client.folderCLIUploadFiles(ctx, shareID, filePlans, targets, *parallel)
	if errors.Is(ctx.Err(), context.Canceled) {
		printFolderTransferResults(*jsonOutput, "folder_push", shareID, tree, results, limits.ReplicationFactor)
		return interruptExitCode
	}
	failed := printFolderTransferResults(*jsonOutput, "folder_push", shareID, tree, results, limits.ReplicationFactor)
	if failed {
		return 1
	}
	return 0
}

func runFolderPull(args []string) int {
	fs := flag.NewFlagSet("idoud folder pull", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	server := fs.String("server", defaultServerURL, "idoud server origin")
	jsonOutput := fs.Bool("json", false, "schema-versioned JSON")
	sessionFile := fs.String("session-file", "", "read an optional reader/writer session from a 0600 file")
	remotePath := fs.String("path", "", "remote file or directory to pull")
	parallel := fs.Int("parallel", 4, "bounded concurrent downloads (1-8)")
	if err := fs.Parse(normalizeInterspersedArgs(fs, args)); err != nil {
		fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
		return 2
	}
	if fs.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "idoud: usage: idoud folder pull FOLDER LOCAL_DIR [--session-file FILE] [--path REMOTE_PATH]")
		return 2
	}
	if *parallel < 1 || *parallel > folderCLITransferParallelMax {
		fmt.Fprintf(os.Stderr, "idoud: --parallel must be between 1 and %d\n", folderCLITransferParallelMax)
		return 2
	}
	client, shareID, err := newFolderCLIClient(*server, fs.Arg(0), *sessionFile)
	if err != nil {
		printFolderCommandError(*jsonOutput, "invalid_folder", err)
		return 2
	}
	ctx, stopSignals := newInterruptContext()
	defer stopSignals()
	descriptor, err := client.descriptor(ctx, shareID)
	if err != nil {
		return printFolderTransferFailure(*jsonOutput, "folder_pull", "descriptor_failed", err)
	}
	if !descriptor.Folder.PermittedActions.Download {
		return printFolderTransferFailure(*jsonOutput, "folder_pull", "auth_required", errors.New("the server did not authorize folder downloads for this session"))
	}
	limits, err := folderCLILimitsFromDescriptor(descriptor, false)
	if err != nil {
		return printFolderTransferFailure(*jsonOutput, "folder_pull", "protocol_upgrade_required", err)
	}
	pathParts, err := folderCLINormalizeRelativePath(strings.ReplaceAll(*remotePath, "\\", "/"), limits)
	if err != nil {
		return printFolderTransferFailure(*jsonOutput, "folder_pull", "unsupported_name", err)
	}
	remoteTree, err := client.folderCLIBuildRemoteTree(ctx, shareID, descriptor, pathParts, limits)
	if err != nil {
		return printFolderTransferFailure(*jsonOutput, "folder_pull", folderCLITransferErrorCode(err, "remote_preflight_failed"), err)
	}
	destination, err := folderCLIPreflightPullDestination(fs.Arg(1), remoteTree)
	if err != nil {
		return printFolderTransferFailure(*jsonOutput, "folder_pull", "local_destination_conflict", err)
	}
	if err := client.folderCLIHeadRemoteFiles(ctx, shareID, &remoteTree, *parallel); err != nil {
		return printFolderTransferFailure(*jsonOutput, "folder_pull", folderCLITransferErrorCode(err, "remote_preflight_failed"), err)
	}
	if err := folderCLICreatePullDirectories(destination, remoteTree.Directories); err != nil {
		return printFolderTransferFailure(*jsonOutput, "folder_pull", "local_write_failed", err)
	}
	results := client.folderCLIDownloadFiles(ctx, shareID, destination, remoteTree.Files, *parallel)
	if metadataErr := folderCLIApplyPullDirectoryMetadata(destination, remoteTree.Directories); metadataErr != nil {
		results = append(results, folderCLITransferResult{Path: ".", State: "recovery", Error: metadataErr.Error()})
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		printFolderTransferResults(*jsonOutput, "folder_pull", shareID, remoteTree, results, limits.ReplicationFactor)
		return interruptExitCode
	}
	failed := printFolderTransferResults(*jsonOutput, "folder_pull", shareID, remoteTree, results, limits.ReplicationFactor)
	if failed {
		return 1
	}
	return 0
}

func folderCLILimitsFromDescriptor(descriptor folderCLIDescriptorResponse, write bool) (folderCLIPortableLimits, error) {
	limits := descriptor.Folder.Limits
	if descriptor.SchemaVersion < 1 || limits.MaxComponentUnits < 1 || limits.MaxComponentUnits > 255 ||
		limits.MaxEncodedPath < 1 || limits.MaxEncodedPath > 4096 || limits.MaxDepth < 1 || limits.MaxDepth > 128 ||
		limits.MaxActiveEntries < 1 || limits.MaxDirectChildren < 1 || limits.MaxMetadataBytes < 1 {
		return folderCLIPortableLimits{}, errors.New("server omitted the bounded portable-folder limits")
	}
	out := folderCLIPortableLimits{
		MaxComponentUnits: limits.MaxComponentUnits, MaxEncodedPath: limits.MaxEncodedPath, MaxDepth: limits.MaxDepth,
		MaxActiveEntries: limits.MaxActiveEntries, MaxDirectChildren: limits.MaxDirectChildren, MaxMetadataBytes: limits.MaxMetadataBytes,
		ProviderPayloadBytes: limits.ProviderPayloadBytes, MaxUploadObjects: limits.MaxUploadObjects,
		MaxUploadBlocks: limits.MaxUploadBlocks, ReplicationFactor: limits.ReplicationFactor, RemainingEntries: limits.RemainingEntries,
	}
	if !write {
		return out, nil
	}
	if out.ProviderPayloadBytes < 1 || out.ProviderPayloadBytes > folderCLIProviderPayloadMaximum ||
		out.MaxUploadObjects < 1 || out.MaxUploadObjects > 8192 || out.MaxUploadBlocks < 1 || out.MaxUploadBlocks > 8192 ||
		(out.ReplicationFactor != 1 && out.ReplicationFactor != 2) {
		return folderCLIPortableLimits{}, errors.New("server omitted a safe bounded upload and durability plan")
	}
	return out, nil
}

func folderCLINormalizeComponent(value string, limits folderCLIPortableLimits) (string, string, error) {
	if !utf8.ValidString(value) {
		return "", "", fmt.Errorf("unsupported portable name %q: invalid UTF-8", value)
	}
	display := norm.NFC.String(value)
	if display == "" || display == "." || display == ".." {
		return "", "", fmt.Errorf("unsupported portable name %q: empty and dot components are reserved", value)
	}
	if strings.HasSuffix(display, ".") || strings.HasSuffix(display, " ") {
		return "", "", fmt.Errorf("unsupported portable name %q: trailing dots and spaces are not portable", value)
	}
	for _, character := range display {
		if unicode.IsControl(character) || strings.ContainsRune("/\\<>:\"|?*", character) {
			return "", "", fmt.Errorf("unsupported portable name %q: contains a reserved character", value)
		}
	}
	if len([]byte(display)) > limits.MaxComponentUnits || len(utf16.Encode([]rune(display))) > limits.MaxComponentUnits {
		return "", "", fmt.Errorf("unsupported portable name %q: component exceeds the negotiated %d-unit limit", value, limits.MaxComponentUnits)
	}
	base := display
	if dot := strings.IndexByte(base, '.'); dot >= 0 {
		base = base[:dot]
	}
	upper := strings.ToUpper(base)
	reserved := upper == "CON" || upper == "PRN" || upper == "AUX" || upper == "NUL"
	if len(upper) == 4 && (upper[:3] == "COM" || upper[:3] == "LPT") && upper[3] >= '1' && upper[3] <= '9' {
		reserved = true
	}
	if reserved {
		return "", "", fmt.Errorf("unsupported portable name %q: Windows device names are reserved", value)
	}
	return display, norm.NFC.String(folderCLIFoldCaser.String(display)), nil
}

func folderCLINormalizeRelativePath(value string, limits folderCLIPortableLimits) ([]string, error) {
	if value == "" || value == "." {
		return nil, nil
	}
	if strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") || strings.Contains(value, "//") {
		return nil, fmt.Errorf("unsupported portable path %q: path must be relative without empty components", value)
	}
	parts := strings.Split(value, "/")
	if len(parts) > limits.MaxDepth {
		return nil, fmt.Errorf("unsupported portable path %q: depth exceeds %d", value, limits.MaxDepth)
	}
	normalized := make([]string, 0, len(parts))
	encoded := 0
	for _, part := range parts {
		display, _, err := folderCLINormalizeComponent(part, limits)
		if err != nil {
			return nil, err
		}
		if len(normalized) > 0 {
			encoded++
		}
		encoded += len([]byte(display))
		if encoded > limits.MaxEncodedPath {
			return nil, fmt.Errorf("unsupported portable path %q: encoded path exceeds %d bytes", value, limits.MaxEncodedPath)
		}
		normalized = append(normalized, display)
	}
	return normalized, nil
}

func folderCLIBuildLocalTree(root string, baseParts []string, limits folderCLIPortableLimits) (folderCLILocalTree, error) {
	tree := folderCLILocalTree{}
	siblingKeys := make(map[string]map[string]string)
	directWidths := make(map[string]int64)
	seenPaths := make(map[string]struct{})
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rawPath := filepath.ToSlash(relative)
		parts, err := folderCLINormalizeRelativePath(rawPath, limits)
		if err != nil {
			return err
		}
		if _, err := folderCLINormalizeRelativePath(strings.Join(append(append([]string{}, baseParts...), parts...), "/"), limits); err != nil {
			return err
		}
		remotePath := strings.Join(parts, "/")
		if _, duplicate := seenPaths[remotePath]; duplicate {
			return fmt.Errorf("portable path collision at %q", remotePath)
		}
		seenPaths[remotePath] = struct{}{}
		parentPath := strings.Join(parts[:len(parts)-1], "/")
		name := parts[len(parts)-1]
		_, key, _ := folderCLINormalizeComponent(name, limits)
		if siblingKeys[parentPath] == nil {
			siblingKeys[parentPath] = make(map[string]string)
		}
		if prior := siblingKeys[parentPath][key]; prior != "" && prior != remotePath {
			return fmt.Errorf("portable case-insensitive collision between %q and %q", prior, remotePath)
		}
		siblingKeys[parentPath][key] = remotePath
		directWidths[parentPath]++
		if directWidths[parentPath] > limits.MaxDirectChildren {
			return fmt.Errorf("directory %q exceeds the negotiated direct-child limit", parentPath)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsupported symbolic link %q", rawPath)
		}
		record := folderCLILocalEntry{Path: remotePath, Parts: parts, ParentPath: parentPath, Name: name, LocalPath: path, InitialInfo: info}
		switch {
		case info.IsDir():
			record.Kind = "directory"
			tree.Directories = append(tree.Directories, record)
		case info.Mode().IsRegular():
			record.Kind = "file"
			record.Size = info.Size()
			record.Mtime = max(int64(1), info.ModTime().Unix())
			record.Executable = info.Mode().Perm()&0o111 != 0
			objects := int64(0)
			if record.Size > 0 {
				objects = 1 + (record.Size-1)/limits.ProviderPayloadBytes
			}
			if objects > int64(limits.MaxUploadObjects) || objects > int64(limits.MaxUploadBlocks) {
				return fmt.Errorf("%q requires %d blocks, above the negotiated limit", remotePath, objects)
			}
			if tree.TotalBytes > math.MaxInt64-record.Size || tree.TotalObjects > math.MaxInt64-objects {
				return errors.New("selected transfer size exceeds supported integer bounds")
			}
			tree.TotalBytes += record.Size
			tree.TotalObjects += objects
			tree.Files = append(tree.Files, record)
		default:
			return fmt.Errorf("unsupported special filesystem entry %q", rawPath)
		}
		tree.EntryCount++
		tree.MetadataSize += int64(len([]byte(remotePath)))
		if tree.EntryCount > limits.MaxActiveEntries || tree.MetadataSize > limits.MaxMetadataBytes {
			return errors.New("selected tree exceeds the negotiated folder metadata limits")
		}
		return nil
	})
	if err != nil {
		return folderCLILocalTree{}, err
	}
	sort.Slice(tree.Directories, func(i, j int) bool {
		return len(tree.Directories[i].Parts) < len(tree.Directories[j].Parts) ||
			(len(tree.Directories[i].Parts) == len(tree.Directories[j].Parts) && tree.Directories[i].Path < tree.Directories[j].Path)
	})
	sort.Slice(tree.Files, func(i, j int) bool { return tree.Files[i].Path < tree.Files[j].Path })
	return tree, nil
}

func folderCLIHashLocalFiles(ctx context.Context, entries []folderCLILocalEntry, limits folderCLIPortableLimits, progress bool) ([]folderCLIFileUploadPlan, error) {
	plans := make([]folderCLIFileUploadPlan, len(entries))
	buffer := make([]byte, limits.ProviderPayloadBytes)
	for index, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		file, err := os.Open(entry.LocalPath)
		if err != nil {
			return nil, fmt.Errorf("open %q: %w", entry.Path, err)
		}
		current, statErr := file.Stat()
		if statErr != nil || !folderCLISameLocalFile(entry, current) {
			_ = file.Close()
			if statErr != nil {
				return nil, fmt.Errorf("stat %q: %w", entry.Path, statErr)
			}
			return nil, fmt.Errorf("%q changed after recursive preflight", entry.Path)
		}
		plan := folderCLIFileUploadPlan{Entry: entry}
		for objectIndex, start := int64(0), int64(0); start < entry.Size; objectIndex, start = objectIndex+1, start+limits.ProviderPayloadBytes {
			length := min(limits.ProviderPayloadBytes, entry.Size-start)
			body := buffer[:length]
			if _, err := io.ReadFull(file, body); err != nil {
				_ = file.Close()
				return nil, fmt.Errorf("read %q: %w", entry.Path, err)
			}
			digest := sha256.Sum256(body)
			hash := hex.EncodeToString(digest[:])
			plan.Objects = append(plan.Objects, folderCLIUploadObjectPlan{Index: objectIndex, Size: length, SHA256: hash, Classification: "single", Start: start})
			plan.Blocks = append(plan.Blocks, folderCLIUploadBlockPlan{Index: objectIndex, Kind: "upload", Length: length, SHA256: hash, CRC32: crc32.ChecksumIEEE(body), ObjectIndex: objectIndex})
		}
		current, statErr = file.Stat()
		closeErr := file.Close()
		if statErr != nil || !folderCLISameLocalFile(entry, current) {
			if statErr != nil {
				return nil, fmt.Errorf("stat %q after hashing: %w", entry.Path, statErr)
			}
			return nil, fmt.Errorf("%q changed while it was hashed", entry.Path)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close %q: %w", entry.Path, closeErr)
		}
		plans[index] = plan
		if progress {
			fmt.Fprintf(os.Stderr, "preflight hashed %s (%d/%d)\n", entry.Path, index+1, len(entries))
		}
	}
	return plans, nil
}

func folderCLISameLocalFile(entry folderCLILocalEntry, current os.FileInfo) bool {
	return current != nil && current.Mode().IsRegular() && current.Size() == entry.Size && current.ModTime().Equal(entry.InitialInfo.ModTime()) &&
		current.Mode().Perm() == entry.InitialInfo.Mode().Perm() && os.SameFile(entry.InitialInfo, current)
}

func (c folderCLIClient) folderCLIDestination(ctx context.Context, shareID string, parts []string) (folderCLIEntry, int64, error) {
	descriptor, err := c.descriptor(ctx, shareID)
	if err != nil {
		return folderCLIEntry{}, 0, err
	}
	parentID := descriptor.Folder.RootEntryID
	var sequence int64
	if len(parts) == 0 {
		_, parent, listedSequence, err := c.listAllSnapshot(ctx, shareID, parentID)
		return parent, listedSequence, err
	}
	for index, component := range parts {
		entries, parent, listedSequence, err := c.listAllSnapshot(ctx, shareID, parentID)
		if err != nil {
			return folderCLIEntry{}, 0, err
		}
		if sequence == 0 {
			sequence = listedSequence
		} else if sequence != listedSequence {
			return folderCLIEntry{}, 0, errors.New("folder changed while resolving the remote destination")
		}
		found := folderCLIActiveChild(entries, component)
		if found.ID == "" {
			return folderCLIEntry{}, 0, fmt.Errorf("remote path component %q was not found", component)
		}
		if index == len(parts)-1 {
			return found, sequence, nil
		}
		if found.Kind != "directory" && found.Kind != "root" {
			return folderCLIEntry{}, 0, fmt.Errorf("remote path component %q is not a directory", component)
		}
		_ = parent
		parentID = found.ID
	}
	return folderCLIEntry{}, 0, errors.New("remote destination could not be resolved")
}

type folderCLIRemotePreflightNode struct {
	Exists      bool
	Entry       folderCLIEntry
	Snapshot    []folderCLIEntry
	Parent      folderCLIEntry
	Sequence    int64
	SnapshotSet bool
}

func (c folderCLIClient) folderCLIPreflightRemotePush(ctx context.Context, shareID string, destination folderCLIEntry, expectedSequence int64, tree folderCLILocalTree, limits folderCLIPortableLimits) error {
	directories := map[string]*folderCLIRemotePreflightNode{"": {Exists: true, Entry: destination}}
	files := make(map[string]*folderCLIRemotePreflightNode, len(tree.Files))
	loadParent := func(node *folderCLIRemotePreflightNode) error {
		if node.SnapshotSet || !node.Exists {
			return nil
		}
		entries, parent, sequence, err := c.listAllSnapshot(ctx, shareID, node.Entry.ID)
		if err != nil {
			return err
		}
		if expectedSequence > 0 && sequence != expectedSequence {
			return errors.New("folder changed during the read-only push preflight")
		}
		node.Snapshot, node.Parent, node.Sequence, node.SnapshotSet = entries, parent, sequence, true
		return nil
	}
	for _, record := range tree.Directories {
		parent := directories[record.ParentPath]
		if parent == nil {
			return fmt.Errorf("local tree parent %q was not preflighted", record.ParentPath)
		}
		if err := loadParent(parent); err != nil {
			return err
		}
		node := &folderCLIRemotePreflightNode{}
		if parent.Exists {
			existing := folderCLIActiveChild(parent.Snapshot, record.Name)
			if existing.ID != "" {
				if existing.Kind != "directory" {
					return fmt.Errorf("remote path %q is already a file", record.Path)
				}
				node.Exists, node.Entry = true, existing
			}
		}
		directories[record.Path] = node
	}
	for _, record := range tree.Files {
		parent := directories[record.ParentPath]
		if parent == nil {
			return fmt.Errorf("local file parent %q was not preflighted", record.ParentPath)
		}
		if err := loadParent(parent); err != nil {
			return err
		}
		node := &folderCLIRemotePreflightNode{}
		if parent.Exists {
			existing := folderCLIActiveChild(parent.Snapshot, record.Name)
			if existing.ID != "" {
				if existing.Kind != "file" {
					return fmt.Errorf("remote path %q is already a directory", record.Path)
				}
				node.Exists, node.Entry = true, existing
			}
		}
		files[record.Path] = node
	}
	additions := make(map[string]int64)
	for _, record := range tree.Directories {
		if !directories[record.Path].Exists {
			additions[record.ParentPath]++
		}
	}
	for _, record := range tree.Files {
		if !files[record.Path].Exists {
			additions[record.ParentPath]++
		}
	}
	var totalAdditions int64
	for parentPath, count := range additions {
		totalAdditions += count
		parent := directories[parentPath]
		if parent != nil && parent.Exists {
			if err := loadParent(parent); err != nil {
				return err
			}
			active := int64(0)
			for _, entry := range parent.Snapshot {
				if entry.State == "active" {
					active++
				}
			}
			if active+count > limits.MaxDirectChildren {
				return fmt.Errorf("remote directory %q would exceed its direct-child limit", parentPath)
			}
		}
	}
	if limits.RemainingEntries != nil && totalAdditions > *limits.RemainingEntries {
		return fmt.Errorf("push needs %d new entries but the folder permits %d", totalAdditions, *limits.RemainingEntries)
	}
	return nil
}

func (c folderCLIClient) folderCLIMaterializePush(ctx context.Context, shareID string, destination folderCLIEntry, tree folderCLILocalTree) (map[string]folderCLIRemoteTarget, error) {
	directories := map[string]folderCLIEntry{"": destination}
	for _, record := range tree.Directories {
		parent := directories[record.ParentPath]
		if parent.ID == "" {
			return nil, fmt.Errorf("remote parent %q is unavailable", record.ParentPath)
		}
		entry, err := c.folderCLIEnsureTransferEntry(ctx, shareID, parent, record, "directory")
		if err != nil {
			return nil, fmt.Errorf("create directory %q: %w", record.Path, err)
		}
		directories[record.Path] = entry
	}
	targets := make(map[string]folderCLIRemoteTarget, len(tree.Files))
	for _, record := range tree.Files {
		parent := directories[record.ParentPath]
		if parent.ID == "" {
			return nil, fmt.Errorf("remote parent %q is unavailable", record.ParentPath)
		}
		entry, err := c.folderCLIEnsureTransferEntry(ctx, shareID, parent, record, "file")
		if err != nil {
			return nil, fmt.Errorf("create file target %q: %w", record.Path, err)
		}
		targets[record.Path] = folderCLIRemoteTarget{Entry: entry, Parent: parent}
	}
	return targets, nil
}

func (c folderCLIClient) folderCLIEnsureTransferEntry(ctx context.Context, shareID string, parent folderCLIEntry, record folderCLILocalEntry, kind string) (folderCLIEntry, error) {
	for attempt := 0; attempt < 4; attempt++ {
		entries, currentParent, sequence, err := c.listAllSnapshot(ctx, shareID, parent.ID)
		if err != nil {
			return folderCLIEntry{}, err
		}
		if existing := folderCLIActiveChild(entries, record.Name); existing.ID != "" {
			if existing.Kind != kind {
				return folderCLIEntry{}, &folderCLIRequestError{Status: http.StatusConflict, Code: "revision_conflict", Message: "an entry with another kind now occupies the path"}
			}
			return existing, nil
		}
		operationID, err := newFolderCLIOperationID()
		if err != nil {
			return folderCLIEntry{}, err
		}
		mutationType := "mkdir"
		if kind == "file" {
			mutationType = "create_pending_file"
		}
		body := map[string]any{
			"operationId": operationID, "type": mutationType, "parentId": currentParent.ID, "name": record.Name,
			"expectedFolderSequence": sequence, "expectedParentRevision": currentParent.EntryRevision,
		}
		body["mtime"] = record.Mtime
		if kind == "file" {
			body["executable"] = record.Executable
		}
		var response folderCLIMutationResponse
		payload := mustJSON(body)
		err = c.folderCLIJSONIdempotent(ctx, http.MethodPost, "/v1/folders/"+url.PathEscape(shareID)+"/mutations", payload, &response)
		if err == nil {
			return response.Result.Entry, nil
		}
		if folderCLIErrorCode(err) != "revision_conflict" || attempt == 3 {
			return folderCLIEntry{}, err
		}
	}
	return folderCLIEntry{}, errors.New("entry creation retry budget exhausted")
}

func folderCLIActiveChild(entries []folderCLIEntry, name string) folderCLIEntry {
	_, wanted, err := folderCLINormalizeComponent(name, folderCLIPortableLimits{MaxComponentUnits: 255})
	if err != nil {
		return folderCLIEntry{}
	}
	for _, entry := range entries {
		if entry.State != "active" {
			continue
		}
		_, key, normalizeErr := folderCLINormalizeComponent(entry.Name, folderCLIPortableLimits{MaxComponentUnits: 255})
		if normalizeErr == nil && key == wanted {
			return entry
		}
	}
	return folderCLIEntry{}
}

func (c folderCLIClient) folderCLIUploadFiles(ctx context.Context, shareID string, plans []folderCLIFileUploadPlan, targets map[string]folderCLIRemoteTarget, parallel int) []folderCLITransferResult {
	results := make([]folderCLITransferResult, len(plans))
	jobs := make(chan int)
	var workers sync.WaitGroup
	workerCount := min(parallel, len(plans))
	for worker := 0; worker < workerCount; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				plan := plans[index]
				result := folderCLITransferResult{Path: plan.Entry.Path, State: "pending", Bytes: plan.Entry.Size}
				commit, err := c.folderCLIUploadFile(ctx, shareID, plan, targets[plan.Entry.Path])
				if err != nil {
					result.State, result.Error = folderCLITransferStateForError(err), err.Error()
				} else {
					result.State, result.EntryID, result.VersionID = commit.State, commit.Entry.ID, commit.Version.ID
				}
				results[index] = result
			}
		}()
	}
	for index := range plans {
		if ctx.Err() != nil {
			break
		}
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	for index := range results {
		if results[index].Path == "" {
			results[index] = folderCLITransferResult{Path: plans[index].Entry.Path, State: "recovery", Bytes: plans[index].Entry.Size, Error: "transfer cancelled before upload"}
		}
	}
	return results
}

func (c folderCLIClient) folderCLIUploadFile(ctx context.Context, shareID string, plan folderCLIFileUploadPlan, target folderCLIRemoteTarget) (folderCLIUploadCommit, error) {
	file, err := os.Open(plan.Entry.LocalPath)
	if err != nil {
		return folderCLIUploadCommit{}, err
	}
	defer file.Close()
	currentInfo, err := file.Stat()
	if err != nil || !folderCLISameLocalFile(plan.Entry, currentInfo) {
		return folderCLIUploadCommit{}, fmt.Errorf("%q changed after hashing", plan.Entry.Path)
	}
	entries, parent, sequence, err := c.listAllSnapshot(ctx, shareID, target.Parent.ID)
	if err != nil {
		return folderCLIUploadCommit{}, err
	}
	current := folderCLIActiveChild(entries, plan.Entry.Name)
	if current.ID == "" || current.ID != target.Entry.ID {
		return folderCLIUploadCommit{}, &folderCLIRequestError{Status: http.StatusConflict, Code: "revision_conflict", Message: "upload target changed after preflight"}
	}
	operationID, err := newFolderCLIOperationID()
	if err != nil {
		return folderCLIUploadCommit{}, err
	}
	prepareBody := struct {
		OperationID            string                      `json:"operationId"`
		EntryID                string                      `json:"entryId"`
		ParentID               string                      `json:"parentId"`
		BaseVersionID          string                      `json:"baseVersionId"`
		ExpectedFolderSequence int64                       `json:"expectedFolderSequence"`
		ExpectedEntryRevision  int64                       `json:"expectedEntryRevision"`
		ExpectedParentRevision int64                       `json:"expectedParentRevision"`
		LogicalSize            int64                       `json:"logicalSize"`
		Mtime                  int64                       `json:"mtime"`
		Executable             bool                        `json:"executable"`
		Objects                []folderCLIUploadObjectPlan `json:"objects"`
		Blocks                 []folderCLIUploadBlockPlan  `json:"blocks"`
	}{
		OperationID: operationID, EntryID: current.ID, ParentID: parent.ID, BaseVersionID: current.VersionID,
		ExpectedFolderSequence: sequence, ExpectedEntryRevision: current.EntryRevision, ExpectedParentRevision: parent.EntryRevision,
		LogicalSize: plan.Entry.Size, Mtime: plan.Entry.Mtime, Executable: plan.Entry.Executable, Objects: plan.Objects, Blocks: plan.Blocks,
	}
	payload, err := json.Marshal(prepareBody)
	if err != nil {
		return folderCLIUploadCommit{}, err
	}
	var prepared folderCLIPreparedUpload
	if err := c.folderCLIJSONIdempotent(ctx, http.MethodPost, "/v1/folders/"+url.PathEscape(shareID)+"/uploads/prepare", payload, &prepared); err != nil {
		return folderCLIUploadCommit{}, err
	}
	if prepared.SchemaVersion < 1 || prepared.OperationID != operationID || prepared.RequiredReplicas < 1 || prepared.RequiredReplicas > 2 || len(prepared.Objects) != len(plan.Objects) {
		return folderCLIUploadCommit{}, errors.New("server returned an invalid prepared upload plan")
	}
	sort.Slice(prepared.Objects, func(i, j int) bool { return prepared.Objects[i].Index < prepared.Objects[j].Index })
	for index, object := range prepared.Objects {
		expected := plan.Objects[index]
		if object.Index != int64(index) || object.Size != expected.Size || object.SHA256 != expected.SHA256 || object.ID == "" {
			return folderCLIUploadCommit{}, errors.New("server changed the prepared provider-object mapping")
		}
		if err := c.folderCLIUploadObject(ctx, shareID, operationID, prepared.RequiredReplicas, object, file, expected.Start); err != nil {
			return folderCLIUploadCommit{}, err
		}
	}
	currentInfo, err = file.Stat()
	if err != nil || !folderCLISameLocalFile(plan.Entry, currentInfo) {
		return folderCLIUploadCommit{}, fmt.Errorf("%q changed while it was uploaded; the pending version was not published", plan.Entry.Path)
	}
	return c.folderCLICommitUpload(ctx, shareID, operationID)
}

func (c folderCLIClient) folderCLIUploadObject(ctx context.Context, shareID string, operationID string, requiredReplicas int, object struct {
	Index            int64  `json:"index"`
	ID               string `json:"id"`
	Size             int64  `json:"size"`
	SHA256           string `json:"sha256"`
	State            string `json:"state"`
	VerifiedReplicas int    `json:"verifiedReplicas"`
	UploadURL        string `json:"uploadUrl"`
}, file *os.File, start int64) error {
	target, err := c.folderCLIResolveSameOrigin(object.UploadURL)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < folderCLITransferRetryAttempts; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, target, io.NewSectionReader(file, start, object.Size))
		if err != nil {
			return err
		}
		request.ContentLength = object.Size
		request.Header.Set("Content-Type", "application/octet-stream")
		if c.session != "" {
			request.Header.Set("Authorization", "Bearer "+c.session)
		}
		var response struct {
			SchemaVersion    int    `json:"schemaVersion"`
			OperationID      string `json:"operationId"`
			ObjectID         string `json:"objectId"`
			State            string `json:"state"`
			VerifiedReplicas int    `json:"verifiedReplicas"`
			RequiredReplicas int    `json:"requiredReplicas"`
		}
		err = c.folderCLIDoTransferJSON(request, &response)
		if err == nil {
			if response.SchemaVersion < 1 || response.OperationID != operationID || response.ObjectID != object.ID || response.RequiredReplicas != requiredReplicas {
				return errors.New("server returned inconsistent provider-object status")
			}
			return nil
		}
		if ctx.Err() != nil || !folderCLITransferRetryable(err) || attempt == folderCLITransferRetryAttempts-1 {
			return err
		}
		var status folderCLIPreparedUpload
		statusErr := c.jsonRequest(ctx, http.MethodGet, "/v1/folders/"+url.PathEscape(shareID)+"/uploads/"+url.PathEscape(operationID), nil, &status)
		if statusErr == nil {
			for _, candidate := range status.Objects {
				if candidate.Index == object.Index && candidate.ID == object.ID && candidate.VerifiedReplicas >= requiredReplicas {
					return nil
				}
			}
		} else if folderCLIRequestPermanent(statusErr) || !folderCLITransferRetryable(statusErr) {
			return statusErr
		}
		if err := folderCLIWaitRetry(ctx, folderCLIRetryDelay(err, attempt)); err != nil {
			return err
		}
	}
	return errors.New("provider object upload retry budget exhausted")
}

func (c folderCLIClient) folderCLICommitUpload(ctx context.Context, shareID string, operationID string) (folderCLIUploadCommit, error) {
	deadline := time.Now().Add(time.Minute)
	path := "/v1/folders/" + url.PathEscape(shareID) + "/uploads/" + url.PathEscape(operationID) + "/commit"
	for {
		var response folderCLIUploadCommit
		err := c.folderCLIJSONIdempotent(ctx, http.MethodPost, path, []byte("{}"), &response)
		if err == nil {
			if response.SchemaVersion < 1 || response.OperationID != operationID || response.Entry.ID == "" || (response.State != "remote_committed" && response.State != "conflicted") {
				return folderCLIUploadCommit{}, errors.New("server returned an invalid upload commit result")
			}
			return response, nil
		}
		if folderCLIErrorCode(err) != "replica_pending" || time.Now().After(deadline) {
			return folderCLIUploadCommit{}, err
		}
		if err := folderCLIWaitRetry(ctx, 2*time.Second); err != nil {
			return folderCLIUploadCommit{}, err
		}
	}
}

func (c folderCLIClient) folderCLIJSONIdempotent(ctx context.Context, method string, path string, body []byte, out any) error {
	var last error
	for attempt := 0; attempt < folderCLITransferRetryAttempts; attempt++ {
		last = c.jsonRequest(ctx, method, path, body, out)
		if last == nil || ctx.Err() != nil || !folderCLITransferRetryable(last) || attempt == folderCLITransferRetryAttempts-1 {
			return last
		}
		if err := folderCLIWaitRetry(ctx, folderCLIRetryDelay(last, attempt)); err != nil {
			return err
		}
	}
	return last
}

func (c folderCLIClient) folderCLIDoTransferJSON(request *http.Request, out any) error {
	response, err := c.folderCLITransferClient().Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, folderCLITransferResponseMax+1))
	if err != nil {
		return err
	}
	if int64(len(payload)) > folderCLITransferResponseMax {
		return errors.New("server transfer response exceeded the bounded JSON limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var envelope folderCLIError
		if json.Unmarshal(payload, &envelope) == nil && envelope.Error.Code != "" {
			return &folderCLIRequestError{Status: response.StatusCode, Code: envelope.Error.Code, Message: envelope.Error.Message, RetryAfter: response.Header.Get("Retry-After")}
		}
		return &folderCLIRequestError{Status: response.StatusCode, Message: fmt.Sprintf("server returned HTTP %d", response.StatusCode), RetryAfter: response.Header.Get("Retry-After")}
	}
	if out != nil && json.Unmarshal(payload, out) != nil {
		return errors.New("server returned invalid transfer JSON")
	}
	return nil
}

func (c folderCLIClient) folderCLITransferClient() *http.Client {
	transport := http.DefaultTransport
	if c.client != nil && c.client.Transport != nil {
		transport = c.client.Transport
	}
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func (c folderCLIClient) folderCLIResolveSameOrigin(raw string) (string, error) {
	base, err := url.Parse(c.baseURL)
	if err != nil {
		return "", err
	}
	target, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", errors.New("server returned an invalid transfer target")
	}
	target = base.ResolveReference(target)
	if target.Scheme != base.Scheme || target.Host != base.Host || target.User != nil || target.Fragment != "" {
		return "", errors.New("server returned a cross-origin transfer target")
	}
	return target.String(), nil
}

func folderCLITransferRetryable(err error) bool {
	var localErr *folderCLINonRetryableError
	if errors.As(err, &localErr) {
		return false
	}
	var requestErr *folderCLIRequestError
	if !errors.As(err, &requestErr) {
		return true
	}
	return requestErr.Status == http.StatusRequestTimeout || requestErr.Status == 425 || requestErr.Status == http.StatusTooManyRequests || requestErr.Status >= 500
}

type folderCLINonRetryableError struct{ err error }

func (e *folderCLINonRetryableError) Error() string { return e.err.Error() }
func (e *folderCLINonRetryableError) Unwrap() error { return e.err }

func folderCLIRequestPermanent(err error) bool {
	var requestErr *folderCLIRequestError
	return errors.As(err, &requestErr) && (requestErr.Status == http.StatusUnauthorized || requestErr.Status == http.StatusForbidden || requestErr.Status == http.StatusNotFound)
}

func folderCLIRetryDelay(err error, attempt int) time.Duration {
	var requestErr *folderCLIRequestError
	if errors.As(err, &requestErr) && requestErr.RetryAfter != "" {
		if seconds, parseErr := strconv.ParseFloat(requestErr.RetryAfter, 64); parseErr == nil && seconds > 0 {
			return time.Duration(seconds * float64(time.Second))
		}
		if deadline, parseErr := http.ParseTime(requestErr.RetryAfter); parseErr == nil && time.Until(deadline) > 0 {
			return time.Until(deadline)
		}
	}
	return min(4*time.Second, 500*time.Millisecond*time.Duration(1<<attempt))
}

func folderCLIWaitRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func folderCLITransferStateForError(err error) string {
	switch folderCLIErrorCode(err) {
	case "write_capability_required", "blocked_auth", "auth_required":
		return "blocked_auth"
	case "replica_pending", "local_durable_not_remote":
		return "replica_pending"
	case "revision_conflict":
		return "conflicted"
	default:
		return "recovery"
	}
}

func folderCLITransferErrorCode(err error, fallback string) string {
	if code := folderCLIErrorCode(err); code != "" {
		return code
	}
	return fallback
}

func (c folderCLIClient) folderCLIBuildRemoteTree(ctx context.Context, shareID string, descriptor folderCLIDescriptorResponse, pathParts []string, limits folderCLIPortableLimits) (folderCLILocalTree, error) {
	rootID := descriptor.Folder.RootEntryID
	var source folderCLIEntry
	var snapshotSequence int64
	if len(pathParts) == 0 {
		_, parent, sequence, err := c.listAllSnapshot(ctx, shareID, rootID)
		if err != nil {
			return folderCLILocalTree{}, err
		}
		source, snapshotSequence = parent, sequence
	} else {
		parentID := rootID
		for _, component := range pathParts {
			entries, _, sequence, err := c.listAllSnapshot(ctx, shareID, parentID)
			if err != nil {
				return folderCLILocalTree{}, err
			}
			if snapshotSequence == 0 {
				snapshotSequence = sequence
			} else if snapshotSequence != sequence {
				return folderCLILocalTree{}, errors.New("folder changed while resolving the pull source")
			}
			source = folderCLIActivePublicChild(entries, component)
			if source.ID == "" {
				return folderCLILocalTree{}, fmt.Errorf("remote path component %q was not found", component)
			}
			parentID = source.ID
		}
	}
	tree := folderCLILocalTree{}
	if source.Kind == "file" {
		record, err := folderCLIRemoteRecord(source, []string{source.Name}, limits)
		if err != nil {
			return folderCLILocalTree{}, err
		}
		tree.Files = append(tree.Files, record)
		tree.EntryCount, tree.MetadataSize = 1, int64(len([]byte(record.Path)))
		return tree, nil
	}
	if source.Kind != "directory" && source.Kind != "root" {
		return folderCLILocalTree{}, errors.New("remote pull source is not a portable file or directory")
	}
	type queueItem struct {
		Entry folderCLIEntry
		Path  []string
	}
	queue := []queueItem{{Entry: source}}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		entries, _, sequence, err := c.listAllSnapshot(ctx, shareID, current.Entry.ID)
		if err != nil {
			return folderCLILocalTree{}, err
		}
		if snapshotSequence == 0 {
			snapshotSequence = sequence
		} else if snapshotSequence != sequence {
			return folderCLILocalTree{}, errors.New("folder changed during recursive pull preflight")
		}
		activeCount := int64(0)
		for _, child := range entries {
			if !folderCLIEntryPublicActive(child) {
				continue
			}
			activeCount++
			parts := append(append([]string{}, current.Path...), child.Name)
			record, err := folderCLIRemoteRecord(child, parts, limits)
			if err != nil {
				return folderCLILocalTree{}, err
			}
			switch child.Kind {
			case "directory":
				tree.Directories = append(tree.Directories, record)
				queue = append(queue, queueItem{Entry: child, Path: parts})
			case "file":
				tree.Files = append(tree.Files, record)
			default:
				return folderCLILocalTree{}, fmt.Errorf("remote path %q has unsupported kind %q", record.Path, child.Kind)
			}
			tree.EntryCount++
			tree.MetadataSize += int64(len([]byte(record.Path)))
			if tree.EntryCount > limits.MaxActiveEntries || tree.MetadataSize > limits.MaxMetadataBytes {
				return folderCLILocalTree{}, errors.New("remote tree exceeds negotiated pull metadata limits")
			}
		}
		if activeCount > limits.MaxDirectChildren {
			return folderCLILocalTree{}, errors.New("remote directory exceeds negotiated direct-child bounds")
		}
	}
	sort.Slice(tree.Directories, func(i, j int) bool {
		return len(tree.Directories[i].Parts) < len(tree.Directories[j].Parts) ||
			(len(tree.Directories[i].Parts) == len(tree.Directories[j].Parts) && tree.Directories[i].Path < tree.Directories[j].Path)
	})
	sort.Slice(tree.Files, func(i, j int) bool { return tree.Files[i].Path < tree.Files[j].Path })
	return tree, nil
}

func folderCLIRemoteRecord(entry folderCLIEntry, parts []string, limits folderCLIPortableLimits) (folderCLILocalEntry, error) {
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		display, _, err := folderCLINormalizeComponent(part, limits)
		if err != nil || display != part {
			if err != nil {
				return folderCLILocalEntry{}, err
			}
			return folderCLILocalEntry{}, fmt.Errorf("server returned non-NFC portable name %q", part)
		}
		normalized = append(normalized, display)
	}
	if _, err := folderCLINormalizeRelativePath(strings.Join(normalized, "/"), limits); err != nil {
		return folderCLILocalEntry{}, err
	}
	path := strings.Join(normalized, "/")
	return folderCLILocalEntry{
		Path: path, Parts: normalized, ParentPath: strings.Join(normalized[:len(normalized)-1], "/"), Name: normalized[len(normalized)-1],
		Kind: entry.Kind, Mtime: entry.Mtime, Executable: entry.Executable, RemoteID: entry.ID, VersionID: entry.VersionID,
	}, nil
}

func folderCLIEntryPublicActive(entry folderCLIEntry) bool {
	return entry.State == "active" && (entry.Visibility == "" || entry.Visibility == "public")
}

func folderCLIActivePublicChild(entries []folderCLIEntry, name string) folderCLIEntry {
	active := make([]folderCLIEntry, 0, len(entries))
	for _, entry := range entries {
		if folderCLIEntryPublicActive(entry) {
			active = append(active, entry)
		}
	}
	return folderCLIActiveChild(active, name)
}

func folderCLIPreflightPullDestination(raw string, tree folderCLILocalTree) (string, error) {
	destination, err := filepath.Abs(raw)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(destination)
	switch {
	case err == nil:
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return "", errors.New("LOCAL_DIR must be a real directory, not a file or symbolic link")
		}
		entries, readErr := os.ReadDir(destination)
		if readErr != nil {
			return "", readErr
		}
		if len(entries) != 0 {
			return "", errors.New("LOCAL_DIR must be absent or empty; pull never overwrites existing entries")
		}
	case errors.Is(err, os.ErrNotExist):
		parent := filepath.Dir(destination)
		parentInfo, parentErr := os.Stat(parent)
		if parentErr != nil || !parentInfo.IsDir() {
			return "", errors.New("LOCAL_DIR parent must already exist and be a directory")
		}
	default:
		return "", err
	}
	seen := make(map[string]string)
	for _, entry := range append(append([]folderCLILocalEntry{}, tree.Directories...), tree.Files...) {
		target := filepath.Join(destination, filepath.FromSlash(entry.Path))
		relative, err := filepath.Rel(destination, target)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return "", fmt.Errorf("remote path %q escapes LOCAL_DIR", entry.Path)
		}
		key := norm.NFC.String(folderCLIFoldCaser.String(target))
		if prior := seen[key]; prior != "" && prior != entry.Path {
			return "", fmt.Errorf("remote paths %q and %q collide locally", prior, entry.Path)
		}
		seen[key] = entry.Path
		if _, statErr := os.Lstat(target); statErr == nil {
			return "", fmt.Errorf("local target %q already exists", target)
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return "", statErr
		}
	}
	return destination, nil
}

func (c folderCLIClient) folderCLIHeadRemoteFiles(ctx context.Context, shareID string, tree *folderCLILocalTree, parallel int) error {
	files := tree.Files
	jobs := make(chan int)
	errorsByIndex := make([]error, len(files))
	var workers sync.WaitGroup
	for worker := 0; worker < min(parallel, len(files)); worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				metadata, err := c.folderCLIHeadRemoteFile(ctx, shareID, files[index])
				if err == nil {
					files[index].Size = metadata.Size
					files[index].Mtime = metadata.Mtime
					files[index].ETag = metadata.ETag
					files[index].VersionID = metadata.VersionID
				}
				errorsByIndex[index] = err
			}
		}()
	}
	for index := range files {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	for index, err := range errorsByIndex {
		if err != nil {
			return fmt.Errorf("inspect %q: %w", files[index].Path, err)
		}
	}
	var total int64
	for _, file := range files {
		if file.Size < 0 || total > math.MaxInt64-file.Size {
			return errors.New("remote tree byte size exceeds supported integer bounds")
		}
		total += file.Size
	}
	tree.TotalBytes = total
	return nil
}

type folderCLIRemoteContentMetadata struct {
	Size      int64
	Mtime     int64
	ETag      string
	VersionID string
}

func (c folderCLIClient) folderCLIHeadRemoteFile(ctx context.Context, shareID string, entry folderCLILocalEntry) (folderCLIRemoteContentMetadata, error) {
	if entry.RemoteID == "" {
		return folderCLIRemoteContentMetadata{}, errors.New("remote entry identity is missing")
	}
	var last error
	for attempt := 0; attempt < folderCLITransferRetryAttempts; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodHead, strings.TrimRight(c.baseURL, "/")+"/v1/folders/"+url.PathEscape(shareID)+"/entries/"+url.PathEscape(entry.RemoteID)+"/content", nil)
		if err != nil {
			return folderCLIRemoteContentMetadata{}, err
		}
		if c.session != "" {
			request.Header.Set("Authorization", "Bearer "+c.session)
		}
		response, err := c.folderCLITransferClient().Do(request)
		if err == nil {
			if response.StatusCode == http.StatusOK {
				if response.ContentLength < 0 || response.Header.Get("ETag") == "" {
					_ = response.Body.Close()
					return folderCLIRemoteContentMetadata{}, errors.New("server omitted immutable content metadata")
				}
				mtime := entry.Mtime
				if parsed, parseErr := http.ParseTime(response.Header.Get("Last-Modified")); parseErr == nil {
					mtime = parsed.Unix()
				}
				metadata := folderCLIRemoteContentMetadata{Size: response.ContentLength, Mtime: mtime, ETag: response.Header.Get("ETag"), VersionID: response.Header.Get("X-Idoud-Folder-Version")}
				_ = response.Body.Close()
				return metadata, nil
			}
			last = folderCLIHTTPResponseError(response)
			_ = response.Body.Close()
		} else {
			last = err
		}
		if ctx.Err() != nil || !folderCLITransferRetryable(last) || attempt == folderCLITransferRetryAttempts-1 {
			return folderCLIRemoteContentMetadata{}, last
		}
		if err := folderCLIWaitRetry(ctx, folderCLIRetryDelay(last, attempt)); err != nil {
			return folderCLIRemoteContentMetadata{}, err
		}
	}
	return folderCLIRemoteContentMetadata{}, last
}

func folderCLICreatePullDirectories(destination string, directories []folderCLILocalEntry) error {
	if err := os.Mkdir(destination, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	for _, directory := range directories {
		if err := os.Mkdir(filepath.Join(destination, filepath.FromSlash(directory.Path)), 0o755); err != nil {
			return fmt.Errorf("create directory %q: %w", directory.Path, err)
		}
	}
	return nil
}

func folderCLIApplyPullDirectoryMetadata(destination string, directories []folderCLILocalEntry) error {
	ordered := append([]folderCLILocalEntry(nil), directories...)
	sort.Slice(ordered, func(i, j int) bool {
		return len(ordered[i].Parts) > len(ordered[j].Parts) ||
			(len(ordered[i].Parts) == len(ordered[j].Parts) && ordered[i].Path > ordered[j].Path)
	})
	for _, directory := range ordered {
		path := filepath.Join(destination, filepath.FromSlash(directory.Path))
		if err := os.Chmod(path, 0o755); err != nil {
			return fmt.Errorf("set directory mode %q: %w", directory.Path, err)
		}
		if directory.Mtime > 0 {
			mtime := time.Unix(directory.Mtime, 0)
			if err := os.Chtimes(path, mtime, mtime); err != nil {
				return fmt.Errorf("set directory mtime %q: %w", directory.Path, err)
			}
		}
	}
	return nil
}

func (c folderCLIClient) folderCLIDownloadFiles(ctx context.Context, shareID string, destination string, files []folderCLILocalEntry, parallel int) []folderCLITransferResult {
	results := make([]folderCLITransferResult, len(files))
	jobs := make(chan int)
	var workers sync.WaitGroup
	for worker := 0; worker < min(parallel, len(files)); worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				entry := files[index]
				result := folderCLITransferResult{Path: entry.Path, State: "pending", Bytes: entry.Size}
				if err := c.folderCLIDownloadFile(ctx, shareID, destination, entry); err != nil {
					result.State, result.Error = folderCLITransferStateForError(err), err.Error()
				} else {
					result.State, result.EntryID, result.VersionID = "remote_committed", entry.RemoteID, entry.VersionID
				}
				results[index] = result
			}
		}()
	}
	for index := range files {
		if ctx.Err() != nil {
			break
		}
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	for index := range results {
		if results[index].Path == "" {
			results[index] = folderCLITransferResult{Path: files[index].Path, State: "recovery", Bytes: files[index].Size, Error: "transfer cancelled before download"}
		}
	}
	return results
}

func (c folderCLIClient) folderCLIDownloadFile(ctx context.Context, shareID string, destination string, entry folderCLILocalEntry) error {
	if entry.RemoteID == "" {
		return errors.New("remote entry identity is missing")
	}
	target := filepath.Join(destination, filepath.FromSlash(entry.Path))
	for attempt := 0; attempt < folderCLITransferRetryAttempts; attempt++ {
		temp, err := folderCLICreatePullTemp(filepath.Dir(target))
		if err != nil {
			return &folderCLINonRetryableError{err: err}
		}
		tempPath := temp.Name()
		committed := false
		func() {
			defer func() {
				_ = temp.Close()
				if !committed {
					_ = os.Remove(tempPath)
				}
			}()
			request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.baseURL, "/")+"/v1/folders/"+url.PathEscape(shareID)+"/entries/"+url.PathEscape(entry.RemoteID)+"/content", nil)
			if requestErr != nil {
				err = requestErr
				return
			}
			request.Header.Set("If-Match", entry.ETag)
			if c.session != "" {
				request.Header.Set("Authorization", "Bearer "+c.session)
			}
			response, requestErr := c.folderCLITransferClient().Do(request)
			if requestErr != nil {
				err = requestErr
				return
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				err = folderCLIHTTPResponseError(response)
				return
			}
			if response.ContentLength != entry.Size || response.Header.Get("ETag") != entry.ETag || response.Header.Get("X-Idoud-Folder-Version") != entry.VersionID {
				err = &folderCLIRequestError{Status: http.StatusConflict, Code: "revision_conflict", Message: "remote immutable version changed after pull preflight"}
				return
			}
			written, copyErr := io.CopyBuffer(temp, response.Body, make([]byte, 128*1024))
			if copyErr != nil || written != entry.Size {
				if copyErr != nil {
					var pathErr *os.PathError
					if errors.As(copyErr, &pathErr) {
						err = &folderCLINonRetryableError{err: copyErr}
					} else {
						err = copyErr
					}
				} else {
					err = io.ErrUnexpectedEOF
				}
				return
			}
			mode := os.FileMode(0o644)
			if entry.Executable {
				mode = 0o755
			}
			if chmodErr := temp.Chmod(mode); chmodErr != nil {
				err = &folderCLINonRetryableError{err: chmodErr}
				return
			}
			if syncErr := temp.Sync(); syncErr != nil {
				err = &folderCLINonRetryableError{err: syncErr}
				return
			}
			if closeErr := temp.Close(); closeErr != nil {
				err = &folderCLINonRetryableError{err: closeErr}
				return
			}
			if entry.Mtime > 0 {
				mtime := time.Unix(entry.Mtime, 0)
				if timeErr := os.Chtimes(tempPath, mtime, mtime); timeErr != nil {
					err = &folderCLINonRetryableError{err: timeErr}
					return
				}
			}
			// A hard-link publication is an atomic no-replace operation on the
			// same filesystem. Unlike os.Rename on Unix, it cannot overwrite a
			// target created by another local process after preflight.
			if linkErr := os.Link(tempPath, target); linkErr != nil {
				if errors.Is(linkErr, os.ErrExist) {
					err = &folderCLIRequestError{Status: http.StatusConflict, Code: "revision_conflict", Message: "local target appeared after pull preflight; it was not overwritten"}
				} else {
					err = &folderCLINonRetryableError{err: linkErr}
				}
				return
			}
			committed = true
			_ = os.Remove(tempPath)
		}()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil || !folderCLITransferRetryable(err) || attempt == folderCLITransferRetryAttempts-1 {
			return err
		}
		if waitErr := folderCLIWaitRetry(ctx, folderCLIRetryDelay(err, attempt)); waitErr != nil {
			return waitErr
		}
	}
	return errors.New("download retry budget exhausted")
}

func folderCLICreatePullTemp(directory string) (*os.File, error) {
	for attempts := 0; attempts < 8; attempts++ {
		operationID, err := newFolderCLIOperationID()
		if err != nil {
			return nil, err
		}
		path := filepath.Join(directory, ".idoud-partial-"+operationID)
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	return nil, errors.New("could not reserve a local temporary file")
}

func folderCLIHTTPResponseError(response *http.Response) error {
	payload, _ := io.ReadAll(io.LimitReader(response.Body, folderCLITransferResponseMax))
	var envelope folderCLIError
	if json.Unmarshal(payload, &envelope) == nil && envelope.Error.Code != "" {
		return &folderCLIRequestError{Status: response.StatusCode, Code: envelope.Error.Code, Message: envelope.Error.Message, RetryAfter: response.Header.Get("Retry-After")}
	}
	return &folderCLIRequestError{Status: response.StatusCode, Message: fmt.Sprintf("server returned HTTP %d", response.StatusCode), RetryAfter: response.Header.Get("Retry-After")}
}

func printFolderTransferFailure(jsonOutput bool, transferType string, code string, err error) int {
	if jsonOutput {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"schema_version": folderCLIOutputSchemaVersion, "ok": false, "type": transferType,
			"error": map[string]string{"code": code, "detail": err.Error()},
		})
		return 1
	}
	fmt.Fprintf(os.Stderr, "idoud: %s: %v\n", code, err)
	return 1
}

func printFolderTransferResults(jsonOutput bool, transferType string, shareID string, tree folderCLILocalTree, results []folderCLITransferResult, replicationFactor int) bool {
	failed := false
	for _, result := range results {
		if result.Error != "" || (result.State != "remote_committed" && result.State != "") {
			failed = true
		}
	}
	if jsonOutput {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"schema_version": folderCLIOutputSchemaVersion, "ok": !failed, "type": transferType,
			"result": map[string]any{
				"share_id": shareID, "entries": tree.EntryCount, "files": len(tree.Files), "bytes": tree.TotalBytes,
				"replication_factor": replicationFactor, "items": results,
			},
		})
		return failed
	}
	for _, result := range results {
		if result.Error == "" {
			fmt.Fprintf(os.Stdout, "%-18s  %12d  %s\n", result.State, result.Bytes, result.Path)
		} else {
			fmt.Fprintf(os.Stderr, "%-18s  %12d  %s: %s\n", result.State, result.Bytes, result.Path, result.Error)
		}
	}
	if !failed {
		fmt.Fprintf(os.Stdout, "%d files, %d bytes complete\n", len(tree.Files), tree.TotalBytes)
	}
	return failed
}
