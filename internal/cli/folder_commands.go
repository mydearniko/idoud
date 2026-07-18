package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"golang.org/x/term"
)

const folderCLIOutputSchemaVersion = 1

type folderCLIError struct {
	SchemaVersion int `json:"schemaVersion"`
	Error         struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type folderCLIDescriptorResponse struct {
	SchemaVersion int `json:"schemaVersion"`
	Folder        struct {
		ShareID         string `json:"shareId"`
		Name            string `json:"name"`
		RootEntryID     string `json:"rootEntryId"`
		Sequence        int64  `json:"sequence"`
		ReadPolicy      string `json:"readPolicy"`
		State           string `json:"state"`
		ShareGeneration int64  `json:"shareGeneration"`
		WriteGeneration int64  `json:"writeGeneration"`
	} `json:"folder"`
}

type folderCLIEntry struct {
	ID               string `json:"id"`
	ParentID         string `json:"parentId"`
	Name             string `json:"name"`
	Kind             string `json:"kind"`
	VersionID        string `json:"versionId"`
	EntryRevision    int64  `json:"entryRevision"`
	ChildSetRevision int64  `json:"childSetRevision"`
	State            string `json:"state"`
	Mtime            int64  `json:"mtime"`
}

type folderCLIEntriesResponse struct {
	SchemaVersion int              `json:"schemaVersion"`
	Sequence      int64            `json:"sequence"`
	Parent        folderCLIEntry   `json:"parent"`
	Entries       []folderCLIEntry `json:"entries"`
	NextCursor    string           `json:"nextCursor"`
}

type folderCLIVersion struct {
	ID               string `json:"id"`
	BaseVersionID    string `json:"baseVersionId"`
	LogicalSize      int64  `json:"logicalSize"`
	CRC32            uint32 `json:"crc32"`
	ContentHash      string `json:"contentHash"`
	Mtime            int64  `json:"mtime"`
	Executable       bool   `json:"executable"`
	CreationSequence int64  `json:"creationSequence"`
	State            string `json:"state"`
	CreatedAt        int64  `json:"createdAt"`
}

type folderCLIHistoryResponse struct {
	SchemaVersion int                `json:"schemaVersion"`
	Sequence      int64              `json:"sequence"`
	Entry         folderCLIEntry     `json:"entry"`
	Versions      []folderCLIVersion `json:"versions"`
	NextCursor    string             `json:"nextCursor"`
}

type folderCLIPendingWrite struct {
	OperationID    string          `json:"operationId"`
	TargetEntryID  string          `json:"targetEntryId"`
	TargetParentID string          `json:"targetParentId"`
	BaseVersionID  string          `json:"baseVersionId"`
	State          string          `json:"state"`
	Recovery       json.RawMessage `json:"recovery"`
	CreatedAt      int64           `json:"createdAt"`
	UpdatedAt      int64           `json:"updatedAt"`
}

type folderCLIInventoryResponse struct {
	SchemaVersion int                     `json:"schemaVersion"`
	Sequence      int64                   `json:"sequence"`
	Entries       []folderCLIEntry        `json:"entries"`
	PendingWrites []folderCLIPendingWrite `json:"pendingWrites"`
	NextCursor    string                  `json:"nextCursor"`
}

type folderCLIClient struct {
	baseURL string
	session string
	client  *http.Client
}

func runFolderCommand(args []string) int {
	if len(args) == 0 {
		printFolderUsage(os.Stdout)
		return 0
	}
	switch strings.ToLower(strings.TrimSpace(args[0])) {
	case "create":
		return runFolderCreate(args[1:])
	case "ls":
		return runFolderList(args[1:])
	case "status":
		return runFolderStatus(args[1:])
	case "auth":
		return runFolderAuth(args[1:])
	case "history":
		return runFolderHistory(args[1:])
	case "trash":
		return runFolderTrash(args[1:])
	case "restore":
		return runFolderRestore(args[1:])
	case "recovery":
		return runFolderRecovery(args[1:])
	case "rotate-share-id":
		return runFolderRotateShareID(args[1:])
	case "rotate-write-key":
		return runFolderRotateWriteKey(args[1:])
	case "push", "pull", "flush":
		fmt.Fprintf(os.Stderr, "idoud: folder %s is gated until its server/client protocol phase is enabled\n", args[0])
		return 1
	case "help", "-h", "--help":
		printFolderUsage(os.Stdout)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "idoud: unknown folder command %q\n", args[0])
		printFolderUsage(os.Stderr)
		return 2
	}
}

func runFolderCreate(args []string) int {
	fs := flag.NewFlagSet("idoud folder create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	server := fs.String("server", defaultServerURL, "idoud server origin")
	jsonOutput := fs.Bool("json", false, "schema-versioned JSON without secrets")
	writeKeyFile := fs.String("write-key-file", "", "write the one-time key to a new 0600 file")
	sessionFile := fs.String("session-file", "", "exchange and write a derived writer session to a new 0600 file")
	readPasswordStdin := fs.Bool("read-password-stdin", false, "read optional folder password from stdin")
	if err := fs.Parse(normalizeInterspersedArgs(fs, args)); err != nil {
		fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
		return 2
	}
	if fs.NArg() > 1 {
		fmt.Fprintln(os.Stderr, "idoud: folder create accepts at most one name")
		return 2
	}
	if *jsonOutput && strings.TrimSpace(*writeKeyFile) == "" && strings.TrimSpace(*sessionFile) == "" {
		fmt.Fprintln(os.Stderr, "idoud: --json requires --write-key-file or --session-file so the one-time write capability is not lost")
		return 2
	}
	if strings.TrimSpace(*sessionFile) != "" && strings.TrimSpace(*writeKeyFile) == "" {
		fmt.Fprintln(os.Stderr, "idoud: --session-file also requires --write-key-file so an exchange failure cannot lose the one-time write capability")
		return 2
	}
	if strings.TrimSpace(*sessionFile) != "" && filepath.Clean(*writeKeyFile) == filepath.Clean(*sessionFile) {
		fmt.Fprintln(os.Stderr, "idoud: --write-key-file and --session-file must be different files")
		return 2
	}
	var keyDestination *reservedSecretFile
	if strings.TrimSpace(*writeKeyFile) != "" {
		var reserveErr error
		keyDestination, reserveErr = reserveSecretFileExclusive(*writeKeyFile)
		if reserveErr != nil {
			printFolderCommandError(*jsonOutput, "credential_file_failed", reserveErr)
			return 1
		}
		defer keyDestination.Abort()
	}
	var sessionDestination *reservedSecretFile
	if strings.TrimSpace(*sessionFile) != "" {
		var reserveErr error
		sessionDestination, reserveErr = reserveSecretFileExclusive(*sessionFile)
		if reserveErr != nil {
			printFolderCommandError(*jsonOutput, "credential_file_failed", reserveErr)
			return 1
		}
		defer sessionDestination.Abort()
	}
	name := "Folder"
	if fs.NArg() == 1 && strings.TrimSpace(fs.Arg(0)) != "" {
		name = fs.Arg(0)
	}
	readPassword := ""
	if *readPasswordStdin {
		secret, err := io.ReadAll(io.LimitReader(os.Stdin, 4097))
		if err != nil {
			fmt.Fprintf(os.Stderr, "idoud: read password: %v\n", err)
			return 1
		}
		readPassword = strings.TrimRight(string(secret), "\r\n")
	}
	base, err := normalizeFolderServer(*server)
	if err != nil {
		fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
		return 2
	}
	client := folderCLIClient{baseURL: base, client: &http.Client{Timeout: 30 * time.Second}}
	requestBody, _ := json.Marshal(map[string]string{"name": name, "readPassword": readPassword})
	var response struct {
		SchemaVersion int `json:"schemaVersion"`
		Folder        struct {
			ShareID         string `json:"shareId"`
			Name            string `json:"name"`
			RootEntryID     string `json:"rootEntryId"`
			Sequence        int64  `json:"sequence"`
			WriteGeneration int64  `json:"writeGeneration"`
		} `json:"folder"`
		PublicURL string `json:"publicUrl"`
		WriteKey  string `json:"writeKey"`
	}
	if err := client.jsonRequest(context.Background(), http.MethodPost, "/v1/folders", requestBody, &response); err != nil {
		printFolderCommandError(*jsonOutput, "create_failed", err)
		return 1
	}
	if response.WriteKey == "" {
		printFolderCommandError(*jsonOutput, "protocol_error", errors.New("server omitted one-time write key"))
		return 1
	}
	if keyDestination != nil {
		if err := keyDestination.Commit(response.WriteKey); err != nil {
			printFolderCommandError(*jsonOutput, "credential_file_failed", err)
			return 1
		}
	}
	if sessionDestination != nil {
		session, err := client.exchangeWriteKey(context.Background(), response.Folder.ShareID, response.WriteKey, "idoud CLI")
		if err != nil {
			printFolderCommandError(*jsonOutput, "session_exchange_failed", err)
			return 1
		}
		if err := sessionDestination.Commit(session); err != nil {
			printFolderCommandError(*jsonOutput, "credential_file_failed", err)
			return 1
		}
	}
	if *jsonOutput {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"schema_version": folderCLIOutputSchemaVersion,
			"ok":             true,
			"type":           "folder_create",
			"result": map[string]any{
				"public_url":          response.PublicURL,
				"share_id":            response.Folder.ShareID,
				"name":                response.Folder.Name,
				"root_entry_id":       response.Folder.RootEntryID,
				"sequence":            response.Folder.Sequence,
				"write_key_in_output": false,
				"write_key_file":      strings.TrimSpace(*writeKeyFile),
				"session_file":        strings.TrimSpace(*sessionFile),
			},
		})
		return 0
	}
	fmt.Fprintln(os.Stdout, response.PublicURL)
	if *writeKeyFile != "" {
		fmt.Fprintf(os.Stderr, "write key saved to %s (mode 0600)\n", *writeKeyFile)
	} else if *sessionFile == "" {
		fmt.Fprintf(os.Stderr, "write key (shown once): %s\n", response.WriteKey)
		fmt.Fprintln(os.Stderr, "store it safely; it cannot be recovered from the public link")
	}
	if *sessionFile != "" {
		fmt.Fprintf(os.Stderr, "writer session saved to %s (mode 0600); recovery write key saved to %s\n", *sessionFile, *writeKeyFile)
	}
	return 0
}

func runFolderList(args []string) int {
	fs := flag.NewFlagSet("idoud folder ls", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	server := fs.String("server", defaultServerURL, "idoud server origin")
	jsonOutput := fs.Bool("json", false, "schema-versioned JSON")
	sessionFile := fs.String("session-file", "", "read a session from a 0600 file")
	if err := fs.Parse(normalizeInterspersedArgs(fs, args)); err != nil {
		fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
		return 2
	}
	if fs.NArg() < 1 || fs.NArg() > 2 {
		fmt.Fprintln(os.Stderr, "idoud: usage: idoud folder ls FOLDER [PATH]")
		return 2
	}
	base, shareID, err := parseFolderReference(*server, fs.Arg(0))
	if err != nil {
		printFolderCommandError(*jsonOutput, "invalid_folder", err)
		return 2
	}
	session, err := readOptionalSecretFile(*sessionFile)
	if err != nil {
		printFolderCommandError(*jsonOutput, "credential_file_failed", err)
		return 1
	}
	client := folderCLIClient{baseURL: base, session: session, client: &http.Client{Timeout: 30 * time.Second}}
	descriptor, err := client.descriptor(context.Background(), shareID)
	if err != nil {
		printFolderCommandError(*jsonOutput, "descriptor_failed", err)
		return 1
	}
	parentID := descriptor.Folder.RootEntryID
	pathValue := ""
	if fs.NArg() == 2 {
		pathValue = strings.Trim(strings.ReplaceAll(fs.Arg(1), "\\", "/"), "/")
	}
	if pathValue != "" {
		for _, component := range strings.Split(pathValue, "/") {
			entries, err := client.listAll(context.Background(), shareID, parentID)
			if err != nil {
				printFolderCommandError(*jsonOutput, "list_failed", err)
				return 1
			}
			found := false
			for _, entry := range entries {
				if strings.EqualFold(entry.Name, component) && (entry.Kind == "directory" || entry.Kind == "root") {
					parentID = entry.ID
					found = true
					break
				}
			}
			if !found {
				printFolderCommandError(*jsonOutput, "not_found", fmt.Errorf("folder path component %q not found", component))
				return 1
			}
		}
	}
	entries, err := client.listAll(context.Background(), shareID, parentID)
	if err != nil {
		printFolderCommandError(*jsonOutput, "list_failed", err)
		return 1
	}
	if *jsonOutput {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"schema_version": folderCLIOutputSchemaVersion,
			"ok":             true,
			"type":           "folder_list",
			"result":         map[string]any{"share_id": shareID, "path": pathValue, "entries": entries},
		})
		return 0
	}
	for _, entry := range entries {
		kind := "file"
		if entry.Kind == "directory" {
			kind = "dir "
		}
		fmt.Fprintf(os.Stdout, "%s  %s\n", kind, entry.Name)
	}
	return 0
}

func runFolderStatus(args []string) int {
	fs := flag.NewFlagSet("idoud folder status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	server := fs.String("server", defaultServerURL, "idoud server origin")
	jsonOutput := fs.Bool("json", false, "schema-versioned JSON")
	sessionFile := fs.String("session-file", "", "read a session from a 0600 file")
	if err := fs.Parse(normalizeInterspersedArgs(fs, args)); err != nil {
		fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "idoud: usage: idoud folder status FOLDER")
		return 2
	}
	base, shareID, err := parseFolderReference(*server, fs.Arg(0))
	if err != nil {
		printFolderCommandError(*jsonOutput, "invalid_folder", err)
		return 2
	}
	session, err := readOptionalSecretFile(*sessionFile)
	if err != nil {
		printFolderCommandError(*jsonOutput, "credential_file_failed", err)
		return 1
	}
	client := folderCLIClient{baseURL: base, session: session, client: &http.Client{Timeout: 30 * time.Second}}
	descriptor, err := client.descriptor(context.Background(), shareID)
	if err != nil {
		printFolderCommandError(*jsonOutput, "descriptor_failed", err)
		return 1
	}
	if *jsonOutput {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"schema_version": folderCLIOutputSchemaVersion, "ok": true, "type": "folder_status", "result": descriptor.Folder})
		return 0
	}
	fmt.Fprintf(os.Stdout, "%s\nstate: %s\nsequence: %d\nread policy: %s\n", descriptor.Folder.Name, descriptor.Folder.State, descriptor.Folder.Sequence, descriptor.Folder.ReadPolicy)
	return 0
}

func runFolderAuth(args []string) int {
	fs := flag.NewFlagSet("idoud folder auth", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	server := fs.String("server", defaultServerURL, "idoud server origin")
	writeMode := fs.Bool("write", false, "exchange the writer key")
	secretStdin := fs.Bool("secret-stdin", false, "read the password/key from stdin")
	sessionFile := fs.String("session-file", "", "write the derived session to a new 0600 file")
	if err := fs.Parse(normalizeInterspersedArgs(fs, args)); err != nil {
		fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
		return 2
	}
	if fs.NArg() != 1 || strings.TrimSpace(*sessionFile) == "" {
		fmt.Fprintln(os.Stderr, "idoud: usage: idoud folder auth [--write] [--secret-stdin] --session-file FILE FOLDER")
		return 2
	}
	base, shareID, err := parseFolderReference(*server, fs.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
		return 2
	}
	secret, err := readInteractiveFolderSecret(*secretStdin, *writeMode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
		return 1
	}
	client := folderCLIClient{baseURL: base, client: &http.Client{Timeout: 30 * time.Second}}
	var token string
	if *writeMode {
		token, err = client.exchangeWriteKey(context.Background(), shareID, secret, "idoud CLI")
	} else {
		requestBody, _ := json.Marshal(map[string]string{"password": secret, "deviceLabel": "idoud CLI"})
		var response struct {
			SessionToken string `json:"sessionToken"`
		}
		err = client.jsonRequest(context.Background(), http.MethodPost, "/v1/folders/"+url.PathEscape(shareID)+"/auth/read", requestBody, &response)
		token = response.SessionToken
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "idoud: folder authorization failed: %v\n", err)
		return 1
	}
	if err := writeSecretFileExclusive(*sessionFile, token); err != nil {
		fmt.Fprintf(os.Stderr, "idoud: save session: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stdout, "session saved to %s (mode 0600)\n", *sessionFile)
	return 0
}

func runFolderHistory(args []string) int {
	fs := flag.NewFlagSet("idoud folder history", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	server := fs.String("server", defaultServerURL, "idoud server origin")
	jsonOutput := fs.Bool("json", false, "schema-versioned JSON")
	sessionFile := fs.String("session-file", "", "read a writer session from a 0600 file")
	if err := fs.Parse(normalizeInterspersedArgs(fs, args)); err != nil {
		fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
		return 2
	}
	if fs.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "idoud: usage: idoud folder history FOLDER PATH --session-file FILE")
		return 2
	}
	client, shareID, err := newFolderCLIClient(*server, fs.Arg(0), *sessionFile)
	if err != nil {
		printFolderCommandError(*jsonOutput, "invalid_folder", err)
		return 2
	}
	_, entry, _, _, err := client.resolvePath(context.Background(), shareID, fs.Arg(1))
	if err != nil {
		printFolderCommandError(*jsonOutput, "not_found", err)
		return 1
	}
	if entry.Kind != "file" {
		printFolderCommandError(*jsonOutput, "invalid_entry", errors.New("history is available only for files"))
		return 1
	}
	versions, sequence, err := client.historyAll(context.Background(), shareID, entry.ID)
	if err != nil {
		printFolderCommandError(*jsonOutput, "history_failed", err)
		return 1
	}
	if *jsonOutput {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"schema_version": folderCLIOutputSchemaVersion, "ok": true, "type": "folder_history",
			"result": map[string]any{"share_id": shareID, "path": fs.Arg(1), "entry": entry, "sequence": sequence, "versions": versions},
		})
		return 0
	}
	for _, version := range versions {
		created := time.Unix(version.CreatedAt, 0).UTC().Format(time.RFC3339)
		fmt.Fprintf(os.Stdout, "%s  %12d  %-16s  %s\n", created, version.LogicalSize, version.State, version.ID)
	}
	return 0
}

func runFolderTrash(args []string) int {
	if len(args) > 0 && strings.EqualFold(strings.TrimSpace(args[0]), "list") {
		return runFolderInventoryList("trash", args[1:])
	}
	fs := flag.NewFlagSet("idoud folder trash", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	server := fs.String("server", defaultServerURL, "idoud server origin")
	jsonOutput := fs.Bool("json", false, "schema-versioned JSON")
	sessionFile := fs.String("session-file", "", "read a writer session from a 0600 file")
	if err := fs.Parse(normalizeInterspersedArgs(fs, args)); err != nil {
		fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
		return 2
	}
	if fs.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "idoud: usage: idoud folder trash FOLDER PATH --session-file FILE")
		return 2
	}
	client, shareID, err := newFolderCLIClient(*server, fs.Arg(0), *sessionFile)
	if err != nil {
		printFolderCommandError(*jsonOutput, "invalid_folder", err)
		return 2
	}
	_, entry, parent, sequence, err := client.resolvePath(context.Background(), shareID, fs.Arg(1))
	if err != nil {
		printFolderCommandError(*jsonOutput, "not_found", err)
		return 1
	}
	operationID, err := newFolderCLIOperationID()
	if err != nil {
		printFolderCommandError(*jsonOutput, "operation_identity_failed", err)
		return 1
	}
	result, err := client.mutate(context.Background(), shareID, map[string]any{
		"operationId": operationID, "type": "trash", "entryId": entry.ID,
		"expectedFolderSequence": sequence, "expectedEntryRevision": entry.EntryRevision,
		"expectedParentRevision": parent.EntryRevision, "expectedChildSetRevision": entry.ChildSetRevision,
	})
	if err != nil {
		printFolderCommandError(*jsonOutput, "trash_failed", err)
		return 1
	}
	return printFolderMutationResult(*jsonOutput, "folder_trash", shareID, result)
}

func runFolderRestore(args []string) int {
	fs := flag.NewFlagSet("idoud folder restore", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	server := fs.String("server", defaultServerURL, "idoud server origin")
	jsonOutput := fs.Bool("json", false, "schema-versioned JSON")
	sessionFile := fs.String("session-file", "", "read a writer session from a 0600 file")
	if err := fs.Parse(normalizeInterspersedArgs(fs, args)); err != nil {
		fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
		return 2
	}
	if fs.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "idoud: usage: idoud folder restore FOLDER ENTRY_ID --session-file FILE")
		return 2
	}
	client, shareID, err := newFolderCLIClient(*server, fs.Arg(0), *sessionFile)
	if err != nil {
		printFolderCommandError(*jsonOutput, "invalid_folder", err)
		return 2
	}
	entryID := strings.TrimSpace(fs.Arg(1))
	var entry folderCLIEntry
	found := false
	for _, scope := range []string{"trash", "recovery"} {
		entries, _, _, inventoryErr := client.inventoryAll(context.Background(), shareID, scope)
		if inventoryErr != nil {
			printFolderCommandError(*jsonOutput, "inventory_failed", inventoryErr)
			return 1
		}
		for _, candidate := range entries {
			if candidate.ID == entryID {
				entry, found = candidate, true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		printFolderCommandError(*jsonOutput, "not_found", errors.New("retained entry was not found in trash or recovery"))
		return 1
	}
	_, parent, sequence, err := client.listAllSnapshot(context.Background(), shareID, entry.ParentID)
	if err != nil {
		printFolderCommandError(*jsonOutput, "parent_unavailable", err)
		return 1
	}
	operationID, err := newFolderCLIOperationID()
	if err != nil {
		printFolderCommandError(*jsonOutput, "operation_identity_failed", err)
		return 1
	}
	result, err := client.mutate(context.Background(), shareID, map[string]any{
		"operationId": operationID, "type": "restore", "entryId": entry.ID,
		"expectedFolderSequence": sequence, "expectedEntryRevision": entry.EntryRevision,
		"expectedParentRevision": parent.EntryRevision, "expectedChildSetRevision": entry.ChildSetRevision,
	})
	if err != nil {
		printFolderCommandError(*jsonOutput, "restore_failed", err)
		return 1
	}
	return printFolderMutationResult(*jsonOutput, "folder_restore", shareID, result)
}

func runFolderRecovery(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "idoud: usage: idoud folder recovery list|restore|export ...")
		return 2
	}
	switch strings.ToLower(strings.TrimSpace(args[0])) {
	case "list":
		return runFolderInventoryList("recovery", args[1:])
	case "restore":
		return runFolderRestore(args[1:])
	case "export":
		fmt.Fprintln(os.Stderr, "idoud: folder recovery export is gated until the durable local journal phase is enabled")
		return 1
	default:
		fmt.Fprintf(os.Stderr, "idoud: unknown folder recovery command %q\n", args[0])
		return 2
	}
}

func runFolderInventoryList(scope string, args []string) int {
	fs := flag.NewFlagSet("idoud folder "+scope+" list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	server := fs.String("server", defaultServerURL, "idoud server origin")
	jsonOutput := fs.Bool("json", false, "schema-versioned JSON")
	sessionFile := fs.String("session-file", "", "read a writer session from a 0600 file")
	if err := fs.Parse(normalizeInterspersedArgs(fs, args)); err != nil {
		fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "idoud: usage: idoud folder %s list FOLDER --session-file FILE\n", scope)
		return 2
	}
	client, shareID, err := newFolderCLIClient(*server, fs.Arg(0), *sessionFile)
	if err != nil {
		printFolderCommandError(*jsonOutput, "invalid_folder", err)
		return 2
	}
	entries, pending, sequence, err := client.inventoryAll(context.Background(), shareID, scope)
	if err != nil {
		printFolderCommandError(*jsonOutput, "inventory_failed", err)
		return 1
	}
	if *jsonOutput {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"schema_version": folderCLIOutputSchemaVersion, "ok": true, "type": "folder_" + scope,
			"result": map[string]any{"share_id": shareID, "sequence": sequence, "entries": entries, "pending_writes": pending},
		})
		return 0
	}
	for _, entry := range entries {
		fmt.Fprintf(os.Stdout, "%-12s  %s  %s\n", entry.State, entry.ID, entry.Name)
	}
	for _, write := range pending {
		fmt.Fprintf(os.Stdout, "%-12s  %s  pending write\n", write.State, write.OperationID)
	}
	return 0
}

func runFolderRotateShareID(args []string) int {
	fs := flag.NewFlagSet("idoud folder rotate-share-id", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	server := fs.String("server", defaultServerURL, "idoud server origin")
	jsonOutput := fs.Bool("json", false, "schema-versioned JSON")
	sessionFile := fs.String("session-file", "", "read a writer session from a 0600 file")
	if err := fs.Parse(normalizeInterspersedArgs(fs, args)); err != nil {
		fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "idoud: usage: idoud folder rotate-share-id FOLDER --session-file FILE")
		return 2
	}
	client, shareID, err := newFolderCLIClient(*server, fs.Arg(0), *sessionFile)
	if err != nil {
		printFolderCommandError(*jsonOutput, "invalid_folder", err)
		return 2
	}
	descriptor, err := client.descriptor(context.Background(), shareID)
	if err != nil {
		printFolderCommandError(*jsonOutput, "descriptor_failed", err)
		return 1
	}
	var response struct {
		ShareID         string `json:"shareId"`
		PublicURL       string `json:"publicUrl"`
		ShareGeneration int64  `json:"shareGeneration"`
	}
	if err := client.jsonRequest(context.Background(), http.MethodPost, "/v1/folders/"+url.PathEscape(shareID)+"/rotate-share-id", mustJSON(map[string]any{"expectedGeneration": descriptor.Folder.ShareGeneration}), &response); err != nil {
		printFolderCommandError(*jsonOutput, "rotate_share_failed", err)
		return 1
	}
	if *jsonOutput {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"schema_version": folderCLIOutputSchemaVersion, "ok": true, "type": "folder_rotate_share_id",
			"result": map[string]any{"share_id": response.ShareID, "public_url": response.PublicURL, "share_generation": response.ShareGeneration, "prior_session_invalidated": true},
		})
		return 0
	}
	fmt.Fprintf(os.Stdout, "%s\n", response.PublicURL)
	fmt.Fprintln(os.Stderr, "all sessions derived from the previous public link are now invalid")
	return 0
}

func runFolderRotateWriteKey(args []string) int {
	fs := flag.NewFlagSet("idoud folder rotate-write-key", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	server := fs.String("server", defaultServerURL, "idoud server origin")
	jsonOutput := fs.Bool("json", false, "schema-versioned JSON without secrets")
	sessionFile := fs.String("session-file", "", "read the current writer session from a 0600 file")
	writeKeyFile := fs.String("write-key-file", "", "write the new one-time key to a new 0600 file")
	if err := fs.Parse(normalizeInterspersedArgs(fs, args)); err != nil {
		fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
		return 2
	}
	if fs.NArg() != 1 || strings.TrimSpace(*writeKeyFile) == "" {
		fmt.Fprintln(os.Stderr, "idoud: usage: idoud folder rotate-write-key FOLDER --session-file FILE --write-key-file NEW_FILE")
		return 2
	}
	if strings.TrimSpace(*sessionFile) != "" && filepath.Clean(*sessionFile) == filepath.Clean(*writeKeyFile) {
		fmt.Fprintln(os.Stderr, "idoud: the new write-key file must differ from the current session file")
		return 2
	}
	destination, err := reserveSecretFileExclusive(*writeKeyFile)
	if err != nil {
		printFolderCommandError(*jsonOutput, "credential_file_failed", err)
		return 1
	}
	defer destination.Abort()
	client, shareID, err := newFolderCLIClient(*server, fs.Arg(0), *sessionFile)
	if err != nil {
		printFolderCommandError(*jsonOutput, "invalid_folder", err)
		return 2
	}
	descriptor, err := client.descriptor(context.Background(), shareID)
	if err != nil {
		printFolderCommandError(*jsonOutput, "descriptor_failed", err)
		return 1
	}
	var response struct {
		WriteKey        string `json:"writeKey"`
		WriteGeneration int64  `json:"writeGeneration"`
	}
	if err := client.jsonRequest(context.Background(), http.MethodPost, "/v1/folders/"+url.PathEscape(shareID)+"/rotate-write-key", mustJSON(map[string]any{"expectedGeneration": descriptor.Folder.WriteGeneration}), &response); err != nil {
		printFolderCommandError(*jsonOutput, "rotate_write_failed", err)
		return 1
	}
	if response.WriteKey == "" {
		printFolderCommandError(*jsonOutput, "protocol_error", errors.New("server omitted the rotated write key"))
		return 1
	}
	if err := destination.Commit(response.WriteKey); err != nil {
		printFolderCommandError(*jsonOutput, "credential_file_failed", err)
		return 1
	}
	if *jsonOutput {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"schema_version": folderCLIOutputSchemaVersion, "ok": true, "type": "folder_rotate_write_key",
			"result": map[string]any{"write_generation": response.WriteGeneration, "write_key_file": *writeKeyFile, "write_key_in_output": false, "prior_writer_sessions_invalidated": true},
		})
		return 0
	}
	fmt.Fprintf(os.Stdout, "new write key saved to %s (mode 0600)\n", *writeKeyFile)
	fmt.Fprintln(os.Stderr, "all previous writer sessions are now invalid")
	return 0
}

func runMountCommand(args []string) int {
	if len(args) > 0 {
		switch strings.ToLower(strings.TrimSpace(args[0])) {
		case "list":
			fmt.Fprintln(os.Stdout, "no active idoud mounts")
			return 0
		case "status", "flush", "unmount":
			fmt.Fprintln(os.Stderr, "idoud: no matching active mount supervisor")
			return 1
		}
	}
	writeRequested := false
	backgroundRequested := false
	positionals := make([]string, 0, 2)
	for _, arg := range args {
		switch arg {
		case "--write":
			writeRequested = true
		case "--background":
			backgroundRequested = true
		default:
			positionals = append(positionals, arg)
		}
	}
	if len(positionals) != 2 {
		fmt.Fprintln(os.Stderr, "idoud: usage: idoud mount FOLDER MOUNTPOINT [--write] [--background]")
		return 2
	}
	if command, link, missing := missingMountBridge(); missing {
		fmt.Fprintf(os.Stderr, "idoud: bridge_missing: native mount bridge is not installed\ninstall: %s\n%s\n", command, link)
		return 1
	}
	mode := "read-only foreground"
	if writeRequested {
		mode = "writable foreground"
	}
	if backgroundRequested {
		mode = strings.Replace(mode, "foreground", "background", 1)
	}
	fmt.Fprintf(os.Stderr, "idoud: protocol_upgrade_required: native mount core is gated in this build (%s requested)\n", mode)
	return 1
}

func missingMountBridge() (command string, link string, missing bool) {
	switch runtime.GOOS {
	case "linux":
		if _, err := os.Stat("/dev/fuse"); err == nil {
			return "", "", false
		}
		return "sudo apt install fuse3", "https://github.com/hanwen/go-fuse", true
	case "darwin":
		if _, err := os.Stat("/Library/Filesystems/macfuse.fs"); err == nil {
			return "", "", false
		}
		return "brew install --cask macfuse", "https://macfuse.github.io/", true
	case "windows":
		if _, err := os.Stat(`C:\\Program Files (x86)\\WinFsp\\bin\\winfsp-x64.dll`); err == nil {
			return "", "", false
		}
		return "winget install WinFsp.WinFsp", "https://winfsp.dev/", true
	default:
		return "install a supported FUSE bridge", "https://idoud.cc/", true
	}
}

func (c folderCLIClient) descriptor(ctx context.Context, shareID string) (folderCLIDescriptorResponse, error) {
	var response folderCLIDescriptorResponse
	err := c.jsonRequest(ctx, http.MethodGet, "/v1/folders/"+url.PathEscape(shareID), nil, &response)
	return response, err
}

func (c folderCLIClient) listAll(ctx context.Context, shareID string, parentID string) ([]folderCLIEntry, error) {
	entries, _, _, err := c.listAllSnapshot(ctx, shareID, parentID)
	return entries, err
}

func (c folderCLIClient) listAllSnapshot(ctx context.Context, shareID string, parentID string) ([]folderCLIEntry, folderCLIEntry, int64, error) {
	entries := make([]folderCLIEntry, 0, 128)
	cursor := ""
	var parent folderCLIEntry
	var sequence int64
	for {
		query := url.Values{}
		query.Set("parent", parentID)
		query.Set("limit", "1000")
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		var page folderCLIEntriesResponse
		if err := c.jsonRequest(ctx, http.MethodGet, "/v1/folders/"+url.PathEscape(shareID)+"/entries?"+query.Encode(), nil, &page); err != nil {
			return nil, folderCLIEntry{}, 0, err
		}
		if sequence == 0 {
			sequence = page.Sequence
			parent = page.Parent
		} else if page.Sequence != sequence {
			return nil, folderCLIEntry{}, 0, errors.New("folder listing snapshot changed between pages")
		}
		entries = append(entries, page.Entries...)
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Kind != entries[j].Kind {
			return entries[i].Kind == "directory"
		}
		return strings.ToLower(entries[i].Name) < strings.ToLower(entries[j].Name)
	})
	return entries, parent, sequence, nil
}

func (c folderCLIClient) resolvePath(ctx context.Context, shareID string, pathValue string) (folderCLIDescriptorResponse, folderCLIEntry, folderCLIEntry, int64, error) {
	descriptor, err := c.descriptor(ctx, shareID)
	if err != nil {
		return folderCLIDescriptorResponse{}, folderCLIEntry{}, folderCLIEntry{}, 0, err
	}
	pathValue = strings.Trim(strings.ReplaceAll(pathValue, "\\", "/"), "/")
	if pathValue == "" {
		return descriptor, folderCLIEntry{}, folderCLIEntry{}, 0, errors.New("a non-root folder path is required")
	}
	parentID := descriptor.Folder.RootEntryID
	components := strings.Split(pathValue, "/")
	for index, component := range components {
		entries, parent, sequence, err := c.listAllSnapshot(ctx, shareID, parentID)
		if err != nil {
			return descriptor, folderCLIEntry{}, folderCLIEntry{}, 0, err
		}
		var found folderCLIEntry
		matched := false
		for _, entry := range entries {
			if strings.EqualFold(entry.Name, component) {
				found, matched = entry, true
				break
			}
		}
		if !matched {
			return descriptor, folderCLIEntry{}, folderCLIEntry{}, 0, fmt.Errorf("folder path component %q not found", component)
		}
		if index == len(components)-1 {
			return descriptor, found, parent, sequence, nil
		}
		if found.Kind != "directory" && found.Kind != "root" {
			return descriptor, folderCLIEntry{}, folderCLIEntry{}, 0, fmt.Errorf("folder path component %q is not a directory", component)
		}
		parentID = found.ID
	}
	return descriptor, folderCLIEntry{}, folderCLIEntry{}, 0, errors.New("folder path could not be resolved")
}

func (c folderCLIClient) historyAll(ctx context.Context, shareID string, entryID string) ([]folderCLIVersion, int64, error) {
	versions := make([]folderCLIVersion, 0, 16)
	cursor := ""
	var sequence int64
	for {
		query := url.Values{}
		query.Set("entry", entryID)
		query.Set("limit", "1000")
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		var page folderCLIHistoryResponse
		if err := c.jsonRequest(ctx, http.MethodGet, "/v1/folders/"+url.PathEscape(shareID)+"/history?"+query.Encode(), nil, &page); err != nil {
			return nil, 0, err
		}
		if sequence == 0 {
			sequence = page.Sequence
		} else if page.Sequence != sequence {
			return nil, 0, errors.New("folder history snapshot changed between pages")
		}
		versions = append(versions, page.Versions...)
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	return versions, sequence, nil
}

func (c folderCLIClient) inventoryAll(ctx context.Context, shareID string, scope string) ([]folderCLIEntry, []folderCLIPendingWrite, int64, error) {
	entries := make([]folderCLIEntry, 0, 32)
	pending := make([]folderCLIPendingWrite, 0, 8)
	pendingSeen := make(map[string]struct{})
	cursor := ""
	var sequence int64
	for {
		query := url.Values{}
		query.Set("limit", "1000")
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		var page folderCLIInventoryResponse
		if err := c.jsonRequest(ctx, http.MethodGet, "/v1/folders/"+url.PathEscape(shareID)+"/"+url.PathEscape(scope)+"?"+query.Encode(), nil, &page); err != nil {
			return nil, nil, 0, err
		}
		if sequence == 0 {
			sequence = page.Sequence
		} else if page.Sequence != sequence {
			return nil, nil, 0, errors.New("folder inventory snapshot changed between pages")
		}
		entries = append(entries, page.Entries...)
		for _, write := range page.PendingWrites {
			if _, duplicate := pendingSeen[write.OperationID]; duplicate {
				continue
			}
			pendingSeen[write.OperationID] = struct{}{}
			pending = append(pending, write)
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	return entries, pending, sequence, nil
}

type folderCLIMutationResponse struct {
	SchemaVersion int `json:"schemaVersion"`
	Result        struct {
		OperationID string         `json:"operationId"`
		Sequence    int64          `json:"sequence"`
		Entry       folderCLIEntry `json:"entry"`
		Replayed    bool           `json:"replayed"`
	} `json:"result"`
}

func (c folderCLIClient) mutate(ctx context.Context, shareID string, request map[string]any) (folderCLIMutationResponse, error) {
	var response folderCLIMutationResponse
	err := c.jsonRequest(ctx, http.MethodPost, "/v1/folders/"+url.PathEscape(shareID)+"/mutations", mustJSON(request), &response)
	return response, err
}

func newFolderCLIClient(server string, reference string, sessionFile string) (folderCLIClient, string, error) {
	base, shareID, err := parseFolderReference(server, reference)
	if err != nil {
		return folderCLIClient{}, "", err
	}
	session, err := readOptionalSecretFile(sessionFile)
	if err != nil {
		return folderCLIClient{}, "", err
	}
	return folderCLIClient{baseURL: base, session: session, client: &http.Client{Timeout: 30 * time.Second}}, shareID, nil
}

func newFolderCLIOperationID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

func mustJSON(value any) []byte {
	payload, _ := json.Marshal(value)
	return payload
}

func printFolderMutationResult(jsonOutput bool, resultType string, shareID string, response folderCLIMutationResponse) int {
	if jsonOutput {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"schema_version": folderCLIOutputSchemaVersion, "ok": true, "type": resultType,
			"result": map[string]any{"share_id": shareID, "sequence": response.Result.Sequence, "entry": response.Result.Entry, "replayed": response.Result.Replayed},
		})
		return 0
	}
	fmt.Fprintf(os.Stdout, "%s  %s\n", response.Result.Entry.State, response.Result.Entry.Name)
	return 0
}

func (c folderCLIClient) exchangeWriteKey(ctx context.Context, shareID string, writeKey string, label string) (string, error) {
	requestBody, _ := json.Marshal(map[string]string{"writeKey": writeKey, "deviceLabel": label})
	var response struct {
		SessionToken string `json:"sessionToken"`
	}
	if err := c.jsonRequest(ctx, http.MethodPost, "/v1/folders/"+url.PathEscape(shareID)+"/auth/write", requestBody, &response); err != nil {
		return "", err
	}
	if response.SessionToken == "" {
		return "", errors.New("server omitted writer session")
	}
	return response.SessionToken, nil
}

func (c folderCLIClient) jsonRequest(ctx context.Context, method string, path string, body []byte, out any) error {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.baseURL, "/")+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.session != "" {
		req.Header.Set("Authorization", "Bearer "+c.session)
	}
	client := c.client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr folderCLIError
		if json.Unmarshal(payload, &apiErr) == nil && apiErr.Error.Code != "" {
			return fmt.Errorf("%s: %s", apiErr.Error.Code, apiErr.Error.Message)
		}
		return fmt.Errorf("server returned HTTP %d", resp.StatusCode)
	}
	if out != nil {
		if err := json.Unmarshal(payload, out); err != nil {
			return fmt.Errorf("decode server response: %w", err)
		}
	}
	return nil
}

func normalizeFolderServer(raw string) (string, error) {
	bases, err := normalizeServerURLs(raw)
	if err != nil {
		return "", err
	}
	if len(bases) != 1 {
		return "", errors.New("folder commands require one server origin")
	}
	return strings.TrimRight(bases[0].String(), "/"), nil
}

func parseFolderReference(server string, reference string) (base string, shareID string, err error) {
	base, err = normalizeFolderServer(server)
	if err != nil {
		return "", "", err
	}
	reference = strings.TrimSpace(reference)
	if reference == "" {
		return "", "", errors.New("folder reference is required")
	}
	if strings.Contains(reference, "://") {
		parsed, parseErr := url.Parse(reference)
		if parseErr != nil || parsed.Scheme == "" || parsed.Host == "" {
			return "", "", errors.New("invalid folder URL")
		}
		parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
		if len(parts) < 2 || (parts[0] != "f" && !(len(parts) >= 3 && parts[0] == "v1" && parts[1] == "folders")) {
			return "", "", errors.New("URL is not an idoud folder reference")
		}
		shareIndex := 1
		if parts[0] == "v1" {
			shareIndex = 2
		}
		shareID, err = url.PathUnescape(parts[shareIndex])
		if err != nil {
			return "", "", errors.New("invalid folder share id")
		}
		return parsed.Scheme + "://" + parsed.Host, shareID, nil
	}
	return base, strings.Trim(reference, "/"), nil
}

func writeSecretFileExclusive(path string, secret string) error {
	destination, err := reserveSecretFileExclusive(path)
	if err != nil {
		return err
	}
	defer destination.Abort()
	return destination.Commit(secret)
}

type reservedSecretFile struct {
	path      string
	file      *os.File
	committed bool
}

func reserveSecretFileExclusive(path string) (*reservedSecretFile, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("credential file path is required")
	}
	clean := filepath.Clean(path)
	file, err := os.OpenFile(clean, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	return &reservedSecretFile{path: clean, file: file}, nil
}

func (destination *reservedSecretFile) Commit(secret string) error {
	if destination == nil || destination.file == nil || destination.committed {
		return errors.New("credential file reservation is unavailable")
	}
	if _, err := io.WriteString(destination.file, secret+"\n"); err != nil {
		return err
	}
	if err := destination.file.Sync(); err != nil {
		return err
	}
	if err := destination.file.Close(); err != nil {
		return err
	}
	destination.file = nil
	if runtime.GOOS != "windows" {
		parent, err := os.Open(filepath.Dir(destination.path))
		if err != nil {
			return err
		}
		syncErr := parent.Sync()
		closeErr := parent.Close()
		if syncErr != nil {
			return syncErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	destination.committed = true
	return nil
}

func (destination *reservedSecretFile) Abort() {
	if destination == nil || destination.committed {
		return
	}
	if destination.file != nil {
		_ = destination.file.Close()
		destination.file = nil
	}
	_ = os.Remove(destination.path)
}

func readOptionalSecretFile(path string) (string, error) {
	if path = strings.TrimSpace(path); path == "" {
		return "", nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("credential file %s must not be accessible by group or others", path)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	secret := strings.TrimSpace(string(payload))
	if secret == "" {
		return "", errors.New("credential file is empty")
	}
	return secret, nil
}

func readInteractiveFolderSecret(fromStdin bool, writeMode bool) (string, error) {
	label := "read password"
	if writeMode {
		label = "write key"
	}
	if fromStdin {
		payload, err := io.ReadAll(io.LimitReader(os.Stdin, 8193))
		if err != nil {
			return "", err
		}
		secret := strings.TrimRight(string(payload), "\r\n")
		if secret == "" {
			return "", fmt.Errorf("%s is empty", label)
		}
		return secret, nil
	}
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		return "", fmt.Errorf("%s must be read interactively or with --secret-stdin", label)
	}
	fmt.Fprintf(os.Stderr, "%s: ", label)
	payload, err := term.ReadPassword(fd)
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", err
	}
	if len(payload) == 0 {
		return "", fmt.Errorf("%s is empty", label)
	}
	return string(payload), nil
}

func printFolderCommandError(jsonOutput bool, code string, err error) {
	if jsonOutput {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"schema_version": folderCLIOutputSchemaVersion,
			"ok":             false,
			"type":           "folder_error",
			"error":          map[string]string{"code": code, "detail": err.Error()},
		})
		return
	}
	fmt.Fprintf(os.Stderr, "idoud: %v\n", err)
}

func printFolderUsage(w io.Writer) {
	fmt.Fprintln(w, `idoud live folders

USAGE
  idoud folder create [name] [--write-key-file FILE] [--session-file FILE]
  idoud folder ls FOLDER [PATH] [--session-file FILE]
  idoud folder status FOLDER [--session-file FILE]
  idoud folder auth [--write] [--secret-stdin] --session-file FILE FOLDER
  idoud folder history FOLDER PATH --session-file FILE
  idoud folder trash FOLDER PATH --session-file FILE
  idoud folder trash list FOLDER --session-file FILE
  idoud folder restore FOLDER ENTRY_ID --session-file FILE
  idoud folder recovery list|restore ...
  idoud folder rotate-share-id FOLDER --session-file FILE
  idoud folder rotate-write-key FOLDER --session-file FILE --write-key-file NEW_FILE
  idoud folder push LOCAL_DIR FOLDER
  idoud folder pull FOLDER LOCAL_DIR
  idoud mount FOLDER MOUNTPOINT [--write] [--background]

Folder and mount operations are explicit. Existing idoud FILE, -D, and -z behavior is unchanged.`)
}
