# idoud CLI

Small, static command-line client for uploading to and downloading from idoud.

## Install

Linux and macOS:

```bash
curl -fsSL https://raw.githubusercontent.com/mydearniko/idoud/main/install.sh | sh
```

Windows PowerShell:

```powershell
irm https://raw.githubusercontent.com/mydearniko/idoud/main/install.ps1 | iex
```

Build from source:

```bash
go build -trimpath -ldflags="-s -w" -o idoud .
```

## Use

```bash
# Upload a file and print its public URL.
idoud archive.zip

# Stream a directory as an LZ4-compressed tar archive and upload it immediately.
# This names the upload after the selected path, for example project.tar.lz4.
idoud -z ./project

# Upload standard input with an explicit filename.
cat archive.zip | idoud --stdin --name archive.zip

# Protect an upload and limit downloads.
idoud archive.zip --password secret --download-limit 3

# Download by public URL or file ID.
idoud --download https://idoud.cc/AbC123/archive.zip
idoud --download AbC123 --download-output ./archive.zip

# Install the newest release for this operating system and CPU.
idoud update
```

Run `idoud --help` for user-facing options and `idoud --version` for build
identification.

## Behavior

- The CLI asks the public API for an upload or download plan.
- Upload chunks are distributed across the plan's active node origins. Route
  health is shared by every worker, so one failed probe moves the transfer to
  another route—or directly to the plan's standby—without sending every chunk
  through the same failure first. The public server is only the final emergency
  relay, not the normal data path.
- Normal-file uploads keep a small local resume token. Re-running the same
  command reuses the existing upload and skips chunks already stored by the
  provider.
- Large/high-latency uploads separate active request-body writes from requests
  waiting for server confirmation, preventing slow connections from creating a
  long completion tail.
- Downloads fetch independent byte ranges from the plan's mirrors and write
  directly to a persistent `.idoud.part` file. Re-running the command skips
  verified ranges and atomically promotes the file when complete.
- Transfers retry interruptions for 24 hours by default; individual CLI chunk
  requests allow two minutes for durable provider confirmation. Change these
  windows with `--resume-timeout` and `--request-timeout`.
- Standard-input uploads use at most 256 MiB of complete retryable chunk
  buffers at the production chunk size and work with either known or unknown
  input size.
- `-z`/`--archive` streams a standards-compatible `.tar.lz4` archive directly
  into the uploader without creating a temporary archive. Absolute source paths
  are never stored in the tar, symlinks are preserved rather than followed, and
  LZ4 compression runs concurrently across the available CPU capacity.
- Diagnostics go to stderr. Successful machine-readable output stays isolated
  on stdout.
- `idoud update` discovers the latest release without using the rate-limited
  GitHub API, verifies the selected platform binary against the published
  SHA-256 checksums, runs its version check, and atomically replaces the current
  executable. Linux, macOS, and Windows release targets are supported.

## Automation

`--output url` is the default and prints one URL (upload) or output path
(download). `--output none` suppresses success output when only the exit status
matters. `--output json` or `--json` emits one schema-versioned JSON document
for success, help, and errors.

Stable JSON error codes are:

- `usage_error`
- `input_error`
- `upload_failed`
- `download_failed`

## Operator diagnostics

Set `IDOUD_SHOW_OPERATOR_FLAGS=1` before `idoud --help` to show transport,
address-pinning, and speed-test controls. These flags are intentionally hidden
from normal help because server-provided plans are the production source of
truth.

## Development

```bash
go test ./...
go vet ./...
```
