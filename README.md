# idoud CLI

Small, static command-line client for uploading to and downloading from idoud.

## Install

Linux and macOS:

```bash
curl -fsSL https://idoud.cc/s | sh
```

This is a short redirect to this repository's canonical `install.sh`. Run the
same command again at any time to install the newest GitHub release, including
when the installed CLI is too old to update itself.

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

# Archive every shell-expanded path in one stream. Multiple paths default to
# archive.tar.lz4; use --name to choose another upload name.
idoud -z *

# Piped stdin is detected automatically, including its file extension.
cat archive.zip | idoud

# An extensionless override keeps your name and adds the detected suffix.
# For tar data compressed with LZ4, this uploads as backup.tar.lz4.
cat archive.tar.lz4 | idoud --name backup

# Protect an upload and limit downloads.
idoud archive.zip --password secret --download-limit 3

# Download by public URL or file ID.
idoud --download https://idoud.cc/AbC123/archive.zip
idoud --download AbC123 --download-output ./archive.zip

# Install the newest release for this operating system and CPU.
idoud --update

# Create a live mutable folder. The one-time write key and the derived writer
# session are written only to new mode-0600 files, never to command output.
idoud folder create Project \
  --write-key-file ./project.write-key \
  --session-file ./project.session

# Recursively publish a fully preflighted local directory. A writer session is
# required; the raw write key is never accepted on the command line.
idoud folder push ./project https://idoud.cc/f/SHARE_ID \
  --session-file ./project.session

# Stream the current public namespace into a new or empty local directory.
# Password-protected folders use a reader session created by `folder auth`.
idoud folder pull https://idoud.cc/f/SHARE_ID ./project-copy
```

Run `idoud --help` for the compact command reference, `idoud --help-all` for
every advanced option, and standalone `idoud -v`, `idoud -V`, or
`idoud --version` for build identification. With a transfer input, `-v` retains
its existing `--verbose` meaning.

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
- Standard-input and `-z` uploads keep complete retryable chunks in RAM only.
  Their data window starts at up to 64 requests on machines with ample
  headroom, allocates buffers lazily, and grows after real confirmations when
  the source, route plan, and live host/container memory permit it. Before the
  unknown stream has produced multiple parts, connection warmup opens one
  initial payload lane while active routes race in parallel; slow route probes
  rejoin in the background instead of blocking that first healthy route. The
  remaining lanes open concurrently as useful parts materialize. A 256 MiB cap
  resolves to the established 24-request window;
  smaller machines automatically use a still smaller bounded window.
  `-M`/`--stream-memory` sets an explicit cap.
- Piped stdin is an automatic upload input, so `producer | idoud` needs no
  stdin flag or filename. When the name is automatic—or `-n`/`--name` is
  extensionless—the CLI incrementally inspects a replayable in-memory prefix,
  normally only 512 bytes, and appends the detected extension without changing
  or dropping stream bytes. Compound tar compression is recognized for Gzip,
  Bzip2, and LZ4 (for example `.tar.lz4`); unknown binary data safely falls
  back to `.bin`. Supplying an extension skips inspection entirely.
- `-z`/`--archive` accepts one or more paths and streams one standards-compatible
  `.tar.lz4` archive directly into the uploader without creating a temporary
  archive. This includes commands such as `idoud -z *`; Unix shells expand the
  operands before launch, while the CLI expands `*` and `?` on Windows when the
  native shell passes them through. Operands become top-level tar entries in
  argument order. A single operand names the upload after that path, while
  multiple operands default to `archive.tar.lz4`.
  Absolute source paths are never stored in the tar, symlinks are preserved
  rather than followed, and conflicting top-level names are rejected before an
  upload starts. LZ4 compression runs concurrently across the available CPU
  capacity. One ordered read-ahead stage overlaps independent file reads across
  all operands while preserving byte-identical tar order; its worker count and
  hard RAM bound scale down from live OS/container headroom, and very-low-memory
  systems use the sequential path.
- Diagnostics go to stderr. Successful machine-readable output stays isolated
  on stdout.
- Live-folder commands are explicit and do not change `idoud FILE`, `-D`, or
  `-z`. Recursive push validates the complete portable tree and negotiated
  limits, hashes every file in provider-sized buffers before transmitting any
  payload, creates writer-only targets before the first provider PUT, and
  publishes each immutable version only after the server reports its configured
  replica factor. Concurrent same-path changes are reported as conflict copies,
  never silently merged or overwritten.
- `folder pull` paginates direct-child listings, rejects non-portable or special
  entries, pins every file to the ETag and immutable version observed during
  preflight, and streams into fsynced temporary files. Final publication is an
  atomic no-replace operation: the destination must be absent or empty and no
  existing local entry is deleted or overwritten. Transfer concurrency is
  bounded by `--parallel` (maximum 8), and JSON output never includes sessions,
  write keys, provider URLs, or signed data grants.
- Interactive terminals get a compact idoud-styled transfer display on stderr.
  One progressive heading gains the name, size, and route plan as they become
  available, then remains as the single summary above the progress bar.
  Uploads show smooth, retry-safe request-body progress as `sent` and separately
  show provider-confirmed `stored` bytes, so retry traffic is never double
  counted or mistaken for durability. Download progress counts bytes written to
  the resumable part file. Rates use rolling byte windows, known-size ETAs
  include slow stdin input, and finalization/file sync remain separate phases.
- Unknown-size stdin and `-z` streams show read/packed, sent, and confirmed
  stored bytes, rate, activity, and retries without inventing a percentage or ETA.
  Redirected stderr remains quiet in the default `auto` mode, `--no-progress`
  disables the display, and `NO_COLOR` preserves the layout without terminal
  colors.
- Windows Terminal v1.6+ automatically mirrors interactive transfer state into
  its tab progress ring and Windows taskbar through OSC 9;4, for both native
  Windows and WSL sessions. Planning, connection setup, and unknown-size
  streams are indeterminate; known uploads advance smoothly with sent bytes but
  reserve 100% for provider-confirmed storage; downloads use bytes written to
  disk; retry/stall states are warnings; failures report the error state; and
  every completion clears the terminal state. `IDOUD_TAB_PROGRESS=on` enables
  the protocol on another compatible interactive TTY when automatic Windows
  Terminal detection is unavailable, while `off` disables it. Redirected
  output, `--progress=plain`, and `--no-progress` never receive these terminal
  control sequences.
- `--progress=lines` (or `-g lines`) keeps the interactive spinner, progress
  bar, rate, ETA, part counts, and completion layout, but writes every refresh
  as a separate timestamped line without cursor-control sequences. Unchanged
  snapshots are collapsed to a one-second heartbeat. It remains enabled when
  stderr is redirected, and `IDOUD_PROGRESS=lines` selects it by default.
- `--non-interactive` (or `--progress=plain`) emits timestamped, ANSI-free,
  line-oriented progress to stderr even when redirected. It separately reports
  request-body bytes, fully written request bodies, provider-confirmed bytes,
  rolling and end-to-end rates, active requests, retries, confirmation latency,
  and ETA. `IDOUD_PROGRESS=plain` enables the same mode for services and scripts.
- Known random-access files start their final range inside the initial bounded
  concurrency window. This avoids a cold serial final request while preserving
  the server-advertised concurrency ceiling and durable-confirmation semantics.
- `idoud --update` (or `idoud -a`) discovers the latest release without using
  the rate-limited GitHub API, verifies the selected platform binary against the
  published SHA-256 checksums, runs its version check, and atomically replaces
  the current executable. The legacy positional `idoud update` remains
  available, but uploads a local regular file named `update` when one exists.
  Linux, macOS, and Windows release targets are supported.

## Automation

`--output url` is the default and prints one URL (upload) or output path
(download). `--output none` suppresses success output when only the exit status
matters. `--output json` or `--json` emits one schema-versioned JSON document
for success, help, and errors.

Capture a complete human- and AI-readable transfer log without disturbing the
URL or JSON written to stdout:

```sh
idoud -g lines archive.bin 2>idoud-pretty.log
idoud --non-interactive archive.bin 2>idoud-transfer.log
cat archive.bin | idoud --name archive.bin --progress=plain \
  2>idoud-transfer.log
```

Stable JSON error codes are:

- `usage_error`
- `input_error`
- `upload_failed`
- `download_failed`

## Operator diagnostics

Run `idoud --help-all` (or `idoud -A`) to show transport tuning and
address-pinning controls. `IDOUD_SHOW_OPERATOR_FLAGS=1 idoud --help` remains a
compatibility alias. Advanced controls stay out of compact help because
server-provided plans are the production source of truth.

## Development

```bash
go test ./...
go vet ./...
```
