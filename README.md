# idoud CLI

Standalone Go CLI uploader for idoud.

This project is intentionally isolated under `cli/` so it can be moved into a separate public repository.

## Project layout

- `main.go`: CLI entrypoint.
- `types.go`: shared constants/types and HTTP error wrappers.
- `flags.go`: argument parsing and validation.
- `source.go`: file/stdin source opening logic.
- `upload.go`: upload engine, retries, finalization checks, transport tuning, and helpers.

## Features

- Upload a single file:
  - `idoud archive.zip`
- Upload from stdin:
  - `cat archive.zip | idoud --stdin --name archive.zip`
- Full chunked upload flow:
  - sends `PUT` requests with `Content-Range`
  - uses `X-Upload-Key` across all chunks
  - retries retryable chunk failures (409/429/5xx/network)
  - waits for finalization via `/v1/files/{id}` readiness checks (with short server-side wait hints when supported)
- Optional upload password and download limit headers.

## Build

```bash
cd cli
go build -o idoud .
```

## Usage

```bash
idoud [flags] <file>
idoud --stdin [--name <filename> | <filename>] [flags]
```

### Examples

```bash
idoud archive.zip
cat archive.zip | idoud --stdin --name archive.zip
cat archive.zip | idoud --stdin archive.zip
idoud --server https://idoud.cc --parallel 16 archive.zip
idoud --server https://s1.example,https://s2.example archive.zip
idoud --server https://idoud.cc --subdomains 2 archive.zip
idoud --ips 104.16.230.132,104.16.230.133 --server https://s1.example,https://s2.example archive.zip
idoud --password "secret" --download-limit 3 archive.zip
dd if=/dev/zero bs=1M count=6000 | idoud --stdin --name bench.bin --parallel 60 --speedtest --debug
```

## Important flags

- `--server` server origin, or comma-separated origins for per-chunk round-robin (default `https://idoud.cc`)
- `--stdin` read payload from stdin instead of a path argument
- `--stdin-size` known stdin size hint for stdin uploads
- `--name` upload filename override (recommended with `--stdin`)
- `--chunk-size` chunk size for `Content-Range` uploads (must be exactly `3145728` bytes / 3 MiB)
- `--parallel` parallel non-final chunk uploads (default `12`)
- `--subdomains` force numbered upload subdomains `0..N-1` on `idoud.cc` origin
- `--ips` force chunk upload destination IPs (comma-separated), round-robin by chunk index
- `--no-ipv6` disable IPv6 and force IPv4-only connections
- `--no-subdomains` disable numbered subdomain upload routing (alias: `--nosub`)
- `--speedtest` benchmark ingest path with server-side sink mode (no persisted file output)
- `--retries` retries per chunk (default `6`)
- `--hedge-delay` speculative duplicate delay for slow non-final chunks (default `0s`, disabled)
- `--debug` print live chunk concurrency, retries, throughput, and 7-sample moving average speed to stderr
- `--request-timeout` timeout for non-final chunk requests (default `45s`)
- `--final-request-timeout` timeout for final chunk request (default `95s`)
- `--finalize-recovery-timeout` readiness wait after uncertain final chunk responses (default `95s`)
- `--finalize-poll-interval` readiness poll interval (default `1.2s`)
- `--finalize-timeout` max wait for server finalization (default `20m`)
- `--password` sets `X-Upload-Password`
- `--download-limit` sets `X-Upload-Download-Limit`
- `--insecure` skip TLS verification
- `--verbose` enable retry/finalization logs to stderr

## Notes

- Stdin uploads are streamed with bounded RAM using a chunk buffer pool.
- For unknown stdin size, the CLI still uploads in parallel using chunked `Content-Range: bytes .../*` requests and marks only the last chunk with `X-Upload-Final: 1`.
- The final URL is printed to stdout on success.
