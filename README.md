# idoud CLI

Standalone Go CLI uploader for idoud.

This project is intentionally isolated under `cli/` so it can be moved into a separate public repository.

## Features

- Upload a single file:
  - `idoud archive.zip`
- Upload from stdin:
  - `cat archive.zip | idoud --stdin --name archive.zip`
- Full chunked upload flow:
  - sends `PUT` requests with `Content-Range`
  - uses `X-Upload-Key` across all chunks
  - retries retryable chunk failures (409/429/5xx/network)
  - waits for finalization via `/v1/files/{id}` polling
- Optional upload password and download limit headers.

## Build

```bash
cd cli
go build -o idoud .
```

## Usage

```bash
idoud [flags] <file>
idoud --stdin [--name <filename>] [flags]
```

### Examples

```bash
idoud archive.zip
cat archive.zip | idoud --stdin --name archive.zip
idoud --server https://idoud.cc --chunk-size 8MiB --parallel 16 archive.zip
idoud --password "secret" --download-limit 3 archive.zip
```

## Important flags

- `--server` server origin (default `https://idoud.cc`)
- `--stdin` read payload from stdin instead of a path argument
- `--stdin-size` known stdin size hint for stdin uploads
- `--name` upload filename override (recommended with `--stdin`)
- `--chunk-size` chunk size for `Content-Range` uploads (default `3MiB`)
- `--parallel` parallel non-final chunk uploads (default `12`)
- `--retries` retries per chunk (default `6`)
- `--debug` print live chunk concurrency, retries, throughput, and 7-sample moving average speed to stderr
- `--request-timeout` timeout for non-final chunk requests (default `45s`)
- `--final-request-timeout` timeout for final chunk request (default `95s`)
- `--finalize-timeout` max wait for server finalization (default `20m`)
- `--password` sets `X-Upload-Password`
- `--download-limit` sets `X-Upload-Download-Limit`
- `--insecure` skip TLS verification
- `--verbose` enable retry/finalization logs to stderr

## Notes

- Stdin uploads are streamed with bounded RAM using a chunk buffer pool.
- For unknown stdin size, the CLI still uploads in parallel using chunked `Content-Range: bytes .../*` requests and marks only the last chunk with `X-Upload-Final: 1`.
- The final URL is printed to stdout on success.
