# worker.Request

Part of the BPM ↔ AI integration. Takes work from
[Integrator.Api](https://github.com/d9293/halyk-genai-integrator) and does the
outside calls the API never makes itself:

- **documents:** download each task file from its Attstore link and stream it into S3 (MinIO locally) for AI
- **submissions:** send ready tasks to AI, at most as many as Integrator.Api allows at once

It has no database: all state lives in Integrator.Api. Several instances can run
side by side; Integrator.Api's claims make sure no two take the same work.

## Run

Needs Integrator.Api on `INTEGRATOR_URL` and the local MinIO (infra) with
`make minio-init` done in the integrator repo.

```sh
make run      # go run . --config_file dev.env
```

Liveness: http://localhost:8083/health

On Ctrl+C / SIGTERM it stops claiming new work, lets work in progress finish
for up to `SHUTDOWN_GRACE`, then cancels what is left. Unfinished work is
handed out again by Integrator.Api once its claim expires.

## Develop

```sh
make test     # go test -race ./...

# Check the client against a running Integrator.Api (registers one test task):
CONTRACT_INTEGRATOR_URL=http://localhost:8080 go test -run Contract ./internal/client/integrator/
```

## Layout

```
internal/app        wiring, start and orderly shutdown
internal/config     the only package that reads configuration
internal/runner     repeats a loop's rounds, with backoff when idle
internal/client/integrator  typed client for Integrator.Api's /internal/v1 worker API
internal/source     streamed download of a document link, with a size limit
internal/storage/s3 streamed multipart upload to S3 / MinIO
internal/usecase/document   the document loop: claim → copy (with claim renewal) → complete / failure
```

Copies are stored as
`{bucket}/{operation}/{YYYY-MM-DD}/{requestGuid}-{position}-{uploadId}{ext}`, e.g.
`s3://bpm-bucket/register-compare/2026-10-18/dddd…-0-8f3a2c1e.pdf`. The upload
ID makes every copy a new object, so a retry never overwrites an earlier one.

Coming next: `client/ai`, `usecase/submission`.

### Storage test

Against the local MinIO (writes under `test/worker-request/`):

```sh
S3_TEST_ENDPOINT=http://localhost:9000 S3_TEST_BUCKET=bpm-bucket \
S3_TEST_ACCESS_KEY_ID=… S3_TEST_SECRET_ACCESS_KEY=… go test ./internal/storage/s3/
```
