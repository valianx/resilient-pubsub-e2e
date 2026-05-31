# resilient-pubsub-e2e

End-to-end consumer test suite for the `resilient-pubsub` library.
Runs a plain Node.js worker (no framework) against the
**Google Cloud Pub/Sub emulator** and verifies the full publish-subscribe
round-trip, context propagation, retry behavior, dead-letter routing, and
graceful shutdown.

## What this is

This repository exists to enforce the ergonomics budget of `resilient-pubsub`
on real consumer code. It is one of two e2e repos:

- `resilient-pubsub-e2e` — plain Node worker (this repo)
- `resilient-pubsub-e2e-nestjs` — NestJS app (separate repo)

## Library pin

The library is consumed as a **git dependency** pinned to a specific commit SHA:

```json
"resilient-pubsub": "github:valianx/resilient-pubsub#0003e7bc9e2ca85455bf75973d269f9bc73364e0"
```

The `prepare` script (`pnpm run build`) in the library runs automatically during
`pnpm install`, so `dist/` is built from source without manual steps.

## Run locally

### Prerequisites

- Node.js >= 24
- pnpm >= 11
- Docker (to run the Pub/Sub emulator)

### 1. Start the Pub/Sub emulator

```bash
docker run --rm -p 8681:8681 messagebird/gcloud-pubsub-emulator:latest
```

### 2. Set environment variables

```bash
export PUBSUB_EMULATOR_HOST=localhost:8681
export PROJECT_ID=e2e-project    # optional — this is the default
```

### 3. Install dependencies

```bash
pnpm install --no-frozen-lockfile
```

### 4. Type-check (no emulator required)

```bash
pnpm typecheck
```

### 5. Run e2e tests (emulator required)

```bash
pnpm test
```

## CI

GitHub Actions workflow: `.github/workflows/e2e.yml`

Triggers: push to `main`, pull requests targeting `main`, `workflow_dispatch`.

The workflow spins up `messagebird/gcloud-pubsub-emulator:latest` as a service
container on port `8681`, installs dependencies, type-checks, and runs the full
e2e suite.

## Test suites

See `TEST-MATRIX.md` for a complete table of suites and their coverage.

## Project structure

```
resilient-pubsub-e2e/
├── lib/
│   └── harness.ts               # Shared setup: client, unique names, create/delete helpers
├── test/
│   ├── publish-subscribe.e2e.test.ts
│   ├── context-propagation.e2e.test.ts
│   ├── retry-publish.e2e.test.ts
│   ├── dead-letter.e2e.test.ts
│   └── graceful-stop.e2e.test.ts
├── .github/workflows/e2e.yml    # CI pipeline
├── vitest.config.ts
├── tsconfig.json
└── package.json
```
