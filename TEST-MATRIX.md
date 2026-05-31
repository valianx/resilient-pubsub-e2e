# Test Matrix

| Suite file | What it covers | Key assertion | Requires emulator |
|---|---|---|---|
| `publish-subscribe.e2e.test.ts` | Basic round-trip: `publish({ body })` → subscriber receives same typed body → ack | `received.orderId === published.orderId` | Yes |
| `context-propagation.e2e.test.ts` | Allowlist-gated propagation: `traceparent` and allowlisted `x-tenant-id` propagate; non-allowlisted `x-secret` is dropped | `headers['traceparent']` present; `headers['x-tenant-id']` present; `headers['x-secret']` undefined | Yes |
| `retry-publish.e2e.test.ts` | Happy-path publish returns `messageId`; forced-failure publish (bad topic) rejects `ResilientPubSubError{ kind: 'publish' }` | `isResilientPubSubError(caught) === true` | Yes |
| `dead-letter.e2e.test.ts` | Handler always throws → nacks → `getDeliveryAttempt(meta)` increments across redeliveries; optional poll for DLQ forwarding | **Authoritative:** delivery-attempt values strictly increase; **Optional (best-effort):** message arrives on DLQ subscription within 15 s | Yes |
| `graceful-stop.e2e.test.ts` | In-flight slow handler; `stop()` resolves AFTER handler completes; `stopResolvedAt >= handlerCompletedAt` | Timestamps: `stopResolvedAt >= handlerCompletedAt` | Yes |

## Assertion authority

| Assertion | Authority level | Notes |
|---|---|---|
| Round-trip body delivery | Definitive | Core contract — must pass in every CI run |
| Header propagation / drop | Definitive | Security contract — must pass |
| Happy-path publish messageId | Definitive | Basic liveness |
| Forced-failure publish error type | Definitive | Error surface contract |
| Delivery-attempt increment | Definitive | Emulator reliably increments; this is the DLQ authoritative assertion |
| DLQ forwarding via poll | Best-effort | Emulator latency varies; non-fatal if not observed within 15 s window |
| stop() after in-flight drain | Definitive | Graceful-drain contract |

## What can only be verified in CI

- Emulator container startup and gRPC connectivity on `localhost:8681`
- The full test run in an isolated environment (no shared state from dev machine)
- The `prepare` build of the git-dependency `resilient-pubsub` via `pnpm install`
