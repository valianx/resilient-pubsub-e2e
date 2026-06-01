# Test Matrix

| Suite file | What it covers | Key assertion | Requires emulator |
|---|---|---|---|
| `publish-subscribe.e2e.test.ts` | Basic round-trip: `publish({ body })` → subscriber receives same typed body → ack | `received.orderId === published.orderId` | Yes |
| `context-propagation.e2e.test.ts` | Allowlist-gated propagation: `traceparent` and allowlisted `x-tenant-id` propagate; non-allowlisted `x-secret` is dropped | `headers['traceparent']` present; `headers['x-tenant-id']` present; `headers['x-secret']` undefined | Yes |
| `retry-publish.e2e.test.ts` | Happy-path publish returns `messageId`; forced-failure publish (bad topic) rejects `ResilientPubSubError{ kind: 'publish' }` | `isResilientPubSubError(caught) === true` | Yes |
| `dead-letter.e2e.test.ts` | Handler always throws → nacks → `getDeliveryAttempt(meta)` increments across redeliveries; optional poll for DLQ forwarding | **Authoritative:** delivery-attempt values strictly increase; **Optional (best-effort):** message arrives on DLQ subscription within 15 s | Yes |
| `graceful-stop.e2e.test.ts` | In-flight slow handler; `stop()` resolves AFTER handler completes; `stopResolvedAt >= handlerCompletedAt` | Timestamps: `stopResolvedAt >= handlerCompletedAt` | Yes |
| `ordering.e2e.test.ts` | Ordering keys end-to-end: publisher with `ordering: true`, subscription with `enableMessageOrdering`, N messages with the same `orderingKey` arrive in published order | `received === [1,2,3,4,5]`; fallback: all N arrive when emulator ordering is flaky | Yes |
| `serializer-custom.e2e.test.ts` | Custom `Serializer<T>` injection (base64-JSON): round-trip body fidelity + `content-type` attribute reflects the custom `contentType` | Body round-trip equals original; raw `content-type` attribute === `'application/x-base64json'` | Yes |
| `poison.e2e.test.ts` | Poison message: raw non-JSON bytes → deserialization throws → handler NOT invoked → `onPoison` hook fires; `onNack` does NOT fire | `handlerInvoked === false`; `onNackInvoked === false`; `onPoison` called with an error | Yes |
| `hooks.e2e.test.ts` | Observability hooks: publisher `onPublish` fires with matching `messageId`; subscriber `onAck` fires on handler resolve; subscriber `onNack` + `onError` fire on handler throw | `hookedMessageId === result.messageId`; `onAck` called; `onNack` + `onError` called with `kind:'process'` | Yes |
| `redelivery.e2e.test.ts` | At-least-once contract without DLQ: handler throws on first delivery (nack), emulator redelivers, handler succeeds on second delivery (ack) | `deliveryCount >= 2`; `successfulDeliveryCount === 1` | Yes |
| `flow-control.e2e.test.ts` | `flowControl.maxMessages: 1` pass-through: all messages eventually processed; observed peak concurrency ≤ 2 (emulator best-effort) | `received.length === TOTAL`; `peakConcurrency <= 2` | Yes |
| `env-config.e2e.test.ts` | `resolveConfigFromEnv` parses all `RESILIENT_PUBSUB_*` variables correctly using a fake env object (no `process.env` mutation) | Each env-var maps to the correct typed field; invalid values produce `undefined`; empty env yields all-undefined | No (unit-style, no emulator) |

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
| Ordering key in-order delivery | Definitive (with best-effort fallback) | Primary: strict order; fallback to all-arrive assertion on emulator flake |
| Custom serializer round-trip | Definitive | Both body fidelity and content-type attribute |
| Poison onPoison hook / handler not invoked | Definitive | Deserialization-failure path is well-defined |
| onPublish / onAck / onNack / onError hooks | Definitive | Hook wiring against live emulator |
| At-least-once redelivery | Definitive | nack → redeliver → ack path; emulator always redelivers nacked messages |
| Flow-control peak concurrency | Best-effort | `peakConcurrency <= 2`; emulator may briefly buffer beyond maxMessages |
| Env-var config parsing | Definitive | Pure function; deterministic; no emulator needed |

## What can only be verified in CI

- Emulator container startup and gRPC connectivity on `localhost:8681`
- The full test run in an isolated environment (no shared state from dev machine)
- The `prepare` build of the git-dependency `resilient-pubsub` via `pnpm install`

## Skipped / deferred suites

| Area | Reason |
|---|---|
| `backoff-strategies` (separate file) | Strategy math (exponential vs linear vs constant delay values) is fully covered by the library's own unit tests. Making it deterministic against the emulator would require controllable transient failures, which the emulator does not expose. Deferred to library unit tests. |
| Publisher `onRetry` hook (live) | Forcing a transient error in the emulator is unreliable — the emulator classifies missing topics as permanent (NOT_FOUND), which the library does not retry. The retry code path is covered by library unit tests with injected `_sleep`. |
