/**
 * Suite: graceful-stop
 *
 * Verifies that stop() completes the in-flight handler before resolving.
 *
 * Scenario:
 *   1. Start a subscriber whose handler takes HANDLER_DELAY_MS to complete.
 *   2. Publish a message and wait until the handler has started (signaled via
 *      a promise resolved inside the handler at the start of its work).
 *   3. Call stop() while the handler is still running.
 *   4. Assert stop() resolves AFTER the handler completes (not before).
 *   5. Assert the handler actually finished its work (resolvedAtMs is set).
 *
 * The stopTimeoutMs is set to STOP_TIMEOUT_MS > HANDLER_DELAY_MS so the
 * graceful-drain path (not the timeout-nack path) is exercised.
 */

import { describe, it, beforeAll, afterAll, expect } from 'vitest';
import { PubSub } from '@google-cloud/pubsub';
import { createResilientPublisher } from 'resilient-pubsub/publisher';
import { createResilientSubscriber } from 'resilient-pubsub/subscriber';
import {
  createClient,
  uniqueNames,
  ensureTopic,
  ensureSubscription,
  deleteSub,
  deleteTopic,
} from '../lib/harness.js';

// ── Types ────────────────────────────────────────────────────────────────────

interface SlowPayload {
  seq: number;
}

// ── Constants ────────────────────────────────────────────────────────────────

/** How long the in-flight handler sleeps before resolving. */
const HANDLER_DELAY_MS = 1_000;

/** stop() timeout — longer than handler delay so graceful drain runs. */
const STOP_TIMEOUT_MS = 10_000;

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('graceful-stop');
let client: PubSub;

beforeAll(async () => {
  client = createClient();
  await ensureTopic(client, names.topic);
  await ensureSubscription(client, names.topic, names.sub);
});

afterAll(async () => {
  await deleteSub(client, names.sub);
  await deleteTopic(client, names.topic);
  await client.close();
});

// ── Tests ────────────────────────────────────────────────────────────────────

describe('graceful stop', () => {
  it('resolves stop() only after the in-flight handler has completed', async () => {
    const publisher = createResilientPublisher<SlowPayload>({
      topic: names.topic,
      pubSubClient: client,
      retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
    });

    // Track handler lifecycle with timestamps
    let handlerStartedAt: number | undefined;
    let handlerCompletedAt: number | undefined;
    let stopResolvedAt: number | undefined;

    // Signals that the handler has started its slow work
    let signalHandlerStarted!: () => void;
    const handlerStartedPromise = new Promise<void>((resolve) => {
      signalHandlerStarted = resolve;
    });

    const subscriber = createResilientSubscriber<SlowPayload>({
      subscription: names.sub,
      pubSubClient: client,
      stopTimeoutMs: STOP_TIMEOUT_MS,
      flowControl: { maxMessages: 1 },
    });

    subscriber.on(async () => {
      handlerStartedAt = Date.now();
      signalHandlerStarted();

      // Simulate slow work
      await new Promise<void>((r) => setTimeout(r, HANDLER_DELAY_MS));

      handlerCompletedAt = Date.now();
    });

    subscriber.start();

    // Give the subscriber a moment to attach before publishing
    await new Promise<void>((r) => setTimeout(r, 200));

    await publisher.publish({ body: { seq: 1 } });

    // Wait until the handler has actually started processing
    await handlerStartedPromise;

    // Call stop() while the handler is still sleeping
    await subscriber.stop();
    stopResolvedAt = Date.now();

    // The handler must have started before stop() was called
    expect(handlerStartedAt).toBeDefined();

    // The handler must have completed — graceful drain waited for it
    expect(handlerCompletedAt).toBeDefined();

    // stop() must have resolved AFTER the handler finished
    expect(stopResolvedAt).toBeGreaterThanOrEqual(handlerCompletedAt as number);

    // The handler delay must be measurable (sanity check)
    const elapsed = (handlerCompletedAt as number) - (handlerStartedAt as number);
    expect(elapsed).toBeGreaterThanOrEqual(HANDLER_DELAY_MS - 100); // 100ms tolerance
  });
});
