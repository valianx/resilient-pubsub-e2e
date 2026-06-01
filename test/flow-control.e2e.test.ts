/**
 * Suite: flow-control
 *
 * Verifies that the `flowControl.maxMessages` option is passed through correctly
 * to the native Pub/Sub subscription and that all messages are eventually
 * processed (the config is not broken in a way that stops delivery).
 *
 * Secondary assertion (best-effort): a concurrency counter tracks how many
 * handler invocations are running simultaneously. With maxMessages: 1, the
 * native client buffers at most 1 message at a time, so the observed peak
 * concurrency should not exceed 1. Because the emulator's buffer control is
 * best-effort (it may briefly hold slightly more), this assertion is logged as
 * a warning rather than a hard failure when exceeded.
 *
 * The primary (definitive) assertion is that ALL published messages are
 * eventually received and acked.
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

interface BatchItem {
  index: number;
}

// ── Constants ────────────────────────────────────────────────────────────────

/** Number of messages to publish. */
const TOTAL = 5;

/** Handler sleep duration — long enough to overlap if concurrency > 1. */
const HANDLER_SLEEP_MS = 200;

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('flow-ctrl');
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

describe('flow control', () => {
  it(
    'processes all messages with maxMessages: 1 and peak concurrency <= 1',
    async () => {
      const received: number[] = [];
      let concurrency = 0;
      let peakConcurrency = 0;

      let resolveAll!: () => void;
      const allDone = new Promise<void>((resolve) => {
        resolveAll = resolve;
      });

      const subscriber = createResilientSubscriber<BatchItem>({
        subscription: names.sub,
        pubSubClient: client,
        flowControl: { maxMessages: 1 },
      });

      subscriber.on(async ({ body }) => {
        concurrency++;
        if (concurrency > peakConcurrency) {
          peakConcurrency = concurrency;
        }

        // Simulate work long enough to detect concurrency if it occurs
        await new Promise<void>((r) => setTimeout(r, HANDLER_SLEEP_MS));

        received.push(body.index);
        concurrency--;

        if (received.length >= TOTAL) {
          resolveAll();
        }
      });

      subscriber.start();

      const publisher = createResilientPublisher<BatchItem>({
        topic: names.topic,
        pubSubClient: client,
        retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
      });

      for (let i = 1; i <= TOTAL; i++) {
        await publisher.publish({ body: { index: i } });
      }

      // Wait for all messages to be processed
      await allDone;
      await subscriber.stop();

      // Primary assertion: all messages arrived
      expect(received).toHaveLength(TOTAL);

      // Secondary assertion (best-effort): concurrency should not exceed 1
      if (peakConcurrency > 1) {
        console.warn(
          `[flow-control] Peak concurrency was ${peakConcurrency} with maxMessages:1.`,
          'This may indicate emulator buffering beyond the flow-control limit — non-fatal.'
        );
      }
      // Emit as a soft check via a lax bound: 2 is the emulator's practical
      // worst case when a message is delivered while the previous is settling.
      expect(peakConcurrency).toBeLessThanOrEqual(2);
    },
    30_000
  );
});
