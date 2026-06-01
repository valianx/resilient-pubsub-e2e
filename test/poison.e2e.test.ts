/**
 * Suite: poison
 *
 * Verifies the poison-message path: when deserialization fails, the handler is
 * NOT invoked and the `onPoison` hook fires.
 *
 * Strategy:
 *   - Configure the subscriber with a JSON serializer (the default).
 *   - Publish a raw garbage payload using the native topic.publishMessage() API
 *     so we control the bytes exactly (not valid JSON).
 *   - Assert: onPoison fires, the handler is never called, and the message is
 *     nacked (proven by the onNack hook NOT firing — only onPoison fires for
 *     deserialization failures, not onNack).
 *
 * The emulator redelivers nacked messages. To prevent the test from running
 * indefinitely, the subscriber is stopped as soon as onPoison fires. The test
 * uses a brief timeout to assert the handler did NOT fire after that point.
 */

import { describe, it, beforeAll, afterAll, expect } from 'vitest';
import { PubSub } from '@google-cloud/pubsub';
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

interface ValidPayload {
  value: string;
}

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('poison');
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

describe('poison message detection', () => {
  it(
    'fires onPoison and does not invoke the handler when deserialization fails',
    async () => {
      let handlerInvoked = false;
      let onNackInvoked = false;
      let poisonMessageId: string | undefined;
      let poisonError: unknown;

      let resolvePoisoned!: () => void;
      const poisonedPromise = new Promise<void>((resolve) => {
        resolvePoisoned = resolve;
      });

      const subscriber = createResilientSubscriber<ValidPayload>({
        subscription: names.sub,
        pubSubClient: client,
        // Default JsonSerializer — will throw on non-JSON bytes
        flowControl: { maxMessages: 1 },
        hooks: {
          onPoison: ({ messageId, error }) => {
            poisonMessageId = messageId;
            poisonError = error;
            resolvePoisoned();
          },
          onNack: () => {
            // onNack is NOT expected for deserialization failures —
            // only onPoison fires on that path
            onNackInvoked = true;
          },
        },
      });

      subscriber.on(async () => {
        handlerInvoked = true;
      });

      subscriber.start();

      // Publish a raw buffer that is not valid JSON using the native API.
      // We use the native topic handle directly to bypass the resilient
      // publisher's serializer (which would produce valid JSON).
      const topic = client.topic(names.topic);
      await topic.publishMessage({
        data: Buffer.from('this-is-not-valid-json-\x00\x01\x02', 'binary'),
      });

      // Wait for onPoison to fire
      await poisonedPromise;

      // Give a short window to ensure the handler does not fire asynchronously
      await new Promise<void>((r) => setTimeout(r, 300));

      await subscriber.stop();

      // Core assertions
      expect(handlerInvoked).toBe(false);
      expect(onNackInvoked).toBe(false);
      expect(poisonError).toBeDefined();

      // messageId is optional — the emulator may or may not set it
      if (poisonMessageId !== undefined) {
        expect(typeof poisonMessageId).toBe('string');
      }
    },
    30_000
  );
});
