/**
 * Suite: hooks
 *
 * Verifies observability hooks fire correctly against the real emulator.
 *
 * Hooks tested:
 *   - Publisher `onPublish`: fires on a successful publish with a non-empty messageId.
 *   - Subscriber `onAck`: fires after the handler resolves successfully.
 *   - Subscriber `onNack`: fires when the handler throws.
 *   - Subscriber `onError`: fires when the handler throws (with ResilientPubSubError).
 *
 * Publisher `onRetry`: is not tested here because forcing a transient failure
 * deterministically against the emulator is unreliable (NOT_FOUND is classified
 * as permanent, not transient). The retry path is covered by unit tests in the
 * library's own test suite. We document this limitation.
 *
 * Each hook scenario uses an isolated promise-resolved-in-hook pattern so that
 * assertions are non-racy.
 */

import { describe, it, beforeAll, afterAll, expect } from 'vitest';
import { PubSub } from '@google-cloud/pubsub';
import { createResilientPublisher } from 'resilient-pubsub/publisher';
import { createResilientSubscriber } from 'resilient-pubsub/subscriber';
import { isResilientPubSubError } from 'resilient-pubsub/errors';
import {
  createClient,
  uniqueNames,
  ensureTopic,
  ensureSubscription,
  deleteSub,
  deleteTopic,
} from '../lib/harness.js';

// ── Types ────────────────────────────────────────────────────────────────────

interface SampleEvent {
  id: string;
}

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('hooks');
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

describe('observability hooks', () => {
  it(
    'publisher onPublish fires with a non-empty messageId on success',
    async () => {
      let hookedMessageId: string | undefined;

      let resolveHook!: () => void;
      const hookFired = new Promise<void>((resolve) => {
        resolveHook = resolve;
      });

      const publisher = createResilientPublisher<SampleEvent>({
        topic: names.topic,
        pubSubClient: client,
        retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
        hooks: {
          onPublish: ({ messageId }) => {
            hookedMessageId = messageId;
            resolveHook();
          },
        },
      });

      const result = await publisher.publish({ body: { id: 'hook-pub-1' } });

      // onPublish is called synchronously inside publish before it returns,
      // but we await the promise defensively to avoid races.
      await hookFired;

      expect(typeof hookedMessageId).toBe('string');
      expect(hookedMessageId!.length).toBeGreaterThan(0);
      // The hook should report the same messageId as the publish result
      expect(hookedMessageId).toBe(result.messageId);
    },
    30_000
  );

  it(
    'subscriber onAck fires after a successful handler resolution',
    async () => {
      let ackedMessageId: string | undefined;

      let resolveAck!: () => void;
      const ackFired = new Promise<void>((resolve) => {
        resolveAck = resolve;
      });

      const subscriber = createResilientSubscriber<SampleEvent>({
        subscription: names.sub,
        pubSubClient: client,
        flowControl: { maxMessages: 1 },
        hooks: {
          onAck: ({ messageId }) => {
            ackedMessageId = messageId;
            resolveAck();
          },
        },
      });

      subscriber.on(async () => {
        // Handler resolves — message should be acked
      });

      subscriber.start();

      const publisher = createResilientPublisher<SampleEvent>({
        topic: names.topic,
        pubSubClient: client,
        retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
      });

      await publisher.publish({ body: { id: 'hook-ack-1' } });

      await ackFired;
      await subscriber.stop();

      // messageId is optional per the hook signature; assert its shape when present
      if (ackedMessageId !== undefined) {
        expect(typeof ackedMessageId).toBe('string');
        expect(ackedMessageId.length).toBeGreaterThan(0);
      } else {
        // The emulator may not set messageId in all versions — non-fatal
        expect(ackedMessageId).toBeUndefined();
      }
    },
    30_000
  );

  it(
    'subscriber onNack and onError fire when the handler throws',
    async () => {
      let nackedMessageId: string | undefined;
      let errorReceived: unknown;

      let resolveNack!: () => void;
      const nackFired = new Promise<void>((resolve) => {
        resolveNack = resolve;
      });

      // Stop after first nack to avoid the emulator redelivering indefinitely
      let hookFiredOnce = false;

      const subscriber = createResilientSubscriber<SampleEvent>({
        subscription: names.sub,
        pubSubClient: client,
        flowControl: { maxMessages: 1 },
        hooks: {
          onError: (err) => {
            if (!hookFiredOnce) {
              errorReceived = err;
            }
          },
          onNack: ({ messageId }) => {
            if (!hookFiredOnce) {
              hookFiredOnce = true;
              nackedMessageId = messageId;
              resolveNack();
            }
          },
        },
      });

      subscriber.on(async () => {
        throw new Error('intentional handler failure for onNack test');
      });

      subscriber.start();

      const publisher = createResilientPublisher<SampleEvent>({
        topic: names.topic,
        pubSubClient: client,
        retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
      });

      await publisher.publish({ body: { id: 'hook-nack-1' } });

      await nackFired;
      await subscriber.stop();

      // onError must have received a ResilientPubSubError with kind:'process'
      expect(isResilientPubSubError(errorReceived)).toBe(true);
      if (isResilientPubSubError(errorReceived)) {
        expect(errorReceived.kind).toBe('process');
      }

      // onNack messageId is optional — assert shape when present
      if (nackedMessageId !== undefined) {
        expect(typeof nackedMessageId).toBe('string');
      }
    },
    30_000
  );
});
