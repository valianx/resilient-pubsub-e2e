/**
 * Suite: serializer-custom
 *
 * Verifies that a custom Serializer<T> implementation can be injected into both
 * the publisher and subscriber for a complete round-trip.
 *
 * The custom serializer used here is a base64-wrapped JSON format with a custom
 * content-type of 'application/x-base64json'. The publisher encodes bodies to
 * base64(JSON), the subscriber decodes them, and the test asserts:
 *   1. The round-trip body equals the original.
 *   2. The `content-type` attribute on the received message reflects the custom
 *      contentType (verified via the publisher's onPublish hook + a separate raw
 *      pull using a low-level subscriber client to inspect attributes directly).
 *
 * The `content-type` attribute is set by the library's buildAttributes() helper
 * using `serializer.contentType`, so verifying it end-to-end proves the custom
 * serializer is wired correctly throughout the envelope.
 */

import { describe, it, beforeAll, afterAll, expect } from 'vitest';
import { PubSub, v1 } from '@google-cloud/pubsub';
import { createResilientPublisher } from 'resilient-pubsub/publisher';
import { createResilientSubscriber } from 'resilient-pubsub/subscriber';
import type { Serializer } from 'resilient-pubsub/envelope';
import { SerializationError } from 'resilient-pubsub/envelope';
import {
  PROJECT_ID,
  createClient,
  uniqueNames,
  ensureTopic,
  ensureSubscription,
  deleteSub,
  deleteTopic,
} from '../lib/harness.js';

// ── Types ────────────────────────────────────────────────────────────────────

interface UserProfile {
  userId: string;
  email: string;
}

// ── Custom serializer ────────────────────────────────────────────────────────

/**
 * Base64-encoded JSON serializer.
 *
 * serialize:   JSON.stringify → UTF-8 bytes → base64 string → UTF-8 bytes
 * deserialize: UTF-8 bytes → base64 string → original JSON string → parse
 *
 * Content-type: 'application/x-base64json'
 */
const base64JsonSerializer: Serializer<UserProfile> = {
  contentType: 'application/x-base64json',

  serialize(body: UserProfile): Uint8Array {
    const json = JSON.stringify(body);
    const b64 = Buffer.from(json, 'utf8').toString('base64');
    return Buffer.from(b64, 'utf8');
  },

  deserialize(data: Uint8Array): UserProfile {
    const b64 = Buffer.from(data).toString('utf8');
    let json: string;
    try {
      json = Buffer.from(b64, 'base64').toString('utf8');
    } catch (cause) {
      throw new SerializationError('Failed to base64-decode message payload', cause);
    }
    try {
      return JSON.parse(json) as UserProfile;
    } catch (cause) {
      throw new SerializationError('Failed to JSON-parse base64-decoded payload', cause);
    }
  },
};

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('custom-ser');
let client: PubSub;
let subFqn: string;

beforeAll(async () => {
  client = createClient();
  subFqn = `projects/${PROJECT_ID}/subscriptions/${names.sub}`;
  await ensureTopic(client, names.topic);
  await ensureSubscription(client, names.topic, names.sub);
});

afterAll(async () => {
  await deleteSub(client, names.sub);
  await deleteTopic(client, names.topic);
  await client.close();
});

// ── Tests ────────────────────────────────────────────────────────────────────

describe('custom serializer round-trip', () => {
  it(
    'delivers the body correctly through a custom base64json serializer',
    async () => {
      const payload: UserProfile = { userId: 'u-001', email: 'test@example.com' };

      const publisher = createResilientPublisher<UserProfile>({
        topic: names.topic,
        pubSubClient: client,
        serializer: base64JsonSerializer,
        retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
      });

      let resolveBody!: (body: UserProfile) => void;
      const bodyPromise = new Promise<UserProfile>((resolve) => {
        resolveBody = resolve;
      });

      const subscriber = createResilientSubscriber<UserProfile>({
        subscription: names.sub,
        pubSubClient: client,
        serializer: base64JsonSerializer,
        flowControl: { maxMessages: 1 },
      });

      subscriber.on(async ({ body }) => {
        resolveBody(body);
      });

      subscriber.start();

      await publisher.publish({ body: payload });

      const received = await bodyPromise;
      await subscriber.stop();

      expect(received.userId).toBe(payload.userId);
      expect(received.email).toBe(payload.email);
    },
    30_000
  );

  it(
    'sets content-type attribute to the custom serializer contentType',
    async () => {
      // Publish a message using the custom serializer and inspect the raw
      // attributes via a low-level synchronous pull to verify content-type.
      const payload: UserProfile = { userId: 'u-002', email: 'attr@example.com' };

      const publisher = createResilientPublisher<UserProfile>({
        topic: names.topic,
        pubSubClient: client,
        serializer: base64JsonSerializer,
        retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
      });

      await publisher.publish({ body: payload });

      // Use the low-level v1 SubscriberClient to pull the message with its
      // raw attributes intact, without going through the resilient subscriber.
      const rawConfig = await client.getClientConfig();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const rawClient = new v1.SubscriberClient(rawConfig as any);

      const POLL_INTERVAL_MS = 500;
      const POLL_TIMEOUT_MS = 15_000;
      const deadline = Date.now() + POLL_TIMEOUT_MS;

      let contentTypeAttribute: string | undefined;

      try {
        while (Date.now() < deadline && contentTypeAttribute === undefined) {
          const [response] = await rawClient.pull({
            subscription: subFqn,
            maxMessages: 1,
            returnImmediately: true,
          });

          const messages = response.receivedMessages ?? [];

          if (messages.length > 0) {
            const msg = messages[0]!;
            const attrs = msg.message?.attributes ?? {};
            // attrs is typed as { [k: string]: string } by the proto
            contentTypeAttribute = (attrs as Record<string, string>)['content-type'];

            // Acknowledge so the message does not redeliver
            if (msg.ackId) {
              await rawClient.acknowledge({
                subscription: subFqn,
                ackIds: [msg.ackId],
              });
            }
          } else {
            await new Promise<void>((r) => setTimeout(r, POLL_INTERVAL_MS));
          }
        }
      } finally {
        await rawClient.close();
      }

      expect(contentTypeAttribute).toBe('application/x-base64json');
    },
    30_000
  );
});
