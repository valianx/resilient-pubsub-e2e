/**
 * Suite: retry-publish
 *
 * Verifies publish behavior:
 *   1. A normal publish resolves with a non-empty messageId (happy path).
 *   2. A publish targeting a non-existent topic rejects with
 *      ResilientPubSubError{ kind: 'publish' }.
 *
 * The second assertion uses a bad topic name to force a deterministic failure.
 * The emulator returns NOT_FOUND (gRPC 5), which the library classifies as
 * 'permanent', so it rejects immediately without consuming the retry budget.
 *
 * Note: the library classifies NOT_FOUND as 'permanent', meaning it rejects on
 * the first attempt rather than exhausting all retries. This is correct behavior
 * (no point retrying a missing topic). We set maxAttempts: 1 in the forced-
 * failure case to make the test deterministic and fast.
 */

import { describe, it, beforeAll, afterAll, expect } from 'vitest';
import { PubSub } from '@google-cloud/pubsub';
import { createResilientPublisher } from 'resilient-pubsub/publisher';
import { isResilientPubSubError } from 'resilient-pubsub/errors';
import {
  createClient,
  uniqueNames,
  ensureTopic,
  deleteTopic,
} from '../lib/harness.js';

// ── Types ────────────────────────────────────────────────────────────────────

interface Event {
  eventId: string;
}

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('retry-pub');
let client: PubSub;

beforeAll(async () => {
  client = createClient();
  await ensureTopic(client, names.topic);
});

afterAll(async () => {
  await deleteTopic(client, names.topic);
  await client.close();
});

// ── Tests ────────────────────────────────────────────────────────────────────

describe('publish retry behavior', () => {
  it('resolves with a messageId on a successful publish', async () => {
    const publisher = createResilientPublisher<Event>({
      topic: names.topic,
      pubSubClient: client,
      retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
    });

    const result = await publisher.publish({ body: { eventId: 'evt-001' } });

    expect(typeof result.messageId).toBe('string');
    expect(result.messageId.length).toBeGreaterThan(0);
  });

  it('rejects with ResilientPubSubError when the topic does not exist', async () => {
    const publisher = createResilientPublisher<Event>({
      topic: 'this-topic-does-not-exist-e2e',
      pubSubClient: client,
      // maxAttempts: 1 makes the test fast — permanent errors reject immediately anyway
      retry: { maxAttempts: 1, initialDelay: 50, jitter: 'none' },
    });

    let caught: unknown;
    try {
      await publisher.publish({ body: { eventId: 'evt-002' } });
    } catch (err) {
      caught = err;
    }

    expect(caught).toBeDefined();
    expect(isResilientPubSubError(caught)).toBe(true);

    if (isResilientPubSubError(caught)) {
      expect(caught.kind).toBe('publish');
    }
  });
});
