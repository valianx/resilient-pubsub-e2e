/**
 * Suite: context-propagation
 *
 * Verifies the allowlist-gated propagation model:
 *   - `traceparent` propagates automatically (always in allowlist).
 *   - `x-tenant-id` propagates because it is explicitly allowlisted.
 *   - `x-secret` is DROPPED because it is NOT on the allowlist.
 *
 * Both publisher and subscriber must use the same propagation options for the
 * round-trip to be symmetric (as documented in the library).
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

interface Ping {
  id: string;
}

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('ctx-prop');
let client: PubSub;

const PROPAGATION_OPTS = { allowlist: ['x-tenant-id'] };

// A realistic-looking W3C traceparent
const TRACE_PARENT = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';

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

describe('context propagation with allowlist', () => {
  it('delivers traceparent and x-tenant-id but drops x-secret', async () => {
    const publisher = createResilientPublisher<Ping>({
      topic: names.topic,
      pubSubClient: client,
      propagation: PROPAGATION_OPTS,
      retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
    });

    let resolveHeaders!: (h: Record<string, string>) => void;
    const headersPromise = new Promise<Record<string, string>>((resolve) => {
      resolveHeaders = resolve;
    });

    const subscriber = createResilientSubscriber<Ping>({
      subscription: names.sub,
      pubSubClient: client,
      propagation: PROPAGATION_OPTS,
      flowControl: { maxMessages: 1 },
    });

    subscriber.on(async ({ headers }) => {
      resolveHeaders(headers);
    });

    subscriber.start();

    await publisher.publish({
      body: { id: 'ping-1' },
      headers: {
        traceparent: TRACE_PARENT,
        'x-tenant-id': 'acme-corp',
        'x-secret': 'super-secret-value',
      },
    });

    const headers = await headersPromise;

    // W3C trace header must propagate automatically
    expect(headers['traceparent']).toBe(TRACE_PARENT);

    // Allowlisted business header must propagate
    expect(headers['x-tenant-id']).toBe('acme-corp');

    // Non-allowlisted header must be absent
    expect(headers['x-secret']).toBeUndefined();

    await subscriber.stop();
  });
});
