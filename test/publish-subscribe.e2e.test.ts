/**
 * Suite: publish-subscribe
 *
 * Verifies the fundamental round-trip: publish({ body }) → subscriber receives
 * the same typed body → handler resolves → message is acked.
 *
 * Assertion strategy:
 *   - Resolve a Promise inside the handler; await it with a finite timeout.
 *   - If the promise resolves with the expected body, the test passes.
 *   - Call stop() after the first message is received.
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

interface OrderCreated {
  orderId: string;
  amount: number;
}

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('pub-sub');
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

describe('publish → subscribe round-trip', () => {
  it('delivers the published body to the subscriber handler', async () => {
    const body: OrderCreated = { orderId: 'ord-001', amount: 49.99 };

    const publisher = createResilientPublisher<OrderCreated>({
      topic: names.topic,
      pubSubClient: client,
      retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
    });

    // Promise that resolves when the handler runs with the expected message.
    let resolveReceived!: (msg: OrderCreated) => void;
    const receivedPromise = new Promise<OrderCreated>((resolve) => {
      resolveReceived = resolve;
    });

    const subscriber = createResilientSubscriber<OrderCreated>({
      subscription: names.sub,
      pubSubClient: client,
      flowControl: { maxMessages: 1 },
    });

    subscriber.on(async ({ body: received }) => {
      resolveReceived(received);
    });

    subscriber.start();

    // Publish after subscriber is listening
    const result = await publisher.publish({ body });
    expect(typeof result.messageId).toBe('string');
    expect(result.messageId.length).toBeGreaterThan(0);

    // Wait for handler to fire (30 s vitest timeout covers this)
    const received = await receivedPromise;
    expect(received.orderId).toBe(body.orderId);
    expect(received.amount).toBe(body.amount);

    await subscriber.stop();
  });
});
