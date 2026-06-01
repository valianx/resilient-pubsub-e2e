/**
 * Suite: redelivery
 *
 * Verifies the at-least-once delivery contract WITHOUT a dead-letter policy:
 *
 *   1. The handler throws on the first delivery → message is nacked.
 *   2. The emulator redelivers the message.
 *   3. The handler succeeds on the second delivery → message is acked.
 *
 * This proves the nack→redeliver→reprocess path that underpins idempotent
 * consumer patterns (the fundamental at-least-once guarantee).
 *
 * Implementation:
 *   - A delivery counter in the handler tracks attempts.
 *   - Attempt 1: throw → nack.
 *   - Attempt 2+: resolve → ack → signal the test.
 *   - The test resolves the awaited promise only after a successful ack.
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

interface RetryableEvent {
  taskId: string;
}

// ── Fixture ──────────────────────────────────────────────────────────────────

const names = uniqueNames('redelivery');
let client: PubSub;

beforeAll(async () => {
  client = createClient();
  await ensureTopic(client, names.topic);
  // Plain subscription — no dead-letter policy
  await ensureSubscription(client, names.topic, names.sub);
});

afterAll(async () => {
  await deleteSub(client, names.sub);
  await deleteTopic(client, names.topic);
  await client.close();
});

// ── Tests ────────────────────────────────────────────────────────────────────

describe('at-least-once redelivery', () => {
  it(
    'redelivers and eventually acks a nacked message (nack → redeliver → success)',
    async () => {
      let deliveryCount = 0;
      let successfulDeliveryCount = 0;

      let resolveSuccess!: () => void;
      const successPromise = new Promise<void>((resolve) => {
        resolveSuccess = resolve;
      });

      const subscriber = createResilientSubscriber<RetryableEvent>({
        subscription: names.sub,
        pubSubClient: client,
        // Tight flow control: process one message at a time so redeliveries
        // arrive in a controlled manner without queue build-up
        flowControl: { maxMessages: 1 },
      });

      subscriber.on(async ({ body }) => {
        deliveryCount++;

        if (deliveryCount === 1) {
          // First delivery: throw to trigger nack → redeliver
          throw new Error(`[redelivery] First delivery of ${body.taskId} — intentional nack`);
        }

        // Second delivery (or later): succeed → ack
        successfulDeliveryCount++;
        resolveSuccess();
      });

      subscriber.start();

      const publisher = createResilientPublisher<RetryableEvent>({
        topic: names.topic,
        pubSubClient: client,
        retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
      });

      await publisher.publish({ body: { taskId: 'task-001' } });

      // Wait for the message to be successfully processed after redelivery
      await successPromise;
      await subscriber.stop();

      // At least 2 deliveries: the initial nack + the successful redeliver
      expect(deliveryCount).toBeGreaterThanOrEqual(2);
      // Exactly one successful (acked) delivery
      expect(successfulDeliveryCount).toBe(1);
    },
    30_000
  );
});
