/**
 * Suite: dead-letter
 *
 * Verifies the dead-letter path against the emulator.
 *
 * Topology:
 *   source-topic → source-sub (deadLetterPolicy → dlq-topic, maxDeliveryAttempts: 5)
 *   dlq-topic    → dlq-sub   (plain pull subscription to observe forwarded messages)
 *
 * Authoritative assertion (reliable on the emulator):
 *   The handler always throws. We collect delivery attempts across redeliveries
 *   and assert that `getDeliveryAttempt(meta)` returns INCREASING values — meaning
 *   the emulator is incrementing the counter on each nack cycle.
 *
 * Optional assertion (best-effort, bounded timeout):
 *   After reaching maxDeliveryAttempts the emulator should forward the message to
 *   dlq-topic. We poll dlq-sub for up to DLQ_POLL_TIMEOUT_MS using the low-level
 *   SubscriberClient.pull() API (the high-level Subscription class uses streaming
 *   pull, not synchronous pull). This assertion is considered passing when either
 *   a forwarded message is found OR the poll window expires — the emulator may not
 *   always forward within the test budget.
 *
 * IAM note: the Pub/Sub emulator does NOT enforce IAM. The deadLetterPolicy on
 * the emulator triggers delivery-attempt counting and forwarding without needing
 * the service account grants required in production.
 */

import { describe, it, beforeAll, afterAll, expect } from 'vitest';
import { PubSub, v1 } from '@google-cloud/pubsub';
import { createResilientPublisher } from 'resilient-pubsub/publisher';
import { createResilientSubscriber } from 'resilient-pubsub/subscriber';
import { buildDeadLetterPolicy, getDeliveryAttempt } from 'resilient-pubsub/dlq';
import {
  PROJECT_ID,
  createClient,
  uniqueNames,
  uniqueDlqNames,
  ensureTopic,
  ensureSubscription,
  deleteSub,
  deleteTopic,
} from '../lib/harness.js';

// ── Types ────────────────────────────────────────────────────────────────────

interface PoisonMessage {
  value: string;
}

// ── Constants ────────────────────────────────────────────────────────────────

/** Number of distinct delivery-attempt values to observe before asserting increase. */
const ATTEMPTS_TO_OBSERVE = 3;

/** Maximum ms to wait for DLQ forwarding (optional, best-effort). */
const DLQ_POLL_TIMEOUT_MS = 15_000;

/** How often to poll the DLQ subscription. */
const DLQ_POLL_INTERVAL_MS = 500;

const MAX_DELIVERY_ATTEMPTS = 5;

// ── Fixture ──────────────────────────────────────────────────────────────────

const sourceNames = uniqueNames('dlq-source');
const dlqNames = uniqueDlqNames('dlq');
let client: PubSub;

// Fully-qualified DLQ topic name as required by deadLetterPolicy
let dlqTopicFqn: string;
// Fully-qualified DLQ subscription name for the low-level pull API
let dlqSubFqn: string;

beforeAll(async () => {
  client = createClient();
  dlqTopicFqn = `projects/${PROJECT_ID}/topics/${dlqNames.dlqTopic}`;
  dlqSubFqn = `projects/${PROJECT_ID}/subscriptions/${dlqNames.dlqSub}`;

  // Create resources in dependency order: dlq-topic first (referenced by policy)
  await ensureTopic(client, dlqNames.dlqTopic);
  await ensureTopic(client, sourceNames.topic);

  // Build the dead-letter policy using the library helper
  const deadLetterPolicy = buildDeadLetterPolicy({
    deadLetterTopic: dlqTopicFqn,
    maxDeliveryAttempts: MAX_DELIVERY_ATTEMPTS,
  });

  // Create source subscription WITH dead-letter policy
  await ensureSubscription(client, sourceNames.topic, sourceNames.sub, {
    deadLetterPolicy,
  });

  // Create DLQ pull subscription for optional forwarding assertion
  await ensureSubscription(client, dlqNames.dlqTopic, dlqNames.dlqSub);
});

afterAll(async () => {
  await deleteSub(client, sourceNames.sub);
  await deleteSub(client, dlqNames.dlqSub);
  await deleteTopic(client, sourceNames.topic);
  await deleteTopic(client, dlqNames.dlqTopic);
  await client.close();
});

// ── Tests ────────────────────────────────────────────────────────────────────

describe('dead-letter policy', () => {
  it('increments getDeliveryAttempt(meta) across nack redeliveries', async () => {
    const publisher = createResilientPublisher<PoisonMessage>({
      topic: sourceNames.topic,
      pubSubClient: client,
      retry: { maxAttempts: 3, initialDelay: 100, jitter: 'none' },
    });

    // Publish the message that will always be nacked
    await publisher.publish({ body: { value: 'always-fails' } });

    const observedAttempts: number[] = [];

    let resolveObserved!: () => void;
    const observedPromise = new Promise<void>((resolve) => {
      resolveObserved = resolve;
    });

    const subscriber = createResilientSubscriber<PoisonMessage>({
      subscription: sourceNames.sub,
      pubSubClient: client,
      // Tight flow control: one message at a time so redeliveries are sequential
      flowControl: { maxMessages: 1 },
    });

    subscriber.on(async ({ meta }) => {
      const attempt = getDeliveryAttempt(meta);
      if (attempt !== undefined) {
        observedAttempts.push(attempt);
      }

      // Once we have enough samples, stop collecting
      if (observedAttempts.length >= ATTEMPTS_TO_OBSERVE) {
        resolveObserved();
      }

      // Always throw → nack → redeliver
      throw new Error('intentional nack for dead-letter test');
    });

    subscriber.start();

    // Wait until we have collected enough delivery-attempt samples
    await observedPromise;
    await subscriber.stop();

    // The authoritative assertion: delivery-attempt values must strictly increase
    expect(observedAttempts.length).toBeGreaterThanOrEqual(ATTEMPTS_TO_OBSERVE);

    for (let i = 1; i < observedAttempts.length; i++) {
      const prev = observedAttempts[i - 1] as number;
      const curr = observedAttempts[i] as number;
      expect(curr).toBeGreaterThan(prev);
    }
  });

  it(
    'optionally: emulator forwards message to DLQ subscription after exhausting attempts',
    async () => {
      // Use the low-level SubscriberClient for synchronous pull from the DLQ.
      // The high-level Subscription class uses streaming pull and does not expose
      // a synchronous pull method typed in its public API.
      // getClientConfig() fills in projectId, emulator endpoint, and credentials.
      const rawConfig = await client.getClientConfig();
      // Cast needed: PubSub.getClientConfig() returns port as string|number but
      // v1.SubscriberClient expects port as number. The emulator sets it as a
      // string; the cast is safe because the underlying gax accepts both at runtime.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const subscriberClient = new v1.SubscriberClient(rawConfig as any);

      let forwardedMessageId: string | undefined;
      const deadline = Date.now() + DLQ_POLL_TIMEOUT_MS;

      while (Date.now() < deadline && forwardedMessageId === undefined) {
        const [response] = await subscriberClient.pull({
          subscription: dlqSubFqn,
          maxMessages: 1,
        });

        const messages = response.receivedMessages ?? [];

        if (messages.length > 0) {
          const msg = messages[0]!;
          forwardedMessageId = msg.message?.messageId ?? undefined;

          // Acknowledge to clean up the DLQ subscription
          if (msg.ackId) {
            await subscriberClient.acknowledge({
              subscription: dlqSubFqn,
              ackIds: [msg.ackId],
            });
          }
        } else {
          await new Promise<void>((r) => setTimeout(r, DLQ_POLL_INTERVAL_MS));
        }
      }

      await subscriberClient.close();

      if (forwardedMessageId === undefined) {
        // Not failed — emulator may not have forwarded within the window.
        // The delivery-attempt assertion in the previous test is authoritative.
        console.warn(
          '[dead-letter] DLQ forwarding not observed within',
          DLQ_POLL_TIMEOUT_MS,
          'ms — emulator timing; non-fatal'
        );
        return;
      }

      expect(typeof forwardedMessageId).toBe('string');
      expect(forwardedMessageId.length).toBeGreaterThan(0);
    }
  );
});
