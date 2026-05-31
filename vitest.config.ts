import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Each suite runs sequentially to avoid cross-suite Pub/Sub race conditions
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },
    testTimeout: 30_000,
    hookTimeout: 30_000,
    // Run test files in the test/ directory
    include: ['test/**/*.e2e.test.ts'],
    // Explicit reporter for CI clarity
    reporter: ['verbose'],
  },
});
