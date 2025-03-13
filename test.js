const { GlideClient } = require('@valkey/valkey-glide');
const IoValkey = require('iovalkey');
const { performance } = require('perf_hooks');

// Configuration
const HOST = 'localhost';
const PORT = 6379;
const ITERATIONS = 10000; // Number of operations per test
const PIPELINE_SIZE = 100; // Number of commands in a pipeline batch

async function runBenchmark() {
  console.log('Starting Valkey Performance Benchmark');
  console.log(`Testing with ${ITERATIONS} iterations per operation`);
  console.log('----------------------------------------');

  // Initialize clients
  const glideClient = await GlideClient.createClient({
    addresses: [{ host: HOST, port: PORT }]
  });

  const iovalkeyCient = new IoValkey({
    host: HOST,
    port: PORT
  });

  // Wait for connections to be established
  await new Promise(resolve => setTimeout(resolve, 1000));

  // Flush database to ensure clean state
  await glideClient.flushall();

  // Test scenarios
  const tests = [
    { name: 'SET', test: testSet },
    { name: 'GET', test: testGet },
    { name: 'INCR', test: testIncr },
    { name: 'HSET/HGET', test: testHash },
    { name: 'PIPELINE', test: testPipeline },
  ];

  // Run tests
  for (const test of tests) {
    console.log(`\nRunning ${test.name} test:`);
    await test.test(glideClient, iovalkeyCient);
  }

  // Clean up
  await glideClient.flushall();
  glideClient.close();
  iovalkeyCient.disconnect();

  console.log('\nBenchmark complete');
}

// Helper to measure execution time
async function timeExecution(name, fn) {
  const start = performance.now();
  await fn();
  const end = performance.now();
  const duration = end - start;
  console.log(`${name}: ${duration.toFixed(2)}ms (${(ITERATIONS / (duration / 1000)).toFixed(2)} ops/sec)`);
  return duration;
}

// Test basic SET operation
async function testSet(glideClient, iovalkeyCient) {
  // Test Valkey Glide
  const glideTime = await timeExecution('Valkey Glide SET', async () => {
    for (let i = 0; i < ITERATIONS; i++) {
      await glideClient.set(`key:${i}`, `value:${i}`);
    }
  });

  // Test IoValkey
  const iovalkeyCientTime = await timeExecution('IoValkey SET', async () => {
    for (let i = 0; i < ITERATIONS; i++) {
      await iovalkeyCient.set(`key:${i}`, `value:${i}`);
    }
  });

  // Calculate performance difference
  const diff = ((iovalkeyCientTime / glideTime) - 1) * 100;
  console.log(`Valkey Glide is ${diff.toFixed(2)}% ${diff > 0 ? 'faster' : 'slower'} than IoValkey for SET operations`);
}

// Test basic GET operation
async function testGet(glideClient, iovalkeyCient) {
  // Ensure keys exist
  for (let i = 0; i < ITERATIONS; i++) {
    await glideClient.set(`key:${i}`, `value:${i}`);
  }

  // Test Valkey Glide
  const glideTime = await timeExecution('Valkey Glide GET', async () => {
    for (let i = 0; i < ITERATIONS; i++) {
      await glideClient.get(`key:${i}`);
    }
  });

  // Test IoValkey
  const iovalkeyCientTime = await timeExecution('IoValkey GET', async () => {
    for (let i = 0; i < ITERATIONS; i++) {
      await iovalkeyCient.get(`key:${i}`);
    }
  });

  // Calculate performance difference
  const diff = ((iovalkeyCientTime / glideTime) - 1) * 100;
  console.log(`Valkey Glide is ${diff.toFixed(2)}% ${diff > 0 ? 'faster' : 'slower'} than IoValkey for GET operations`);
}

// Test INCR operation
async function testIncr(glideClient, iovalkeyCient) {
  // Reset counter
  await glideClient.set('counter:glide', '0');
  await glideClient.set('counter:iovalkey', '0');

  // Test Valkey Glide
  const glideTime = await timeExecution('Valkey Glide INCR', async () => {
    for (let i = 0; i < ITERATIONS; i++) {
      await glideClient.incr('counter:glide');
    }
  });

  // Test IoValkey
  const iovalkeyCientTime = await timeExecution('IoValkey INCR', async () => {
    for (let i = 0; i < ITERATIONS; i++) {
      await iovalkeyCient.incr('counter:iovalkey');
    }
  });

  // Calculate performance difference
  const diff = ((iovalkeyCientTime / glideTime) - 1) * 100;
  console.log(`Valkey Glide is ${diff.toFixed(2)}% ${diff > 0 ? 'faster' : 'slower'} than IoValkey for INCR operations`);
}

// Test Hash operations (HSET/HGET)
async function testHash(glideClient, iovalkeyCient) {
  // Test Valkey Glide
  const glideTime = await timeExecution('Valkey Glide HSET/HGET', async () => {
    for (let i = 0; i < ITERATIONS; i++) {
      const key = `hash:${Math.floor(i / 100)}`;
      const field = `field:${i % 100}`;
      const value = `value:${i}`;
      await glideClient.hset(key, field, value);
      await glideClient.hget(key, field);
    }
  });

  // Test IoValkey
  const iovalkeyCientTime = await timeExecution('IoValkey HSET/HGET', async () => {
    for (let i = 0; i < ITERATIONS; i++) {
      const key = `hash:${Math.floor(i / 100)}`;
      const field = `field:${i % 100}`;
      const value = `value:${i}`;
      await iovalkeyCient.hset(key, field, value);
      await iovalkeyCient.hget(key, field);
    }
  });

  // Calculate performance difference
  const diff = ((iovalkeyCientTime / glideTime) - 1) * 100;
  console.log(`Valkey Glide is ${diff.toFixed(2)}% ${diff > 0 ? 'faster' : 'slower'} than IoValkey for Hash operations`);
}

// Test Pipeline operations
async function testPipeline(glideClient, iovalkeyCient) {
  const totalOps = ITERATIONS;
  const batchSize = PIPELINE_SIZE;
  const batches = Math.ceil(totalOps / batchSize);

  // Create Transaction for Valkey Glide
  const { Transaction } = require('@valkey/valkey-glide');
  
  // Test Valkey Glide
  const glideTime = await timeExecution('Valkey Glide Pipeline', async () => {
    for (let b = 0; b < batches; b++) {
      const transaction = new Transaction();
      
      for (let i = 0; i < batchSize && b * batchSize + i < totalOps; i++) {
        const index = b * batchSize + i;
        transaction.set(`pipeline:${index}`, `value:${index}`);
      }
      
      await glideClient.exec(transaction);
    }
  });

  // Test IoValkey
  const iovalkeyCientTime = await timeExecution('IoValkey Pipeline', async () => {
    for (let b = 0; b < batches; b++) {
      const pipeline = iovalkeyCient.pipeline();
      
      for (let i = 0; i < batchSize && b * batchSize + i < totalOps; i++) {
        const index = b * batchSize + i;
        pipeline.set(`pipeline:${index}`, `value:${index}`);
      }
      
      await pipeline.exec();
    }
  });

  // Calculate performance difference
  const diff = ((iovalkeyCientTime / glideTime) - 1) * 100;
  console.log(`Valkey Glide is ${diff.toFixed(2)}% ${diff > 0 ? 'faster' : 'slower'} than IoValkey for Pipeline operations`);
}

// Run the benchmark
runBenchmark().catch(error => {
  console.error('Benchmark error:', error);
  process.exit(1);
});