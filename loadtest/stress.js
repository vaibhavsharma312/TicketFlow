// loadtest/stress.js
// Usage: node loadtest/stress.js [concurrency] [totalRequests]
// Example: node loadtest/stress.js 50 200

const http = require('http');

const CONCURRENCY = parseInt(process.argv[2]) || 50;
const TOTAL = parseInt(process.argv[3]) || 200;
const API_HOST = 'localhost';
const API_PORT = 3000;

// We'll hit both endpoints to compare
const ENDPOINTS = [
  { path: '/api/book', label: 'PESSIMISTIC' },
  { path: '/api/book-optimistic', label: 'OPTIMISTIC' },
];

function makeRequest(path, seatId, requestId) {
  return new Promise((resolve) => {
    const startTime = Date.now();
    const body = JSON.stringify({
      userId: 1,
      eventId: 1,
      seatId: seatId,
      idempotencyKey: `load-${path}-${requestId}-${Date.now()}`,
    });

    const options = {
      hostname: API_HOST,
      port: API_PORT,
      path: path,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    };

    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        resolve({
          status: res.statusCode,
          duration: Date.now() - startTime,
          body: data,
        });
      });
    });

    req.on('error', (err) => {
      resolve({
        status: 0,
        duration: Date.now() - startTime,
        error: err.message,
      });
    });

    req.write(body);
    req.end();
  });
}

async function seedFreshData() {
  return new Promise((resolve, reject) => {
    const { exec } = require('child_process');
    exec('npm run seed', (err, stdout, stderr) => {
      if (err) reject(err);
      else resolve(stdout);
    });
  });
}

async function runLoadTest(endpoint, seatStart) {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`  Load Test: ${endpoint.label} (${endpoint.path})`);
  console.log(`  Concurrency: ${CONCURRENCY} | Total requests: ${TOTAL}`);
  console.log(`  Seats targeted: ${seatStart} to ${seatStart + 9} (10 seats)`);
  console.log(`${'═'.repeat(60)}\n`);

  const results = { 201: 0, 409: 0, 200: 0, 500: 0, 0: 0 };
  const durations = [];
  let completed = 0;

  // Create all request promises
  const promises = [];
  for (let i = 0; i < TOTAL; i++) {
    const seatId = seatStart + (i % 10); // distribute across 10 seats
    promises.push(
      makeRequest(endpoint.path, seatId, i).then((res) => {
        completed++;
        results[res.status] = (results[res.status] || 0) + 1;
        durations.push(res.duration);

        // Progress bar
        if (completed % 20 === 0 || completed === TOTAL) {
          const pct = Math.round((completed / TOTAL) * 100);
          process.stdout.write(`\r  Progress: ${completed}/${TOTAL} (${pct}%)`);
        }
      })
    );
  }

  // Run in batches of CONCURRENCY
  const startTime = Date.now();
  for (let i = 0; i < promises.length; i += CONCURRENCY) {
    await Promise.all(promises.slice(i, i + CONCURRENCY));
  }
  const totalTime = Date.now() - startTime;

  // Stats
  durations.sort((a, b) => a - b);
  const avg = Math.round(durations.reduce((a, b) => a + b, 0) / durations.length);
  const p50 = durations[Math.floor(durations.length * 0.5)];
  const p95 = durations[Math.floor(durations.length * 0.95)];
  const p99 = durations[Math.floor(durations.length * 0.99)];
  const rps = Math.round((TOTAL / totalTime) * 1000);

  console.log('\n');
  console.log('  ┌─────────────────────────────────────┐');
  console.log('  │           RESULTS SUMMARY            │');
  console.log('  ├─────────────────────────────────────┤');
  console.log(`  │  201 Created (booked):    ${String(results[201] || 0).padStart(8)} │`);
  console.log(`  │  409 Conflict (rejected): ${String(results[409] || 0).padStart(8)} │`);
  console.log(`  │  200 Idempotent (retry):  ${String(results[200] || 0).padStart(8)} │`);
  console.log(`  │  500 Server error:        ${String(results[500] || 0).padStart(8)} │`);
  console.log(`  │  0   Network error:       ${String(results[0] || 0).padStart(8)} │`);
  console.log('  ├─────────────────────────────────────┤');
  console.log(`  │  Total time:         ${String(totalTime + 'ms').padStart(12)} │`);
  console.log(`  │  Requests/sec:       ${String(rps).padStart(12)} │`);
  console.log(`  │  Avg latency:        ${String(avg + 'ms').padStart(12)} │`);
  console.log(`  │  P50 latency:        ${String(p50 + 'ms').padStart(12)} │`);
  console.log(`  │  P95 latency:        ${String(p95 + 'ms').padStart(12)} │`);
  console.log(`  │  P99 latency:        ${String(p99 + 'ms').padStart(12)} │`);
  console.log('  └─────────────────────────────────────┘');

  // Verify data integrity
  const expectedBookings = results[201] || 0;
  console.log(`\n  Expected exactly 10 successful bookings (10 seats).`);
  console.log(`  Got: ${expectedBookings} bookings.`);
  if (expectedBookings === 10) {
    console.log('  ✅ PASS — No double bookings, no missed bookings!');
  } else if (expectedBookings < 10) {
    console.log('  ⚠️  Fewer than 10 — some seats may have had conflict retries exhausted');
  } else {
    console.log('  ❌ FAIL — More than 10 bookings = DOUBLE BOOKING detected!');
  }

  return results;
}

async function main() {
  console.log('TicketFlow Load Test');
  console.log(`Node.js ${process.version}`);

  // --- Test 1: Pessimistic ---
  console.log('\n🔄 Seeding fresh data for pessimistic test...');
  await seedFreshData();
  await runLoadTest(ENDPOINTS[0], 1);

  // --- Test 2: Optimistic ---
  console.log('\n\n🔄 Seeding fresh data for optimistic test...');
  await seedFreshData();
  await runLoadTest(ENDPOINTS[1], 1);

  // --- Verify in DB ---
  console.log('\n\n📊 Final DB verification...');
  const { Pool } = require('pg');
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL ||
      'postgres://ticketflow:ticketflow@localhost:5433/ticketflow',
  });
  const seats = await pool.query(
    "SELECT seat_number, status, version FROM seats WHERE event_id = 1 ORDER BY id"
  );
  console.log('\n  Seat Status After Optimistic Test:');
  console.log('  ┌──────────┬───────────┬─────────┐');
  console.log('  │ Seat     │ Status    │ Version │');
  console.log('  ├──────────┼───────────┼─────────┤');
  for (const row of seats.rows) {
    console.log(
      `  │ ${row.seat_number.padEnd(8)} │ ${row.status.padEnd(9)} │ ${String(row.version).padStart(7)} │`
    );
  }
  console.log('  └──────────┴───────────┴─────────┘');
  await pool.end();

  console.log('\n✅ Load test complete!\n');
}

main().catch((err) => {
  console.error('Load test failed:', err);
  process.exit(1);
});
