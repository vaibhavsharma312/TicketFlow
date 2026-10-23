// loadtest/deadlock.js
//
// Proves that acquiring row locks in a consistent (ascending) order prevents
// deadlocks in multi-seat bookings.
//
// The setup: two requests arrive at the same instant wanting the SAME seats,
// but listed in OPPOSITE order.
//
//   Request A wants [1, 2, 3, 4, 5]
//   Request B wants [5, 4, 3, 2, 1]
//
// UNSAFE  — each transaction locks in the order it was given. A holds seat 1
//           and waits for 5; B holds seat 5 and waits for 1. Cycle → Postgres
//           kills one transaction with SQLSTATE 40P01 (deadlock_detected).
//
// SAFE    — the service sorts ids ascending first, so BOTH transactions try
//           1, 2, 3, 4, 5 in that order. B simply queues behind A on seat 1.
//           No cycle is possible, so no deadlock. One wins, one gets 409.
//
// Usage:  node loadtest/deadlock.js
//         node loadtest/deadlock.js 6      (6 competing request pairs)

const http = require('http');

const PAIRS = parseInt(process.argv[2]) || 4;
const HOST = 'localhost';
const PORT = 3000;

function request(path, body) {
  return new Promise((resolve) => {
    const payload = JSON.stringify(body);
    const req = http.request(
      {
        hostname: HOST,
        port: PORT,
        path,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload),
        },
      },
      (res) => {
        let data = '';
        res.on('data', (c) => (data += c));
        res.on('end', () => {
          let parsed = {};
          try { parsed = JSON.parse(data); } catch {}
          resolve({ status: res.statusCode, body: parsed });
        });
      }
    );
    req.on('error', (e) => resolve({ status: 0, body: { error: e.message } }));
    req.write(payload);
    req.end();
  });
}

function reset() {
  return request('/api/admin/reset', {});
}

async function runRound(label, unsafe) {
  await reset();
  await new Promise((r) => setTimeout(r, 600));

  const path = unsafe ? '/api/book-multiple?unsafe=1' : '/api/book-multiple';
  const ascending = [1, 2, 3, 4, 5];
  const descending = [5, 4, 3, 2, 1];

  console.log(`\n${'═'.repeat(62)}`);
  console.log(`  ${label}`);
  console.log(`  ${PAIRS} pairs of requests, each pair wants the same 5 seats`);
  console.log(`  in OPPOSITE order:  [1,2,3,4,5]  vs  [5,4,3,2,1]`);
  console.log(`${'═'.repeat(62)}\n`);

  const promises = [];
  for (let i = 0; i < PAIRS; i++) {
    promises.push(
      request(path, {
        userId: 1, eventId: 1, seatIds: ascending,
        idempotencyKey: `dl-asc-${i}-${Date.now()}`,
      })
    );
    promises.push(
      request(path, {
        userId: 2, eventId: 1, seatIds: descending,
        idempotencyKey: `dl-desc-${i}-${Date.now()}`,
      })
    );
  }

  const t0 = Date.now();
  const results = await Promise.all(promises);
  const ms = Date.now() - t0;

  let booked = 0, conflict = 0, deadlock = 0, error = 0;

  for (const r of results) {
    if (r.status === 201) booked++;
    else if (r.status === 409 && r.body.deadlock) deadlock++;
    else if (r.status === 409) conflict++;
    else error++;
  }

  console.log(`  Completed in ${ms}ms\n`);
  console.log(`  ✅ 201 Booked (got all 5 seats):   ${booked}`);
  console.log(`  🚫 409 Conflict (seats taken):     ${conflict}`);
  console.log(`  💀 409 Deadlock (SQLSTATE 40P01):  ${deadlock}`);
  console.log(`  ❌ Other errors:                    ${error}\n`);

  if (deadlock > 0) {
    console.log(`  → Postgres detected ${deadlock} deadlock(s) and aborted those`);
    console.log(`    transactions. This is what inconsistent lock ordering causes.`);
  } else {
    console.log(`  → Zero deadlocks. Every transaction locked seats in the same`);
    console.log(`    ascending order, so no wait-cycle could form.`);
  }

  return { booked, conflict, deadlock, error };
}

async function main() {
  console.log('\nTicketFlow — Deadlock Ordering Demo');
  console.log('Make sure the API is running: npm run dev');

  const unsafeResult = await runRound(
    'ROUND 1 — UNSAFE: lock in the order the client asked for',
    true
  );

  const safeResult = await runRound(
    'ROUND 2 — SAFE: sort seat ids ascending, then lock',
    false
  );

  console.log(`\n${'═'.repeat(62)}`);
  console.log('  SUMMARY');
  console.log(`${'═'.repeat(62)}`);
  console.log(`                     UNSAFE      SAFE`);
  console.log(`  Booked             ${String(unsafeResult.booked).padStart(6)}   ${String(safeResult.booked).padStart(7)}`);
  console.log(`  Conflicts          ${String(unsafeResult.conflict).padStart(6)}   ${String(safeResult.conflict).padStart(7)}`);
  console.log(`  Deadlocks          ${String(unsafeResult.deadlock).padStart(6)}   ${String(safeResult.deadlock).padStart(7)}`);
  console.log('');

  if (safeResult.deadlock === 0) {
    console.log('  🏆 Ascending lock ordering eliminated deadlocks.\n');
  }

  console.log('  Note: exactly 1 request should succeed per round — the 5 seats');
  console.log('  are booked all-or-nothing, so only one basket can win.\n');
  console.log('  Deadlocks are timing-dependent. If Round 1 shows 0 deadlocks,');
  console.log('  run again or raise the pair count: node loadtest/deadlock.js 8\n');
}

main().catch((err) => {
  console.error('Demo failed:', err.message);
  process.exit(1);
});
