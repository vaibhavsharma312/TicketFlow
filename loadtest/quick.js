// loadtest/quick.js
// Quick test: fire N concurrent requests at ONE seat to test locking
// Usage: node loadtest/quick.js [requests] [endpoint]
// Example: node loadtest/quick.js 20 pessimistic
//          node loadtest/quick.js 20 optimistic

const http = require('http');

const N = parseInt(process.argv[2]) || 20;
const MODE = process.argv[3] || 'pessimistic';
const PATH = MODE === 'optimistic' ? '/api/book-optimistic' : '/api/book';

console.log(`\nFiring ${N} concurrent requests at ONE seat (${MODE})...\n`);

function makeRequest(i) {
  return new Promise((resolve) => {
    const body = JSON.stringify({
      userId: 1,
      eventId: 1,
      seatId: 1,
      idempotencyKey: `quick-${MODE}-${i}-${Date.now()}`,
    });

    const req = http.request(
      {
        hostname: 'localhost', port: 3000, path: PATH,
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
      },
      (res) => {
        let data = '';
        res.on('data', (chunk) => (data += chunk));
        res.on('end', () => resolve({ status: res.statusCode, body: data }));
      }
    );
    req.on('error', (err) => resolve({ status: 0, error: err.message }));
    req.write(body);
    req.end();
  });
}

async function main() {
  const promises = [];
  for (let i = 0; i < N; i++) promises.push(makeRequest(i));

  const results = await Promise.all(promises);
  const counts = {};
  for (const r of results) counts[r.status] = (counts[r.status] || 0) + 1;

  console.log('Results:');
  for (const [status, count] of Object.entries(counts)) {
    console.log(`  HTTP ${status}: ${count}`);
  }
  console.log(`\nExpected: exactly 1x 201, rest 409. ${counts[201] === 1 ? '✅ PASS' : '❌ FAIL'}`);
}

main();
