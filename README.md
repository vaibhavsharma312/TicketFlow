# TicketFlow

A ticket booking backend built to solve one hard problem properly: **when hundreds of people click "Book" on the same seat at the same instant, exactly one of them should get it.**

Everything in this project — the locking, the queue, the replica, the hold-and-pay flow — exists to answer that question, and to make the answer visible and testable rather than theoretical.

**Live demo:** _(https://ticketflow-erq6.onrender.com/)_

---

## Table of contents

- [The problem](#the-problem)
- [What's inside](#whats-inside)
- [Architecture](#architecture)
- [Quick start](#quick-start)
- [How it works](#how-it-works)
  - [1. Pessimistic locking](#1-pessimistic-locking--select--for-update)
  - [2. Optimistic locking](#2-optimistic-locking--version-based-cas)
  - [3. Deadlock prevention](#3-deadlock-prevention--ascending-lock-order)
  - [4. Two-phase booking](#4-two-phase-booking--hold--pay--confirm)
  - [5. Idempotency](#5-idempotency--two-layers)
  - [6. Kafka async processing](#6-kafka--async-side-effects)
  - [7. Read replica](#7-read-replica--readwrite-splitting)
  - [8. Cache and virtual queue](#8-cache-and-virtual-queue)
- [API reference](#api-reference)
- [Testing and demos](#testing-and-demos)
- [Project structure](#project-structure)
- [Design decisions](#design-decisions)
- [Known limitations](#known-limitations)

---

## The problem

Here is booking code that looks completely reasonable and is completely broken:

```js
const seat = await db.query('SELECT status FROM seats WHERE id = $1', [seatId]);
if (seat.rows[0].status !== 'available') return { error: 'taken' };

await db.query("UPDATE seats SET status = 'booked' WHERE id = $1", [seatId]);
await db.query('INSERT INTO bookings (user_id, seat_id) VALUES ($1, $2)', [userId, seatId]);
```

Two users hitting this at the same moment:

```
TIME   ALICE                      BOB                        SEAT A1
────   ────────────────────────   ────────────────────────   ───────
t=0    SELECT → 'available'                                  available
t=1                               SELECT → 'available'       available   ← both read the same thing
t=2    UPDATE → 'booked'                                     booked
t=3                               UPDATE → 'booked'          booked
t=4    INSERT booking (Alice)                                booked
t=5                               INSERT booking (Bob)       booked

RESULT: two confirmed bookings for one seat.
```

This is the **check-then-act race**: the state you validated at `t=0` was no longer true by the time you acted on it at `t=2`. Wrapping it in `BEGIN`/`COMMIT` does *not* fix it — PostgreSQL's default isolation level (`READ COMMITTED`) lets two transactions read the same row concurrently without blocking each other.

Fixing this properly is what the rest of this project is about.

---

## What's inside

| Capability | What it demonstrates |
|---|---|
| **Pessimistic locking** | `SELECT ... FOR UPDATE` — take the row lock first, then act |
| **Optimistic locking** | Version-column compare-and-swap with bounded retries |
| **Deadlock prevention** | Multi-seat bookings acquire locks in a consistent ascending order |
| **Two-phase booking** | Hold → pay → confirm, so no row lock is ever held across a payment |
| **Idempotency** | Retry-safe at the API layer *and* in the Kafka consumer |
| **Async processing** | Kafka producer fires after commit; a separate worker consumes |
| **Read replica** | Streaming replication with read/write splitting and read-your-writes |
| **Caching** | Redis-backed seat-map cache with write-through invalidation |
| **Virtual queue** | Waiting-room admission control for high-demand sales |
| **Observability** | Structured JSON logs with request correlation IDs, live metrics endpoint |
| **Load testing** | Scripts that empirically prove zero double-bookings under contention |

Every one of these has a runnable demo — either a script in `loadtest/` or a button in the UI.

---

## Architecture

```
                        ┌──────────────────┐
                        │  Browser / curl  │
                        └────────┬─────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │   Express API (Node.js)  │
                    │                          │
                    │   Routes                 │
                    │     ↓                    │
                    │   Controllers            │
                    │     ↓                    │
                    │   Services  ← locking    │
                    │     ↓         lives here │
                    │   Models                 │
                    └──┬──────────┬─────────┬──┘
            writes     │          │ reads   │ events
                       │          │         │
        ┌──────────────▼──┐   ┌───▼──────┐  │
        │ Postgres PRIMARY│   │  Redis   │  │
        │      :5433      │   │  cache   │  │
        │                 │   └───┬──────┘  │
        │ FOR UPDATE      │       │ miss    │
        │ WHERE version=N │       │         │
        └────────┬────────┘       │         │
                 │ streaming      │         │
                 │ replication    │         │
        ┌────────▼────────┐◄──────┘         │
        │ Postgres REPLICA│                 │
        │  :5434 (r/o)    │                 │
        └─────────────────┘                 │
                                            │
                          ┌─────────────────▼─────┐
                          │   Kafka (KRaft)       │
                          │   topic: booking-     │
                          │          events       │
                          └───────────┬───────────┘
                                      │
                          ┌───────────▼───────────┐
                          │   Worker process      │
                          │   idempotent consumer │
                          │   → email, analytics  │
                          └───────────────────────┘
```

**Two long-running processes.** The API (`npm run dev`) owns HTTP and transactions. The worker (`npm run worker`) consumes Kafka and does the slow side-effects. They scale and fail independently — if email delivery backs up, bookings keep working.

---

## Quick start

**Prerequisites:** Node.js 20+, Docker Desktop running.

```bash
git clone https://github.com/tusharsaini1413/ticketflow.git
cd ticketflow
npm install

# Start Postgres (primary + replica), Kafka, and Redis
docker compose up -d

# The replica takes a base backup from the primary on first boot — give it time
sleep 45 && docker compose ps          # all four should read "healthy"

npm run migrate                        # create tables
npm run seed                           # 4 events, ~50 seats, 3 users
npm run dev                            # start the API
```

Open **http://localhost:3000**

Optionally, in a second terminal:

```bash
npm run worker                         # watch Kafka events being consumed
```

**Testing across two devices** (both on the same WiFi):

```bash
ipconfig getifaddr en0                 # macOS — e.g. 192.168.1.5
```

Open `http://<that-ip>:3000` on the other device. Book a seat on one, watch it turn red on the other. This is the most convincing demo of the whole project.

---

## How it works

### 1. Pessimistic locking — `SELECT ... FOR UPDATE`

Inside a transaction, `FOR UPDATE` reads the row **and** takes a row-level exclusive lock on it. Any other transaction trying to `SELECT ... FOR UPDATE`, `UPDATE`, or `DELETE` that row blocks until this transaction commits or rolls back. Plain `SELECT` is not blocked — reads stay fast.

```js
await client.query('BEGIN');

const seat = await getSeatForUpdate(client, seatId);   // ← lock acquired here
if (seat.status !== 'available') {
  await client.query('ROLLBACK');
  return { error: 'already booked', status: 409 };
}

await updateSeatStatus(client, seatId, 'booked');
await createBooking(client, userId, eventId, seatId);

await client.query('COMMIT');                          // ← lock released here
```

Back to Alice and Bob:

```
ALICE                              BOB
─────                              ───
BEGIN
SELECT ... FOR UPDATE
  → lock on A1, status='available'
                                   BEGIN
                                   SELECT ... FOR UPDATE
                                     → blocks, waits for Alice
UPDATE → 'booked'                    │
INSERT booking                       │
COMMIT ─────────────────────────────►│ lock released
                                     → returns status='booked'
                                   ROLLBACK, return 409 Conflict
```

Bob doesn't fail with a crash or a duplicate — he waits, then sees the *current* state and gets a clean 409.

**Why `pool.connect()` and not `pool.query()`:** `pool.query()` grabs any idle connection per call, so `BEGIN` and `COMMIT` could land on different connections and would not be the same transaction. `pool.connect()` reserves one client for the whole transaction. Every model function takes `client` as its first argument for exactly this reason — a lock belongs to a connection.

### 2. Optimistic locking — version-based CAS

The opposite bet: assume conflicts are rare, take no lock, and detect collisions at write time.

Every seat has a `version` integer that increments on every update. Read the version, then make the write conditional on it still being that value:

```sql
UPDATE seats
SET status = 'booked', version = version + 1
WHERE id = $1 AND status = 'available' AND version = $2
```

If anyone modified the row in between, `version` no longer matches, the `WHERE` matches zero rows, and `rowCount` comes back `0`. That's the conflict signal.

```
ALICE                                BOB
─────                                ───
SELECT → version=0                   SELECT → version=0     ← neither blocks
UPDATE ... WHERE version=0
  → rowCount 1  (WIN, version→1)
                                     UPDATE ... WHERE version=0
                                       → rowCount 0  (LOST)
                                     ROLLBACK, retry
                                     SELECT → status='booked' → 409
```

Note that `rowCount = 0` is **not** an error — PostgreSQL doesn't raise on an `UPDATE` that matches nothing. The application has to inspect `rowCount` to learn what happened.

Retries are capped at `MAX_RETRIES = 3`. Under load testing, successful requests never needed more than 2, so 3 leaves one attempt of headroom. Retrying more under heavy contention just adds load without improving the odds — better to return 409 quickly and let the user act.

**Which one is better?** Neither. It's a workload question:

| | Pessimistic | Optimistic |
|---|---|---|
| On conflict | Later transactions **wait** | Later transactions **retry** |
| Read cost | Lock overhead on every read | None |
| Best for | High contention (one hot seat) | Low contention (spread across seats) |
| Cost when wrong | Queue buildup, connections held | Wasted work from repeated retries |

Both are exposed as separate endpoints so the comparison can actually be run, not just argued about.

### 3. Deadlock prevention — ascending lock order

Booking several seats means holding several row locks at once, which introduces a failure mode a single-seat booking can't have:

```
TRANSACTION A                    TRANSACTION B
─────────────                    ─────────────
locks seat 3                     locks seat 7
wants seat 7 → blocked           wants seat 3 → blocked

A waits on B. B waits on A. Neither can ever proceed.
```

PostgreSQL detects the cycle and kills one transaction with SQLSTATE `40P01` (`deadlock_detected`). That's a survivable error, but it's better not to create the situation at all.

The fix is one line: **make every transaction acquire locks in the same global order.**

```js
// De-duplicate, then sort ASCENDING. This is the deadlock prevention.
const orderedSeatIds = [...new Set(seatIds.map(Number))].sort((a, b) => a - b);

for (const seatId of orderedSeatIds) {
  const seat = await getSeatForUpdate(client, seatId);
  // ... validate
}
```

Now a user asking for `[7, 2, 5]` and a user asking for `[5, 7, 2]` both lock `2 → 5 → 7`. A wait-*cycle* becomes structurally impossible — the second transaction simply queues behind the first on the lowest id. Waiting still happens; deadlock cannot.

A retry on `40P01` remains in the code as defence in depth, in case some future code path locks in a different order.

The UI shows this live: select several seats out of order and it displays

```
🔐 you clicked: [7, 2, 5] → locks taken ascending: [2, 5, 7] (deadlock-safe)
```

Multi-seat bookings are also **all-or-nothing**: seats are locked and validated in one pass, and only if *every* seat is available does the transaction write anything. One unavailable seat rolls back the whole basket, so nobody ends up with a partial booking.

### 4. Two-phase booking — hold → pay → confirm

Real bookings involve payment, and payment takes minutes. You cannot hold a database row lock while a user types their card details — that lock would block every other user for the entire duration.

The solution is to split booking into two short transactions with the slow part in between:

```
Phase 1 — HOLD                        (transaction ~15ms)
  lock the seats (ascending order)
  status → 'held', held_until = now() + 5 min
  create bookings with status 'pending'
  COMMIT  ← locks released HERE

        ⏳ user pays — seconds or minutes, no lock held

Phase 2 — CONFIRM                     (transaction ~15ms)
  payment gateway calls back
  re-lock the seats briefly
  verify the hold has not expired
  status → 'booked', bookings → 'confirmed'
  COMMIT
```

The seat is protected the whole time — it's marked `held`, so nobody else can take it — but the *lock* is only held for milliseconds at each end.

**Expiry is handled two ways**, deliberately:

- **Lazy** — when someone tries to take a seat, a `held` seat whose `held_until` has passed is treated as available. This means a lapsed hold is never actually unbookable, even if no background job has run.
- **Sweeper** — a job every 30s flips expired holds back to `available`. This isn't load-bearing for correctness; it just keeps the data tidy so the seat map shows the seat as free without someone having to try it first.

The UI shows the countdown, and the seat map shows held seats in amber. Open two browser tabs to see it: hold seats in one, and they appear amber and unclickable in the other.

### 5. Idempotency — two layers

Networks retry. Users double-click. Message brokers redeliver. Every write path here is safe to repeat.

**API layer** — the client sends an `idempotencyKey`. The `bookings.idempotency_key` column has a `UNIQUE` constraint, so the database itself refuses a duplicate. A repeat of the same request returns the *original* booking with `200 OK` instead of creating a second one with `201 Created`.

**Consumer layer** — Kafka guarantees *at-least-once* delivery, meaning the same message can legitimately arrive twice (a producer retry after a lost ack, or a consumer that crashed after processing but before committing its offset). Before doing any side effect, the worker checks a `processed_messages` table keyed on `(consumer, message_key)`, where the key is `topic-partition-offset` — globally unique in Kafka by construction.

```js
const messageId = `${topic}-${partition}-${message.offset}`;

if (await isAlreadyProcessed(CONSUMER_NAME, messageId)) return;  // skip
await processBookingConfirmed(value);
await markAsProcessed(CONSUMER_NAME, messageId);
```

The `(consumer, message_key)` composite key means adding a second consumer later doesn't make it skip messages the first one already handled — each consumer tracks its own progress.

The `markAsProcessed` insert catches SQLSTATE `23505` (`unique_violation`) and treats it as "already done" rather than an error, so two workers racing on the same message resolve cleanly.

**Payment confirmation** is idempotent too — a gateway that sends its webhook twice gets `alreadyProcessed: true` on the second call, not a double booking.

### 6. Kafka — async side-effects

A confirmed booking should trigger email, SMS, analytics, and partner webhooks. Doing that inside the request would mean:

```js
await bookSeat(...);          //  15ms
await sendEmail(...);         // 500ms
await sendSMS(...);           // 300ms
await trackAnalytics(...);    // 200ms
await notifyPartners(...);    // 400ms
                              // ≈1.4s response, and email downtime breaks booking
```

Instead the API publishes one event and moves on. A separate worker does the slow work at its own pace.

**Two details that matter:**

**Publish after `COMMIT`, never before.** If the event went out first and the commit then failed, the worker would email a confirmation for a booking that does not exist. Publishing after commit means the only remaining failure is "booking exists but the email is late" — recoverable, and far better than lying to the user.

**Fire-and-forget.** The publish isn't awaited, and failures are logged rather than thrown:

```js
await client.query('COMMIT');

sendBookingEvent({ ...booking })
  .catch(err => logger.error({ err: err.message }, 'kafka send failed (non-fatal)'));
```

If Kafka is down, the booking still succeeds. The user has their seat; only the notification is lost.

> **Honest note:** Kafka is overkill at this project's actual scale — an in-process queue would work. It's here because the *pattern* is what a high-throughput system needs, and because durability and replay are real advantages once traffic justifies them. The gap it leaves is described under [Known limitations](#known-limitations).

### 7. Read replica — read/write splitting

During a sale, the overwhelming majority of traffic is reads: "which seats are still free?" Those don't need the primary.

A second PostgreSQL container runs as a streaming-replication standby. Reads (event list, seat map, booking history) go to the replica; every write goes to the primary.

```js
const primaryPool = new Pool({ connectionString: process.env.DATABASE_URL });

const replicaPool = process.env.DATABASE_URL_REPLICA
  ? new Pool({ connectionString: process.env.DATABASE_URL_REPLICA })
  : primaryPool;                       // graceful fallback
```

**The catch — replication lag.** Replication is asynchronous, so a user who just booked could immediately read from the replica and see their own seat as still available. That is a genuinely confusing bug.

The fix is **read-your-writes consistency**: right after a write, that user's next read is routed to the primary. The UI passes `?fresh=1`, and every read response carries an `X-Read-Source` header (`replica`, `primary`, or `cache`) so the routing is observable rather than assumed.

`GET /api/replication-status` reports the primary and replica LSNs and the lag in seconds; the header badge in the UI shows it live.

Verify the replica is real:

```bash
# The replica reports that it is a standby
docker exec ticketflow-postgres-replica psql -U ticketflow -d ticketflow \
  -c "SELECT pg_is_in_recovery();"        # → t

# And it physically refuses writes
docker exec ticketflow-postgres-replica psql -U ticketflow -d ticketflow \
  -c "INSERT INTO users (email) VALUES ('x@y.com');"
# → ERROR: cannot execute INSERT in a read-only transaction
```

### 8. Cache and virtual queue

**Cache.** Seat maps are the hottest endpoint during a sale and don't need to be perfectly current. They're cached for a few seconds and invalidated on every write, so the staleness window is small and bounded. Responses carry `X-Cache: HIT | MISS | BYPASS`.

**Virtual queue.** Letting 100,000 users hit the booking endpoint the moment a sale opens just relocates the stampede onto the seat rows. A waiting room admits a bounded number of users at a time and makes the rest poll for their position — the pattern Ticketmaster and similar platforms use.

```bash
curl -X POST localhost:3000/api/queue/join
# → { "token": "...", "position": 0, "admitted": true }
```

It's **disabled by default** (`QUEUE_ENABLED=false`) so it doesn't interfere with the load tests. Set it to `true` to enforce admission on `/api/hold`.

Both the cache and the queue **fall back to in-process implementations** when `REDIS_URL` isn't set, so the app deploys unchanged to hosts without a Redis add-on.

---

## API reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Liveness check |
| `GET` | `/metrics` | Request counts, booking outcomes, latency, uptime |
| `GET` | `/api/events` | All events with live seat availability |
| `GET` | `/api/events/:id/seats` | Event details plus its seat map |
| `POST` | `/api/book` | Book one seat — pessimistic locking |
| `POST` | `/api/book-optimistic` | Book one seat — optimistic CAS |
| `POST` | `/api/book-multiple` | Book several seats, all-or-nothing, ascending lock order |
| `POST` | `/api/hold` | Phase 1 — hold seats, returns a `paymentId` |
| `POST` | `/api/payment/confirm` | Phase 2 — settle payment (idempotent) |
| `GET` | `/api/hold/:paymentId` | Hold status and seconds remaining |
| `GET` | `/api/bookings?userId=1` | A user's booking history |
| `GET` | `/api/replication-status` | Primary/replica LSN and lag |
| `POST` | `/api/queue/join` | Join the virtual waiting room |
| `GET` | `/api/queue/status?token=` | Position in the queue |
| `POST` | `/api/admin/reset` | Re-seed the database |

Reads accept `?fresh=1` to bypass both the cache and the replica.

**Status codes used deliberately:** `201` a booking was created · `200` an idempotent repeat, nothing new created · `409` the seat is taken · `410` the hold expired · `429` still in the waiting room.

### Example

```bash
# Phase 1 — hold three seats (note they're requested out of order)
curl -X POST localhost:3000/api/hold \
  -H 'Content-Type: application/json' \
  -d '{"userId":1,"eventId":1,"seatIds":[3,1,2]}'

# → { "paymentId": "pay_63dc...", "totalPrice": 14997,
#     "expiresAt": "...", "bookings": [ A1, A2, A3 ] }   ← locked in ascending order

# Phase 2 — settle it
curl -X POST localhost:3000/api/payment/confirm \
  -H 'Content-Type: application/json' \
  -d '{"paymentId":"pay_63dc...","success":true}'

# Run the same call again → { "alreadyProcessed": true }  (idempotent)
```

---

## Testing and demos

```bash
# 20 concurrent requests at ONE seat — expects exactly 1 winner
node loadtest/quick.js 20 pessimistic
node loadtest/quick.js 20 optimistic

# 200 requests across 10 seats, both strategies, with p50/p95/p99 latencies
node loadtest/stress.js 50 200

# Deadlock ordering: unsafe vs safe, side by side
node loadtest/deadlock.js
```

Sample output:

```
Firing 20 concurrent requests at ONE seat (pessimistic)...

Results:
  HTTP 201: 1
  HTTP 409: 19

Expected: exactly 1x 201, rest 409.  ✅ PASS
```

That single result is the whole point of the project: 20 simultaneous requests, one winner, no double booking.

The same demos are available as buttons in the UI under **Concurrency Lab**, which is easier to show someone than a terminal.

**About the deadlock demo:** both rounds pass `?delay=150`, which pauses between lock acquisitions. A deadlock is a timing race — without widening that window each transaction finishes in about a millisecond and the two never interleave, so the race can't be reproduced. The delay applies to both rounds so the comparison stays fair.

---

## Project structure

```
ticketflow/
├── src/
│   ├── config/
│   │   ├── db.js               primary + replica pools, graceful fallback
│   │   ├── kafka.js            KafkaJS client, producer, consumer
│   │   ├── cache.js            Redis facade with in-memory fallback
│   │   └── logger.js           Pino structured logging
│   ├── db/
│   │   ├── migrations/         001 schema · 002 event fields · 003 booking
│   │   │                       groups · 004 hold + payment
│   │   ├── migrate.js          tracked, transactional migration runner
│   │   └── seed.js             4 events, ~50 seats, 3 users
│   ├── models/                 raw SQL only — no business logic
│   ├── services/
│   │   ├── bookingService.js   ★ pessimistic, optimistic, multi-seat
│   │   ├── holdService.js      ★ two-phase booking
│   │   ├── eventService.js
│   │   └── queueService.js     virtual waiting room
│   ├── controllers/            HTTP in, HTTP out
│   ├── routes/                 URL → controller
│   ├── kafka/                  producer + consumer idempotency
│   ├── middleware/             error handler, request logger, metrics
│   ├── jobs/expireHolds.js     background hold sweeper
│   └── index.js                Express entry point
├── workers/                    Kafka consumer process
├── loadtest/                   quick.js · stress.js · deadlock.js
├── public/index.html           BookMyShow-style UI
└── docker-compose.yml          Postgres ×2 · Kafka · Redis
```

The layering isn't ceremony. Because locking lives entirely in `services/`, adding optimistic locking alongside pessimistic touched one file — the HTTP and SQL layers were untouched.

**Tech:** Node.js 20 · Express 5 · PostgreSQL 17 · Apache Kafka 3.9.2 (KRaft) · Redis 7 · `pg` · KafkaJS · Pino · Docker Compose.

---

## Design decisions

**Raw SQL instead of an ORM.** The locking semantics *are* the project. ORMs abstract `SELECT ... FOR UPDATE` behind helpers, and you have to read their docs to know what SQL actually runs. Raw queries give a one-to-one mapping between the code and what hits the database. In a production codebase I'd still use an ORM for routine CRUD and drop to raw SQL on the hot path.

**Two booking endpoints, not one.** In a real product you'd pick one strategy. Here the comparison is the point — the same load test runs against both, and the throughput and conflict-rate differences are measurable rather than theoretical.

**Kafka 3.9.2, pinned.** KafkaJS is effectively unmaintained and has a known incompatibility with Kafka 4.x's group-coordinator changes — consumers fail to join their group. 3.9.2 is the last 3.x line and works. Choosing a broker version from the client's compatibility matrix rather than reaching for "latest" is the actual engineering decision here.

**KRaft mode, no ZooKeeper.** One fewer distributed system to run, and ZooKeeper is being removed from Kafka anyway.

**Postgres on host port 5433.** Remapped to coexist with a Homebrew PostgreSQL already bound to 5432. Worth knowing that `localhost:5432` connecting successfully doesn't mean you reached the container — check what's actually listening before assuming.

**Every optional dependency degrades gracefully.** No replica → reads go to the primary. No Redis → in-process cache and queue. No Kafka → bookings still succeed, the event is logged as lost. The app runs on a laptop and on a free hosting tier without code changes.

---

## Known limitations

Being explicit about what this doesn't do:

**Kafka publish is not transactional with the commit.** If the process crashes in the window between `COMMIT` and the Kafka publish, the booking is real but no event is ever emitted. The correct fix is the **transactional outbox pattern**: write the event to an `events_outbox` table inside the same transaction, and have a separate poller forward it to Kafka. Not implemented here; fire-after-commit was chosen for simplicity, accepting a lost notification as the failure mode.

**The consumer can duplicate a side effect on a narrow crash.** The worker processes, then marks. Crashing between those two steps means the email was sent but not recorded, so a redelivery sends it again. Closing this needs either the side effect and the mark in one transaction, or end-to-end idempotency in the downstream service (most email providers support a dedupe key).

**Single broker, single primary.** Replication factor 1 on Kafka, no automatic failover on Postgres. Production needs a 3-broker cluster with `min.insync.replicas=2` and managed Postgres with streaming failover.

**No authentication.** `userId` comes from the request body. Production needs JWT or sessions, with the user identity taken from the token rather than trusted from the client. `/api/admin/reset` is also unauthenticated — fine for a demo, dangerous anywhere else.

**Metrics are in-process.** Counters reset on restart and aren't shared across instances. Production would expose Prometheus-format metrics and scrape them.

**Load tested to 200 concurrent requests on a laptop.** The architecture is designed for more, but that's the number actually measured. Scaling further would mean partitioning the seats table by `event_id`, adding API replicas behind a load balancer, and increasing Kafka partitions — all reasoned about here, none of it verified at scale.

**No integration tests.** Correctness is verified by load-test scripts rather than a proper test suite. Tests-first would have been the better discipline, particularly for idempotency, where the test *is* the specification.

---

Built by **Tushar Saini** · M.Tech, IIIT Allahabad
