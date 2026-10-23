// src/jobs/expireHolds.js
//
// Background sweeper: releases seats whose hold window lapsed.
//
// Holds are also checked lazily on read (see isTakeable in holdService), so a
// lapsed seat is never actually unbookable. This job just keeps the table tidy
// and makes the UI show the seat as free without someone having to try it.

const pool = require('../config/db');
const logger = require('../config/logger');
const cache = require('../config/cache');

const INTERVAL_MS = parseInt(process.env.EXPIRY_SWEEP_MS, 10) || 30000;

async function expireHolds() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const released = await client.query(
      `UPDATE seats
       SET status = 'available', held_until = NULL, held_by = NULL,
           version = version + 1, updated_at = now()
       WHERE status = 'held' AND held_until < now()
       RETURNING id, seat_number`
    );

    if (released.rowCount > 0) {
      await client.query(
        `UPDATE bookings SET status = 'cancelled', payment_status = 'failed'
         WHERE status = 'pending' AND expires_at < now()`
      );
    }

    await client.query('COMMIT');

    if (released.rowCount > 0) {
      await cache.invalidate('seats:');
      await cache.invalidate('events:');
      logger.info(
        { count: released.rowCount, seats: released.rows.map((r) => r.seat_number) },
        'expired holds released'
      );
    }

    return released.rowCount;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    logger.error({ err: err.message }, 'expiry sweep failed');
    return 0;
  } finally {
    client.release();
  }
}

function start() {
  logger.info({ intervalMs: INTERVAL_MS }, 'hold expiry sweeper started');
  setInterval(expireHolds, INTERVAL_MS).unref();
  expireHolds();
}

module.exports = { start, expireHolds };
