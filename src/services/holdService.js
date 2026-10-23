// src/services/holdService.js
//
// Two-phase booking.
//
// Phase 1 (hold)    — lock the seats, mark them 'held' with an expiry, write
//                     'pending' bookings, COMMIT. The row locks are released
//                     immediately, so nobody waits while the user pays.
// Phase 2 (confirm) — the payment gateway calls back. We lock the seats again
//                     briefly, verify the hold has not expired, and flip
//                     'held' -> 'booked' / 'pending' -> 'confirmed'.
//
// The whole point: a database row lock is never held across a slow external
// operation. Payment can take minutes; the lock is only ever held for the
// few milliseconds each phase needs.

const crypto = require('crypto');
const pool = require('../config/db');
const bookingModel = require('../models/bookingModel');
const { sendBookingEvent } = require('../kafka/producer');
const logger = require('../config/logger');
const cache = require('../config/cache');

const HOLD_MINUTES = parseInt(process.env.HOLD_MINUTES, 10) || 5;

function newPaymentId() {
  return 'pay_' + crypto.randomBytes(10).toString('hex');
}

// A seat is takeable if it is available, or if it is held but the hold lapsed.
function isTakeable(seat) {
  if (seat.status === 'available') return true;
  if (seat.status === 'held' && seat.held_until && new Date(seat.held_until) < new Date()) {
    return true; // lazy expiry — the sweeper may not have run yet
  }
  return false;
}

// ── Phase 1: HOLD ──
async function holdSeats(userId, eventId, seatIds, idempotencyKey) {
  const ordered = [...new Set(seatIds.map(Number))].sort((a, b) => a - b); // deadlock-safe
  const paymentId = newPaymentId();
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    if (idempotencyKey) {
      const existing = await bookingModel.findBookingsByGroupId(client, idempotencyKey);
      if (existing.length > 0) {
        await client.query('COMMIT');
        return { alreadyExisted: true, bookings: existing, paymentId: existing[0].payment_id };
      }
    }

    const held = [];
    for (const seatId of ordered) {
      const seat = await bookingModel.getSeatForUpdate(client, seatId);

      if (!seat) {
        await client.query('ROLLBACK');
        return { error: `Seat ${seatId} not found`, status: 404 };
      }
      if (seat.event_id !== eventId) {
        await client.query('ROLLBACK');
        return { error: `Seat ${seat.seat_number} is not part of this event`, status: 400 };
      }
      if (!isTakeable(seat)) {
        await client.query('ROLLBACK');
        return {
          error: `Seat ${seat.seat_number} is already ${seat.status}`,
          conflictSeat: seat.seat_number,
          status: 409,
        };
      }
      held.push(seat);
    }

    const expiresAt = new Date(Date.now() + HOLD_MINUTES * 60 * 1000);
    const bookings = [];
    let totalPrice = 0;

    for (const seat of held) {
      await client.query(
        `UPDATE seats
         SET status = 'held', held_until = $1, held_by = $2,
             version = version + 1, updated_at = now()
         WHERE id = $3`,
        [expiresAt, userId, seat.id]
      );

      const result = await client.query(
        `INSERT INTO bookings
           (user_id, event_id, seat_id, status, idempotency_key, booking_group_id,
            payment_id, payment_status, expires_at)
         VALUES ($1, $2, $3, 'pending', $4, $5, $6, 'pending', $7)
         RETURNING id, status, created_at`,
        [
          userId, eventId, seat.id,
          idempotencyKey ? `${idempotencyKey}:${seat.id}` : null,
          idempotencyKey || paymentId,
          paymentId, expiresAt,
        ]
      );

      bookings.push({
        ...result.rows[0],
        seat_id: seat.id,
        seat_number: seat.seat_number,
        price: seat.price,
      });
      totalPrice += Number(seat.price || 0);
    }

    await client.query('COMMIT'); // locks released here — before any payment

    await cache.invalidate('seats:');
    await cache.invalidate('events:');

    logger.info(
      { userId, eventId, seatIds: ordered, paymentId, expiresAt, holdMinutes: HOLD_MINUTES },
      'seats held, awaiting payment'
    );

    return { paymentId, bookings, totalPrice, expiresAt, holdMinutes: HOLD_MINUTES };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

// ── Phase 2: CONFIRM (payment gateway callback) ──
async function confirmPayment(paymentId, success = true) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const pending = await client.query(
      `SELECT b.id, b.seat_id, b.status, b.user_id, b.event_id, b.expires_at, s.seat_number
       FROM bookings b JOIN seats s ON s.id = b.seat_id
       WHERE b.payment_id = $1
       ORDER BY b.seat_id`,
      [paymentId]
    );

    if (pending.rows.length === 0) {
      await client.query('ROLLBACK');
      return { error: 'Unknown payment id', status: 404 };
    }

    // Idempotency: a repeated webhook must not double-book or error.
    const already = pending.rows.every((b) => b.status !== 'pending');
    if (already) {
      await client.query('COMMIT');
      return {
        alreadyProcessed: true,
        status: 200,
        bookings: pending.rows,
        finalStatus: pending.rows[0].status,
      };
    }

    const expired = new Date(pending.rows[0].expires_at) < new Date();
    if (expired) {
      // Hold lapsed while the user was paying — release everything.
      for (const b of pending.rows) {
        await client.query(
          `UPDATE seats SET status = 'available', held_until = NULL, held_by = NULL,
                            version = version + 1, updated_at = now()
           WHERE id = $1 AND status = 'held'`,
          [b.seat_id]
        );
        await client.query(
          `UPDATE bookings SET status = 'cancelled', payment_status = 'failed' WHERE id = $1`,
          [b.id]
        );
      }
      await client.query('COMMIT');
      await cache.invalidate('seats:');
      logger.warn({ paymentId }, 'hold expired before payment confirmation');
      return { error: 'Hold expired — seats released. Please book again.', status: 410 };
    }

    // Lock the seats again, ascending, for the brief flip.
    const seatIds = pending.rows.map((b) => b.seat_id).sort((a, b) => a - b);
    for (const seatId of seatIds) {
      await bookingModel.getSeatForUpdate(client, seatId);
    }

    if (success) {
      for (const b of pending.rows) {
        await client.query(
          `UPDATE seats SET status = 'booked', held_until = NULL, held_by = NULL,
                            version = version + 1, updated_at = now()
           WHERE id = $1`,
          [b.seat_id]
        );
        await client.query(
          `UPDATE bookings SET status = 'confirmed', payment_status = 'paid' WHERE id = $1`,
          [b.id]
        );
      }
    } else {
      for (const b of pending.rows) {
        await client.query(
          `UPDATE seats SET status = 'available', held_until = NULL, held_by = NULL,
                            version = version + 1, updated_at = now()
           WHERE id = $1`,
          [b.seat_id]
        );
        await client.query(
          `UPDATE bookings SET status = 'failed', payment_status = 'failed' WHERE id = $1`,
          [b.id]
        );
      }
    }

    await client.query('COMMIT');
    await cache.invalidate('seats:');
    await cache.invalidate('events:');

    if (success) {
      pending.rows.forEach((b) => {
        sendBookingEvent({
          id: b.id, user_id: b.user_id, event_id: b.event_id, seat_id: b.seat_id,
        }).catch((e) => logger.error({ err: e.message }, 'kafka send failed (non-fatal)'));
      });
    }

    logger.info({ paymentId, success, seats: seatIds.length }, 'payment settled');

    return {
      status: 200,
      success,
      bookings: pending.rows,
      finalStatus: success ? 'confirmed' : 'failed',
    };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

async function getHoldStatus(paymentId) {
  const result = await pool.query(
    `SELECT b.id, b.status, b.payment_status, b.expires_at, s.seat_number, s.price
     FROM bookings b JOIN seats s ON s.id = b.seat_id
     WHERE b.payment_id = $1 ORDER BY b.seat_id`,
    [paymentId]
  );
  if (result.rows.length === 0) return null;

  const expiresAt = result.rows[0].expires_at;
  return {
    paymentId,
    status: result.rows[0].status,
    paymentStatus: result.rows[0].payment_status,
    expiresAt,
    secondsRemaining: Math.max(0, Math.floor((new Date(expiresAt) - new Date()) / 1000)),
    seats: result.rows.map((r) => ({ seat_number: r.seat_number, price: r.price })),
    totalPrice: result.rows.reduce((a, r) => a + Number(r.price || 0), 0),
  };
}

module.exports = { holdSeats, confirmPayment, getHoldStatus, HOLD_MINUTES };
