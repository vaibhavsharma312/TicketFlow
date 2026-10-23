// src/services/bookingService.js
const pool = require('../config/db');
const bookingModel = require('../models/bookingModel');
const { sendBookingEvent } = require('../kafka/producer');
const logger = require('../config/logger');
const { recordBooking } = require('../middleware/metrics');
const cache = require('../config/cache');

const MAX_RETRIES = 3;          // optimistic CAS retries
const MAX_DEADLOCK_RETRIES = 3; // deadlock / serialization retries

const DEADLOCK_DETECTED = '40P01';
const SERIALIZATION_FAILURE = '40001';

function isRetryableTxError(err) {
  return err && (err.code === DEADLOCK_DETECTED || err.code === SERIALIZATION_FAILURE);
}

function invalidateSeatCache() {
  cache.invalidate('seats:').catch(() => {});
  cache.invalidate('events:').catch(() => {});
}

function publishBookingEvent(booking, userId, eventId, seatId) {
  sendBookingEvent({ ...booking, user_id: userId, event_id: eventId, seat_id: seatId })
    .catch((err) => logger.error({ err: err.message }, 'kafka send failed (non-fatal)'));
}

// ───────────────────────────────────────────────────────────
//  SINGLE SEAT — PESSIMISTIC (SELECT ... FOR UPDATE)
// ───────────────────────────────────────────────────────────
async function bookSeatPessimistic(userId, eventId, seatId, idempotencyKey) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    if (idempotencyKey) {
      const existing = await bookingModel.findBookingByIdempotencyKey(client, idempotencyKey);
      if (existing) {
        await client.query('COMMIT');
        return { booking: existing, alreadyExisted: true };
      }
    }

    const seat = await bookingModel.getSeatForUpdate(client, seatId);

    if (!seat) {
      await client.query('ROLLBACK');
      return { error: 'Seat not found', status: 404 };
    }

    if (seat.status !== 'available') {
      await client.query('ROLLBACK');
      recordBooking('pessimistic', 'conflict');
      return { error: `Seat is already ${seat.status}`, status: 409 };
    }

    await bookingModel.updateSeatStatus(client, seatId, 'booked');
    const booking = await bookingModel.createBooking(
      client, userId, eventId, seatId, idempotencyKey
    );

    await client.query('COMMIT');

    recordBooking('pessimistic', 'success');
    logger.info({ bookingId: booking.id, seatId, userId }, 'pessimistic booking confirmed');
    publishBookingEvent(booking, userId, eventId, seatId);
    invalidateSeatCache();

    return { booking, alreadyExisted: false };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

// ───────────────────────────────────────────────────────────
//  SINGLE SEAT — OPTIMISTIC (version compare-and-swap)
// ───────────────────────────────────────────────────────────
async function bookSeatOptimistic(userId, eventId, seatId, idempotencyKey) {
  const client = await pool.connect();
  try {
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      await client.query('BEGIN');

      if (idempotencyKey) {
        const existing = await bookingModel.findBookingByIdempotencyKey(client, idempotencyKey);
        if (existing) {
          await client.query('COMMIT');
          return { booking: existing, alreadyExisted: true };
        }
      }

      const seatResult = await client.query(
        'SELECT id, event_id, status, version FROM seats WHERE id = $1',
        [seatId]
      );
      const seat = seatResult.rows[0];

      if (!seat) {
        await client.query('ROLLBACK');
        return { error: 'Seat not found', status: 404 };
      }

      if (seat.status !== 'available') {
        await client.query('ROLLBACK');
        recordBooking('optimistic', 'conflict');
        return { error: `Seat is already ${seat.status}`, status: 409 };
      }

      const updated = await bookingModel.updateSeatOptimistic(client, seatId, seat.version);

      if (updated === 0) {
        await client.query('ROLLBACK');
        recordBooking('optimistic', 'retry');
        logger.warn({ seatId, attempt, maxRetries: MAX_RETRIES }, 'optimistic conflict, retrying');
        continue;
      }

      const booking = await bookingModel.createBooking(
        client, userId, eventId, seatId, idempotencyKey
      );

      await client.query('COMMIT');

      recordBooking('optimistic', 'success');
      logger.info({ bookingId: booking.id, seatId, userId, attempt }, 'optimistic booking confirmed');
      publishBookingEvent(booking, userId, eventId, seatId);
    invalidateSeatCache();

      return { booking, alreadyExisted: false };
    }

    recordBooking('optimistic', 'conflict');
    return { error: 'Seat booking conflict, please retry', status: 409 };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

// ───────────────────────────────────────────────────────────
//  MULTI-SEAT — DEADLOCK-SAFE
//
//  Problem: two users each want seats {3, 7}. If A locks 3 then 7, and
//  B locks 7 then 3, each waits on a lock the other holds — a deadlock.
//  Postgres detects the cycle and kills one transaction with SQLSTATE 40P01.
//
//  Fix: every transaction takes locks in the SAME global order. Sorting the
//  seat ids ascending before locking means a wait cycle can never form.
//  The 40P01 retry below is defence in depth, not the primary mechanism.
// ───────────────────────────────────────────────────────────
async function bookMultipleSeats(userId, eventId, seatIds, idempotencyKey, options = {}) {
  const { unsafeOrder = false } = options;

  // De-duplicate, then sort ASCENDING. This line is the deadlock prevention.
  const unique = [...new Set(seatIds.map(Number))];
  const orderedSeatIds = unsafeOrder ? unique : unique.slice().sort((a, b) => a - b);

  const groupId = idempotencyKey || null;

  for (let attempt = 1; attempt <= MAX_DEADLOCK_RETRIES; attempt++) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      // Group idempotency: retrying the whole basket returns the same bookings.
      if (groupId) {
        const existing = await bookingModel.findBookingsByGroupId(client, groupId);
        if (existing.length > 0) {
          await client.query('COMMIT');
          return { bookings: existing, alreadyExisted: true };
        }
      }

      // Phase 1 — lock every seat, in ascending id order.
      const lockedSeats = [];
      for (const seatId of orderedSeatIds) {
        const seat = await bookingModel.getSeatForUpdate(client, seatId);

        if (!seat) {
          await client.query('ROLLBACK');
          return { error: `Seat ${seatId} not found`, status: 404 };
        }
        if (seat.event_id !== eventId) {
          await client.query('ROLLBACK');
          return { error: `Seat ${seat.seat_number} does not belong to this event`, status: 400 };
        }
        if (seat.status !== 'available') {
          // All-or-nothing: one unavailable seat cancels the whole basket.
          await client.query('ROLLBACK');
          recordBooking('pessimistic', 'conflict');
          return {
            error: `Seat ${seat.seat_number} is already ${seat.status}`,
            conflictSeat: seat.seat_number,
            status: 409,
          };
        }
        lockedSeats.push(seat);
      }

      // Phase 2 — all seats locked and available, so every write will succeed.
      const bookings = [];
      let totalPrice = 0;

      for (const seat of lockedSeats) {
        await bookingModel.updateSeatStatus(client, seat.id, 'booked');
        const booking = await bookingModel.createBooking(
          client,
          userId,
          eventId,
          seat.id,
          groupId ? `${groupId}:${seat.id}` : null,
          groupId
        );
        bookings.push({
          ...booking,
          seat_id: seat.id,
          seat_number: seat.seat_number,
          price: seat.price,
        });
        totalPrice += Number(seat.price || 0);
      }

      await client.query('COMMIT');

      bookings.forEach((b) => {
        recordBooking('pessimistic', 'success');
        publishBookingEvent(b, userId, eventId, b.seat_id);
      });
      invalidateSeatCache();

      logger.info(
        {
          userId,
          eventId,
          lockOrder: orderedSeatIds,
          ordering: unsafeOrder ? 'as-requested (UNSAFE demo)' : 'ascending seat id',
          count: bookings.length,
          totalPrice,
          attempt,
        },
        'multi-seat booking confirmed'
      );

      return { bookings, totalPrice, alreadyExisted: false, lockOrder: orderedSeatIds };
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});

      if (isRetryableTxError(err) && attempt < MAX_DEADLOCK_RETRIES) {
        logger.warn(
          { code: err.code, seatIds: orderedSeatIds, attempt, maxRetries: MAX_DEADLOCK_RETRIES },
          'deadlock detected by Postgres, retrying transaction'
        );
        continue;
      }

      if (isRetryableTxError(err)) {
        return {
          error: 'Could not acquire all seats (deadlock). Please try again.',
          status: 409,
          deadlock: true,
          code: err.code,
        };
      }

      throw err;
    } finally {
      client.release();
    }
  }

  return { error: 'Booking failed after retries', status: 409 };
}

module.exports = { bookSeatPessimistic, bookSeatOptimistic, bookMultipleSeats };
