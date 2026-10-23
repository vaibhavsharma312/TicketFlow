// src/controllers/bookingController.js
const bookingService = require('../services/bookingService');

async function bookSeat(req, res, next) {
  try {
    const { userId, eventId, seatId, idempotencyKey } = req.body;

    if (!userId || !eventId || !seatId) {
      return res.status(400).json({ error: 'userId, eventId, and seatId are required' });
    }

    const result = await bookingService.bookSeatPessimistic(
      userId, eventId, seatId, idempotencyKey
    );

    if (result.error) {
      return res.status(result.status).json({ error: result.error });
    }

    res.status(result.alreadyExisted ? 200 : 201).json({ booking: result.booking });
  } catch (err) {
    next(err);
  }
}

async function bookSeatOptimistic(req, res, next) {
  try {
    const { userId, eventId, seatId, idempotencyKey } = req.body;

    if (!userId || !eventId || !seatId) {
      return res.status(400).json({ error: 'userId, eventId, and seatId are required' });
    }

    const result = await bookingService.bookSeatOptimistic(
      userId, eventId, seatId, idempotencyKey
    );

    if (result.error) {
      return res.status(result.status).json({ error: result.error });
    }

    res.status(result.alreadyExisted ? 200 : 201).json({ booking: result.booking });
  } catch (err) {
    next(err);
  }
}

// Multi-seat: all-or-nothing, locks taken in ascending seat id order.
async function bookMultipleSeats(req, res, next) {
  try {
    const { userId, eventId, seatIds, idempotencyKey } = req.body;

    if (!userId || !eventId || !Array.isArray(seatIds) || seatIds.length === 0) {
      return res.status(400).json({
        error: 'userId, eventId, and a non-empty seatIds array are required',
      });
    }

    if (seatIds.length > 10) {
      return res.status(400).json({ error: 'Cannot book more than 10 seats at once' });
    }

    // ?unsafe=1 skips the ascending sort — used only by the deadlock demo
    // to show what the ordering is actually protecting against.
    const unsafeOrder = req.query.unsafe === '1';

    const result = await bookingService.bookMultipleSeats(
      userId, eventId, seatIds, idempotencyKey, { unsafeOrder }
    );

    if (result.error) {
      return res.status(result.status).json({
        error: result.error,
        conflictSeat: result.conflictSeat,
        deadlock: result.deadlock,
      });
    }

    res.status(result.alreadyExisted ? 200 : 201).json({
      bookings: result.bookings,
      count: result.bookings.length,
      totalPrice: result.totalPrice,
      lockOrder: result.lockOrder,
      alreadyExisted: result.alreadyExisted,
    });
  } catch (err) {
    next(err);
  }
}

module.exports = { bookSeat, bookSeatOptimistic, bookMultipleSeats };
