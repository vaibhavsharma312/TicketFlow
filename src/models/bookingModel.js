// src/models/bookingModel.js
// Every function takes `client` (a dedicated connection), never the pool —
// a lock only holds for the transaction on that one connection.

// Pessimistic: read the row AND take a row-level exclusive lock on it.
async function getSeatForUpdate(client, seatId) {
  const result = await client.query(
    'SELECT id, event_id, seat_number, status, version, price FROM seats WHERE id = $1 FOR UPDATE',
    [seatId]
  );
  return result.rows[0] || null;
}

async function updateSeatStatus(client, seatId, status) {
  await client.query(
    `UPDATE seats SET status = $1, version = version + 1, updated_at = now()
     WHERE id = $2`,
    [status, seatId]
  );
}

// Optimistic: compare-and-swap. rowCount 1 = we won, 0 = someone changed it first.
async function updateSeatOptimistic(client, seatId, expectedVersion) {
  const result = await client.query(
    `UPDATE seats
     SET status = 'booked', version = version + 1, updated_at = now()
     WHERE id = $1 AND status = 'available' AND version = $2`,
    [seatId, expectedVersion]
  );
  return result.rowCount;
}

async function createBooking(client, userId, eventId, seatId, idempotencyKey, groupId = null) {
  const result = await client.query(
    `INSERT INTO bookings (user_id, event_id, seat_id, status, idempotency_key, booking_group_id)
     VALUES ($1, $2, $3, 'confirmed', $4, $5)
     RETURNING id, status, created_at, booking_group_id`,
    [userId, eventId, seatId, idempotencyKey, groupId]
  );
  return result.rows[0];
}

async function findBookingByIdempotencyKey(client, idempotencyKey) {
  const result = await client.query(
    'SELECT id, user_id, event_id, seat_id, status, created_at FROM bookings WHERE idempotency_key = $1',
    [idempotencyKey]
  );
  return result.rows[0] || null;
}

async function findBookingsByGroupId(client, groupId) {
  const result = await client.query(
    `SELECT b.id, b.user_id, b.event_id, b.seat_id, b.status, b.created_at, s.seat_number
     FROM bookings b JOIN seats s ON s.id = b.seat_id
     WHERE b.booking_group_id = $1
     ORDER BY b.seat_id`,
    [groupId]
  );
  return result.rows;
}

module.exports = {
  getSeatForUpdate,
  updateSeatStatus,
  updateSeatOptimistic,
  createBooking,
  findBookingByIdempotencyKey,
  findBookingsByGroupId,
};
