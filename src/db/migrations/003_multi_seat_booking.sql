-- 003_multi_seat_booking.sql
-- Group several seat bookings made in one transaction under a single group id.

ALTER TABLE bookings ADD COLUMN IF NOT EXISTS booking_group_id TEXT;

CREATE INDEX IF NOT EXISTS idx_bookings_group_id ON bookings(booking_group_id);
