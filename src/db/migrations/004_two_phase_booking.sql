-- 004_two_phase_booking.sql
-- Two-phase booking: a seat is HELD briefly, payment happens outside the
-- transaction, then the hold is confirmed or expires back to available.

-- How long a seat stays held while the user pays.
ALTER TABLE seats ADD COLUMN IF NOT EXISTS held_until TIMESTAMPTZ;
ALTER TABLE seats ADD COLUMN IF NOT EXISTS held_by    INTEGER REFERENCES users(id);

-- Payment tracking on the booking row.
ALTER TABLE bookings ADD COLUMN IF NOT EXISTS payment_id     TEXT;
ALTER TABLE bookings ADD COLUMN IF NOT EXISTS payment_status TEXT NOT NULL DEFAULT 'none';
ALTER TABLE bookings ADD COLUMN IF NOT EXISTS expires_at     TIMESTAMPTZ;

-- One payment can cover several seats, so this is NOT unique.
CREATE INDEX IF NOT EXISTS idx_bookings_payment_id ON bookings(payment_id);

-- Expiry sweeper reads this constantly; partial index keeps it cheap.
CREATE INDEX IF NOT EXISTS idx_seats_held_until ON seats(held_until) WHERE status = 'held';
