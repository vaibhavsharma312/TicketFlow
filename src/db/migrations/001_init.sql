-- 001_init.sql — TicketFlow initial schema

CREATE TABLE IF NOT EXISTS users (
  id          SERIAL PRIMARY KEY,
  email       TEXT UNIQUE NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events (
  id          SERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  venue       TEXT,
  event_time  TIMESTAMPTZ,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS seats (
  id          SERIAL PRIMARY KEY,
  event_id    INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  seat_number TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'available'
              CHECK (status IN ('available','held','booked')),
  version     INTEGER NOT NULL DEFAULT 0,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (event_id, seat_number)
);

CREATE INDEX IF NOT EXISTS idx_seats_event_id ON seats(event_id);

CREATE TABLE IF NOT EXISTS bookings (
  id              SERIAL PRIMARY KEY,
  user_id         INTEGER NOT NULL REFERENCES users(id),
  event_id        INTEGER NOT NULL REFERENCES events(id),
  seat_id         INTEGER NOT NULL REFERENCES seats(id),
  status          TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending','confirmed','failed','cancelled')),
  idempotency_key TEXT UNIQUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bookings_seat_id ON bookings(seat_id);

CREATE TABLE IF NOT EXISTS processed_messages (
  id          SERIAL PRIMARY KEY,
  consumer    TEXT NOT NULL,
  message_key TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (consumer, message_key)
);
