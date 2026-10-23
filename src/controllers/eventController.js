// src/controllers/eventController.js
const eventService = require('../services/eventService');
const { hasReplica } = require('../config/db');
const cache = require('../config/cache');

// Reads are cached briefly. Seat maps are the hottest endpoint during a sale:
// almost all traffic is "what is still free", and none of it needs to be
// perfectly current. Writes invalidate the prefix, so the window is small.
const SEATS_TTL = parseInt(process.env.CACHE_TTL_SECONDS, 10) || 3;
const EVENTS_TTL = 10;

// ?fresh=1 bypasses both the cache and the replica — used right after a write
// so a user always sees their own booking (read-your-writes).
function wantsFresh(req) {
  return req.query.fresh === '1' || req.query.fresh === 'true';
}

async function listEvents(req, res, next) {
  try {
    const fresh = wantsFresh(req);
    const key = 'events:list';

    if (!fresh) {
      const cached = await cache.get(key);
      if (cached) {
        res.set('X-Cache', 'HIT');
        res.set('X-Read-Source', 'cache');
        return res.json(cached);
      }
    }

    const events = await eventService.listEvents(fresh);
    if (!fresh) await cache.set(key, events, EVENTS_TTL);

    res.set('X-Cache', fresh ? 'BYPASS' : 'MISS');
    res.set('X-Read-Source', fresh || !hasReplica ? 'primary' : 'replica');
    res.json(events);
  } catch (err) { next(err); }
}

async function getEventWithSeats(req, res, next) {
  try {
    const eventId = parseInt(req.params.id, 10);
    if (isNaN(eventId)) return res.status(400).json({ error: 'Invalid event ID' });

    const fresh = wantsFresh(req);
    const key = `seats:event:${eventId}`;

    if (!fresh) {
      const cached = await cache.get(key);
      if (cached) {
        res.set('X-Cache', 'HIT');
        res.set('X-Read-Source', 'cache');
        return res.json(cached);
      }
    }

    const event = await eventService.getEventWithSeats(eventId, fresh);
    if (!event) return res.status(404).json({ error: 'Event not found' });

    if (!fresh) await cache.set(key, event, SEATS_TTL);

    res.set('X-Cache', fresh ? 'BYPASS' : 'MISS');
    res.set('X-Read-Source', fresh || !hasReplica ? 'primary' : 'replica');
    res.json(event);
  } catch (err) { next(err); }
}

module.exports = { listEvents, getEventWithSeats };
