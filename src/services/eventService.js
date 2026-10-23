// src/services/eventService.js
const eventModel = require('../models/eventModel');
const seatModel = require('../models/seatModel');

async function listEvents(fresh = false) {
  return eventModel.getAllEvents(fresh);
}

async function getEventWithSeats(eventId, fresh = false) {
  const event = await eventModel.getEventById(eventId, fresh);
  if (!event) return null;

  const seats = await seatModel.getSeatsByEventId(eventId, fresh);
  return { ...event, seats };
}

module.exports = { listEvents, getEventWithSeats };
