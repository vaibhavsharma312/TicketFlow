// src/routes/eventRoutes.js
const { Router } = require('express');
const eventController = require('../controllers/eventController');

const router = Router();

router.get('/events', eventController.listEvents);
router.get('/events/:id/seats', eventController.getEventWithSeats);

module.exports = router;
