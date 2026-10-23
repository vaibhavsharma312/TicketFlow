// src/routes/bookingRoutes.js
const { Router } = require('express');
const bookingController = require('../controllers/bookingController');

const router = Router();

router.post('/book', bookingController.bookSeat);
router.post('/book-optimistic', bookingController.bookSeatOptimistic);
router.post('/book-multiple', bookingController.bookMultipleSeats);

module.exports = router;
