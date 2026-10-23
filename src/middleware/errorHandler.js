// src/middleware/errorHandler.js
const logger = require('../config/logger');

function errorHandler(err, req, res, next) {
  const reqId = req.log ? undefined : `err-${Date.now()}`;
  const log = req.log || logger.child({ reqId });

  log.error({ err: err.message, stack: err.stack }, 'unhandled error');

  res.status(err.status || 500).json({
    error: err.message || 'Internal server error',
  });
}

module.exports = errorHandler;
