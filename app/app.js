var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');

var schedule = require('node-schedule');

var indexRouter = require('./routes/index');
var apiRouter = require('./routes/api');

var { run: runConsumer, shutdown: shutdownConsumer } = require('./consumer');

let shuttingDown = false;
var app = express();

var job = schedule.scheduleJob('*/5 * * * * *', function () {
  if (!shuttingDown) {
    runConsumer();
  }
});

app.locals.pluralize = require('pluralize');

app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());

app.use('/', indexRouter);
app.use('/api', apiRouter);

// catch 404 and forward to error handler
app.use(function (req, res, next) {
  next(createError(404));
});

// error handler
app.use(function (err, req, res, next) {
  // render the error page
  res.status(err.status || 500);
  res.json({ error: err });
});

function shutdown() {
  if (!shuttingDown) {
    shuttingDown = true;
    console.log('Received kill signal. Shutting down...');
    // Wait 3 seconds before starting cleanup
    setTimeout(cleanup, 3000);
  }
}

function cleanup() {
  console.log('Service no longer accepting traffic');

  shutdownConsumer().then(() => console.log('consumer shutdown'));
  schedule.gracefulShutdown().then(() => console.log('scheduler shutdown'));

  // Wait 10 seconds max before hard exiting
  setTimeout(() => process.exit(), 10000);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
process.on('SIGUSR1', shutdown);
process.on('SIGUSR2', shutdown);
process.on('exit', () => {
  console.log('exit.');
});

module.exports = app;
