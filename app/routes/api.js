var express = require('express');
var router = express.Router();
var config = require('config');

const dataWarehouse = require('../dataWarehouse');
const API_KEY = config.get('api.key');

router.get('/notes', function (req, res) {
  // first check the api key
  const apikey = req.headers['x-api-key'];
  if (API_KEY !== apikey) {
    res.sendStatus(401);
  } else {
    const username = req.headers['x-chefs-user-username'];

    const results = dataWarehouse.fetch(username, req.query.id);
    res.status(200).json(results);
  }
});

module.exports = router;
