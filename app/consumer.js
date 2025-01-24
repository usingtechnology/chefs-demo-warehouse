const { AckPolicy, JSONCodec } = require('nats');
const Cryptr = require('cryptr');
const falsey = require('falsey');
const { v4: uuidv4 } = require('uuid');
var config = require('config');

const dataWarehouse = require('./dataWarehouse');

// different connection libraries if we are using websockets or nats protocols.
const WEBSOCKETS = !falsey(config.get('eventStreamService.websockets'));

let natsConnect;
if (WEBSOCKETS) {
  console.log('connect via ws');
  // shim the websocket library
  globalThis.WebSocket = require('websocket').w3cwebsocket;
  const { connect } = require('nats.ws');
  natsConnect = connect;
} else {
  console.log('connect via nats');
  const { connect } = require('nats');
  natsConnect = connect;
}

// connection info
let servers = [];
if (config.get('eventStreamService.servers')) {
  servers = config.get('eventStreamService.servers').split(',');
} else {
  // running locally
  servers = 'localhost:4222,localhost:4223,localhost:4224'.split(',');
}

let nc = undefined; // nats connection
let js = undefined; // jet stream
let jsm = undefined; // jet stream manager
let consumer = undefined; // pull consumer (ordered, ephemeral)

// stream info
const STREAM_NAME = config.get('eventStreamService.streamName');
const FILTER_SUBJECTS = ['PUBLIC.forms.>', 'PRIVATE.forms.>'];
const MAX_MESSAGES = 2;
const SOURCE_FILTER = config.get('eventStreamService.source') || false;
const DURABLE_NAME = config.get('eventStreamService.durableName') || uuidv4();
const ENCRYPTION_KEY = config.get('eventStreamService.encryptionKey') || undefined;
const USERNAME = config.get('eventStreamService.consumerUsername') || 'chefsConsumer';
const PASSWORD = config.get('eventStreamService.consumerPassword') || 'password';

const processMsg = (m) => {
  // illustrate grabbing the sequence and timestamp from the nats message...
  try {
    const ts = new Date(m.info.timestampNanos / 1000000).toISOString();
    console.log(`msg seq: ${m.seq}, subject: ${m.subject}, timestamp: ${ts}, streamSequence: ${m.info.streamSequence}, deliverySequence: ${m.info.deliverySequence}`);
    // illustrate (one way of) grabbing message content as json
    const jsonCodec = JSONCodec();
    const data = jsonCodec.decode(m.data);
    let process = true;
    if (SOURCE_FILTER) {
      process = data['meta']['source'] === SOURCE_FILTER;
      if (!process) {
        console.log(`  not processing message. filter = ${SOURCE_FILTER}, meta.source = ${data['meta']['source']}`);
      }
    }
    const warehouseSubmissionCreated = 'submission' === data?.meta?.class && 'created' === data?.meta?.type && data?.meta?.formMetadata?.id && data?.meta?.formMetadata?.warehouse;
    if (process && warehouseSubmissionCreated) {
      console.log(data);
      try {
        if (data && data['error']) {
          console.log(`error with payload: ${data['error']['message']}`);
        } else if (data && data['payload'] && data['payload']['data'] && ENCRYPTION_KEY) {
          const cryptr = new Cryptr(ENCRYPTION_KEY);
          const decryptedData = cryptr.decrypt(data['payload']['data']);
          const jsonData = JSON.parse(decryptedData);
          console.log('decrypted payload data:');
          console.log(jsonData);

          const formId = data.meta.formMetadata.id;
          const username = jsonData.submission.data.username;
          dataWarehouse.save(username, formId, m.seq, jsonData);
        }
      } catch (err) {
        console.error('  Error decrypting payload.data - check ENCRYPTION_KEY');
      }
    }
  } catch (e) {
    console.error(`Error printing message: ${e.message}`);
  }
};

const init = async () => {
  if (nc && nc.info != undefined) {
    // already connected.
    return;
  } else {
    // open a connection...
    try {
      // no credentials provided.
      // anonymous connections have read access to the stream
      console.log(`connect to nats server(s) ${servers} as '${USERNAME}'...`);
      nc = await natsConnect({
        servers: servers,
        reconnectTimeWait: 10 * 1000, // 10s
        user: USERNAME,
        pass: PASSWORD,
      });

      console.log('access jetstream...');
      js = nc.jetstream();
      console.log('get jetstream manager...');
      jsm = await js.jetstreamManager();
      await jsm.consumers.add(STREAM_NAME, {
        ack_policy: AckPolicy.Explicit,
        durable_name: DURABLE_NAME,
      });
      console.log(`get consumer: stream = ${STREAM_NAME}, durable name = ${DURABLE_NAME}...`);
      consumer = await js.consumers.get(STREAM_NAME, DURABLE_NAME);
    } catch (e) {
      console.error(e);
    }
  }
};

const pull = async () => {
  if (consumer) {
    console.log('fetch...');
    let iter = await consumer.fetch({
      filterSubjects: FILTER_SUBJECTS,
      max_messages: MAX_MESSAGES,
    });
    for await (const m of iter) {
      processMsg(m);
      m.ack();
    }
  }
};

const run = async () => {
  await init();
  await pull();
};

const shutdown = async () => {
  if (nc) {
    console.log('drain connection...');
    try {
      await nc.drain();
    } catch {}
  }
};

module.exports = { run, shutdown };
