const https = require('https');
const request = require('request');
const util = require('util');
const Kafka = require('node-rdkafka');
const get = util.promisify(request.get);
const post = util.promisify(request.post);

const consumer_key = 'twitter api key'; // Add your API key here
const consumer_secret = 'twitter secret key'; // Add your API secret key here

const bearerTokenURL = new URL('https://api.twitter.com/oauth2/token');
const streamURL = new URL('https://api.twitter.com/labs/1/tweets/stream/sample');

function createProducer(onDeliveryReport) {
  const producer = new Kafka.Producer({
    'bootstrap.servers': '<server name>',
    'sasl.username': '<user name>',
    'sasl.password': '<password>',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'dr_msg_cb': true
  });

  return new Promise((resolve, reject) => {
    producer
      .on('ready', () => resolve(producer))
      .on('delivery-report', onDeliveryReport)
      .on('event.error', (err) => {
        console.warn('event.error', err);
        reject(err);
      });
    producer.connect();
  });
}

async function bearerToken (auth) {
  const requestConfig = {
    url: bearerTokenURL,
    auth: {
      user: consumer_key,
      pass: consumer_secret,
    },
    form: {
      grant_type: 'client_credentials',
    },
    headers: {
      'User-Agent': 'TwitterDevSampledStreamQuickStartJS',
    },
  };

  const response = await post(requestConfig);
  return JSON.parse(response.body).access_token;
}

async function streamConnect(token) {
  const producer = await createProducer( (err, report) => {
    if (err) {
      console.warn('Error producing', err)
    } else {
      const {topic, partition, value} = report;
      console.log(`Successfully produced record to topic "${topic}" partition ${partition} ${value}`);
    }
  });
  // Listen to the stream
  const config = {
    url: 'https://api.twitter.com/labs/1/tweets/stream/sample?format=compact',
    auth: {
      bearer: token,
    },
    headers: {
      'User-Agent': 'TwitterDevSampledStreamQuickStartJS',
    },
    timeout: 20000,
  };

  const stream = request.get(config);

  stream.on('data', data => {
    try {
      const json = JSON.parse(data);
      console.log(json);
      const key = 'twitterdata';
      const value = Buffer.from(data);
  
      console.log(`Producing record ${key}\t${value}`);
  
      producer.produce("twittersampledata", -1, value, key);


    } catch (e) {
      // Keep alive signal received. Do nothing.
    }
  }).on('error', error => {
    if (error.code === 'ETIMEDOUT') {
      stream.emit('timeout');
    }
  });

  return stream;
}

(async () => {

  let token;

  try {
    // Exchange your credentials for a Bearer token
    token = await bearerToken({consumer_key, consumer_secret});
  } catch (e) {
    console.error(`Could not generate a Bearer token. Please check that your credentials are correct and that the Sampled Stream preview is enabled in your Labs dashboard. (${e})`);
    process.exit(-1);
  }

  const stream = streamConnect(token);
  stream.on('timeout', () => {
    // Reconnect on error
    console.warn('A connection error occurred. Reconnecting…');
    streamConnect(token);
  });
})();