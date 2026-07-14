'use strict';

const { MongoClient } = require('mongodb');
const { Kafka, CompressionTypes } = require('kafkajs');

const KAFKA_ENABLED = process.env.KAFKA_ENABLED !== 'false';
const MONGO_ENABLED  = process.env.MONGO_ENABLED  !== 'false';

let mongoClient = null;
let mongoCollection = null;
let instagramAccountClient = null;
let kafkaProducer = null;

async function connectMongo() {
  if (!MONGO_ENABLED) return;
  try {
    const uri = `mongodb://${encodeURIComponent(process.env.MONGO_USERNAME)}:${encodeURIComponent(process.env.MONGO_PASSWORD)}@${process.env.MONGO_HOST}:${process.env.MONGO_PORT}`;
    mongoClient = new MongoClient(uri, { serverSelectionTimeoutMS: 8000 });
    await mongoClient.connect();
    const initialDbName = process.env.MONGO_DB_NAME;
    const initialCollectionName = process.env.MONGO_COLLECTION_LIST || process.env.MONGO_COLLECTION;
    mongoCollection = mongoClient.db(initialDbName).collection(initialCollectionName);
    console.log('[MONGO] Connected ✓');
  } catch (e) {
    console.error('[MONGO] Connection failed:', e.message);
  }
}

async function connectKafka() {
  if (!KAFKA_ENABLED) return;
  try {
    const brokers = (process.env.KAFKA_BOOTSTRAP || '').split(',').map(b => b.trim()).filter(Boolean);
    const kafka = new Kafka({ clientId: 'webinject-dashboard', brokers });
    kafkaProducer = kafka.producer();
    await kafkaProducer.connect();
    console.log('[KAFKA] Connected ✓');
  } catch (e) {
    console.error('[KAFKA] Connection failed:', e.message);
  }
}

function hasInstagramAccountConfig() {
  return !!(
    process.env.MONGO_USER &&
    process.env.MONGO_PASS &&
    (process.env.MONGO_ACCOUNT_HOST || process.env.MONGO_HOST) &&
    (process.env.MONGO_ACCOUNT_PORT || process.env.MONGO_PORT) &&
    process.env.MONGO_DB_ACCOUNT &&
    process.env.MONGO_COLLECTION_ACCOUNT
  );
}

async function connectInstagramAccountMongo() {
  if (!hasInstagramAccountConfig()) return;
  try {
    const accountHost = process.env.MONGO_ACCOUNT_HOST || process.env.MONGO_HOST;
    const accountPort = process.env.MONGO_ACCOUNT_PORT || process.env.MONGO_PORT;
    const uri = `mongodb://${encodeURIComponent(process.env.MONGO_USER)}:${encodeURIComponent(process.env.MONGO_PASS)}@${accountHost}:${accountPort}`;
    instagramAccountClient = new MongoClient(uri, { serverSelectionTimeoutMS: 8000 });
    await instagramAccountClient.connect();
    console.log('[MONGO:IG_ACCOUNT] Connected ✓');
  } catch (e) {
    instagramAccountClient = null;
    console.error('[MONGO:IG_ACCOUNT] Connection failed:', e.message);
  }
}

async function connectAll() {
  await Promise.allSettled([connectMongo(), connectKafka(), connectInstagramAccountMongo()]);
}

/**
 * Get MongoDB collection instance
 * @param {string} [collectionName] - Optional specific collection name, defaults to process.env.MONGO_COLLECTION
 * @param {string} [dbName] - Optional specific database name, defaults to process.env.MONGO_DB_NAME
 */
function getMongoCollection(collectionName, dbName) {
  if (!mongoClient) return null;
  const name = collectionName || process.env.MONGO_COLLECTION_LIST || process.env.MONGO_COLLECTION || 'youtube_list';
  const database = dbName || process.env.MONGO_DB_NAME;
  return mongoClient.db(database).collection(name);
}

function getInstagramAccountCollection() {
  if (!instagramAccountClient) return null;
  return instagramAccountClient
    .db(process.env.MONGO_DB_ACCOUNT)
    .collection(process.env.MONGO_COLLECTION_ACCOUNT);
}

/**
 * Get Kafka producer instance
 */
function getKafkaProducer() {
  return kafkaProducer;
}

/**
 * Sends a message to a specific Kafka topic.
 * @param {string} topic  - The Kafka topic name to send to (per social media source)
 * @param {string} key    - Message key (e.g. post id)
 * @param {object} value  - Message payload object (will be JSON-serialised)
 */
async function produceMessage(topic, key, value) {
  if (!kafkaProducer) throw new Error('Kafka producer not connected');
  if (!topic) throw new Error('Kafka topic is required');
  await kafkaProducer.send({
    topic,
    messages: [{ key: String(key), value: JSON.stringify(value) }],
    compression: CompressionTypes.None,
  });
}

function getStatus() {
  return {
    kafkaEnabled: KAFKA_ENABLED,
    mongoEnabled: MONGO_ENABLED,
    kafkaConnected: !!kafkaProducer,
    mongoConnected: !!mongoCollection,
    instagramAccountMongoConnected: !!instagramAccountClient
  };
}

module.exports = {
  connectAll,
  getMongoCollection,
  getInstagramAccountCollection,
  getKafkaProducer,
  produceMessage,
  getStatus
};
