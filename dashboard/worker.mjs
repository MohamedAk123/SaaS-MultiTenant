import { Kafka, logLevel, Partitioners } from 'kafkajs';
import { pool, initialize, tenants, tenantPassword } from './lib/db.js';
import { setTimeout as delay } from 'node:timers/promises';

if (process.argv[2] === '--credentials') {
  for (const tenant of tenants) console.log(`${tenant} ${tenantPassword(tenant)}`);
  process.exit(0);
}
const kafka = new Kafka({ clientId: 'saas-outbox', brokers: [process.env.KAFKA_BROKER], logLevel: logLevel.WARN });
await initialize();
const admin = kafka.admin();
await admin.connect();
await admin.createTopics({ topics: [{ topic: 'saas-events', numPartitions: 3, replicationFactor: 1 }] });
await admin.disconnect();
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner, allowAutoTopicCreation: false });
await producer.connect();
let stopping = false;
process.on('SIGTERM', () => { stopping = true; });
process.on('SIGINT', () => { stopping = true; });
while (!stopping) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query('SELECT event_id, payload FROM outbox WHERE published_at IS NULL ORDER BY event_id LIMIT 50 FOR UPDATE SKIP LOCKED');
    if (rows.length) {
      await producer.send({ topic: 'saas-events', acks: -1, messages: rows.map(row => ({ key: row.payload.tenant_id, value: JSON.stringify(row.payload) })) });
      await client.query('UPDATE outbox SET published_at = now() WHERE event_id = ANY($1::uuid[])', [rows.map(row => row.event_id)]);
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Outbox retry:', error.message);
  } finally { client.release(); }
  await delay(800);
}
await producer.disconnect();
await pool.end();
