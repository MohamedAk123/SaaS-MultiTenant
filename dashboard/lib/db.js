import pg from 'pg';
import { createHmac, randomBytes, timingSafeEqual } from 'node:crypto';

export const tenants = Array.from({ length: 6 }, (_, i) => `tenant-${i + 1}`);
export const pool = new pg.Pool({ host: process.env.PGHOST, user: 'saas', database: 'saas', password: process.env.SAAS_SECRET, max: 5 });
const schema = `
CREATE TABLE IF NOT EXISTS sessions (token text PRIMARY KEY, tenant_id text NOT NULL, expires timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS activity (id uuid PRIMARY KEY, tenant_id text NOT NULL, kind text NOT NULL,
    resource text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS outbox (event_id uuid PRIMARY KEY REFERENCES activity(id), payload jsonb NOT NULL,
    published_at timestamptz);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(event_id) WHERE published_at IS NULL;`;
let initialized;
export function initialize() {
  if (!initialized) initialized = pool.query(schema).catch(error => { initialized = undefined; throw error; });
  return initialized;
}

export function tenantPassword(tenant) {
  return createHmac('sha256', process.env.SAAS_SECRET).update(tenant).digest('hex').slice(0, 24);
}

export async function login(tenant, password) {
  if (!tenants.includes(tenant) || typeof password !== 'string' || !/^[a-f0-9]{24}$/.test(password)) return null;
  if (!timingSafeEqual(Buffer.from(password), Buffer.from(tenantPassword(tenant)))) return null;
  await initialize();
  const token = randomBytes(32).toString('hex');
  await pool.query("INSERT INTO sessions VALUES ($1, $2, now() + interval '8 hours')", [token, tenant]);
  return token;
}

export async function tenantFor(token) {
  if (typeof token !== 'string' || !/^[a-f0-9]{64}$/.test(token)) return null;
  await initialize();
  const { rows } = await pool.query('SELECT tenant_id FROM sessions WHERE token = $1 AND expires > now()', [token]);
  return rows[0]?.tenant_id ?? null;
}
