import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { pool, tenantPassword } from './lib/db.js';

const origin = process.env.TEST_ORIGIN || 'http://localhost:3000';
const request = (path, { method = 'GET', cookie, body } = {}) => fetch(origin + path, {
  method, headers: { ...(cookie && { Cookie: cookie }), ...(body && { 'Content-Type': 'application/json' }) },
  body: body && JSON.stringify(body),
});
async function session(tenant) {
  const response = await request('/api/session', { method: 'POST', body: { tenant, password: tenantPassword(tenant) } });
  assert.equal(response.status, 200);
  return response.headers.get('set-cookie').split(';')[0];
}

const cookies = [];
try {
  assert.equal((await request('/api/dashboard')).status, 401);
  assert.equal((await request('/api/events', { method: 'POST', body: { kind: 'ACTION' } })).status, 401);
  const first = await session('tenant-1');
  cookies.push(first);
  const second = await session('tenant-2');
  cookies.push(second);
  const events = [];
  const start = Date.now();
  for (const kind of ['ACTION', 'ERROR', 'ACCESS']) {
    const response = await request('/api/events', { method: 'POST', cookie: first, body: { kind, tenant: 'tenant-2' } });
    assert.equal(response.status, 202);
    events.push({ kind, ...await response.json() });
  }
  const pending = new Map(events.map(event => [event.id, event]));
  const measured = [];
  while (pending.size && Date.now() - start < 160000) {
    const response = await request('/api/dashboard?tenant=tenant-2', { cookie: first });
    assert.equal(response.status, 200);
    const view = await response.json();
    assert.equal(view.tenant, 'tenant-1');
    assert(view.historical.every(row => row.tenant_id === 'tenant-1'));
    assert(view.alerts.every(row => row.tenant_id === 'tenant-1'));
    assert((view.rankings ?? []).every(row => row.id.startsWith('tenant-1:')));
    for (const event of view.activity.events) {
      const sent = pending.get(event.id);
      if (sent) {
        assert.equal(event.kind, sent.kind);
        measured.push({ id: event.id, kind: event.kind, milliseconds: Date.now() - Date.parse(sent.createdAt) });
        pending.delete(event.id);
      }
    }
    if (pending.size) await delay(1000);
  }
  assert.equal(pending.size, 0, 'Expected all events to pass through Spark and reach the dashboard');
  const other = await (await request('/api/dashboard', { cookie: second })).json();
  assert.equal(other.tenant, 'tenant-2');
  assert(!other.activity.events.some(row => events.some(sent => sent.id === row.id)));
  const { rows } = await pool.query(`SELECT count(*)::integer AS total FROM activity a JOIN outbox o ON o.event_id = a.id
    WHERE a.tenant_id = 'tenant-1' AND o.published_at IS NOT NULL AND a.id = ANY($1::uuid[])`, [events.map(event => event.id)]);
  assert.equal(rows[0].total, events.length);
  const durations = measured.map(m => m.milliseconds).sort((a,b) => a-b);
  console.log(JSON.stringify({ status: 'PASS', testedAt: new Date().toISOString(),
    definition: 'PostgreSQL created_at to first appearance in authenticated dashboard API (1 s polling)',
    events: measured, median_ms: durations[1], max_ms: durations[2],
    persisted_and_published: rows[0].total, cross_tenant_access_blocked: true }, null, 2));
} finally {
  await Promise.all(cookies.map(cookie => request('/api/session', { method: 'DELETE', cookie })));
  await pool.end();
}
