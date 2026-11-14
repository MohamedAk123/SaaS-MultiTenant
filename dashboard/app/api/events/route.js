import { randomUUID } from 'node:crypto';
import { cookies } from 'next/headers';
import { NextResponse } from 'next/server';
import { pool, tenantFor } from '../../../lib/db.js';

export async function POST(request) {
  const tenant = await tenantFor((await cookies()).get('saas_session')?.value);
  if (!tenant) return NextResponse.json({ error: 'Connexion requise.' }, { status: 401 });
  const { kind, resource = 'commandes' } = await request.json().catch(() => ({}));
  if (!['ACTION', 'ERROR', 'ACCESS'].includes(kind) || !['commandes','restaurants','utilisateurs'].includes(resource)) {
    return NextResponse.json({ error: 'Événement invalide.' }, { status: 400 });
  }
  const client = await pool.connect();
  const id = randomUUID();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query('INSERT INTO activity(id, tenant_id, kind, resource) VALUES($1,$2,$3,$4) RETURNING created_at', [id, tenant, kind, resource]);
    const event = { event_id: id, tenant_id: tenant, kind, resource, event_time: rows[0].created_at.toISOString() };
    await client.query('INSERT INTO outbox(event_id,payload) VALUES ($1,$2)', [id, event]);
    await client.query('COMMIT');
    return NextResponse.json({ id, createdAt: event.event_time }, { status: 202 });
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally { client.release(); }
}
