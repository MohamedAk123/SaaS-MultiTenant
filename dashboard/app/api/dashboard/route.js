import { readFile } from 'node:fs/promises';
import { cookies } from 'next/headers';
import { NextResponse } from 'next/server';
import { pool, tenantFor } from '../../../lib/db.js';

async function file(name, fallback) {
  try { return JSON.parse(await readFile(`/results/${name}`, 'utf8')); }
  catch (error) { if (error.code === 'ENOENT') return fallback; throw error; }
}

export async function GET() {
  const tenant = await tenantFor((await cookies()).get('saas_session')?.value);
  if (!tenant) return NextResponse.json({ error: 'Connexion requise.' }, { status: 401 });
  const [orders, live, graph, rankings, accessGraphs, trace] = await Promise.all([
    file('jalon2/dashboard-data.json', {}), file('live-dashboard.json', {}), file('graphframes.json', {}),
    file('graphx-dashboard.json', {}),
    file('graph-access-dashboard.json', {}),
    pool.query(`SELECT a.id, a.kind, a.created_at, o.published_at
      FROM activity a JOIN outbox o ON o.event_id = a.id
      WHERE a.tenant_id = $1 ORDER BY a.created_at DESC, a.id LIMIT 12`, [tenant]),
  ]);
  const sameTenant = row => row.tenant_id === tenant;
  return NextResponse.json({
    tenant,
    market: tenant.replace('tenant-', 'Market-'),
    historical: (orders.kpi_sample ?? []).filter(sameTenant).sort((a, b) => a.window.start.localeCompare(b.window.start)),
    graph: (graph.tenants ?? orders.graph?.tenants)?.find(sameTenant) ?? null,
    rankings: rankings[tenant] ?? [],
    accessGraph: accessGraphs[tenant] ?? null,
    trace: trace.rows,
    alerts: (graph.anomalies ?? []).filter(sameTenant),
    activity: live[tenant] ?? { counts: {}, events: [], windows: [], updatedAt: null },
  }, { headers: { 'Cache-Control': 'private, no-store' } });
}
