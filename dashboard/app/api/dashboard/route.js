import { readFile } from 'node:fs/promises';
import { cookies } from 'next/headers';
import { NextResponse } from 'next/server';
import { tenantFor } from '../../../lib/db.js';

async function file(name, fallback) {
  try { return JSON.parse(await readFile(`/results/${name}`, 'utf8')); }
  catch (error) { if (error.code === 'ENOENT') return fallback; throw error; }
}

export async function GET() {
  const tenant = await tenantFor((await cookies()).get('saas_session')?.value);
  if (!tenant) return NextResponse.json({ error: 'Connexion requise.' }, { status: 401 });
  const [orders, live, graph, rankings] = await Promise.all([
    file('jalon2/dashboard-data.json', {}), file('live-dashboard.json', {}), file('graphframes.json', {}),
    file('graphx-dashboard.json', {}),
  ]);
  const sameTenant = row => row.tenant_id === tenant;
  return NextResponse.json({
    tenant,
    market: tenant.replace('tenant-', 'Market-'),
    historical: (orders.kpi_sample ?? []).filter(sameTenant).sort((a, b) => a.window.start.localeCompare(b.window.start)),
    graph: (graph.tenants ?? orders.graph?.tenants)?.find(sameTenant) ?? null,
    rankings: rankings[tenant] ?? [],
    alerts: (graph.anomalies ?? []).filter(sameTenant),
    activity: live[tenant] ?? { counts: {}, events: [], windows: [], updatedAt: null },
  }, { headers: { 'Cache-Control': 'private, no-store' } });
}
