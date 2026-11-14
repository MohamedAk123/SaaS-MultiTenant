import { NextResponse } from 'next/server';
import { cookies } from 'next/headers';
import { login, pool } from '../../../lib/db.js';

export async function POST(request) {
  const { tenant, password } = await request.json().catch(() => ({}));
  const token = await login(tenant, password);
  if (!token) return NextResponse.json({ error: 'Identifiants invalides.' }, { status: 401 });
  const response = NextResponse.json({ tenant });
  response.cookies.set('saas_session', token, { httpOnly: true, sameSite: 'strict', path: '/', maxAge: 28800 });
  return response;
}

export async function DELETE() {
  const token = (await cookies()).get('saas_session')?.value;
  if (token) await pool.query('DELETE FROM sessions WHERE token = $1', [token]);
  const response = NextResponse.json({ ok: true });
  response.cookies.delete('saas_session');
  return response;
}
