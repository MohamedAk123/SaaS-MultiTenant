'use client';

import { useEffect, useRef, useState } from 'react';

const number = new Intl.NumberFormat('fr-FR');
const money = new Intl.NumberFormat('fr-FR', { style: 'currency', currency: 'USD' });
const labels = { ACTION: 'Action', ERROR: 'Erreur', ACCESS: 'Accès' };
const clock = value => new Date(value).toLocaleTimeString('fr-FR', { timeZone: 'UTC' });

export default function Dashboard() {
  const [data, setData] = useState(null);
  const [ready, setReady] = useState(false);
  const [notice, setNotice] = useState('');
  const [latency, setLatency] = useState(null);
  const pending = useRef(new Map());
  const refreshVersion = useRef(0);
  const [waiting, setWaiting] = useState(false);

  async function refresh() {
    const version = refreshVersion.current;
    const response = await fetch('/api/dashboard', { cache: 'no-store' });
    if (response.status === 401) {
      if (version === refreshVersion.current) setData(null);
      return;
    }
    if (!response.ok) throw new Error('Impossible de charger les données.');
    const next = await response.json();
    if (version !== refreshVersion.current) return;
    for (const event of next.activity.events) {
      if (pending.current.delete(event.id)) setLatency(Math.max(0, Date.now() - new Date(event.eventTime).getTime()));
    }
    setData(next);
    setWaiting(pending.current.size > 0);
  }

  useEffect(() => {
    const update = () => refresh().catch(error => setNotice(error.message)).finally(() => setReady(true));
    update();
    const interval = setInterval(update, 3000);
    return () => clearInterval(interval);
  }, []);

  async function signIn(event) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    setNotice('');
    try {
      const response = await fetch('/api/session', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tenant: form.get('tenant'), password: form.get('password') }) });
      if (!response.ok) throw new Error(response.status === 401 ? 'Identifiants invalides.' : 'Connexion indisponible.');
      refreshVersion.current++;
      await refresh();
    } catch (error) { setNotice(error.message); }
  }

  async function publish(kind) {
    setNotice('');
    try {
      const response = await fetch('/api/events', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ kind, resource: 'commandes' }) });
      if (!response.ok) throw new Error('Événement non enregistré.');
      const { id } = await response.json();
      pending.current.set(id, true);
      setWaiting(true);
    } catch (error) { setNotice(error.message); }
  }

  async function signOut() {
    try {
      const response = await fetch('/api/session', { method: 'DELETE' });
      if (!response.ok) throw new Error('Déconnexion indisponible.');
    } catch { setNotice('Déconnexion indisponible.'); return; }
    refreshVersion.current++;
    pending.current.clear();
    setWaiting(false);
    setLatency(null);
    setData(null);
  }

  if (!ready) return <main className="login"><p>Chargement…</p></main>;
  if (!data) return <main className="login">
    <p className="brand"><span>◈</span> SaaS Multi-Tenant</p>
    <form onSubmit={signIn}>
      <h1>Connexion</h1>
      <p>Consulte les indicateurs de ton espace.</p>
      <label>Tenant<select name="tenant">{Array.from({ length: 6 }, (_, i) => <option key={i}>tenant-{i + 1}</option>)}</select></label>
      <label>Mot de passe<input type="password" name="password" required autoComplete="current-password" /></label>
      <button className="primary">Se connecter</button>
      {notice && <p role="alert">{notice}</p>}
    </form>
  </main>;

  const { historical, activity, graph, alerts } = data;
  const maxRevenue = Math.max(1, ...historical.map(row => Number(row.revenue_usd)));
  const actions = activity.counts.ACTION ?? 0;
  const errors = activity.counts.ERROR ?? 0;
  const access = activity.counts.ACCESS ?? 0;
  const errorRate = actions + errors + access ? errors / (actions + errors + access) : 0;
  return <>
    <header><a className="brand" href="/"><span>◈</span> SaaS Multi-Tenant</a><nav aria-label="Sections"><a href="#activity">Activité</a><a href="#orders">Commandes</a><a href="#graph">Graphe</a></nav><button onClick={signOut}>Déconnexion</button></header>
    <main className="workspace">
      <div className="intro"><div><p className="eyebrow">{data.tenant} · {data.market}</p><h1>Vue d’ensemble</h1></div><span className="status">{activity.updatedAt ? '● Flux actif' : '○ En attente d’événements'}</span></div>
      <section id="activity">
        <h2>Activité de l’application</h2>
        <div className="cards"><Metric label="Actions" value={actions}/><Metric label="Accès aux ressources" value={access}/><Metric label="Erreurs" value={errors}/><Metric label="Part des erreurs" value={new Intl.NumberFormat('fr-FR', {style:'percent', maximumFractionDigits:1}).format(errorRate)}/></div>
        <div className="panel">
          <div className="panelhead"><h3>Événements récents</h3><div className="buttons">{Object.entries(labels).map(([kind, label]) => <button key={kind} onClick={() => publish(kind)}>Simuler {label.toLowerCase()}</button>)}</div></div>
          <p className="muted" role="status">{notice || (waiting ? 'Événement enregistré. En attente du calcul Spark…' : latency !== null ? `Visible après ${(latency/1000).toFixed(2)} s depuis l’enregistrement.` : 'La vue se rafraîchit toutes les trois secondes.')}</p>
          {activity.events.length ? <div className="scroll"><table><thead><tr><th>Heure UTC</th><th>Type</th><th>Ressource</th></tr></thead><tbody>{activity.events.slice(0,8).map(row => <tr key={row.id}><td>{clock(row.eventTime)}</td><td>{labels[row.kind]}</td><td>{row.resource}</td></tr>)}</tbody></table></div> : <p className="empty">Les événements traités par Spark apparaîtront ici.</p>}
        </div>
        {activity.windows.length > 0 && <div className="panel"><h3>Activité par fenêtre de 10 minutes</h3><table><thead><tr><th>Début UTC</th><th>Événements</th><th>Erreurs</th></tr></thead><tbody>{activity.windows.map(row => <tr key={row.start}><td>{row.start}</td><td>{row.total}</td><td>{row.errors}</td></tr>)}</tbody></table></div>}
      </section>
      <section id="orders"><h2>Commandes · historique DoorDash</h2><div className="panel"><h3>Chiffre d’affaires par fenêtre de 10 minutes</h3>
        {historical.length ? historical.map(row => <div className="bar-row" key={row.window.start}><time>{row.window.start.slice(11,16)} UTC</time><div className="bar"><div style={{ width: `${100 * Number(row.revenue_usd) / maxRevenue}%` }} /></div><strong>{money.format(Number(row.revenue_usd))}</strong><span>{row.orders_delivered} livrées</span></div>) : <p className="empty">Aucune fenêtre disponible pour ce tenant.</p>}
      </div></section>
      <section id="graph"><h2>Graphe des accès</h2>
        {graph && <div className="cards"><Metric label="Sommets" value={graph.vertices}/><Metric label="Arêtes" value={graph.edges}/><Metric label="Ressources" value={graph.resources}/><Metric label="Alertes de simulation" value={alerts.length}/></div>}
        <div className="panel"><h3>Alertes à examiner</h3>{alerts.length ? <ul>{alerts.slice(0,8).map(row => <li key={row.id}><code>{row.id}</code> · {row.rule}</li>)}</ul> : <p className="empty">Aucune alerte pour ce tenant.</p>}</div>
        {data.rankings.length > 0 && <div className="panel"><h3>Sommets les plus centraux (GraphX)</h3><div className="scroll"><table><thead><tr><th>Sommet</th><th>Degré</th><th>PageRank</th></tr></thead><tbody>{data.rankings.map(row => <tr key={row.id}><td>{row.id}</td><td>{row.degree}</td><td>{row.pagerank.toFixed(4)}</td></tr>)}</tbody></table></div></div>}
      </section>
    </main>
  </>;
}

function Metric({ label, value }) {
  return <article className="metric"><span>{label}</span><strong>{typeof value === 'number' ? number.format(value) : value}</strong></article>;
}
