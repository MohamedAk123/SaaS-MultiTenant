'use client';
import { useState } from 'react';
import { compareWindows, explainAccess } from './analysis.js';

const money = new Intl.NumberFormat('fr-FR', {style:'currency', currency:'USD'});
const signedMoney = new Intl.NumberFormat('fr-FR', {style:'currency', currency:'USD', signDisplay:'exceptZero'});
const signedNumber = new Intl.NumberFormat('fr-FR', {signDisplay:'exceptZero'});
const percent = new Intl.NumberFormat('fr-FR', {style:'percent', maximumFractionDigits:1, signDisplay:'exceptZero'});
const seconds = new Intl.NumberFormat('fr-FR', {maximumFractionDigits:2});
const hour = new Intl.DateTimeFormat('fr-FR', {hour:'2-digit', minute:'2-digit', second:'2-digit', timeZone:'UTC'});
const kinds = {ACTION:'Action', ERROR:'Erreur', ACCESS:'Accès'};
const time = value => hour.format(new Date(value));
const windowLabel = value => value.replace('T', ' ').slice(5,16) + ' UTC';

export function WindowComparison({ rows }) {
  const [reference, setReference] = useState('');
  const [current, setCurrent] = useState('');
  if (rows.length < 2) return null;
  const a = rows.find(row => row.time === reference) ?? rows.at(-2);
  const b = rows.find(row => row.time === current) ?? rows.at(-1);
  const delta = compareWindows(a, b);
  const direction = delta.revenue > 0 ? 'up' : delta.revenue < 0 ? 'down' : '';
  return <article className="panel showcase">
    <div className="showcase-heading"><h3>Comparer deux fenêtres</h3><span>Historique DoorDash · 10 min</span></div>
    <div className="demo-fields">
      <label>Référence<select value={a.time} onChange={e => setReference(e.target.value)}>{rows.map(row => <option key={row.time} value={row.time}>{windowLabel(row.time)}</option>)}</select></label>
      <label>Fenêtre comparée<select value={b.time} onChange={e => setCurrent(e.target.value)}>{rows.map(row => <option key={row.time} value={row.time}>{windowLabel(row.time)}</option>)}</select></label>
    </div>
    <div className="comparison">
      <div><span>Chiffre d’affaires</span><strong className={direction}>{signedMoney.format(delta.revenue)}</strong><small>{money.format(a.revenue)} → {money.format(b.revenue)} · {delta.percent === null ? 'Variation relative indisponible (base nulle)' : percent.format(delta.percent)}</small></div>
      <div><span>Commandes livrées</span><strong>{signedNumber.format(delta.delivered)}</strong><small>{a.delivered} → {b.delivered}</small></div>
      <div><span>Commandes créées</span><strong>{signedNumber.format(delta.created)}</strong><small>{a.created} → {b.created}</small></div>
    </div>
  </article>;
}

export function EventTrace({ trace, events, observed, latestId }) {
  const [choice, setChoice] = useState('');
  const row = trace.find(item => item.id === (choice || latestId)) ?? trace[0];
  if (!row) return <article className="panel showcase"><h3>Parcours d’un événement</h3><p className="muted">Simule une action pour suivre son passage dans le pipeline.</p></article>;
  const seen = events.some(event => event.id === row.id) || observed.has(row.id);
  const latency = observed.get(row.id);
  const queued = row.published_at ? Math.max(0, Date.parse(row.published_at) - Date.parse(row.created_at)) : null;
  const steps = [
    ['PostgreSQL', time(row.created_at) + ' UTC', true],
    ['Kafka', row.published_at ? time(row.published_at) + ' UTC' : 'En attente de publication', !!row.published_at],
    ['Spark → Tableau de bord', seen ? 'Événement visible' : 'En attente du résultat', seen],
  ];
  return <article className="panel showcase">
    <div className="showcase-heading"><h3>Parcours d’un événement</h3><span>Observé depuis le tenant connecté</span></div>
    <label className="trace-choice">Événement<select value={row.id} onChange={e => setChoice(e.target.value)}>
      {trace.map(item => <option key={item.id} value={item.id}>{kinds[item.kind]} · {time(item.created_at)} · {item.id.slice(0,8)}</option>)}
    </select></label>
    <ol className="trace-steps">{steps.map(([label, detail, done]) => <li key={label} data-done={done}><strong>{label}</strong><span>{detail}</span></li>)}</ol>
    <p className="muted">{queued === null ? 'Mesure en cours.' : `Confirmation Kafka enregistrée après ${seconds.format(queued/1000)} s.`}
      {latency !== undefined && ` Visible dans cette session après ${seconds.format(latency/1000)} s.`}</p>
  </article>;
}

export function AccessExplanation({ graph }) {
  const [selected, setSelected] = useState('');
  if (!graph) return null;
  const user = graph.nodes.find(node => node.type === 'user');
  const resources = graph.nodes.filter(node => node.type === 'resource');
  const resource = resources.find(node => node.id === selected) ?? resources[0];
  if (!user || !resource) return null;
  const roles = explainAccess(graph, user.id, resource.id);
  return <article className="panel showcase">
    <div className="showcase-heading"><h3>Expliquer un accès</h3><span>Sous-graphe GraphFrames · simulation</span></div>
    <label className="trace-choice">Restaurant<select value={resource.id} onChange={e => setSelected(e.target.value)}>
      {resources.map(node => <option value={node.id} key={node.id}>{node.label}</option>)}
    </select></label>
    <div className="access-path"><b>{user.label}</b><span aria-hidden="true">→</span><div>{roles.length ? roles.map(role => <span key={role.id}>{role.label}</span>) : 'Aucun rôle autorisant cet accès'}</div><span aria-hidden="true">→</span><b>{resource.label}</b></div>
    <p className="muted" aria-live="polite">{roles.length ? `${roles.length} chemin${roles.length === 1 ? '' : 's'} HAS_ROLE → CAN_ACCESS dans le sous-graphe.` : 'Aucun chemin HAS_ROLE → CAN_ACCESS dans le sous-graphe.'}</p>
  </article>;
}
