'use client';
import { useId, useState } from 'react';

const compact = new Intl.NumberFormat('fr-FR', { notation: 'compact', maximumFractionDigits: 1 });
const decimal = new Intl.NumberFormat('fr-FR', { maximumFractionDigits: 4 });
const currency = new Intl.NumberFormat('fr-FR', { style: 'currency', currency: 'USD' });
const colors = ['#187758', '#bb4a23', '#4266af'];
const minute = new Intl.DateTimeFormat('fr-FR', { hour: '2-digit', minute: '2-digit', timeZone: 'UTC' });
const dateTime = new Intl.DateTimeFormat('fr-FR', { day:'numeric', month:'short', hour: '2-digit', minute: '2-digit', timeZone: 'UTC' });

function timestamp(value) {
  const iso = value.replace(' ', 'T');
  return Date.parse(/(?:Z|[+-]\d{2}:?\d{2})$/i.test(iso) ? iso : iso + 'Z');
}

function formatWindow(value, detailed = false) {
  const date = new Date(timestamp(value));
  return Number.isNaN(date.valueOf()) ? value : (detailed ? dateTime : minute).format(date);
}

export function Trend({ title, unit, rows, series }) {
  const id = useId();
  const [selection, select] = useState(null);
  const active = Math.max(0, rows.findIndex(row => row.time === selection));
  const valueLabel = unit === 'USD' ? value => currency.format(value) : value => decimal.format(value);
  const rawMax = Math.max(1, ...rows.flatMap(row => series.map(s => row[s.key])));
  const step = 10 ** Math.floor(Math.log10(rawMax / 4));
  const tick = Math.max(unit === 'USD' ? .01 : 1, Math.ceil(rawMax / (4 * step)) * step);
  const ceiling = tick * 4;
  const firstTime = rows.length ? timestamp(rows[0].time) : 0;
  const span = rows.length ? timestamp(rows.at(-1).time) - firstTime : 0;
  const x = index => rows.length === 1 ? 325 : 56 + 539 * (span > 0 ? (timestamp(rows[index].time) - firstTime) / span : index / (rows.length - 1));
  const y = value => 172 - 147 * value / ceiling;
  const shown = rows.reduce((indices, _, i) => {
    if (!indices.length || x(i) - x(indices.at(-1)) >= 76) indices.push(i);
    return indices;
  }, []);
  return <article className="panel chart-panel">
    <div className="chart-heading"><h3>{title}</h3><span>{unit} · UTC</span></div>
    {rows.length === 0 ? <p className="empty">Aucune fenêtre disponible.</p> : <>
      <div className="chart-scroll"><svg className="trend" viewBox="0 0 612 204" role="img" aria-labelledby={id}>
        <title id={id}>{title} par fenêtre de dix minutes</title>
        {[0,1,2,3,4].map(i => <g key={i}>
          <line className="gridline" x1="56" x2="595" y1={y(i*tick)} y2={y(i*tick)} />
          <text className="axis" x="47" y={y(i*tick)+4} textAnchor="end">{compact.format(i*tick)}</text>
        </g>)}
        <line x1={x(active)} x2={x(active)} y1="22" y2="172" stroke="#9baca8" strokeDasharray="3 4" />
        {series.map(({key}, i) => <g key={key} stroke={colors[i]} fill={colors[i]}>
          <polyline fill="none" strokeWidth="3" strokeLinejoin="round" strokeLinecap="round" points={rows.map((r,j) => `${x(j)},${y(r[key])}`).join(' ')} />
          {rows.map((r,j) => <circle key={r.time} cx={x(j)} cy={y(r[key])} r={j === active ? 5 : 3} stroke="white" strokeWidth="1.5" />)}
        </g>)}
        {shown.map(i => <text key={i} className="axis" x={x(i)} y="193" textAnchor="middle">{formatWindow(rows[i].time)}</text>)}
        {rows.map((row,i) => {
          const left = i === 0 ? 47 : (x(i-1)+x(i))/2;
          const right = i === rows.length-1 ? 608 : (x(i)+x(i+1))/2;
          return <rect className="chart-hit" key={row.time} x={left} y="17" width={right-left} height="161" fill="transparent"
            onPointerEnter={() => select(row.time)} onClick={() => select(row.time)} />;
        })}
      </svg></div>
      <div className="chart-readout">
        <label>Fenêtre <select value={rows[active].time} onChange={e => select(e.target.value)}>
          {rows.map(row => <option value={row.time} key={row.time}>{formatWindow(row.time, true)}</option>)}
        </select></label>
        <div className="chart-values">{series.map((s,i) => <span key={s.key}><i style={{background:colors[i]}} />{s.label} <b>{valueLabel(rows[active][s.key])}</b></span>)}</div>
      </div>
    </>}
  </article>;
}

export function Ranking({ title, rows, name, value, color = colors[0], format = decimal.format }) {
  const max = Math.max(1e-9, ...rows.map(value));
  return <article className="panel"><h3>{title}</h3>
    {rows.length ? <ul className="ranking">{rows.map(row => <li key={name(row)}>
      <span>{name(row)}</span><b>{format(value(row))}</b>
      <div className="ranking-track"><div style={{ width: `${100 * value(row)/max}%`, background: color }} /></div>
    </li>)}</ul> : <p className="empty">Aucune donnée disponible.</p>}
  </article>;
}

export function AccessGraph({ graph }) {
  const [focus, setFocus] = useState(null);
  const marker = useId();
  if (!graph?.nodes.length) return null;
  const active = graph.nodes.some(n => n.id === focus) ? focus : null;
  const connected = new Set([active]);
  graph.edges.filter(e => e.src === active || e.dst === active).forEach(e => { connected.add(e.src); connected.add(e.dst); });
  const positions = new Map();
  const types = [['user','Utilisateur'], ['role','Rôles'], ['resource','Restaurants']];
  const harmonized = {user:'#e3f1fd',role:'#def4e9',resource:'#fff0dd'};
  types.forEach(([type], col) => {
    const nodes = graph.nodes.filter(n => n.type === type);
    nodes.forEach((node,i) => positions.set(node.id, { x: 15 + col*226, y: nodes.length === 1 ? 126 : 60+i*172/(nodes.length-1) }));
  });
  const toggle = id => setFocus(active === id ? null : id);
  return <article className="panel">
    <div className="chart-heading"><h3>Qui peut accéder à quel restaurant ?</h3><button onClick={() => setFocus(null)} disabled={!active}>Tout afficher</button></div>
    <p className="muted">Sous-graphe de simulation. Sélectionne un sommet pour voir ses liens directs.</p>
    <div className="chart-scroll"><svg className="network-chart" viewBox="0 0 635 270" aria-label="Sous-graphe interactif des accès">
      <defs><marker id={marker} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0 0L10 5L0 10Z" fill="#658a81" /></marker></defs>
      {types.map(([type,label],col) => <text className="axis group-label" key={type} x={95+col*226} y="24" textAnchor="middle">{label}</text>)}
      {graph.edges.map(edge => {
        const a = positions.get(edge.src), b = positions.get(edge.dst);
        if (!a || !b) return null;
        return <path key={edge.src+edge.dst} d={`M${a.x+160},${a.y} C${a.x+195},${a.y} ${b.x-34},${b.y} ${b.x-4},${b.y}`}
          fill="none" stroke="#658a81" strokeWidth="2" markerEnd={`url(#${marker})`} opacity={!active || edge.src === active || edge.dst === active ? .85 : .12} />;
      })}
      {graph.nodes.map(node => {
        const pos = positions.get(node.id);
        return <g key={node.id} role="button" tabIndex={0} aria-pressed={active === node.id} aria-label={node.label} className="graph-node"
          onClick={() => toggle(node.id)} onKeyDown={event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); toggle(node.id); } }}
          opacity={!active || connected.has(node.id) ? 1 : .35}>
          <rect x={pos.x} y={pos.y-15} width="160" height="30" rx="7" fill={harmonized[node.type]} stroke={active === node.id ? '#187758' : 'none'} strokeWidth="2" />
          <text x={pos.x+80} y={pos.y+5} textAnchor="middle">{node.label}</text>
        </g>;
      })}
    </svg></div>
  </article>;
}
