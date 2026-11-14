// The dashboard shows only values available in the selected export.
export function compareWindows(reference, current) {
  const previous = Number(reference.revenue);
  const next = Number(current.revenue);
  return {
    revenue: next - previous,
    percent: previous > 0 ? (next - previous) / previous : null,
    delivered: current.delivered - reference.delivered,
    created: current.created - reference.created,
  };
}

export function explainAccess(graph, user, resource) {
  const nodes = new Map(graph.nodes.map(node => [node.id, node]));
  const roles = new Set(graph.edges.filter(edge => edge.src === user && edge.relationship === 'HAS_ROLE').map(edge => edge.dst));
  return graph.edges.filter(edge => edge.dst === resource && edge.relationship === 'CAN_ACCESS' && roles.has(edge.src))
    .map(edge => nodes.get(edge.src)).filter(node => node?.type === 'role');
}
