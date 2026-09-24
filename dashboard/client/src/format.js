export const SLA_PCT = 2.0;

export function fmtPct(n) {
  return n == null || Number.isNaN(Number(n)) ? '—' : `${Number(n).toFixed(2)}%`;
}

export function fmtEta(n) {
  if (n == null || Number.isNaN(Number(n))) return '—';
  const v = Number(n);
  if (v <= 0) return 'now';
  if (v < 1) return `${Math.round(v * 60)}s`;
  return `${v.toFixed(1)} min`;
}

export function fmtTime(iso) {
  try {
    return new Date(iso).toLocaleTimeString();
  } catch {
    return iso ?? '';
  }
}

// Backend already sorts worst-first; the first entry is the worst source.
export function worstSource(sources) {
  return sources && sources.length ? sources[0] : null;
}
