import { fmtPct, fmtEta } from '../format.js';

export default function SourceCard({ source }) {
  const status = (source.status || 'OK').toLowerCase();
  const eta = source.minutes_to_sla_breach;
  const etaHot = status !== 'ok' ? 'eta-hot' : '';
  const showReason = source.top_reason && source.top_reason !== 'NONE';

  return (
    <div className={`card ${status}`}>
      <span className="src">{source.source}</span>
      <span className={`badge ${status}`}>{source.status || 'OK'}</span>
      <div className="metrics">
        <Metric label="Error rate" value={fmtPct(source.current_error_pct)} />
        <Metric label="Forecast" value={fmtPct(source.forecast_error_pct)} small />
        <Metric label="Time to breach" value={fmtEta(eta)} small className={etaHot} />
        <Metric label="Events / window" value={source.total_events ?? '—'} small />
      </div>
      {showReason && (
        <div className="reason">
          Top failure mode: <strong>{source.top_reason}</strong>
        </div>
      )}
    </div>
  );
}

function Metric({ label, value, small, className = '' }) {
  return (
    <div className="metric">
      <div className="k">{label}</div>
      <div className={`v ${small ? 'small' : ''} ${className}`}>{value}</div>
    </div>
  );
}
