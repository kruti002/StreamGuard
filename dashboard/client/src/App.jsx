import { useEventStream } from './useEventStream.js';
import { fmtTime } from './format.js';
import HealthBoard from './components/HealthBoard.jsx';
import ForecastChart from './components/ForecastChart.jsx';
import DlqTable from './components/DlqTable.jsx';

export default function App() {
  const { snapshot, status } = useEventStream('/api/stream');
  const { sources = [], history = {}, dlq = [], updatedAt } = snapshot;

  const breaching = sources.filter((s) => s.status === 'BREACH').length;
  const watching = sources.filter((s) => s.status === 'WATCH').length;

  return (
    <>
      <header className="topbar">
        <div className="brand">
          <span className="logo">🛡️</span>
          <div>
            <h1>StreamGuard</h1>
            <p className="tagline">Shift-left data quality firewall · Confluent Cloud</p>
          </div>
        </div>
        <div className="status-strip">
          <ConnPill status={status} />
          {breaching > 0 && <span className="pill pill-breach">{breaching} breaching</span>}
          {watching > 0 && <span className="pill pill-watch">{watching} watch</span>}
          <span className="pill pill-info">
            SLA error budget: <strong>2.0%</strong>
          </span>
          <span className="muted">{updatedAt ? `updated ${fmtTime(updatedAt)}` : ''}</span>
        </div>
      </header>

      <main>
        <section className="panel">
          <h2>Source health</h2>
          <p className="hint">
            Each upstream source scored live. StreamGuard forecasts the error rate and flags a
            source <em>before</em> it breaches the SLA.
          </p>
          <HealthBoard sources={sources} />
        </section>

        <section className="panel">
          <h2>
            Error-rate forecast <span className="muted">(worst source)</span>
          </h2>
          <p className="hint">
            Solid = observed error rate. Dashed = <code>ML_FORECAST</code>. The red line is the 2%
            SLA. When the forecast crosses it, we alert.
          </p>
          <ForecastChart history={history} sources={sources} />
        </section>

        <section className="panel">
          <h2>
            Dead-letter queue <span className="muted">(quarantined records)</span>
          </h2>
          <p className="hint">
            Every rejected record is preserved with the exact contract rule it broke — never
            silently dropped.
          </p>
          <DlqTable dlq={dlq} />
        </section>
      </main>
    </>
  );
}

function ConnPill({ status }) {
  const map = {
    live: { cls: 'pill-live', text: 'live' },
    connecting: { cls: 'pill-idle', text: 'connecting…' },
    reconnecting: { cls: 'pill-idle', text: 'reconnecting…' },
  };
  const { cls, text } = map[status] || map.connecting;
  return <span className={`pill ${cls}`}>{text}</span>;
}
