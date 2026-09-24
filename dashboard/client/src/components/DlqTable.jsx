import { fmtTime } from '../format.js';

export default function DlqTable({ dlq }) {
  return (
    <table className="dlq">
      <thead>
        <tr>
          <th>Time</th>
          <th>Source</th>
          <th>Event ID</th>
          <th>Failed rules</th>
        </tr>
      </thead>
      <tbody>
        {!dlq || dlq.length === 0 ? (
          <tr>
            <td colSpan={4} className="muted">
              No quarantined records yet.
            </td>
          </tr>
        ) : (
          dlq.map((d, i) => (
            <tr key={`${d.event_id ?? 'null'}-${d.at}-${i}`}>
              <td>{fmtTime(d.at)}</td>
              <td>{d.source ?? '—'}</td>
              <td>{d.event_id ?? <em>null</em>}</td>
              <td className="rules">
                {(d.violations || []).length
                  ? d.violations.map((v, j) => (
                      <span className="tag" key={j}>
                        {v}
                      </span>
                    ))
                  : '—'}
              </td>
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}
