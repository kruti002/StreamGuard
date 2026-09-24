import SourceCard from './SourceCard.jsx';

export default function HealthBoard({ sources }) {
  if (!sources || sources.length === 0) {
    return (
      <div className="board">
        <div className="waiting">
          Waiting for data… start the Flink jobs and the feeder.
        </div>
      </div>
    );
  }
  return (
    <div className="board">
      {sources.map((s) => (
        <SourceCard key={s.source} source={s} />
      ))}
    </div>
  );
}
