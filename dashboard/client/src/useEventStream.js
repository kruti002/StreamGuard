import { useEffect, useRef, useState } from 'react';

// Subscribes to the backend SSE endpoint and returns the latest snapshot plus a
// connection status. Auto-reconnects (EventSource does this natively) and
// tolerates malformed payloads so the UI never crashes on a bad frame.
export function useEventStream(url = '/api/stream') {
  const [snapshot, setSnapshot] = useState({
    sources: [],
    history: {},
    dlq: [],
    updatedAt: null,
  });
  const [status, setStatus] = useState('connecting'); // connecting | live | reconnecting
  const esRef = useRef(null);

  useEffect(() => {
    const es = new EventSource(url);
    esRef.current = es;

    es.onopen = () => setStatus('live');
    es.onerror = () => setStatus('reconnecting');
    es.onmessage = (e) => {
      try {
        setSnapshot(JSON.parse(e.data));
      } catch {
        /* ignore malformed frame */
      }
    };

    return () => es.close();
  }, [url]);

  return { snapshot, status };
}
