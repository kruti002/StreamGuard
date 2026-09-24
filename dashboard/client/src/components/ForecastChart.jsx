import { useEffect, useRef } from 'react';
import { SLA_PCT, worstSource } from '../format.js';

// Canvas line chart: observed error rate vs ML_FORECAST, with the SLA line.
// Redraws whenever the worst source's history changes.
export default function ForecastChart({ history, sources }) {
  const canvasRef = useRef(null);
  const worst = worstSource(sources);
  const series = worst ? history[worst.source] || [] : [];

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const w = canvas.width;
    const h = canvas.height;
    ctx.clearRect(0, 0, w, h);

    const pad = { l: 44, r: 14, t: 16, b: 24 };
    const dataMax = Math.max(
      SLA_PCT * 2,
      ...series.flatMap((p) => [p.current, p.forecast].map(Number).filter((n) => !Number.isNaN(n)))
    );
    // round the axis up to a clean number so the scale reads nicely
    const niceMax = dataMax <= 10 ? 10 : Math.ceil(dataMax / 10) * 10;
    const maxY = niceMax;
    const x = (i) => pad.l + (i / Math.max(1, series.length - 1)) * (w - pad.l - pad.r);
    const y = (v) => h - pad.b - (v / maxY) * (h - pad.t - pad.b);

    // grid + y labels
    ctx.strokeStyle = 'rgba(255,255,255,0.06)';
    ctx.fillStyle = '#8a97b3';
    ctx.font = '11px system-ui';
    for (let g = 0; g <= 4; g++) {
      const val = (maxY / 4) * g;
      const yy = y(val);
      ctx.beginPath();
      ctx.moveTo(pad.l, yy);
      ctx.lineTo(w - pad.r, yy);
      ctx.stroke();
      ctx.fillText(`${val.toFixed(1)}%`, 6, yy + 3);
    }

    // SLA line + label
    ctx.strokeStyle = '#e74c3c';
    ctx.setLineDash([5, 4]);
    ctx.beginPath();
    ctx.moveTo(pad.l, y(SLA_PCT));
    ctx.lineTo(w - pad.r, y(SLA_PCT));
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = '#e74c3c';
    ctx.fillText('2% SLA', w - pad.r - 46, y(SLA_PCT) - 4);

    if (series.length < 2) {
      ctx.fillStyle = '#8a97b3';
      ctx.fillText('Collecting windows…', pad.l + 8, pad.t + 16);
      return;
    }

    const drawLine = (key, color, dash) => {
      ctx.strokeStyle = color;
      ctx.lineWidth = 2;
      ctx.setLineDash(dash);
      ctx.beginPath();
      series.forEach((p, i) => {
        const xx = x(i);
        const yy = y(Number(p[key]) || 0);
        i ? ctx.lineTo(xx, yy) : ctx.moveTo(xx, yy);
      });
      ctx.stroke();
      ctx.setLineDash([]);
    };

    drawLine('current', '#4c8dff', []);
    drawLine('forecast', '#b98bff', [6, 4]);
  }, [series]);

  return (
    <>
      <canvas ref={canvasRef} width={900} height={280} className="chart-canvas" />
      <div className="legend">
        {worst ? (
          <>
            <span className="obs">{worst.source} observed</span>
            <span className="fc">forecast</span>
            <span className="sla">2% SLA</span>
          </>
        ) : null}
      </div>
    </>
  );
}
