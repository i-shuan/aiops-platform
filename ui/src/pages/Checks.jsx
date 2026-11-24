import React from 'react';
import { api } from '../api.js';
import { Badge, ErrorBox, fmtTime } from '../ui.jsx';

const TONE = { ok: 'ok', warn: 'warn', fail: 'danger', skipped: 'neutral' };
const TEXT = { ok: '正常', warn: '連得到，但要注意', fail: '失敗', skipped: '沒有設定' };

export default function Checks({ onUnauthorized }) {
  const [tz, setTz] = React.useState('');
  React.useEffect(() => { api('overview').then((o) => setTz(o.timezone)).catch(() => {}); }, []);
  const [result, setResult] = React.useState(null);
  const [error, setError] = React.useState('');
  const [busy, setBusy] = React.useState(false);
  const [withLlm, setWithLlm] = React.useState(true);
  const run = async () => {
    setBusy(true);
    setError('');
    try {
      setResult(await api('checks', { method: 'POST', body: { llm: withLlm } }));
    } catch (e) {
      if (e.status === 401) onUnauthorized();
      setError(e.message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="stack">
      <div className="row wrap-row">
        <h2 className="grow">連線檢查</h2>
        <label className="check"><input type="checkbox" checked={withLlm} onChange={(e) => setWithLlm(e.target.checked)} /> 也測 LLM（會真的送一句話給模型）</label>
        <button className="btn btn-primary" type="button" disabled={busy} onClick={run}>{busy ? '檢查中…' : '開始檢查'}</button>
      </div>
      <p className="muted">照現在生效的設定，從 aiops-lite 的 Pod 實際連一次資料庫、Grafana、日誌、Tempo、LLM。只讀不寫；結果只代表按下去那一刻。</p>
      <ErrorBox error={error} />
      {result ? (
        <>
          {result.items.map((item) => (
            <section className="card pad" key={item.key}>
              <div className="row wrap-row">
                <h3 className="grow">{item.name}</h3>
                <Badge tone={TONE[item.status]}>{TEXT[item.status]}</Badge>
              </div>
              <p className="wrap">{item.detail}</p>
              {item.hints.length ? <ul className="hints">{item.hints.map((h) => <li key={h} className="wrap">{h}</li>)}</ul> : null}
            </section>
          ))}
          <p className="muted small">檢查時間：{fmtTime(result.checked_at, tz)}</p>
        </>
      ) : null}
    </div>
  );
}
