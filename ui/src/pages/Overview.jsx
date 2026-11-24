import React from 'react';
import { api } from '../api.js';
import { Badge, ErrorBox, Loading, fmtTime, useLoad } from '../ui.jsx';

const ROUND_TONE = { ok: 'ok', degraded: 'warn', failed: 'danger' };
const ROUND_TEXT = { ok: '正常', degraded: '有錯誤但跑完', failed: '失敗' };

function Stat({ label, value, hint, tone }) {
  return (
    <div className={`card stat ${tone ? `stat-${tone}` : ''}`}>
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
      {hint ? <div className="muted small">{hint}</div> : null}
    </div>
  );
}

export default function Overview({ onUnauthorized }) {
  const { data, error, loading, status } = useLoad(() => api('overview'), [], 15000);
  React.useEffect(() => { if (status === 401) onUnauthorized(); }, [status, onUnauthorized]);
  if (loading && !data) return <Loading />;
  if (!data) return <ErrorBox error={error} />;
  const tz = data.timezone;
  const rated = data.feedback.ok + data.feedback.wrong;
  return (
    <div className="stack">
      <ErrorBox error={error} />
      {!data.worker.healthy ? (
        <div className="alert alert-danger" role="alert">工作執行緒不健康：{data.worker.reason}。新的告警會先存進資料庫，但不會被處理。</div>
      ) : null}
      <div className="grid stats">
        <Stat label="進行中的事故" value={data.open_incidents} tone={data.open_incidents ? 'danger' : ''} hint={<a href="#/incidents">看清單</a>} />
        <Stat label="已結案的事故" value={data.closed_incidents} />
        <Stat label="等待處理的告警" value={data.pending_alerts} hint="收到了、還在等 debounce 的" />
        <Stat
          label="LLM 分析"
          value={!data.llm_configured ? '未設定' : data.llm_backing_off ? `${data.llm_backing_off} 張退避中` : '正常'}
          tone={data.llm_backing_off ? 'warn' : ''}
          hint={!data.llm_configured ? '註記第 2 行會是 [未經分析]' : data.llm_backing_off ? '分析失敗，到時間會自動重試' : ''}
        />
        <Stat
          label="RCA 回饋"
          value={rated ? `${data.feedback.ok} 正確／${data.feedback.wrong} 錯誤` : '還沒有'}
          hint={rated ? `共評了 ${rated} 張事故` : '在事故詳情裡可以評'}
        />
      </div>
      <section className="card">
        <div className="card-head">
          <h2>最近 10 輪</h2>
          <span className="muted small">設定來源：config.yaml{data.overridden ? ' ＋ 管理介面的修改' : ''}</span>
        </div>
        {data.rounds.length === 0 ? <div className="muted pad">還沒有跑過任何一輪。</div> : (
          <table>
            <thead><tr><th>開始</th><th>結果</th><th className="num">告警</th><th className="num">進行中事故</th><th>錯誤</th></tr></thead>
            <tbody>
              {data.rounds.map((r) => (
                <tr key={r.started_at}>
                  <td className="nowrap">{fmtTime(r.started_at, tz)}</td>
                  <td><Badge tone={ROUND_TONE[r.status] || 'neutral'}>{ROUND_TEXT[r.status] || r.status}</Badge></td>
                  <td className="num">{r.alerts}</td>
                  <td className="num">{r.open_incidents}</td>
                  <td className="wrap small">{r.errors.length ? r.errors.join('；') : <span className="muted">—</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
      <p className="muted small">時間以 {tz} 顯示；每 15 秒自動更新。事故卡在 Grafana：<a href={data.grafana_url} target="_blank" rel="noreferrer">{data.grafana_url}</a></p>
    </div>
  );
}
