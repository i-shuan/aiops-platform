import React from 'react';
import { api } from '../api.js';
import {
  ErrorBox, FeedbackBadge, Loading, Severity, StatusBadge, SummaryMark, fmtDuration, fmtTime, splitSummary, useLoad,
} from '../ui.jsx';

const PAGE = 50;
const FILTERS = [['', '全部'], ['open', '進行中'], ['closed', '已結案']];

function Feedback({ incident, tz, onSaved }) {
  const [note, setNote] = React.useState('');
  const [busy, setBusy] = React.useState('');
  const [error, setError] = React.useState('');
  const send = async (verdict) => {
    setBusy(verdict);
    setError('');
    try {
      onSaved(await api(`incidents/${incident.uid}/feedback`, { method: 'POST', body: { verdict, note } }));
      setNote('');
    } catch (e) {
      setError(e.message);
    } finally {
      setBusy('');
    }
  };
  return (
    <section>
      <h3>這份分析對嗎？</h3>
      <p className="muted small">回饋存在 aiops-lite 的資料庫，目前只做紀錄，不會自動改變之後的分析。同一張事故可以重評，以最新一筆為準。</p>
      <label htmlFor="note">備註（選填：實際的根因、哪裡講錯）</label>
      <textarea id="note" rows={3} maxLength={2000} value={note} onChange={(e) => setNote(e.target.value)} />
      <ErrorBox error={error} />
      <div className="row">
        <button className="btn btn-ok" type="button" disabled={!!busy} onClick={() => send('ok')}>{busy === 'ok' ? '送出中…' : '分析正確'}</button>
        <button className="btn btn-danger" type="button" disabled={!!busy} onClick={() => send('wrong')}>{busy === 'wrong' ? '送出中…' : '分析錯誤'}</button>
      </div>
      {incident.feedback_history.length ? (
        <ul className="history">
          {incident.feedback_history.map((f) => (
            <li key={f.created_at}>
              <FeedbackBadge feedback={f} /> <span className="muted small">{fmtTime(f.created_at, tz)}</span>
              {f.note ? <div className="wrap">{f.note}</div> : null}
              {f.summary && f.summary !== incident.summary ? <div className="muted small wrap">當時的摘要：{f.summary}</div> : null}
            </li>
          ))}
        </ul>
      ) : null}
    </section>
  );
}

function Detail({ uid, tz, onChanged }) {
  const { data, error, loading, reload } = useLoad(() => api(`incidents/${uid}`), [uid]);
  const [fresh, setFresh] = React.useState(null);
  React.useEffect(() => setFresh(null), [uid]);
  const incident = fresh || data;
  if (loading && !incident) return <Loading />;
  if (!incident) return <ErrorBox error={error} />;
  const { mark, text } = splitSummary(incident.summary);
  return (
    <div className="stack">
      <div className="row wrap-row">
        <a href="#/incidents" className="btn btn-ghost">← 回清單</a>
        <h2 className="grow">事故 <code>{incident.uid}</code></h2>
        <button className="btn btn-ghost" type="button" onClick={() => { setFresh(null); reload(); }}>重新整理</button>
      </div>
      <section className="card pad stack">
        <div className="row wrap-row">
          <StatusBadge status={incident.status} /> <Severity value={incident.severity} />
          <span>受影響系統：<strong>{incident.services.join('、')}</strong></span>
        </div>
        <dl className="facts">
          <div><dt>開始</dt><dd>{fmtTime(incident.started_at, tz)}</dd></div>
          <div><dt>{incident.status === 'open' ? '已持續' : '歷時'}</dt><dd>{fmtDuration(incident.started_at, incident.ended_at)}</dd></div>
          <div><dt>結案</dt><dd>{fmtTime(incident.ended_at, tz)}</dd></div>
          <div><dt>Grafana 註記</dt><dd>{incident.annotated ? `已寫入（編號 ${incident.annotation_id}）` : '還沒寫成功，下一輪會補'}</dd></div>
        </dl>
        <div>
          <h3>摘要 <SummaryMark mark={mark} /></h3>
          <p className="summary wrap">{text || '（沒有摘要）'}</p>
          {!incident.analyzed && incident.llm_next_try ? (
            <p className="muted small">LLM 分析連續失敗 {incident.llm_failures} 次，{fmtTime(incident.llm_next_try, tz)} 之後會再試。</p>
          ) : null}
          {mark.startsWith('LLM') ? <p className="muted small">這是模型根據證據的推論，不是確認過的根因。</p> : null}
        </div>
      </section>
      <section className="card">
        <div className="card-head"><h3>成員告警（{incident.alerts.length}）</h3></div>
        <table>
          <thead><tr><th>系統</th><th>告警</th><th>嚴重度</th><th>狀態</th><th>開始</th><th>恢復</th><th>數值</th></tr></thead>
          <tbody>
            {incident.alerts.map((a) => (
              <tr key={a.fingerprint}>
                <td>{a.service}</td>
                <td className="wrap">{a.rule_url ? <a href={a.rule_url} target="_blank" rel="noreferrer">{a.title}</a> : a.title}</td>
                <td><Severity value={a.severity} /></td>
                <td>{a.status === 'firing' ? '告警中' : '已恢復'}</td>
                <td className="nowrap">{fmtTime(a.starts_at, tz)}</td>
                <td className="nowrap">{fmtTime(a.ends_at, tz)}</td>
                <td className="small">{Object.entries(a.values).map(([k, v]) => `${k}=${v}`).join('、') || '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
      <section className="card pad">
        <Feedback incident={incident} tz={tz} onSaved={(next) => { setFresh(next); onChanged(); }} />
      </section>
    </div>
  );
}

export default function Incidents({ arg, onUnauthorized }) {
  const [filter, setFilter] = React.useState('');
  const [offset, setOffset] = React.useState(0);
  const query = `incidents?limit=${PAGE}&offset=${offset}${filter ? `&status=${filter}` : ''}`;
  const { data, error, loading, status, reload } = useLoad(() => api(query), [query], arg ? 0 : 20000);
  const overview = useLoad(() => api('overview'), []);
  React.useEffect(() => { if (status === 401) onUnauthorized(); }, [status, onUnauthorized]);
  const tz = overview.data?.timezone;
  if (arg) return <Detail uid={arg} tz={tz} onChanged={reload} />;
  return (
    <div className="stack">
      <div className="row wrap-row">
        <h2 className="grow">事故</h2>
        <div className="segmented" role="group" aria-label="篩選狀態">
          {FILTERS.map(([value, label]) => (
            <button key={value} type="button" className={filter === value ? 'active' : ''} aria-pressed={filter === value} onClick={() => { setFilter(value); setOffset(0); }}>
              {label}
            </button>
          ))}
        </div>
      </div>
      <ErrorBox error={error} />
      {loading && !data ? <Loading /> : null}
      {data ? (
        <section className="card">
          {data.incidents.length === 0 ? <div className="muted pad">沒有符合的事故。</div> : (
            <table className="clickable">
              <thead><tr><th>狀態</th><th>嚴重度</th><th>受影響系統</th><th className="num">告警</th><th>開始</th><th>歷時</th><th>摘要</th><th>回饋</th></tr></thead>
              <tbody>
                {data.incidents.map((i) => {
                  const { mark, text } = splitSummary(i.summary);
                  return (
                    <tr key={i.uid} onClick={() => { window.location.hash = `#/incidents/${i.uid}`; }}>
                      <td><StatusBadge status={i.status} /></td>
                      <td><Severity value={i.severity} /></td>
                      <td><a href={`#/incidents/${i.uid}`} onClick={(e) => e.stopPropagation()}>{i.services.join('、')}</a></td>
                      <td className="num">{i.alert_count}</td>
                      <td className="nowrap">{fmtTime(i.started_at, tz)}</td>
                      <td className="nowrap">{fmtDuration(i.started_at, i.ended_at)}</td>
                      <td className="wrap small"><SummaryMark mark={mark} /> <span className="clamp">{text}</span></td>
                      <td><FeedbackBadge feedback={i.feedback} /></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
          <div className="card-foot row">
            <span className="muted small grow">共 {data.total} 張{data.total ? `，第 ${offset + 1}～${offset + data.incidents.length} 張` : ''}</span>
            <button className="btn btn-ghost" type="button" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE))}>上一頁</button>
            <button className="btn btn-ghost" type="button" disabled={offset + PAGE >= data.total} onClick={() => setOffset(offset + PAGE)}>下一頁</button>
          </div>
        </section>
      ) : null}
    </div>
  );
}
