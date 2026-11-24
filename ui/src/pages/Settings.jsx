import React from 'react';
import { api } from '../api.js';
import { Badge, ErrorBox, Loading, fmtTime, useLoad } from '../ui.jsx';

// 欄位定義：path 是 config.yaml 的路徑（跟 /api/settings 回來的 values 同一個結構）。
// 這裡沒有的鍵（Token、金鑰、連線位址、port）只認 config.yaml，後端也不會收。
const SECTIONS = [
  {
    title: '告警標籤',
    note: '告警規則上的標籤名稱。改了之後，各系統的 Grafana 規則與通知路由（matcher）也要跟著改，不然告警進不來。',
    fields: [
      { path: 'labels.detect', label: '納管標籤', help: '值是 true 的告警才會被收進來', type: 'text' },
      { path: 'labels.severity', label: '嚴重度標籤', help: '值是 P0～P3', type: 'text' },
      { path: 'labels.service', label: '系統名稱標籤', help: '收斂與撈日誌都用這個值', type: 'text' },
      { path: 'labels.default_severity', label: '預設嚴重度', help: '嚴重度標籤沒寫或寫錯時當成哪一級', type: 'select', options: ['P0', 'P1', 'P2', 'P3'] },
    ],
  },
  {
    title: '時間',
    fields: [
      { path: 'debounce_seconds', label: '等待收齊（秒）', help: '第一個告警進來後等多久才開始處理', type: 'int' },
      { path: 'followup_seconds', label: '續跑間隔（秒）', help: '有進行中事故時，每隔多久再跑一輪', type: 'int' },
      { path: 'close_after_seconds', label: '結案前的安靜時間（秒）', help: '成員告警全部恢復後，要安靜多久才結案', type: 'int' },
      { path: 'timezone', label: '顯示時區', help: '註記與這個介面顯示的時區，例如 Asia/Taipei', type: 'text' },
    ],
  },
  {
    title: '關係邊',
    note: '上游 → 下游。兩個有告警的系統之間有邊（不分方向）就併成同一張事故；名稱要跟告警的系統名稱標籤值完全一樣。',
    fields: [{ path: 'edges', type: 'edges' }],
  },
  {
    title: '日誌（Elasticsearch）',
    note: '經 Grafana 的資料源代理查。改完可以到「連線檢查」確認撈不撈得到。',
    fields: [
      { path: 'es.datasource_uid', label: 'ES 資料源 uid', help: 'Grafana 上 Elasticsearch 資料源的 uid；留空 = 沒有日誌證據', type: 'text', optional: true },
      { path: 'es.log_index', label: '日誌索引', help: '例如 factory-logs-* 或 fluentd-*', type: 'text' },
      { path: 'es.log_fields.time', label: '時間欄位', type: 'text' },
      { path: 'es.log_fields.service', label: '系統名稱欄位', help: '值要跟告警的系統名稱標籤一樣；K8s 容器日誌是 kubernetes.container_name', type: 'text' },
      { path: 'es.log_fields.level', label: '等級欄位', help: '留空 = 日誌沒有等級欄位，改用訊息文字挑錯誤日誌（會漏撈也會誤撈）', type: 'text', optional: true },
      { path: 'es.log_fields.message', label: '訊息欄位', type: 'text' },
      { path: 'es.error_levels', label: '算錯誤的等級', help: '大小寫不同要分開列；要把 WARN 也算進去就加上', type: 'list' },
      { path: 'es.id_fields', label: '識別碼欄位', help: '錯誤日誌裡要統計出現次數的欄位（機台、批號、載具）', type: 'list' },
      { path: 'es.timeline_fields', label: '做時間線的欄位', help: '必須是上面識別碼欄位裡的；留空 = 不做關聯時間線', type: 'list', optional: true },
      { path: 'es.max_hits', label: '最多撈幾筆錯誤日誌', type: 'int' },
      { path: 'es.window_padding_minutes', label: '時間窗前後多抓（分鐘）', type: 'int' },
    ],
  },
  {
    title: 'ES 裡的 Trace（選用）',
    fields: [
      { path: 'es.trace_index', label: 'Trace 索引', help: '留空 = 不查 ES 裡的 Trace', type: 'text', optional: true },
      { path: 'es.trace_fields.time', label: '時間欄位', type: 'text' },
      { path: 'es.trace_fields.service', label: '系統名稱欄位', type: 'text' },
      { path: 'es.trace_fields.span', label: 'span 名稱欄位', type: 'text' },
      { path: 'es.trace_fields.trace_id', label: 'trace id 欄位', type: 'text' },
      { path: 'es.trace_fields.status', label: '狀態欄位', type: 'text' },
      { path: 'es.trace_fields.error', label: '錯誤訊息欄位', type: 'text' },
      { path: 'es.error_statuses', label: '算錯誤的狀態值', type: 'list' },
    ],
  },
  {
    title: '呼叫鏈（Tempo，選用）',
    fields: [
      { path: 'tempo.datasource_uid', label: 'Tempo 資料源 uid', help: '留空 = 沒有呼叫鏈證據', type: 'text', optional: true },
      { path: 'tempo.trace_id_field', label: '日誌裡的 trace id 欄位', help: '錯誤日誌帶這個欄位時直接取那條 Trace；留空 = 只用搜尋', type: 'text', optional: true },
      { path: 'tempo.max_traces', label: '最多取幾條 Trace', type: 'int' },
      { path: 'tempo.max_spans', label: '每條最多幾個 span', type: 'int' },
    ],
  },
  {
    title: 'LLM',
    note: '端點網址與金鑰只認 config.yaml，這裡看不到也改不了。',
    fields: [
      { path: 'llm.model', label: '模型名稱', help: '留空 = 不分析', type: 'text', optional: true },
      { path: 'llm.temperature', label: 'temperature', help: '留空 = 不送，用模型的預設值（有些模型只接受預設值）', type: 'number', optional: true },
      { path: 'llm.timeout_seconds', label: '逾時（秒）', type: 'number' },
      { path: 'llm.retry_initial_seconds', label: '失敗後第一次等多久（秒）', type: 'int' },
      { path: 'llm.retry_max_seconds', label: '最多等多久（秒）', help: '每失敗一次等待時間加倍，到這個上限為止', type: 'int' },
      { path: 'llm.max_log_samples', label: '給模型看幾筆錯誤日誌', type: 'int' },
      { path: 'llm.max_trace_samples', label: '給模型看幾筆 Trace 片段', type: 'int' },
      { path: 'llm.max_message_chars', label: '單筆訊息截斷長度', type: 'int' },
      { path: 'llm.top_ids', label: '每種識別碼列前幾名', type: 'int' },
      { path: 'llm.max_timeline_ids', label: '替幾個批號做時間線', type: 'int' },
      { path: 'llm.max_timeline_events', label: '每條時間線最多幾筆', type: 'int' },
      { path: 'llm.max_summary_chars', label: '摘要最長幾個字', type: 'int' },
      { path: 'llm.id_pattern', label: '識別碼的樣式（正規表示式）', help: '摘要文字裡符合這個樣式的字串必須出現在證據裡；留空 = 不檢查摘要文字', type: 'text', optional: true },
    ],
  },
  { title: '註記', fields: [{ path: 'annotation.max_rule_links', label: '第 3 行最多列幾條規則連結', type: 'int' }] },
];

const get = (obj, path) => path.split('.').reduce((cur, key) => (cur == null ? cur : cur[key]), obj);
const set = (obj, path, value) => {
  const keys = path.split('.');
  const next = structuredClone(obj);
  keys.slice(0, -1).reduce((cur, key) => cur[key], next)[keys.at(-1)] = value;
  return next;
};
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const allPaths = SECTIONS.flatMap((s) => s.fields.map((f) => f.path));

const toText = (field, value) => {
  if (field.type === 'list') return (value || []).join(', ');
  return value == null ? '' : String(value);
};
// 輸入框的文字 → 要送給後端的值。數字欄位打了不是數字的東西就原樣送出，讓後端的檢查回報錯在哪。
const fromText = (field, text) => {
  const t = text.trim();
  if (field.type === 'list') return t ? t.split(/[,、\n]/).map((s) => s.trim()).filter(Boolean) : [];
  if (field.type === 'int' || field.type === 'number') {
    if (t === '') return field.optional ? null : '';
    const n = Number(t);
    return Number.isFinite(n) ? n : t;
  }
  return t;
};

function Field({ field, value, base, onChange }) {
  const id = `f-${field.path}`;
  const changed = !same(value, base);
  const [text, setText] = React.useState(toText(field, value));
  React.useEffect(() => { if (!same(fromText(field, text), value)) setText(toText(field, value)); }, [value]); // eslint-disable-line react-hooks/exhaustive-deps
  const update = (t) => { setText(t); onChange(fromText(field, t)); };
  return (
    <div className={`field ${changed ? 'field-changed' : ''}`}>
      <label htmlFor={id}>{field.label} <code className="muted small">{field.path}</code> {changed ? <Badge tone="info">已修改</Badge> : null}</label>
      {field.type === 'select' ? (
        <select id={id} value={value} onChange={(e) => onChange(e.target.value)}>
          {field.options.map((o) => <option key={o} value={o}>{o}</option>)}
        </select>
      ) : (
        <input id={id} type="text" inputMode={field.type === 'int' || field.type === 'number' ? 'decimal' : undefined} value={text} spellCheck={false} onChange={(e) => update(e.target.value)} />
      )}
      {field.help ? <div className="muted small">{field.help}</div> : null}
      {changed ? (
        <div className="small">
          config.yaml 的值：<code>{toText(field, base) || '（空）'}</code>{' '}
          <button className="link" type="button" onClick={() => onChange(base)}>還原</button>
        </div>
      ) : null}
    </div>
  );
}

function Edges({ value, base, onChange }) {
  const changed = !same(value, base);
  const update = (index, side, text) => onChange(value.map((edge, i) => (i === index ? (side === 0 ? [text, edge[1]] : [edge[0], text]) : edge)));
  return (
    <div className={`field ${changed ? 'field-changed' : ''}`}>
      {changed ? <div><Badge tone="info">已修改</Badge> <button className="link" type="button" onClick={() => onChange(base)}>還原成 config.yaml 的 {base.length} 條</button></div> : null}
      {value.length === 0 ? <div className="muted">沒有關係邊：只有同一個系統的告警會併在一起。</div> : null}
      {value.map((edge, i) => (
        // eslint-disable-next-line react/no-array-index-key
        <div className="edge" key={i}>
          <input aria-label={`第 ${i + 1} 條的上游`} placeholder="上游" value={edge[0]} spellCheck={false} onChange={(e) => update(i, 0, e.target.value)} />
          <span aria-hidden="true">→</span>
          <input aria-label={`第 ${i + 1} 條的下游`} placeholder="下游" value={edge[1]} spellCheck={false} onChange={(e) => update(i, 1, e.target.value)} />
          <button className="btn btn-ghost" type="button" aria-label={`刪除第 ${i + 1} 條`} onClick={() => onChange(value.filter((_e, j) => j !== i))}>刪除</button>
        </div>
      ))}
      <div><button className="btn btn-ghost" type="button" onClick={() => onChange([...value, ['', '']])}>＋ 新增一條</button></div>
    </div>
  );
}

const describe = (doc) => {
  const lines = [];
  Object.entries(doc).forEach(([key, value]) => {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      Object.entries(value).forEach(([k, v]) => lines.push(`${key}.${k} = ${JSON.stringify(v)}`));
    } else lines.push(`${key} = ${JSON.stringify(value)}`);
  });
  return lines;
};

export default function Settings({ onUnauthorized }) {
  const { data, error, loading, status } = useLoad(() => api('settings'), []);
  const [server, setServer] = React.useState(null);
  const [draft, setDraft] = React.useState(null);
  const [saving, setSaving] = React.useState(false);
  const [saveError, setSaveError] = React.useState('');
  const [saved, setSaved] = React.useState(false);
  React.useEffect(() => { if (status === 401) onUnauthorized(); }, [status, onUnauthorized]);
  React.useEffect(() => { if (data) { setServer(data); setDraft(data.values); } }, [data]);
  if (loading && !draft) return <Loading />;
  if (!draft) return <ErrorBox error={error} />;
  const dirty = allPaths.filter((p) => !same(get(draft, p), get(server.values, p)));
  const overridden = allPaths.filter((p) => !same(get(draft, p), get(server.base, p)));
  const change = (path, value) => { setSaved(false); setDraft((d) => set(d, path, value)); };
  const save = async () => {
    setSaving(true);
    setSaveError('');
    try {
      const next = await api('settings', { method: 'PUT', body: { values: draft } });
      setServer(next);
      setDraft(next.values);
      setSaved(true);
    } catch (e) {
      if (e.status === 401) onUnauthorized();
      setSaveError(e.message);
    } finally {
      setSaving(false);
    }
  };
  return (
    <div className="stack settings">
      <h2>設定</h2>
      <p className="muted">
        這裡改的值存在資料庫，蓋過 config.yaml 的同一項；儲存後下一輪就生效，不用重新部署。
        跟 config.yaml 一樣的值不會存，所以之後 config.yaml 改了，沒在這裡改過的項目會跟著變。
        Token、金鑰、連線位址只認 config.yaml，這裡看不到。
      </p>
      {server.problem ? (
        <div className="alert alert-danger" role="alert">
          資料庫裡的設定跟現在的 config.yaml 合不起來，服務目前只用 config.yaml：{server.problem}。修正下面的值再儲存。
        </div>
      ) : null}
      {SECTIONS.map((section) => (
        <section className="card pad" key={section.title}>
          <h3>{section.title}</h3>
          {section.note ? <p className="muted small">{section.note}</p> : null}
          <div className={section.fields[0].type === 'edges' ? '' : 'grid fields'}>
            {section.fields.map((field) => (field.type === 'edges'
              ? <Edges key={field.path} value={get(draft, field.path)} base={get(server.base, field.path)} onChange={(v) => change(field.path, v)} />
              : <Field key={field.path} field={field} value={get(draft, field.path)} base={get(server.base, field.path)} onChange={(v) => change(field.path, v)} />))}
          </div>
        </section>
      ))}
      <section className="card pad">
        <h3>修改紀錄</h3>
        {server.history.length === 0 ? <div className="muted">還沒有在這裡改過設定。</div> : (
          <ul className="history">
            {server.history.map((h) => (
              <li key={h.changed_at}>
                <span className="muted small">{fmtTime(h.changed_at, server.values.timezone)}</span>
                <div className="small wrap">{describe(h.after).length ? describe(h.after).map((l) => <div key={l}><code>{l}</code></div>) : '全部還原成 config.yaml'}</div>
              </li>
            ))}
          </ul>
        )}
        <p className="muted small">每筆是那次儲存之後「跟 config.yaml 不一樣的全部項目」。只記時間，不記是誰改的（大家共用同一把管理密鑰）。</p>
      </section>
      <div className="savebar" role="region" aria-label="儲存">
        <div className="grow">
          {saveError ? <span className="text-danger" role="alert">存不進去：{saveError}</span>
            : saved && dirty.length === 0 ? <span className="text-ok">已儲存，下一輪生效。</span>
              : <span className="muted">{dirty.length ? `有 ${dirty.length} 項還沒儲存` : `目前有 ${overridden.length} 項跟 config.yaml 不一樣`}</span>}
        </div>
        <button className="btn btn-ghost" type="button" disabled={!dirty.length || saving} onClick={() => { setDraft(server.values); setSaveError(''); }}>放棄修改</button>
        <button className="btn btn-primary" type="button" disabled={!dirty.length || saving} onClick={save}>{saving ? '儲存中…' : '儲存'}</button>
      </div>
    </div>
  );
}
