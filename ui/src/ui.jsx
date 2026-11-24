import React from 'react';

export const fmtTime = (iso, tz) => {
  if (!iso) return '—';
  try {
    return new Intl.DateTimeFormat('zh-TW', {
      timeZone: tz || undefined, month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
    }).format(new Date(iso));
  } catch { return iso; }
};

export const fmtDuration = (fromIso, toIso) => {
  if (!fromIso) return '—';
  const s = Math.max(0, Math.round((new Date(toIso || Date.now()) - new Date(fromIso)) / 1000));
  if (s < 60) return `${s} 秒`;
  if (s < 3600) return `${Math.floor(s / 60)} 分`;
  if (s < 86400) return `${Math.floor(s / 3600)} 小時 ${Math.floor((s % 3600) / 60)} 分`;
  return `${Math.floor(s / 86400)} 天 ${Math.floor((s % 86400) / 3600)} 小時`;
};

export const Badge = ({ tone = 'neutral', children }) => <span className={`badge badge-${tone}`}>{children}</span>;

const SEVERITY_TONE = { P0: 'danger', P1: 'danger', P2: 'warn', P3: 'neutral' };
export const Severity = ({ value }) => <Badge tone={SEVERITY_TONE[value] || 'neutral'}>{value}</Badge>;

export const StatusBadge = ({ status }) => (status === 'open'
  ? <Badge tone="danger">進行中</Badge> : <Badge tone="ok">已結案</Badge>);

export const FeedbackBadge = ({ feedback }) => {
  if (!feedback) return <span className="muted">未評</span>;
  return feedback.verdict === 'ok' ? <Badge tone="ok">分析正確</Badge> : <Badge tone="danger">分析錯誤</Badge>;
};

// 摘要開頭的 [LLM 推論]／[未經分析] 是程式固定加的標示：拆出來當標籤，剩下的才是內文
export const splitSummary = (summary) => {
  const m = /^\[([^\]]+)\]\s*(.*)$/s.exec(summary || '');
  return m ? { mark: m[1], text: m[2] } : { mark: '', text: summary || '' };
};

export const SummaryMark = ({ mark }) => {
  if (!mark) return null;
  return <Badge tone={mark.startsWith('LLM') ? 'info' : 'neutral'}>{mark}</Badge>;
};

export const ErrorBox = ({ error }) => (error ? <div className="alert alert-danger" role="alert">{error}</div> : null);

export const Loading = () => <div className="muted pad">載入中…</div>;

export function useLoad(loader, deps, intervalMs) {
  const [state, setState] = React.useState({ data: null, error: '', loading: true });
  const reload = React.useCallback(async () => {
    try {
      const data = await loader();
      setState({ data, error: '', loading: false });
    } catch (e) {
      setState((prev) => ({ data: prev.data, error: e.message, loading: false, status: e.status }));
    }
  }, deps); // eslint-disable-line react-hooks/exhaustive-deps
  React.useEffect(() => {
    reload();
    if (!intervalMs) return undefined;
    const timer = setInterval(reload, intervalMs);
    return () => clearInterval(timer);
  }, [reload, intervalMs]);
  return { ...state, reload };
}
