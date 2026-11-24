import React from 'react';
import { api, getToken, setToken } from './api.js';
import { ErrorBox } from './ui.jsx';
import Overview from './pages/Overview.jsx';
import Incidents from './pages/Incidents.jsx';
import Settings from './pages/Settings.jsx';
import Checks from './pages/Checks.jsx';

const PAGES = [
  { key: 'overview', label: '總覽', render: (p) => <Overview {...p} /> },
  { key: 'incidents', label: '事故', render: (p) => <Incidents {...p} /> },
  { key: 'settings', label: '設定', render: (p) => <Settings {...p} /> },
  { key: 'checks', label: '連線檢查', render: (p) => <Checks {...p} /> },
];

const pageFromHash = () => {
  const [key, arg] = window.location.hash.replace(/^#\/?/, '').split('/');
  return { key: PAGES.some((p) => p.key === key) ? key : 'overview', arg: arg || '' };
};

function Login({ onLogin }) {
  const [value, setValue] = React.useState('');
  const [error, setError] = React.useState('');
  const [busy, setBusy] = React.useState(false);
  const submit = async (event) => {
    event.preventDefault();
    setBusy(true);
    setError('');
    try {
      await api('overview', { token: value.trim() });
      onLogin(value.trim());
    } catch (e) {
      setError(e.status === 404 ? '管理介面沒有啟用：config.yaml 要設 admin.token' : e.message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="login">
      <form className="card login-card" onSubmit={submit}>
        <h1>aiops-lite 管理</h1>
        <p className="muted">輸入管理密鑰（config.yaml 的 <code>admin.token</code>）。密鑰只留在這個分頁，關掉就要重新輸入。</p>
        <label htmlFor="token">管理密鑰</label>
        <input id="token" type="password" autoComplete="off" autoFocus value={value} onChange={(e) => setValue(e.target.value)} />
        <ErrorBox error={error} />
        <button className="btn btn-primary" type="submit" disabled={busy || !value.trim()}>{busy ? '驗證中…' : '登入'}</button>
      </form>
    </div>
  );
}

export default function App() {
  const [token, setTokenState] = React.useState(getToken());
  const [page, setPage] = React.useState(pageFromHash());
  React.useEffect(() => {
    const onHash = () => setPage(pageFromHash());
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);
  const logout = React.useCallback(() => { setToken(''); setTokenState(''); }, []);
  if (!token) return <Login onLogin={(t) => { setToken(t); setTokenState(t); }} />;
  const current = PAGES.find((p) => p.key === page.key);
  return (
    <div className="shell">
      <header className="topbar">
        <div className="brand">aiops-lite <span className="muted">管理</span></div>
        <nav aria-label="主選單">
          {PAGES.map((p) => (
            <a key={p.key} href={`#/${p.key}`} className={p.key === page.key ? 'active' : ''} aria-current={p.key === page.key ? 'page' : undefined}>
              {p.label}
            </a>
          ))}
        </nav>
        <button className="btn btn-ghost" type="button" onClick={logout}>登出</button>
      </header>
      <main>{current.render({ arg: page.arg, onUnauthorized: logout })}</main>
    </div>
  );
}
