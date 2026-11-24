// 管理密鑰只放在這個分頁的 sessionStorage：關掉分頁就沒了，不寫進 localStorage、不放 cookie。
const KEY = 'aiops-lite-admin-token';

export const getToken = () => {
  try { return sessionStorage.getItem(KEY) || ''; } catch { return ''; }
};
export const setToken = (token) => {
  try { token ? sessionStorage.setItem(KEY, token) : sessionStorage.removeItem(KEY); } catch { /* 私密模式存不了就只留在記憶體 */ }
};

export class ApiError extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

export async function api(path, { method = 'GET', body, token } = {}) {
  let response;
  try {
    response = await fetch(`/api/${path}`, {
      method,
      headers: { Authorization: `Bearer ${token ?? getToken()}`, ...(body ? { 'Content-Type': 'application/json' } : {}) },
      body: body ? JSON.stringify(body) : undefined,
    });
  } catch {
    throw new ApiError(0, '連不上 aiops-lite 服務');
  }
  let data = {};
  try { data = await response.json(); } catch { /* 不是 JSON：下面用狀態碼報錯 */ }
  if (!response.ok) throw new ApiError(response.status, data.error || `HTTP ${response.status}`);
  return data;
}
