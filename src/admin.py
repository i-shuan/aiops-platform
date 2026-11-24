"""管理介面的 API（/api/…）與靜態檔（/ui/…）。HTTP 的收發在 src/server.py，這裡只管「這個請求該回什麼」。

  GET  /api/overview                 現況：進行中事故數、待處理告警數、工作執行緒是否健康、最近幾輪、回饋統計
  GET  /api/incidents?status=&limit=&offset=   事故清單（新的在前），帶每張最新的回饋
  GET  /api/incidents/<uid>          一張事故：成員告警、摘要、退避狀態、全部回饋
  POST /api/incidents/<uid>/feedback {"verdict": "ok"|"wrong", "note": "…"}
  GET  /api/rounds?limit=            每一輪的執行紀錄
  GET  /api/settings                 可調的設定：現在生效的值、config.yaml 的值、哪些被改過、修改歷史
  PUT  /api/settings                 {"values": {…}}：跟 config.yaml 不一樣的才存進資料庫；整份重新驗證，不合法回 400、什麼都不存
  POST /api/checks                   {"llm": true|false}：連線檢查（src/checks.py）

全部要帶 Authorization: Bearer <config.yaml 的 admin.token>。admin.token 沒設 = 管理介面整個關掉（/api 與 /ui 都回 404）。
這裡回出去的東西沒有任何機密：設定只回 main.editable_view 的那些鍵，Token、金鑰、連線字串不在裡面。

回饋只存在自己的資料庫（feedback 表），不寫回 Grafana；目前只是紀錄，還沒有任何程式會拿它去改分析的行為。
"""
from __future__ import annotations

import hmac
import json
import logging
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlsplit

import httpx

from src import checks, report
from src.db import VERDICTS, Database
from src.grouping import Incident
from src.main import ConfigError, Settings, apply_overrides, check_editable, diff_overrides, editable_view

log = logging.getLogger("aiops_lite.admin")

UI_DIR = Path(__file__).resolve().parent.parent / "ui" / "dist"   # 前端編譯後的靜態檔（Dockerfile 會放到這裡）
MAX_NOTE_CHARS = 2000
MAX_PAGE = 200


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status, self.message = status, message


def _iso(t: datetime | None) -> str | None:
    return t.astimezone(timezone.utc).isoformat() if t else None


def incident_view(inc: Incident, settings: Settings, feedback: list[dict[str, Any]], detail: bool = False) -> dict[str, Any]:
    out: dict[str, Any] = {
        "uid": inc.uid, "status": inc.status, "severity": inc.severity, "services": inc.services,
        "alert_count": inc.alert_count, "started_at": _iso(inc.started_at), "last_seen_at": _iso(inc.last_seen_at),
        "ended_at": _iso(inc.ended_at), "summary": inc.summary, "analyzed": inc.analyzed, "annotated": inc.annotated,
        "llm_failures": inc.llm_failures, "llm_next_try": _iso(inc.llm_next_try),
        "feedback": feedback[0] if feedback else None}
    if detail:
        members = sorted(inc.signals.items(), key=lambda kv: (kv[1]["starts_at"], kv[1]["title"]))
        out["alerts"] = [{"fingerprint": fp, "title": m["title"], "service": m["service"], "severity": m["severity"],
                          "status": m["status"], "starts_at": _iso(m["starts_at"]), "ends_at": _iso(m["ends_at"]),
                          "values": m.get("values") or {}, "rule_url": report.rule_url(settings.grafana_url, m["rule_uid"])}
                         for fp, m in members]
        out["feedback_history"] = feedback
        out["annotation_id"] = inc.annotation_id
    return out


class Admin:
    """get_settings：現在生效的設定（config.yaml 疊上資料庫的覆寫）。on_change：設定存好之後叫一下，讓服務立刻套用。
    health：工作執行緒的狀態（None = 健康）。transport 兩個參數只是 HTTP 連線的注入點。"""

    def __init__(self, base: Settings, db: Database, *, get_settings: Callable[[], Settings],
                 on_change: Callable[[], None] = lambda: None, health: Callable[[], str | None] = lambda: None,
                 grafana_transport: httpx.BaseTransport | None = None, llm_transport: httpx.BaseTransport | None = None,
                 ui_dir: Path = UI_DIR):
        self.base, self.db, self.get_settings, self.on_change, self.health = base, db, get_settings, on_change, health
        self.grafana_transport, self.llm_transport, self.ui_dir = grafana_transport, llm_transport, ui_dir

    @property
    def enabled(self) -> bool:
        return bool(self.base.admin_token)

    def authorized(self, header: str) -> bool:
        return self.enabled and hmac.compare_digest(header.encode(), f"Bearer {self.base.admin_token}".encode())

    # ── API ──

    def api(self, method: str, target: str, authorization: str, body: bytes = b"") -> tuple[int, dict[str, Any]]:
        if not self.enabled:
            return 404, {"error": "管理介面沒有啟用（config.yaml 沒有設 admin.token）"}
        if not self.authorized(authorization):
            return 401, {"error": "管理密鑰不對"}
        url = urlsplit(target)
        parts = [p for p in url.path.split("/") if p][1:]   # 去掉開頭的 api
        query = {k: v[-1] for k, v in parse_qs(url.query).items()}
        try:
            payload = json.loads(body) if body.strip() else {}
            if not isinstance(payload, dict):
                raise ApiError(400, "內容要是 JSON 物件")
            return 200, self._route(method, parts, query, payload)
        except ApiError as e:
            return e.status, {"error": e.message}
        except ConfigError as e:
            return 400, {"error": str(e)}
        except json.JSONDecodeError:
            return 400, {"error": "內容不是合法的 JSON"}
        except Exception as e:   # 多半是資料庫斷線：回 500 並留完整的錯誤在 log，不讓 HTTP 執行緒死掉
            log.exception("管理 API %s %s 失敗", method, url.path)
            return 500, {"error": f"{type(e).__name__}：伺服器處理失敗，細節在服務的 log"}

    def _route(self, method: str, parts: list[str], query: dict[str, str], payload: dict[str, Any]) -> dict[str, Any]:
        match (method, parts):
            case ("GET", ["overview"]):
                return self.overview()
            case ("GET", ["incidents"]):
                return self.incidents(query)
            case ("GET", ["incidents", uid]):
                return self.incident(uid)
            case ("POST", ["incidents", uid, "feedback"]):
                return self.add_feedback(uid, payload)
            case ("GET", ["rounds"]):
                return {"rounds": self.db.list_rounds(_limit(query, 50))}
            case ("GET", ["settings"]):
                return self.settings_view()
            case ("PUT", ["settings"]):
                return self.save_settings(payload)
            case ("POST", ["checks"]):
                current = self.get_settings()
                return {"checked_at": _iso(datetime.now(timezone.utc)), "items": checks.run_all(
                    current, self.db, grafana_transport=self.grafana_transport, llm_transport=self.llm_transport,
                    include_llm=payload.get("llm", True) is not False)}
        raise ApiError(404, "沒有這個 API")

    def overview(self) -> dict[str, Any]:
        current = self.get_settings()
        open_items, open_total = self.db.list_incidents("open", limit=MAX_PAGE)
        _closed, closed_total = self.db.list_incidents("closed", limit=1)
        waiting = [i for i in open_items if not i.analyzed and i.llm_next_try is not None]
        return {
            "worker": {"healthy": self.health() is None, "reason": self.health()},
            "open_incidents": open_total, "closed_incidents": closed_total, "pending_alerts": self.db.inbox_size(),
            "llm_configured": bool(current.llm_api_url and current.llm_model), "llm_backing_off": len(waiting),
            "feedback": self.db.feedback_counts(), "overridden": bool(self.db.get_overrides()),
            "grafana_url": current.grafana_url, "timezone": current.tz, "rounds": self.db.list_rounds(10)}

    def incidents(self, query: dict[str, str]) -> dict[str, Any]:
        status = query.get("status") or None
        if status not in (None, "open", "closed"):
            raise ApiError(400, "status 只能是 open 或 closed")
        offset = _integer(query, "offset", 0, 0, 10**9)
        items, total = self.db.list_incidents(status, _limit(query, 50), offset)
        feedback = self.db.feedback_for([i.uid for i in items])
        current = self.get_settings()
        return {"total": total, "offset": offset,
                "incidents": [incident_view(i, current, feedback.get(i.uid, [])) for i in items]}

    def _find(self, uid: str) -> Incident:
        inc = self.db.get_incident(uid)
        if inc is None:
            raise ApiError(404, f"沒有事故 {uid}")
        return inc

    def incident(self, uid: str) -> dict[str, Any]:
        inc = self._find(uid)
        return incident_view(inc, self.get_settings(), self.db.feedback_for([uid]).get(uid, []), detail=True)

    def add_feedback(self, uid: str, payload: dict[str, Any]) -> dict[str, Any]:
        inc = self._find(uid)
        verdict, note = payload.get("verdict"), payload.get("note", "")
        if verdict not in VERDICTS:
            raise ApiError(400, f"verdict 要是 {'、'.join(VERDICTS)} 其中之一")
        if not isinstance(note, str) or len(note) > MAX_NOTE_CHARS:
            raise ApiError(400, f"note 要是文字，最多 {MAX_NOTE_CHARS} 字")
        self.db.add_feedback(uid, verdict, note.strip(), inc.summary, datetime.now(timezone.utc))
        return self.incident(uid)

    def settings_view(self) -> dict[str, Any]:
        overrides = self.db.get_overrides()
        try:
            effective, problem = editable_view(apply_overrides(self.base, overrides)), None
        except ConfigError as e:   # 存的時候是合法的，之後 config.yaml 改了才可能變成不合法：服務退回只用 config.yaml
            effective, problem = editable_view(self.base), str(e)
        return {"values": effective, "base": editable_view(self.base), "overrides": overrides, "problem": problem,
                "history": self.db.overrides_history()}

    def save_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        wanted = payload.get("values")
        check_editable(wanted, "values ")
        overrides = diff_overrides(wanted, editable_view(self.base))
        apply_overrides(self.base, overrides)   # 整份重新驗證；不合法丟 ConfigError → 400，什麼都不存
        if overrides != self.db.get_overrides():
            self.db.save_overrides(overrides, datetime.now(timezone.utc))
            log.info("管理介面改了設定：%s", json.dumps(overrides, ensure_ascii=False))
            self.on_change()
        return self.settings_view()

    # ── 靜態檔 ──

    def static(self, target: str) -> tuple[int, str, bytes]:
        """/ui/… → 前端編譯後的檔案；找不到的路徑回 index.html（前端自己處理路徑）。回傳 (狀態碼, Content-Type, 內容)。"""
        if not self.enabled:
            return 404, "application/json; charset=utf-8", json.dumps({"error": "管理介面沒有啟用"}).encode()
        root = self.ui_dir.resolve()
        relative = urlsplit(target).path.removeprefix("/ui").lstrip("/")
        file = (root / relative).resolve()
        if not relative or not file.is_file() or root not in file.parents:   # 最後一個條件擋 ../ 跳出資料夾
            file = root / "index.html"
        if not file.is_file():
            return 503, "text/plain; charset=utf-8", "管理介面的前端檔案不在映像檔裡（ui/dist）：請重新 build".encode()
        kind = mimetypes.guess_type(file.name)[0] or "application/octet-stream"
        return 200, kind + ("; charset=utf-8" if kind.startswith("text/") or kind.endswith(("javascript", "json")) else ""), \
            file.read_bytes()


def _integer(query: dict[str, str], key: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(query.get(key, default))
    except ValueError:
        raise ApiError(400, f"{key} 要是整數") from None
    if not minimum <= value <= maximum:
        raise ApiError(400, f"{key} 要在 {minimum}～{maximum}")
    return value


def _limit(query: dict[str, str], default: int) -> int:
    return _integer(query, "limit", default, 1, MAX_PAGE)
