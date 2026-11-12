"""送 LLM 之前的敏感資訊遮罩（帳號、憑證、個資）。stages/enrichment/ 裡唯一可以呼叫 BaseLLM.complete 的地方是本檔的 send：
它一定先遮罩再送；遮罩出錯丟 MaskError，**不送**（呼叫端標 mask_failed，不放行原文）。
tests/test_architecture.py 會掃 enrichment/ 裡所有 `.complete` 的引用，只准出現在這裡。

## 遮什麼（只認通用形狀，不針對任何服務名或資料寫規則）
  KEY      PEM 私鑰區塊
  CRED     連線字串裡的帳密（scheme://user:pass@host → scheme://<CRED_…>@host）
  TOKEN    JWT（eyJ…）、Authorization 標頭（Bearer／Basic／Token／Negotiate 後面那串）、常見雲端金鑰前綴形狀
  SECRET   password／pwd／secret／token／api_key／access_key／client_secret／credential 這類鍵的值（= 或 : 後面）
  USER     user／username／login／account name／uid／user id 這類鍵的值；`for user 'x'`、`user "x"` 這種引號帳號
  ACCOUNT  Windows 網域帳號 DOMAIN\\user
  EMAIL    電子郵件
  CARD     13～19 位、通過 Luhn 檢查的卡號形狀（可帶空白或 -）
  NATID    身分證字號形狀（英文字母＋1／2＋8 碼）、美國 SSN 形狀（123-45-6789）
  PHONE    + 開頭的國際電話
  IP       IPv4（保留 :port）、IPv6（用 ipaddress 確認，避免把 12:00:04 這類時間當成位址）
  HOST     主機 FQDN（預設**不遮**，mask_hostnames=True 才遮，見下）

## 刻意不遮
  服務名、資源名（資料庫、佇列、外部 API）、錯誤類別、例外訊息、數字（耗時、筆數、狀態碼）、時間、trace id。
  呼叫端用 keep 傳「這次 RCA 的已知名稱」（實體、事實裡的 target／callee／queue…）：這些名稱就算長得像 IP 或主機名
  （APM 的 destination 常常是 10.0.0.5:5432），也原樣保留——它們是 RCA 要指認的對象，遮了就判不出來。

## 主機名為什麼預設不遮
  稽核想遮、RCA 要用：資料庫、外部閘道、訊息中介在日誌裡多半以主機名出現（db01.prod.internal、tax-gw.example.com），
  是判斷「在等誰」的關鍵線索；遮掉之後 LLM 只看得到 <HOST_…>，抽證據與判根因的品質都會掉。主機名本身也不是帳號、
  憑證或個資。需要遮的環境在節點 params 設 mask_hostnames: true；keep 裡的已知名稱照樣保留。

## 佔位符
  <類別_8 位十六進位>，值 = HMAC-SHA256(行程啟動時產生的隨機鹽, 類別＋原值) 的前 8 碼：
  - 同一個值在同一個行程裡永遠對到同一個佔位符：同一輪的抽證據、判根因、寫摘要三次呼叫看到的是同一個代號，
    LLM 看得出「同一個帳號重複出現」；用流水號（<EMAIL_1>）的話每個算子各數各的，同一個代號會對到不同的人，更糟。
  - 不用存任何對照表（不留模組層級可變狀態，CLAUDE.md §100.3），也不可逆：鹽只在記憶體，外部拿到佔位符無法用字典反查。
  - 行程重啟後代號會變；資料庫裡存的遮罩版引文跨行程對不起來，這是刻意的取捨。

## 做不到的（死區，誠實披露；tests/test_llm_masking.py 有 strict xfail 留存）
  - 自由文字裡的人名（「王小明 下單失敗」「contact John about it」）：沒有固定形狀，正規表示式抓不到。
  - 沒有固定形狀的內部編號（員工編號、會員編號、工單號）：跟一般數字分不開，也不該亂遮數字。
  - 沒有 user=／password= 這類鍵、也沒有引號的帳號（「jsmith logged in」）。
  - 長度、字元集跟 trace id 一樣的無前綴隨機 token：分不出來，遮了會把 trace id 一起遮掉。
  - 不帶 + 的本地電話號碼：跟一般數字分不開。
  所以這不是「去識別化」，只是把有固定形狀的帳號、憑證、個資遮掉。
"""
from __future__ import annotations

import hashlib
import hmac
import re
import secrets
from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable

from aiops.stages.enrichment.base import BaseLLM

MASK_FAILED = "mask_failed"  # 遮罩本身出錯：不送 LLM（rca_undetermined_reason 的代碼）

_SALT = secrets.token_bytes(32)  # 行程層級、建立後不再改；只在記憶體
_PLACEHOLDER = re.compile(r"<[A-Z]+_[0-9a-f]{8}>")


class MaskError(Exception):
    """遮罩過程出錯。呼叫端一律不送 LLM（寧可不送，不放行原文）。不是 LLMError：不能被當成連線失敗去重試。"""


def placeholder(category: str, value: str) -> str:
    digest = hmac.new(_SALT, f"{category}\x00{value}".encode("utf-8"), hashlib.sha256).hexdigest()[:8]
    return f"<{category}_{digest}>"


def _luhn(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch)
        if i % 2:
            d = d * 2 - 9 if d > 4 else d * 2
        total += d
    return total % 10 == 0


def _valid_ipv4(s: str) -> bool:
    return all(int(p) <= 255 for p in s.split("."))


_HEX4 = re.compile(r"[0-9A-Fa-f]{1,4}")
IPV6_GROUPS = 8  # RFC 4291：IPv6 位址固定 8 組 16 位元


def _valid_ipv6(s: str) -> bool:
    """IPv6 文字格式檢查（不用 ipaddress 的例外流程）：最多一個 ::，每組 1～4 位十六進位，
    沒有 :: 時剛好 8 組、有 :: 時少於 8 組。避免把 12:00:04 這類時間、Foo::bar 這類方法引用當成位址。"""
    addr = s.split("%", 1)[0]
    if addr.count("::") > 1 or ":::" in addr:
        return False
    if "::" in addr:
        head, tail = addr.split("::")
        groups = [g for g in (head.split(":") if head else []) + (tail.split(":") if tail else [])]
        return len(groups) < IPV6_GROUPS and all(_HEX4.fullmatch(g) for g in groups)
    groups = addr.split(":")
    return len(groups) == IPV6_GROUPS and all(_HEX4.fullmatch(g) for g in groups)


# 鍵名（不分大小寫）：值要遮成 SECRET 的、要遮成 USER 的
_SECRET_KEYS = (r"pass(?:word|wd)?|pwd|secret|client[_-]?secret|api[_-]?key|apikey|access[_-]?key(?:[_-]?id)?|"
                r"secret[_-]?key|private[_-]?key|token|access[_-]?token|refresh[_-]?token|auth[_-]?token|session[_-]?id|"
                r"credentials?")
_USER_KEYS = r"user(?:[ _-]?(?:name|id))?|login|account(?:[ _-]?name)?|uid|principal|sam[_-]?account[_-]?name"
# 值：引號包起來的整段，或到空白、分隔符為止；不從 < 開始（已經是佔位符的不再遮，遮罩要冪等）
_VALUE = r"(?:\"[^\"\n]*\"|'[^'\n]*'|[^\s,;&\"'<>()\[\]{}]+)"


@dataclass(frozen=True)
class _Rule:
    category: str
    pattern: re.Pattern[str]
    group: int = 0                                  # 要換掉的群組（0 = 整段）
    check: Callable[[str], bool] | None = None      # 形狀對了還要再確認（Luhn、位址合法）


_RULES: tuple[_Rule, ...] = (
    _Rule("KEY", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(?:-----END [A-Z ]*PRIVATE KEY-----|\Z)")),
    _Rule("CRED", re.compile(r"\b[a-zA-Z][a-zA-Z0-9+.-]*://([^/\s:@<>\"']+(?::[^/\s@<>\"']*)?)@"), 1),
    _Rule("TOKEN", re.compile(r"\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]*")),
    _Rule("TOKEN", re.compile(r"\b(?:Bearer|Basic|Token|Negotiate)\s+([A-Za-z0-9._~+/=-]{8,})"), 1),
    _Rule("TOKEN", re.compile(r"\b(?:AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|sk-[A-Za-z0-9_-]{20,}|gh[pousr]_[A-Za-z0-9]{30,}"
                              r"|xox[abprs]-[A-Za-z0-9-]{10,}|AIza[0-9A-Za-z_-]{35})\b")),
    _Rule("SECRET", re.compile(rf"(?i)\b(?:{_SECRET_KEYS})[\"']?\s*[=:]\s*({_VALUE})"), 1),
    _Rule("USER", re.compile(rf"(?i)\b(?:{_USER_KEYS})[\"']?\s*[=:]\s*({_VALUE})"), 1),
    _Rule("USER", re.compile(r"(?i)\b(?:user|account|login)\s+[\"']([^\"'\s<>]+)[\"']"), 1),
    _Rule("EMAIL", re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")),
    _Rule("ACCOUNT", re.compile(r"\b[A-Z][A-Z0-9-]{1,14}\\{1,2}[A-Za-z][A-Za-z0-9._$-]*")),
    _Rule("CARD", re.compile(r"(?<![\d.])\d(?:[ -]?\d){12,18}(?![\d.])"), 0, lambda s: _luhn(re.sub(r"\D", "", s))),
    _Rule("NATID", re.compile(r"\b[A-Z][12]\d{8}\b")),
    _Rule("NATID", re.compile(r"(?<![\d-])\d{3}-\d{2}-\d{4}(?![\d-])")),
    _Rule("PHONE", re.compile(r"(?<![\w+])\+\d{1,3}[ -]?(?:\(\d{1,4}\)[ -]?)?\d{2,4}(?:[ -]?\d{2,4}){1,4}(?!\d)")),
    _Rule("IP", re.compile(r"(?<![\w:.])(?=[0-9A-Fa-f:]*::|(?:[0-9A-Fa-f]{1,4}:){7})[0-9A-Fa-f:]{2,39}(?:%[\w.]+)?(?![\w:])"),
          0, _valid_ipv6),
    _Rule("IP", re.compile(r"(?<![\w.])\d{1,3}(?:\.\d{1,3}){3}(?!\.?\d)"), 0, _valid_ipv4),
)
_HOST = _Rule("HOST", re.compile(
    r"(?<![\w.@/-])(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+"
    r"(?:com|net|org|io|internal|local|lan|corp|intra|intranet|cloud|svc|cluster|home|example|[a-z]{2})(?![\w-]|\.\w)"))


@dataclass(frozen=True)
class MaskPolicy:
    mask_hostnames: bool = False


class Masker:
    """一次 LLM 呼叫用一個（每次新建，不跨輪保留狀態）。counts 記這次遮了哪些類別幾處（只記數量，不記原值）。
    keep：已知名稱（實體、資源），就算長得像 IP／主機名也保留。"""

    def __init__(self, policy: MaskPolicy | None = None, keep: set[str] | frozenset[str] = frozenset()):
        self.policy = policy or MaskPolicy()
        self.rules = _RULES + ((_HOST,) if self.policy.mask_hostnames else ())
        self.counts: Counter[str] = Counter()
        # 只保護「自己會被遮到」的名稱：不會被遮的（auth、payment）不用保護，保護了反而會擋住 auth=… 這種鍵
        self._keep = sorted((k for k in keep if k and self._raw(k) != k), key=len, reverse=True)

    def _raw(self, text: str) -> str:
        for rule in self.rules:
            text = rule.pattern.sub(lambda m, r=rule: self._swap(m, r, count=False), text)
        return text

    def _swap(self, m: re.Match[str], rule: _Rule, count: bool = True) -> str:
        value = m.group(rule.group)
        if not value or _PLACEHOLDER.fullmatch(value) or (rule.check and not rule.check(value)):
            return m.group(0)
        if count:
            self.counts[rule.category] += 1
        start, end = m.start(rule.group) - m.start(0), m.end(rule.group) - m.start(0)
        whole = m.group(0)
        return whole[:start] + placeholder(rule.category, value) + whole[end:]

    def text(self, text: str) -> str:
        """遮一段文字。冪等：已經遮過的再遮一次不變。"""
        if not isinstance(text, str):
            raise MaskError(f"只能遮字串，不是 {type(text).__name__}")
        guarded: dict[str, str] = {}
        for i, name in enumerate(self._keep):
            if name in text:
                token = f"{chr(0xE100 + i)}"  # 私用區字元：不會被任何規則抓到
                guarded[token] = name
                text = text.replace(name, token)
        for rule in self.rules:
            text = rule.pattern.sub(lambda m, r=rule: self._swap(m, r), text)
        for token, name in guarded.items():
            text = text.replace(token, name)
        return text

    def obj(self, obj: Any) -> Any:
        """遞迴遮 JSON 形狀的資料（dict 的鍵與值、list、tuple 裡的字串）；數字、布林、None 原樣。"""
        if isinstance(obj, str):
            return self.text(obj)
        if isinstance(obj, dict):
            return {(self.text(k) if isinstance(k, str) else k): self.obj(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self.obj(v) for v in obj]
        return obj

    def summary(self) -> str:
        return "、".join(f"{k} {v}" for k, v in sorted(self.counts.items())) or "無"


def send(llm: BaseLLM, system: str, prompt: str, masker: Masker) -> str:
    """送 LLM 的唯一出口：整份 prompt 再遮一次（呼叫端應該已經把結構化資料遮過了，這一遍是防漏），然後才呼叫 complete。
    遮罩出錯丟 MaskError，不呼叫 complete。system 是平台寫死的指示，不含資料，不遮。"""
    try:
        sealed = masker.text(prompt)
    except MaskError:
        raise
    except Exception as e:  # noqa: BLE001  遮罩自己的 bug 也不准放行原文
        raise MaskError(f"遮罩失敗（{type(e).__name__}：{e}），不送出") from e
    return llm.complete(system, sealed)


def mask_or_fail(masker: Masker, obj: Any) -> Any:
    """算子在組 prompt 之前遮結構化輸入用：任何例外都轉成 MaskError（呼叫端標 mask_failed，不送）。"""
    try:
        return masker.obj(obj)
    except MaskError:
        raise
    except Exception as e:  # noqa: BLE001
        raise MaskError(f"遮罩失敗（{type(e).__name__}：{e}），不送出") from e


def rca_keep_names(ctx: Any) -> set[str]:
    """這次 RCA 的已知名稱：事件群實體、呼叫圖、trace 的服務、事實裡的實體與資源（target／callee／queue／producer／consumer、
    呼叫來源）、日誌的服務。這些名稱遮了 RCA 就指認不出對象，所以就算長得像 IP 或主機名也保留。"""
    names: set[str] = set()
    for inc in getattr(ctx, "incidents", []):
        names.update(inc.entities)
    graph = getattr(ctx, "graph", None)
    if graph is not None:
        names.update(graph.nodes())
        names.update(n for edge in graph.propagation() for n in edge)
    names.update(s.service for s in getattr(ctx, "rca_spans", []) if getattr(s, "service", ""))
    names.update(r.service for r in getattr(ctx, "rca_logs", []) if getattr(r, "service", ""))
    for f in getattr(ctx, "rca_facts", []):
        names.add(f.entity)
        for k in ("target", "callee", "queue", "producer", "consumer"):
            if f.detail.get(k):
                names.add(str(f.detail[k]))
        names.update(str(k) for k in (f.detail.get("by_caller") or {}))
    names.discard("")
    return names
