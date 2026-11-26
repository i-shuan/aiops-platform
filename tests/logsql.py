"""測試用：只懂 src/grafana.py 會送出的那幾種 LogsQL 的小型解譯器，讓假的 Grafana 能照查詢條件真的去篩資料。

它不是 VictoriaLogs 的替代品。看不懂的語法一律丟 LogsQLError（假的 Grafana 會回 400），所以程式要是開始送新的語法，
測試會直接壞掉，而不是默默回一個看起來合理的結果。它跟真的 VictoriaLogs 有沒有一樣，由 tests/test_grafana.py 的
test_fake_logs_answer_like_the_real_one 把關：同一批資料、同一條查詢，逐條跟 victoria_logs.json 錄下來的真實回應比。

支援的語法：
  篩選   {k="v"}   *   "欄位":in("a","b")   "欄位":="值"   _msg:seq("詞",…)   _msg:i("詞")   _time:>=時間   _time:<時間
         空白＝而且；or；括號
  管線   | sort by (_time) [desc] [partition by ("欄位") limit N]   | limit N
         | stats [by ("欄位")] count() [if (篩選)] as 名稱, …
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Callable

Row = dict[str, Any]
Test = Callable[[Row], bool]

_TOKEN = re.compile(r'''\s*(?:(?P<str>"(?:[^"\\]|\\.)*")|(?P<time>\d{4}-\d\d-\d\dT[\d:.]+Z)|(?P<op>>=|<|[(){}|,:=*])|(?P<word>[A-Za-z0-9_.]+))''')


class LogsQLError(ValueError):
    pass


def when(raw: Any) -> datetime:
    text = re.sub(r"(\.\d{6})\d+", r"\1", str(raw).replace("Z", "+00:00"))
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _tokens(query: str) -> list[tuple[str, str]]:
    out, pos = [], 0
    while pos < len(query):
        if not query[pos:].strip():
            break
        m = _TOKEN.match(query, pos)
        if not m:
            raise LogsQLError(f"cannot parse query [{query}]: unexpected text at {query[pos:pos + 20]!r}")
        kind = m.lastgroup
        out.append((kind, json.loads(m.group(kind)) if kind == "str" else m.group(kind)))
        pos = m.end()
    return out


def _has_word(text: str, phrase: str, start: int = 0, flags: int = 0) -> int:
    """phrase 以完整的詞出現在 text[start:] 的話，回傳它結束的位置；沒有回 -1。詞＝連續的字母、數字、底線。"""
    m = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(phrase)}(?![A-Za-z0-9_])", flags).search(text, start)
    return m.end() if m else -1


class _Parser:
    def __init__(self, query: str):
        self.query, self.items, self.at = query, _tokens(query), 0

    def peek(self, offset: int = 0) -> tuple[str, str]:
        return self.items[self.at + offset] if self.at + offset < len(self.items) else ("end", "")

    def take(self, kind: str | None = None, value: str | None = None) -> str:
        got_kind, got = self.peek()
        if (kind and got_kind != kind) or (value is not None and got != value):
            raise LogsQLError(f"cannot parse query [{self.query}]: unexpected token {got!r}; want {value or kind}")
        self.at += 1
        return got

    def strings(self) -> list[str]:
        self.take("op", "(")
        values = [self.take("str")]
        while self.peek() == ("op", ","):
            self.take()
            values.append(self.take("str"))
        self.take("op", ")")
        return values

    # ── 篩選 ──
    def filter(self) -> Test:
        tests = [self.conjunction()]
        while self.peek() == ("word", "or"):
            self.take()
            tests.append(self.conjunction())
        return lambda row: any(t(row) for t in tests)

    def conjunction(self) -> Test:
        tests = [self.term()]
        while self.peek()[0] in ("str", "word") and self.peek() != ("word", "or") or self.peek() in (("op", "("), ("op", "{"), ("op", "*")):
            tests.append(self.term())
        return lambda row: all(t(row) for t in tests)

    def term(self) -> Test:
        kind, value = self.peek()
        if (kind, value) == ("op", "*"):
            self.take()
            return lambda row: True
        if (kind, value) == ("op", "("):
            self.take()
            inner = self.filter()
            self.take("op", ")")
            return inner
        if (kind, value) == ("op", "{"):
            self.take()
            key = self.take()
            self.take("op", "=")
            wanted = self.take("str")
            self.take("op", "}")
            return lambda row: str(row.get(key, "")) == wanted
        field = self.take()
        if kind not in ("str", "word"):
            raise LogsQLError(f"cannot parse query [{self.query}]: unexpected token {value!r}")
        self.take("op", ":")
        if field == "_time":
            op = self.take("op")
            bound = when(self.take("time"))
            if op not in (">=", "<"):
                raise LogsQLError(f"cannot parse query [{self.query}]: unsupported _time filter {op!r}")
            return (lambda row: when(row["_time"]) >= bound) if op == ">=" else (lambda row: when(row["_time"]) < bound)
        if self.peek() == ("op", "="):
            self.take()
            wanted = self.take("str")
            return lambda row: str(row.get(field, "")) == wanted
        func = self.take("word")
        values = self.strings()
        if func == "in":
            return lambda row: str(row.get(field, "")) in values
        if func == "i" and len(values) == 1:
            return lambda row: _has_word(str(row.get(field, "")), values[0], flags=re.IGNORECASE) >= 0
        if func == "seq":
            def in_order(row: Row) -> bool:
                pos = 0
                for phrase in values:
                    pos = _has_word(str(row.get(field, "")), phrase, pos)
                    if pos < 0:
                        return False
                return True
            return in_order
        raise LogsQLError(f"cannot parse query [{self.query}]: unknown filter {func!r}")

    # ── 管線 ──
    def pipes(self, rows: list[Row]) -> list[Row]:
        while self.peek() == ("op", "|"):
            self.take()
            name = self.take("word")
            if name == "limit":
                rows = rows[:int(self.take("word"))]
            elif name == "sort":
                rows = self.sort(rows)
            elif name == "stats":
                rows = self.stats(rows)
            else:
                raise LogsQLError(f"cannot parse query [{self.query}]: unknown pipe {name!r}")
        if self.peek()[0] != "end":
            raise LogsQLError(f"cannot parse query [{self.query}]: unexpected token {self.peek()[1]!r}")
        return rows

    def sort(self, rows: list[Row]) -> list[Row]:
        self.take("word", "by")
        self.take("op", "(")
        self.take("word", "_time")
        self.take("op", ")")
        reverse = self.peek() == ("word", "desc")
        if reverse:
            self.take()
        rows = sorted(rows, key=lambda row: when(row["_time"]), reverse=reverse)
        if self.peek() == ("word", "partition"):
            self.take()
            self.take("word", "by")
            [field] = self.strings()
            self.take("word", "limit")
            limit, seen, kept = int(self.take("word")), {}, []
            for row in rows:
                key = str(row.get(field, ""))
                seen[key] = seen.get(key, 0) + 1
                if seen[key] <= limit:
                    kept.append(row)
            rows = kept
        return rows

    def stats(self, rows: list[Row]) -> list[Row]:
        group = None
        if self.peek() == ("word", "by"):
            self.take()
            [group] = self.strings()
        counters: list[tuple[str, Test]] = []
        while True:
            self.take("word", "count")
            self.take("op", "(")
            self.take("op", ")")
            test: Test = lambda row: True
            if self.peek() == ("word", "if"):
                self.take()
                self.take("op", "(")
                test = self.filter()
                self.take("op", ")")
            self.take("word", "as")
            counters.append((self.take("word"), test))
            if self.peek() != ("op", ","):
                break
            self.take()
        if group is None:   # 沒有 by：就算一筆資料都沒有，也回一列全部是 0 的
            return [{name: str(sum(1 for row in rows if test(row))) for name, test in counters}]
        keys = list(dict.fromkeys(str(row.get(group, "")) for row in rows))
        return [{group: key, **{name: str(sum(1 for row in rows if str(row.get(group, "")) == key and test(row)))
                                for name, test in counters}} for key in keys]


def run(rows: list[Row], query: str, start: datetime, end: datetime) -> list[Row]:
    """在 rows（VictoriaLogs 回的那種攤平的列）上跑一條查詢。start、end 是請求帶的時間範圍。"""
    parser = _Parser(query)
    keep = parser.filter()
    picked = [row for row in rows if start <= when(row["_time"]) <= end and keep(row)]
    return parser.pipes(picked)
